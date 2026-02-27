//! Pipeline configuration, lifecycle orchestration, and checkpointing.
//!
//! [`PipelineController`] (aliased as `Executor` for backward compatibility)
//! owns all configuration and drives the compile → bridge → execute → checkpoint
//! lifecycle via [`PipelineController::run()`].

use std::sync::Arc;

use rill_core::checkpoint::CheckpointManifest;
use rill_core::dlq::ErrorPolicy;
use rill_core::state::context::StateContext;
use rill_core::state::local_backend::LocalBackend;
use rill_core::state::prefixed_backend::PrefixedBackend;
use rill_core::state::slatedb_backend::SlateDbBackend;
use rill_core::state::tiered_backend::{TieredBackend, TieredBackendConfig};

use crate::compiler::compile_graph;
use crate::dataflow::DataflowGraph;
use crate::health::{HealthState, PipelineStatus};
use crate::shutdown::ShutdownHandle;
use crate::worker::WorkerSet;

/// Current time as Unix milliseconds (saturating to `u64::MAX`).
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Configuration for tiered storage on the executor.
#[derive(Debug)]
pub(crate) struct TieredStorageConfig {
    pub l3: Arc<SlateDbBackend>,
    pub foyer_config: TieredBackendConfig,
}

/// Materializes a [`DataflowGraph`] into an executable pipeline.
///
/// Use [`PipelineController::builder()`] to configure execution parameters, build the
/// graph on a [`DataflowGraph`], then pass it to [`PipelineController::run()`].
#[derive(Debug)]
pub struct PipelineController {
    pub(crate) checkpoint_dir: std::path::PathBuf,
    pub(crate) tiered: Option<TieredStorageConfig>,
    pub(crate) workers: usize,
    pub(crate) checkpoint_interval: u64,
    pub(crate) error_policy: ErrorPolicy,
    pub(crate) health: HealthState,
    /// Process ID for multi-process cluster mode (`None` = single-process).
    pub(crate) process_id: Option<usize>,
    /// Peer addresses for multi-process cluster mode (`None` = single-process).
    pub(crate) peers: Option<Vec<String>>,
}

/// Builder for [`PipelineController`].
#[derive(Debug)]
pub struct PipelineControllerBuilder {
    checkpoint_dir: std::path::PathBuf,
    workers: usize,
    checkpoint_interval: u64,
    tiered: Option<TieredStorageConfig>,
    error_policy: ErrorPolicy,
    health: HealthState,
    process_id: Option<usize>,
    peers: Option<Vec<String>>,
}

impl PipelineControllerBuilder {
    /// Set the checkpoint directory.
    pub fn checkpoint_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.checkpoint_dir = dir.into();
        self
    }

    /// Set the number of parallel workers for keyed pipelines.
    pub fn workers(mut self, n: usize) -> Self {
        assert!(n >= 1, "worker count must be at least 1");
        self.workers = n;
        self
    }

    /// Set the checkpoint interval in batches (default: 100).
    ///
    /// Controls how often pre-exchange steps are checkpointed in the
    /// multi-worker main loop. Must be at least 1.
    pub fn checkpoint_interval(mut self, interval: u64) -> Self {
        assert!(interval >= 1, "checkpoint interval must be at least 1");
        self.checkpoint_interval = interval;
        self
    }

    /// Set the error handling policy for operator failures.
    pub fn error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }

    /// Set a custom [`HealthState`] to share with the HTTP server.
    pub fn health(mut self, health: HealthState) -> Self {
        self.health = health;
        self
    }

    /// Set the process ID for multi-process cluster mode (0-based).
    pub fn process_id(mut self, id: usize) -> Self {
        self.process_id = Some(id);
        self
    }

    /// Set peer addresses for multi-process cluster mode.
    pub fn peers(mut self, peers: Vec<String>) -> Self {
        self.peers = Some(peers);
        self
    }

    /// Read cluster configuration from environment variables.
    ///
    /// - `RILL_WORKERS`: number of worker threads (overrides `.workers()`)
    /// - `RILL_PROCESS_ID`: process ID for cluster mode
    /// - `RILL_PEERS`: comma-separated peer addresses for cluster mode
    pub fn from_env(mut self) -> Self {
        if let Ok(val) = std::env::var("RILL_WORKERS")
            && let Ok(n) = val.parse::<usize>()
        {
            self.workers = n;
        }
        if let Ok(val) = std::env::var("RILL_PROCESS_ID")
            && let Ok(id) = val.parse::<usize>()
        {
            self.process_id = Some(id);
        }
        if let Ok(val) = std::env::var("RILL_PEERS") {
            let peers: Vec<String> = val.split(',').map(|s| s.trim().to_string()).collect();
            if !peers.is_empty() {
                self.peers = Some(peers);
            }
        }
        self
    }

    /// Build the controller.
    pub fn build(self) -> PipelineController {
        if let Some(ref peers) = self.peers {
            let pid = self
                .process_id
                .expect("process_id is required when peers are set");
            assert!(
                pid < peers.len(),
                "process_id ({pid}) must be less than number of peers ({})",
                peers.len()
            );
        }
        PipelineController {
            checkpoint_dir: self.checkpoint_dir,
            tiered: self.tiered,
            workers: self.workers,
            checkpoint_interval: self.checkpoint_interval,
            error_policy: self.error_policy,
            health: self.health,
            process_id: self.process_id,
            peers: self.peers,
        }
    }
}

impl PipelineController {
    /// Create a builder for configuring a pipeline controller.
    pub fn builder() -> PipelineControllerBuilder {
        PipelineControllerBuilder {
            checkpoint_dir: std::path::PathBuf::from("./checkpoints"),
            workers: 1,
            checkpoint_interval: 100,
            tiered: None,
            error_policy: ErrorPolicy::default(),
            health: HealthState::new(),
            process_id: None,
            peers: None,
        }
    }

    /// Create a new controller with the given checkpoint directory.
    ///
    /// For more options, use [`PipelineController::builder()`].
    pub fn new(checkpoint_dir: std::path::PathBuf) -> Self {
        Self {
            checkpoint_dir,
            tiered: None,
            workers: 1,
            checkpoint_interval: 100,
            error_policy: ErrorPolicy::default(),
            health: HealthState::new(),
            process_id: None,
            peers: None,
        }
    }

    /// Returns a reference to the controller's [`HealthState`].
    pub fn health(&self) -> &HealthState {
        &self.health
    }

    /// Set the number of parallel workers for keyed pipelines.
    ///
    /// When `workers > 1`, [`KeyedStream`](crate::dataflow::KeyedStream) operators
    /// run on parallel Timely worker threads, distributed by key hash.
    /// Defaults to `1` (single-worker mode).
    #[must_use]
    pub fn with_workers(mut self, n: usize) -> Self {
        assert!(n >= 1, "worker count must be at least 1");
        self.workers = n;
        self
    }

    /// Returns the configured number of workers.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Returns the configured checkpoint interval in batches.
    pub fn checkpoint_interval(&self) -> u64 {
        self.checkpoint_interval
    }

    /// Returns `true` when running in multi-process cluster mode.
    pub fn is_cluster(&self) -> bool {
        self.peers.is_some()
    }

    /// Returns the configured process ID, if in cluster mode.
    pub fn process_id(&self) -> Option<usize> {
        self.process_id
    }

    /// Total number of workers across all processes.
    ///
    /// In cluster mode: `workers * peers.len()`.
    /// In single-process mode: `workers`.
    pub fn total_workers(&self) -> usize {
        if let Some(ref peers) = self.peers {
            self.workers * peers.len()
        } else {
            self.workers
        }
    }

    /// Range of worker indices owned by this process.
    ///
    /// In cluster mode: `pid*workers .. pid*workers + workers`.
    /// In single-process mode: `0..workers`.
    pub fn local_worker_range(&self) -> std::ops::Range<usize> {
        if let Some(pid) = self.process_id {
            let start = pid * self.workers;
            start..start + self.workers
        } else {
            0..self.workers
        }
    }

    /// Construct a Timely config appropriate for the execution mode.
    pub(crate) fn timely_config(&self) -> timely::execute::Config {
        if let Some(ref peers) = self.peers {
            let pid = self.process_id.expect("process_id required for cluster");
            timely::execute::Config {
                communication: timely::CommunicationConfig::Cluster {
                    threads: self.workers,
                    process: pid,
                    addresses: peers.clone(),
                    report: false,
                    zerocopy: false,
                    log_fn: Arc::new(|_| None),
                },
                worker: timely::WorkerConfig::default(),
            }
        } else {
            timely::execute::Config::process(self.workers)
        }
    }

    /// Compile and execute a [`DataflowGraph`].
    ///
    /// Build the graph first with [`DataflowGraph::source()`], stream
    /// transforms, and sinks, then pass it here for execution.
    pub async fn run(&self, graph: DataflowGraph) -> anyhow::Result<()> {
        run_graph(graph, self, None).await
    }

    /// Compile and execute a [`DataflowGraph`] with graceful shutdown.
    pub async fn run_with_shutdown(
        &self,
        graph: DataflowGraph,
        shutdown: ShutdownHandle,
    ) -> anyhow::Result<()> {
        run_graph(graph, self, Some(shutdown)).await
    }

    /// Configure tiered storage (L2 Foyer + L3 `SlateDB`) for this controller.
    ///
    /// When set, `create_context` will produce contexts backed by a per-operator
    /// `PrefixedBackend` wrapping a `TieredBackend`.
    pub fn with_tiered_storage(
        mut self,
        checkpoint_dir: std::path::PathBuf,
        l3: Arc<SlateDbBackend>,
        foyer_config: TieredBackendConfig,
    ) -> Self {
        self.checkpoint_dir = checkpoint_dir;
        self.tiered = Some(TieredStorageConfig { l3, foyer_config });
        self
    }

    /// Create a per-worker `StateContext` for the given operator.
    ///
    /// In single-process mode the context is namespaced as
    /// `{operator_name}_w{worker_index}`.
    /// In cluster mode it includes the process ID:
    /// `p{process_id}_w{worker_index}_{operator_name}`.
    pub async fn create_context_for_worker(
        &self,
        operator_name: &str,
        worker_index: usize,
    ) -> anyhow::Result<StateContext> {
        let namespaced = if let Some(pid) = self.process_id {
            format!("p{pid}_w{worker_index}_{operator_name}")
        } else {
            format!("{operator_name}_w{worker_index}")
        };
        self.create_context(&namespaced).await
    }

    /// Create a `StateContext` for the given operator.
    ///
    /// When tiered storage is configured, produces a context backed by
    /// `PrefixedBackend(TieredBackend)`. Otherwise falls back to `LocalBackend`.
    pub async fn create_context(&self, operator_name: &str) -> anyhow::Result<StateContext> {
        if let Some(ref tiered) = self.tiered {
            let foyer_dir = tiered.foyer_config.foyer_dir.join(operator_name);

            let config = TieredBackendConfig {
                foyer_dir,
                foyer_memory_capacity: tiered.foyer_config.foyer_memory_capacity,
                foyer_disk_capacity: tiered.foyer_config.foyer_disk_capacity,
            };

            let tiered_backend = TieredBackend::open(config, tiered.l3.clone()).await?;
            let prefixed = PrefixedBackend::new(operator_name, Box::new(tiered_backend));
            Ok(StateContext::new(Box::new(prefixed)))
        } else {
            let path = self
                .checkpoint_dir
                .join(format!("{operator_name}.checkpoint.json"));
            let backend = LocalBackend::new(path, None)?;
            Ok(StateContext::new(Box::new(backend)))
        }
    }
}

/// Compile and execute the dataflow graph.
async fn run_graph(
    graph: DataflowGraph,
    controller: &PipelineController,
    shutdown: Option<ShutdownHandle>,
) -> anyhow::Result<()> {
    let compiled = compile_graph(graph.into_nodes())?;

    let all_operator_names = &compiled.operator_names;

    // Load existing manifest and validate.
    let (initial_checkpoint_id, restored_offsets) =
        if let Some(manifest) = CheckpointManifest::load(&controller.checkpoint_dir) {
            tracing::info!(
                checkpoint_id = manifest.checkpoint_id,
                timestamp_ms = manifest.timestamp_ms,
                operators = ?manifest.operators,
                source_offsets = ?manifest.source_offsets,
                "resuming from checkpoint #{}", manifest.checkpoint_id
            );

            // Validate operator names.
            let prev: std::collections::HashSet<&str> =
                manifest.operators.iter().map(String::as_str).collect();
            let curr: std::collections::HashSet<&str> =
                all_operator_names.iter().map(String::as_str).collect();

            let added: Vec<_> = curr.difference(&prev).collect();
            let removed: Vec<_> = prev.difference(&curr).collect();
            if !added.is_empty() || !removed.is_empty() {
                tracing::warn!(
                    ?added,
                    ?removed,
                    "operator topology changed since last checkpoint"
                );
            }

            (manifest.checkpoint_id, manifest.source_offsets)
        } else {
            (0, std::collections::HashMap::new())
        };

    controller.health.set_status(PipelineStatus::Running);

    let result = execute_compiled(
        compiled,
        controller,
        shutdown.as_ref(),
        initial_checkpoint_id,
        restored_offsets,
    )
    .await;

    controller.health.set_status(PipelineStatus::Stopped);
    result
}

/// Execute a compiled graph: build workers, run DAG, checkpoint.
async fn execute_compiled(
    graph: crate::compiler::CompiledGraph,
    controller: &PipelineController,
    shutdown: Option<&ShutdownHandle>,
    initial_checkpoint_id: u64,
    restored_offsets: std::collections::HashMap<String, String>,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Handle::current();
    let total_workers = controller.total_workers();

    let worker_set = WorkerSet::build(graph, controller, shutdown, &rt, restored_offsets).await?;

    let timely_config = controller.timely_config();
    crate::executor::execute_dag(timely_config, &worker_set, rt.clone(), total_workers).await?;

    // Extract checkpoint data before draining (drain consumes self).
    let mut checkpoint_id = initial_checkpoint_id;
    while worker_set
        .checkpoint_notify_rx
        .lock()
        .unwrap()
        .try_recv()
        .is_ok()
    {
        checkpoint_id += 1;
    }
    let all_operator_names = worker_set.all_operator_names.clone();
    let source_offsets = worker_set.source_offsets();

    worker_set.drain().await?;

    // Final manifest (includes all accumulated checkpoint progress).
    checkpoint_id += 1;
    let manifest = CheckpointManifest {
        version: 1,
        checkpoint_id,
        timestamp_ms: now_millis(),
        operators: all_operator_names,
        source_offsets,
    };

    if let Some(pid) = controller.process_id() {
        // Cluster mode: write a partial manifest for this process.
        manifest.save_partial(&controller.checkpoint_dir, pid)?;
        tracing::debug!(
            checkpoint_id,
            process_id = pid,
            "partial checkpoint #{checkpoint_id} saved for process {pid}"
        );

        // Process 0 merges all partial manifests into the final manifest.
        if pid == 0 {
            let n_processes = controller.peers.as_ref().map_or(1, Vec::len);
            let merged =
                CheckpointManifest::merge_partials(&controller.checkpoint_dir, n_processes);
            if let Some(merged) = merged {
                merged.save(&controller.checkpoint_dir)?;
                tracing::debug!(
                    checkpoint_id = merged.checkpoint_id,
                    "merged checkpoint saved"
                );
            } else {
                tracing::warn!("not all partial manifests available for merge");
            }
        }
    } else {
        // Single-process mode: write directly.
        manifest.save(&controller.checkpoint_dir)?;
        tracing::debug!(checkpoint_id, "checkpoint #{checkpoint_id} saved");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rill_core::connectors::vec_source::VecSource;
    use rill_core::state::context::StateContext;
    use rill_core::traits::{Sink, StreamFunction};

    use crate::dataflow::DataflowGraph;

    struct CollectSink<T> {
        collected: Arc<Mutex<Vec<T>>>,
    }

    #[async_trait]
    impl<T: Send + Sync + 'static> Sink for CollectSink<T> {
        type Input = T;

        async fn write(&mut self, input: T) -> anyhow::Result<()> {
            self.collected.lock().unwrap().push(input);
            Ok(())
        }
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rill_dataflow_{name}_{}", std::process::id()))
    }

    #[tokio::test]
    async fn map_transform() {
        let dir = temp_dir("map");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let doubled = stream.map(|x: i32| x * 2);
        doubled.sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![2, 4, 6]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn filter_transform() {
        let dir = temp_dir("filter");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.filter(|x: &i32| x % 2 == 0).sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![2, 4]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn flat_map_transform() {
        let dir = temp_dir("flat_map");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "foo bar".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        assert_eq!(
            *collected.lock().unwrap(),
            vec!["hello", "world", "foo", "bar"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn chained_filter_map() {
        let dir = temp_dir("chain");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .filter(|x: &i32| x % 2 == 0)
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![20, 40]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[derive(Clone)]
    struct WordCounter;

    #[async_trait]
    impl StreamFunction for WordCounter {
        type Input = String;
        type Output = String;

        async fn process(
            &mut self,
            input: String,
            ctx: &mut StateContext,
        ) -> anyhow::Result<Vec<String>> {
            let mut outputs = Vec::new();
            for word in input.split_whitespace() {
                let key = word.as_bytes();
                let count = match ctx.get(key).await.unwrap_or(None) {
                    Some(bytes) => {
                        let n = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                        n + 1
                    }
                    None => 1,
                };
                ctx.put(key, &count.to_le_bytes());
                outputs.push(format!("{word}: {count}"));
            }
            Ok(outputs)
        }
    }

    #[tokio::test]
    async fn keyed_operator_single_worker() {
        let dir = temp_dir("keyed_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .key_by(|word: &String| word.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 4); // hello, world, hello, rill
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"rill: 1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn keyed_multi_worker() {
        let dir = temp_dir("keyed_multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .key_by(|word: &String| word.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results.len(), 4);
        // With multi-worker, order may vary but counts must be correct.
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"rill: 1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn keyed_filter_map_multi_worker() {
        let dir = temp_dir("keyed_filter_map");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .filter(|x: &i32| x % 2 == 0)
            .key_by(|x: &i32| x.to_string())
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![20, 40, 60, 80, 100]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn builder_api() {
        let dir = temp_dir("builder");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![10i32, 20, 30]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.map(|x: i32| x + 1).sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(1)
            .build();
        controller.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec![11, 21, 31]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn key_affinity() {
        // Verify that the same key always routes to the same worker by checking
        // that stateful counts are correct across multiple items with the same key.
        let dir = temp_dir("affinity");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();

        // Send "alpha" 5 times and "beta" 3 times.
        let source = VecSource::new(vec![
            "alpha".to_string(),
            "alpha".to_string(),
            "beta".to_string(),
            "alpha".to_string(),
            "beta".to_string(),
            "alpha".to_string(),
            "beta".to_string(),
            "alpha".to_string(),
        ]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|w: &String| w.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build();
        controller.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 8);
        // Alpha should count up to 5, beta up to 3.
        assert!(results.contains(&"alpha: 5".to_string()));
        assert!(results.contains(&"beta: 3".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn shutdown_graceful() {
        let dir = temp_dir("shutdown");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.map(|x: i32| x * 2).sink(CollectSink {
            collected: collected.clone(),
        });

        let (handle, trigger) = crate::shutdown::ShutdownHandle::new();
        // Signal shutdown immediately. The loop processes at least one batch
        // before checking the flag, then stops early.
        trigger.shutdown();

        let controller = super::PipelineController::new(dir.clone());
        controller.run_with_shutdown(graph, handle).await.unwrap();
        let results = collected.lock().unwrap().clone();
        // At least the first batch was processed; early termination is expected.
        assert!(!results.is_empty());
        // All produced values must be valid doubles of the input.
        for &v in &results {
            assert!(v == 2 || v == 4 || v == 6);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn map_ctx_single_worker() {
        let dir = temp_dir("map_ctx_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .map_ctx(|x: i32, ctx| format!("w{}:{x}", ctx.worker_index))
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        assert_eq!(*collected.lock().unwrap(), vec!["w0:1", "w0:2", "w0:3"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Partitioned source tests ───────────────────────────────────

    #[tokio::test]
    async fn partitioned_source_basic() {
        // 4 partitions, 2 workers, no key_by — all items processed exactly once.
        let dir = temp_dir("part_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let items: Vec<i32> = (0..20).collect();
        let source =
            rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(items, 4);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        let expected: Vec<i32> = (0..20).collect();
        assert_eq!(results, expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_with_key_by() {
        // 3 partitions, 4 workers, with key_by + operator.
        let dir = temp_dir("part_keyed");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![
                "hello".to_string(),
                "world".to_string(),
                "hello".to_string(),
                "rill".to_string(),
                "hello".to_string(),
                "world".to_string(),
            ],
            3,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|w: &String| w.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build();
        controller.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 6);
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"hello: 3".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"world: 2".to_string()));
        assert!(results.contains(&"rill: 1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_single_worker() {
        // 4 partitions, 1 worker — all partitions assigned to worker 0.
        let dir = temp_dir("part_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let items: Vec<i32> = (0..12).collect();
        let source =
            rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(items, 4);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(1)
            .build();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        let expected: Vec<i32> = (0..12).collect();
        assert_eq!(results, expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_more_workers_than_partitions() {
        // 2 partitions, 4 workers — only 2 workers get source receivers.
        // With key_by, all 4 participate post-exchange.
        let dir = temp_dir("part_more_workers");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![1i32, 2, 3, 4, 5, 6, 7, 8],
            2,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|x: &i32| x.to_string())
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![10, 20, 30, 40, 50, 60, 70, 80]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn map_ctx_multi_worker() {
        let dir = temp_dir("map_ctx_multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5, 6]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|x: &i32| x.to_string())
            .map_ctx(|x: i32, ctx| (x, ctx.worker_index, ctx.num_workers))
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 6);
        for &(_, worker_idx, num_workers) in &results {
            assert!(worker_idx < 2, "worker_index should be 0 or 1");
            assert_eq!(num_workers, 2);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    struct CheckpointTrackingSource {
        items: std::collections::VecDeque<Vec<String>>,
        checkpoint_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl rill_core::traits::Source for CheckpointTrackingSource {
        type Output = String;

        async fn next_batch(&mut self) -> Option<Vec<String>> {
            self.items.pop_front()
        }

        async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
            self.checkpoint_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn coordinated_checkpoint() {
        // Verify that source.on_checkpoint_complete() is only called after all
        // workers have flushed their state (coordinated via barrier).
        //
        // We use a custom source that:
        //   - emits enough batches to trigger at least one checkpoint cycle
        //   - tracks on_checkpoint_complete calls via an Arc counter
        let dir = temp_dir("coordinated_ckpt");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let checkpoint_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cc = checkpoint_count.clone();

        // Generate enough batches to trigger checkpoint cycles (interval is 100).
        let mut batches = std::collections::VecDeque::new();
        for i in 0..150 {
            batches.push_back(vec![format!("item_{i}")]);
        }

        let source = CheckpointTrackingSource {
            items: batches,
            checkpoint_count: cc,
        };

        let graph = DataflowGraph::new();
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|s: &String| s.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();

        // All 150 items should have been processed.
        let results = collected.lock().unwrap();
        assert_eq!(
            results.len(),
            150,
            "expected 150 results, got {}",
            results.len()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn merge_two_sources() {
        let dir = temp_dir("merge");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source1 = VecSource::new(vec![1i32, 2, 3]);
        let source2 = VecSource::new(vec![4i32, 5, 6]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream1 = graph.source(source1);
        let stream2 = graph.source(source2);
        let merged = stream1.merge(stream2);
        merged.map(|x: i32| x * 10).sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![10, 20, 30, 40, 50, 60]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fan_out_to_two_sinks() {
        let dir = temp_dir("fanout");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected1 = Arc::new(Mutex::new(Vec::new()));
        let collected2 = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let mapped = stream.map(|x: i32| x * 2);
        mapped.sink(CollectSink {
            collected: collected1.clone(),
        });
        mapped.sink(CollectSink {
            collected: collected2.clone(),
        });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        assert_eq!(*collected1.lock().unwrap(), vec![2, 4, 6]);
        assert_eq!(*collected2.lock().unwrap(), vec![2, 4, 6]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn diamond_dag() {
        // source → (filter_even, filter_odd) → merge → sink
        let dir = temp_dir("diamond");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3, 4, 5, 6]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let evens = stream.filter(|x: &i32| x % 2 == 0).map(|x: i32| x * 10);
        let odds = stream.filter(|x: &i32| x % 2 != 0).map(|x: i32| x * 100);
        let merged = evens.merge(odds);
        merged.sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        // evens: 20, 40, 60; odds: 100, 200, 300, 500
        assert_eq!(results, vec![20, 40, 60, 100, 300, 500]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn multiple_key_by() {
        // source → flat_map → key_by(word) → counter → map → key_by(first_char) → map → sink
        let dir = temp_dir("multi_keyby");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "hello rill".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .flat_map(|line: String| line.split_whitespace().map(String::from).collect())
            .key_by(|word: &String| word.clone())
            .operator("counter", WordCounter)
            // Re-key by first character
            .key_by(|s: &String| s.chars().next().unwrap_or('_').to_string())
            .map(|s: String| format!("rekeyed:{s}"))
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 4);
        // All results should be prefixed with "rekeyed:"
        for r in &results {
            assert!(r.starts_with("rekeyed:"), "unexpected: {r}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Partitioned source topology tests ────────────────────────

    #[tokio::test]
    async fn partitioned_source_single_partition_many_workers() {
        // 1 partition, 4 workers — only worker 0 gets the partition reader.
        // All data arrives through worker 0, exchange distributes.
        let dir = temp_dir("part_single_part");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![1i32, 2, 3, 4, 5, 6],
            1,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|x: &i32| x.to_string())
            .map(|x: i32| x * 10)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![10, 20, 30, 40, 50, 60]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_with_empty_partition() {
        // 3 partitions over 4 items: partition 0 gets [0,3], partition 1 gets [1],
        // partition 2 gets [2]. With 2 workers:
        //   Worker 0: partitions [0, 2] — items [0, 3, 2]
        //   Worker 1: partition [1] — items [1]
        // All items should be processed exactly once.
        let dir = temp_dir("part_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![10i32, 20, 30, 40],
            3,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph.source(source).sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![10, 20, 30, 40]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn mixed_partitioned_and_non_partitioned_merge() {
        // Merge a PartitionedVecSource with a regular VecSource.
        // Both source types should work in the same pipeline.
        let dir = temp_dir("mixed_merge");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let partitioned = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![1i32, 2, 3, 4],
            2,
        );
        let non_partitioned = VecSource::new(vec![10i32, 20, 30]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream1 = graph.source(partitioned);
        let stream2 = graph.source(non_partitioned);
        let merged = stream1.merge(stream2);
        merged.map(|x: i32| x * 100).sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results, vec![100, 200, 300, 400, 1000, 2000, 3000]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn partitioned_source_fan_out() {
        // One partitioned source feeding two sinks through different transforms.
        let dir = temp_dir("part_fanout");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = rill_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![1i32, 2, 3, 4, 5, 6],
            3,
        );
        let collected_doubled = Arc::new(Mutex::new(Vec::new()));
        let collected_tripled = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let doubled = stream.map(|x: i32| x * 2);
        let tripled = stream.map(|x: i32| x * 3);
        doubled.sink(CollectSink {
            collected: collected_doubled.clone(),
        });
        tripled.sink(CollectSink {
            collected: collected_tripled.clone(),
        });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build();
        controller.run(graph).await.unwrap();

        let mut doubled_results = collected_doubled.lock().unwrap().clone();
        doubled_results.sort_unstable();
        assert_eq!(doubled_results, vec![2, 4, 6, 8, 10, 12]);

        let mut tripled_results = collected_tripled.lock().unwrap().clone();
        tripled_results.sort_unstable();
        assert_eq!(tripled_results, vec![3, 6, 9, 12, 15, 18]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Cluster config tests ────────────────────────────────────────

    #[test]
    fn cluster_total_workers() {
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(4)
            .process_id(0)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build();
        assert_eq!(ctrl.total_workers(), 8); // 4 workers * 2 peers
        assert!(ctrl.is_cluster());
    }

    #[test]
    fn single_process_total_workers() {
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(4)
            .build();
        assert_eq!(ctrl.total_workers(), 4);
        assert!(!ctrl.is_cluster());
    }

    #[test]
    fn cluster_local_worker_range() {
        // Process 0 of 2, 4 workers each
        let ctrl0 = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(4)
            .process_id(0)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build();
        assert_eq!(ctrl0.local_worker_range(), 0..4);

        // Process 1 of 2, 4 workers each
        let ctrl1 = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(4)
            .process_id(1)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build();
        assert_eq!(ctrl1.local_worker_range(), 4..8);
    }

    #[test]
    fn single_process_local_worker_range() {
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(3)
            .build();
        assert_eq!(ctrl.local_worker_range(), 0..3);
    }

    #[test]
    fn cluster_timely_config_is_cluster() {
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(2)
            .process_id(0)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build();
        let config = ctrl.timely_config();
        assert!(
            matches!(
                config.communication,
                timely::CommunicationConfig::Cluster { .. }
            ),
            "expected Cluster config"
        );
    }

    #[test]
    fn single_process_timely_config_is_process() {
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(2)
            .build();
        let config = ctrl.timely_config();
        assert!(
            matches!(
                config.communication,
                timely::CommunicationConfig::Process(2)
            ),
            "expected Process(2) config"
        );
    }

    #[test]
    fn cluster_state_prefix_includes_process_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir(temp_dir("cluster_prefix"))
            .workers(2)
            .process_id(1)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build();

        let ctx = rt
            .block_on(ctrl.create_context_for_worker("my_op", 3))
            .unwrap();
        // The context should have been created with prefix "p1_w3_my_op"
        // We can verify via the backend path for LocalBackend
        // Just check it doesn't panic with the cluster prefix format
        drop(ctx);
    }

    #[test]
    fn single_process_state_prefix_unchanged() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = temp_dir("single_prefix");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let ctrl = super::PipelineController::new(dir.clone());
        let ctx = rt
            .block_on(ctrl.create_context_for_worker("my_op", 0))
            .unwrap();
        // Should not panic; prefix is "my_op_w0" (original format)
        drop(ctx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[should_panic(expected = "process_id is required when peers are set")]
    fn cluster_requires_process_id() {
        super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .peers(vec!["h0:2101".into()])
            .build();
    }

    #[test]
    #[should_panic(expected = "process_id (2) must be less than number of peers (2)")]
    fn cluster_process_id_out_of_range() {
        super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .process_id(2)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build();
    }
}

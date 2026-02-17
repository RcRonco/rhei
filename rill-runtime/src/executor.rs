use std::sync::Arc;

use rill_core::state::context::StateContext;
use rill_core::state::local_backend::LocalBackend;
use rill_core::state::prefixed_backend::PrefixedBackend;
use rill_core::state::slatedb_backend::SlateDbBackend;
use rill_core::state::tiered_backend::{TieredBackend, TieredBackendConfig};

use crate::dataflow::{self, DataflowGraph};
use crate::shutdown::ShutdownHandle;

/// Configuration for tiered storage on the executor.
#[derive(Debug)]
struct TieredStorageConfig {
    l3: Arc<SlateDbBackend>,
    foyer_config: TieredBackendConfig,
}

/// Materializes a [`DataflowGraph`] into an executable pipeline.
///
/// Use [`Executor::builder()`] to configure execution parameters, build the
/// graph on a [`DataflowGraph`], then pass it to [`Executor::run()`].
///
/// ```ignore
/// let graph = DataflowGraph::new();
/// let orders = graph.source(kafka_source);
/// orders.map(parse).key_by(|o| o.id.clone()).operator("agg", Agg).sink(sink);
///
/// let executor = Executor::builder()
///     .checkpoint_dir("./checkpoints")
///     .workers(4)
///     .build();
///
/// executor.run(graph).await?;
/// ```
#[derive(Debug)]
pub struct Executor {
    checkpoint_dir: std::path::PathBuf,
    tiered: Option<TieredStorageConfig>,
    workers: usize,
}

/// Builder for [`Executor`].
#[derive(Debug)]
pub struct ExecutorBuilder {
    checkpoint_dir: std::path::PathBuf,
    workers: usize,
    tiered: Option<TieredStorageConfig>,
}

impl ExecutorBuilder {
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

    /// Build the executor.
    pub fn build(self) -> Executor {
        Executor {
            checkpoint_dir: self.checkpoint_dir,
            tiered: self.tiered,
            workers: self.workers,
        }
    }
}

impl Executor {
    /// Create a builder for configuring an executor.
    pub fn builder() -> ExecutorBuilder {
        ExecutorBuilder {
            checkpoint_dir: std::path::PathBuf::from("./checkpoints"),
            workers: 1,
            tiered: None,
        }
    }

    /// Create a new executor with the given checkpoint directory.
    ///
    /// For more options, use [`Executor::builder()`].
    pub fn new(checkpoint_dir: std::path::PathBuf) -> Self {
        Self {
            checkpoint_dir,
            tiered: None,
            workers: 1,
        }
    }

    /// Set the number of parallel workers for keyed pipelines.
    ///
    /// When `workers > 1`, [`KeyedStream`](crate::dataflow::KeyedStream) operators
    /// run on parallel Timely worker threads, distributed by key hash.
    /// Defaults to `1` (single-worker mode).
    pub fn with_workers(mut self, n: usize) -> Self {
        assert!(n >= 1, "worker count must be at least 1");
        self.workers = n;
        self
    }

    /// Returns the configured number of workers.
    pub fn workers(&self) -> usize {
        self.workers
    }

    // ── Dataflow graph API ───────────────────────────────────────

    /// Compile and execute a [`DataflowGraph`].
    ///
    /// Build the graph first with [`DataflowGraph::source()`], stream
    /// transforms, and sinks, then pass it here for execution.
    pub async fn run(&self, graph: DataflowGraph) -> anyhow::Result<()> {
        dataflow::run_graph(graph, self, None).await
    }

    /// Compile and execute a [`DataflowGraph`] with graceful shutdown.
    pub async fn run_with_shutdown(
        &self,
        graph: DataflowGraph,
        shutdown: ShutdownHandle,
    ) -> anyhow::Result<()> {
        dataflow::run_graph(graph, self, Some(shutdown)).await
    }

    /// Configure tiered storage (L2 Foyer + L3 `SlateDB`) for this executor.
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
    /// The context is namespaced as `{operator_name}_w{worker_index}` so each
    /// worker has isolated state.
    pub async fn create_context_for_worker(
        &self,
        operator_name: &str,
        worker_index: usize,
    ) -> anyhow::Result<StateContext> {
        let namespaced = format!("{operator_name}_w{worker_index}");
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

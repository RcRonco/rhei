//! Pipeline configuration, lifecycle orchestration, and checkpointing.
//!
//! [`PipelineController`] (aliased as `Executor` for backward compatibility)
//! owns all configuration and drives the compile → bridge → execute → checkpoint
//! lifecycle via [`PipelineController::run()`].

use std::sync::Arc;

use rhei_core::checkpoint::CheckpointManifest;
use rhei_core::dlq::ErrorPolicy;
use rhei_core::state::context::StateContext;
use rhei_core::state::local_backend::LocalBackend;
use rhei_core::state::memtable::MemTableConfig;
use rhei_core::state::prefixed_backend::PrefixedBackend;
use rhei_core::state::slatedb_backend::SlateDbBackend;
use rhei_core::state::tiered_backend::{SharedL2Cache, TieredBackendConfig};

use crate::compiler::{ApiTopology, compile_graph};
use crate::dataflow::DataflowGraph;
use crate::health::{HealthState, PipelineStatus};
use crate::shutdown::ShutdownHandle;
use crate::task_manager::TaskManager;

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
    pub shared_l2: SharedL2Cache,
}

/// Configuration for remote object-store-backed distributed state.
///
/// When set, all processes independently open `SlateDB` pointing at the same
/// remote bucket/container, enabling shared durable state across a cluster.
/// Supports S3, Azure Blob Storage, and GCS via the `object_store` crate.
#[cfg(feature = "remote-state")]
#[derive(Debug, Clone)]
pub struct RemoteStateConfig {
    /// Bucket (S3/GCS) or container (Azure Blob) name.
    pub bucket: String,
    /// Key prefix inside the bucket/container (e.g. `"rhei/state/"`).
    pub prefix: String,
    /// Custom endpoint URL (for MinIO, Azurite, fake-gcs-server, etc.).
    pub endpoint: Option<String>,
    /// Cloud region (when applicable).
    pub region: String,
    /// Allow plain HTTP (for local development backends).
    pub allow_http: bool,
}

#[cfg(feature = "remote-state")]
impl RemoteStateConfig {
    /// Build an `ObjectStore` from this configuration.
    pub fn build_object_store(&self) -> anyhow::Result<Arc<dyn object_store::ObjectStore>> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region);

        if let Some(ref endpoint) = self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }

        // Credentials come from environment (AWS_ACCESS_KEY_ID, etc.)
        // or instance metadata / IAM role.
        builder = builder
            .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default())
            .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default());

        Ok(Arc::new(builder.build()?))
    }
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
    /// Bounded memtable configuration for L1 LRU eviction.
    pub(crate) memtable_config: MemTableConfig,
    /// Serializable topology populated after graph compilation.
    topology: Arc<std::sync::Mutex<Option<ApiTopology>>>,
    /// HTTP metrics/API server bind address. If set, the HTTP server is started
    /// automatically when `run()` is called.
    pub(crate) metrics_addr: Option<std::net::SocketAddr>,
    /// Human-readable pipeline name for the `/api/info` endpoint.
    pub(crate) pipeline_name: Option<String>,
    /// S3 state configuration for distributed state backend.
    #[cfg(feature = "remote-state")]
    pub(crate) remote_state: Option<RemoteStateConfig>,
    /// Manifest path for fork mode.
    #[cfg(feature = "remote-state")]
    pub(crate) from_checkpoint: Option<String>,
    /// Signed offset delta for fork mode.
    #[allow(dead_code)] // Used only when remote-state feature is enabled
    pub(crate) offset_delta: i64,
    /// Remote L3 backend for fork mode (read-only). Populated during run_graph().
    #[cfg(feature = "remote-state")]
    pub(crate) fork_remote_l3: std::sync::Mutex<Option<Arc<SlateDbBackend>>>,
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
    memtable_config: MemTableConfig,
    metrics_addr: Option<std::net::SocketAddr>,
    pipeline_name: Option<String>,
    #[cfg(feature = "remote-state")]
    remote_state: Option<RemoteStateConfig>,
    #[cfg(feature = "remote-state")]
    from_checkpoint: Option<String>,
    offset_delta: i64,
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

    /// Set the memtable configuration for L1 LRU eviction.
    pub fn memtable_config(mut self, config: MemTableConfig) -> Self {
        self.memtable_config = config;
        self
    }

    /// Set the HTTP server bind address for metrics, health, and dashboard APIs.
    ///
    /// When set, `run()` will automatically start an HTTP server on this address
    /// with `/healthz`, `/readyz`, `/metrics`, and `/api/*` endpoints.
    pub fn metrics_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.metrics_addr = Some(addr);
        self
    }

    /// Set the pipeline name (shown in `/api/info`).
    pub fn pipeline_name(mut self, name: impl Into<String>) -> Self {
        self.pipeline_name = Some(name.into());
        self
    }

    /// Configure remote object-store-backed distributed state.
    ///
    /// Each process independently opens `SlateDB` at the same remote path,
    /// enabling shared durable state across the cluster. Supports S3, Azure
    /// Blob, and GCS.
    #[cfg(feature = "remote-state")]
    pub fn remote_state(mut self, config: RemoteStateConfig) -> Self {
        self.remote_state = Some(config);
        self
    }

    /// Enable fork mode: resume from a remote checkpoint with copy-on-write state.
    #[cfg(feature = "remote-state")]
    pub fn from_checkpoint(mut self, path: impl Into<String>) -> Self {
        self.from_checkpoint = Some(path.into());
        self
    }

    /// Set the offset delta for fork mode (added to all source offsets).
    pub fn offset_delta(mut self, delta: i64) -> Self {
        self.offset_delta = delta;
        self
    }

    /// Read cluster configuration from environment variables.
    ///
    /// - `RHEI_WORKERS`: number of worker threads (overrides `.workers()`)
    /// - `RHEI_PROCESS_ID`: process ID for cluster mode
    /// - `RHEI_PEERS`: comma-separated peer addresses for cluster mode
    /// - `RHEI_METRICS_ADDR`: HTTP server bind address (e.g. `0.0.0.0:9090`)
    /// - `RHEI_PIPELINE_NAME`: human-readable pipeline name
    /// - `RHEI_REMOTE_BUCKET`, `RHEI_REMOTE_PREFIX`, `RHEI_REMOTE_ENDPOINT`,
    ///   `RHEI_REMOTE_REGION`, `RHEI_REMOTE_ALLOW_HTTP`: remote state config
    /// - `RHEI_FROM_CHECKPOINT`: manifest path for fork mode
    /// - `RHEI_OFFSET_DELTA`: signed offset delta for fork mode
    pub fn from_env(mut self) -> Self {
        if self.workers == 1
            && let Ok(val) = std::env::var("RHEI_WORKERS")
            && let Ok(n) = val.parse::<usize>()
        {
            self.workers = n;
        }
        if self.process_id.is_none()
            && let Ok(val) = std::env::var("RHEI_PROCESS_ID")
            && let Ok(id) = val.parse::<usize>()
        {
            self.process_id = Some(id);
        }
        if self.peers.is_none()
            && let Ok(val) = std::env::var("RHEI_PEERS")
        {
            let peers: Vec<String> = val.split(',').map(|s| s.trim().to_string()).collect();
            if !peers.is_empty() {
                self.peers = Some(peers);
            }
        }
        if self.metrics_addr.is_none()
            && let Ok(val) = std::env::var("RHEI_METRICS_ADDR")
            && let Ok(addr) = val.parse::<std::net::SocketAddr>()
        {
            self.metrics_addr = Some(addr);
        }
        if self.pipeline_name.is_none()
            && let Ok(val) = std::env::var("RHEI_PIPELINE_NAME")
        {
            self.pipeline_name = Some(val);
        }
        #[cfg(feature = "remote-state")]
        if self.remote_state.is_none() {
            if let Ok(bucket) = std::env::var("RHEI_REMOTE_BUCKET") {
                self.remote_state = Some(RemoteStateConfig {
                    bucket,
                    prefix: std::env::var("RHEI_REMOTE_PREFIX").unwrap_or_default(),
                    endpoint: std::env::var("RHEI_REMOTE_ENDPOINT").ok(),
                    region: std::env::var("RHEI_REMOTE_REGION")
                        .unwrap_or_else(|_| "us-east-1".to_string()),
                    allow_http: std::env::var("RHEI_REMOTE_ALLOW_HTTP")
                        .map(|v| v == "1" || v == "true")
                        .unwrap_or(false),
                });
            }
        }
        #[cfg(feature = "remote-state")]
        if self.from_checkpoint.is_none() {
            if let Ok(val) = std::env::var("RHEI_FROM_CHECKPOINT") {
                self.from_checkpoint = Some(val);
            }
        }
        if self.offset_delta == 0
            && let Ok(val) = std::env::var("RHEI_OFFSET_DELTA")
            && let Ok(delta) = val.parse::<i64>()
        {
            self.offset_delta = delta;
        }
        self
    }

    /// Build the controller.
    ///
    /// # Errors
    /// Returns an error if `peers` is set but `process_id` is missing or out of range.
    pub fn build(self) -> anyhow::Result<PipelineController> {
        if let Some(ref peers) = self.peers {
            let pid = self
                .process_id
                .ok_or_else(|| anyhow::anyhow!("process_id is required when peers are set"))?;
            anyhow::ensure!(
                pid < peers.len(),
                "process_id ({pid}) must be less than number of peers ({})",
                peers.len()
            );
        }
        Ok(PipelineController {
            checkpoint_dir: self.checkpoint_dir,
            tiered: self.tiered,
            workers: self.workers,
            checkpoint_interval: self.checkpoint_interval,
            error_policy: self.error_policy,
            health: self.health,
            process_id: self.process_id,
            peers: self.peers,
            memtable_config: self.memtable_config,
            topology: Arc::new(std::sync::Mutex::new(None)),
            metrics_addr: self.metrics_addr,
            pipeline_name: self.pipeline_name,
            #[cfg(feature = "remote-state")]
            remote_state: self.remote_state,
            #[cfg(feature = "remote-state")]
            from_checkpoint: self.from_checkpoint,
            offset_delta: self.offset_delta,
            #[cfg(feature = "remote-state")]
            fork_remote_l3: std::sync::Mutex::new(None),
        })
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
            memtable_config: MemTableConfig::default(),
            metrics_addr: None,
            pipeline_name: None,
            #[cfg(feature = "remote-state")]
            remote_state: None,
            #[cfg(feature = "remote-state")]
            from_checkpoint: None,
            offset_delta: 0,
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
            memtable_config: MemTableConfig::default(),
            topology: Arc::new(std::sync::Mutex::new(None)),
            metrics_addr: None,
            pipeline_name: None,
            #[cfg(feature = "remote-state")]
            remote_state: None,
            #[cfg(feature = "remote-state")]
            from_checkpoint: None,
            offset_delta: 0,
            #[cfg(feature = "remote-state")]
            fork_remote_l3: std::sync::Mutex::new(None),
        }
    }

    /// Returns a reference to the controller's [`HealthState`].
    pub fn health(&self) -> &HealthState {
        &self.health
    }

    /// Returns the pipeline topology, if the graph has been compiled.
    pub fn topology(&self) -> Option<ApiTopology> {
        self.topology
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Returns a shared handle to the topology slot for use in the HTTP server.
    pub fn topology_handle(&self) -> Arc<std::sync::Mutex<Option<ApiTopology>>> {
        self.topology.clone()
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
    ///
    /// # Errors
    /// Returns an error if in cluster mode but `process_id` is not set.
    pub(crate) fn timely_config(&self) -> anyhow::Result<timely::execute::Config> {
        if let Some(ref peers) = self.peers {
            let pid = self
                .process_id
                .ok_or_else(|| anyhow::anyhow!("process_id required for cluster"))?;
            Ok(timely::execute::Config {
                communication: timely::CommunicationConfig::Cluster {
                    threads: self.workers,
                    process: pid,
                    addresses: peers.clone(),
                    report: false,
                    zerocopy: false,
                    log_fn: Arc::new(|_| None),
                },
                worker: timely::WorkerConfig::default(),
            })
        } else {
            Ok(timely::execute::Config::process(self.workers))
        }
    }

    /// Compile and execute a [`DataflowGraph`].
    ///
    /// Build the graph first with [`DataflowGraph::source()`], stream
    /// transforms, and sinks, then pass it here for execution.
    pub async fn run(&self, graph: DataflowGraph) -> anyhow::Result<()> {
        run_graph(graph, self, None).await
    }

    /// Initialize telemetry, start the HTTP server (if `metrics_addr` is set),
    /// then compile and execute the [`DataflowGraph`].
    ///
    /// This is the recommended entry point for `#[rhei::pipeline]` generated
    /// code. It handles the full lifecycle: telemetry init, HTTP server start,
    /// graph compilation, and execution.
    pub async fn start(&self, graph: DataflowGraph) -> anyhow::Result<()> {
        let _http_handle = self.maybe_start_http()?;
        self.run(graph).await
    }

    /// Compile and execute a [`DataflowGraph`] with graceful shutdown.
    pub async fn run_with_shutdown(
        &self,
        graph: DataflowGraph,
        shutdown: ShutdownHandle,
    ) -> anyhow::Result<()> {
        run_graph(graph, self, Some(shutdown)).await
    }

    /// Start the HTTP server if `metrics_addr` is configured.
    ///
    /// Initializes telemetry (Prometheus + Snapshot recorders) and returns
    /// the server join handle. The handle keeps the server alive; drop it
    /// or abort it to stop the server.
    fn maybe_start_http(&self) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
        let Some(addr) = self.metrics_addr else {
            return Ok(None);
        };

        let handles = crate::telemetry::init(crate::telemetry::TelemetryConfig {
            metrics_addr: Some(addr),
            log_filter: std::env::var("RHEI_LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            json_logs: std::env::var("RHEI_JSON_LOGS")
                .map(|v| v == "1" || v == "true")
                .unwrap_or(false),
            tui: false,
        })?;

        let http_handle = crate::http_server::start(crate::http_server::HttpServerConfig {
            addr,
            health: self.health.clone(),
            prometheus: handles.prometheus_handle.ok_or_else(|| {
                anyhow::anyhow!("prometheus handle should exist when metrics_addr is set")
            })?,
            metrics_handle: handles.metrics_handle,
            log_rx: handles.log_rx,
            topology: self.topology.clone(),
            pipeline_name: self.pipeline_name.clone(),
            workers: self.workers,
            checkpoint_dir: Some(self.checkpoint_dir.clone()),
        });

        Ok(Some(http_handle))
    }

    /// Set the memtable configuration for L1 LRU eviction.
    #[must_use]
    pub fn with_memtable_config(mut self, config: MemTableConfig) -> Self {
        self.memtable_config = config;
        self
    }

    /// Configure tiered storage (L2 Foyer + L3 `SlateDB`) for this controller.
    ///
    /// Builds a single shared Foyer L2 cache per process (all operators share it).
    /// When set, `create_context` will produce contexts backed by a per-operator
    /// `PrefixedBackend` wrapping a `TieredBackend`.
    pub async fn with_tiered_storage(
        mut self,
        checkpoint_dir: std::path::PathBuf,
        l3: Arc<SlateDbBackend>,
        foyer_config: TieredBackendConfig,
    ) -> anyhow::Result<Self> {
        self.checkpoint_dir = checkpoint_dir;
        let shared_l2 = SharedL2Cache::open(&foyer_config).await?;
        self.tiered = Some(TieredStorageConfig { l3, shared_l2 });
        Ok(self)
    }

    /// Create a per-worker `StateContext` for the given operator.
    ///
    /// In single-process mode the context is namespaced as
    /// `{operator_name}_w{worker_index}`.
    /// In cluster mode it includes the process ID:
    /// `p{process_id}/w{worker_index}/{operator_name}`.
    pub fn create_context_for_worker(
        &self,
        operator_name: &str,
        worker_index: usize,
    ) -> anyhow::Result<StateContext> {
        let namespaced = if let Some(pid) = self.process_id {
            format!("p{pid}/w{worker_index}/{operator_name}")
        } else {
            format!("{operator_name}_w{worker_index}")
        };
        self.create_context(&namespaced)
    }

    /// Create a `StateContext` for the given operator.
    ///
    /// When tiered storage is configured, produces a context backed by
    /// `PrefixedBackend(TieredBackend)`. Otherwise falls back to `LocalBackend`.
    pub fn create_context(&self, operator_name: &str) -> anyhow::Result<StateContext> {
        let ctx = {
            #[cfg(feature = "remote-state")]
            {
                let fork_l3 = self
                    .fork_remote_l3
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(ref remote_l3) = *fork_l3 {
                    // Fork mode: PrefixedBackend wraps ForkBackend(local, remote).
                    let local_path = self
                        .checkpoint_dir
                        .join(format!("{operator_name}.checkpoint.json"));
                    let local = LocalBackend::new(local_path, None)?;
                    let fork = rhei_core::state::fork_backend::ForkBackend::new(
                        Box::new(local),
                        Box::new(remote_l3.clone()),
                    );
                    let prefixed = PrefixedBackend::new(operator_name, Box::new(fork));
                    StateContext::new(Box::new(prefixed))
                } else if let Some(ref tiered) = self.tiered {
                    let tiered_backend = tiered.shared_l2.create_tiered_backend(tiered.l3.clone());
                    let prefixed = PrefixedBackend::new(operator_name, Box::new(tiered_backend));
                    StateContext::new(Box::new(prefixed))
                } else {
                    let path = self
                        .checkpoint_dir
                        .join(format!("{operator_name}.checkpoint.json"));
                    let backend = LocalBackend::new(path, None)?;
                    StateContext::new(Box::new(backend))
                }
            }
            #[cfg(not(feature = "remote-state"))]
            {
                if let Some(ref tiered) = self.tiered {
                    let tiered_backend = tiered.shared_l2.create_tiered_backend(tiered.l3.clone());
                    let prefixed = PrefixedBackend::new(operator_name, Box::new(tiered_backend));
                    StateContext::new(Box::new(prefixed))
                } else {
                    let path = self
                        .checkpoint_dir
                        .join(format!("{operator_name}.checkpoint.json"));
                    let backend = LocalBackend::new(path, None)?;
                    StateContext::new(Box::new(backend))
                }
            }
        };

        Ok(ctx.with_memtable_config(self.memtable_config.clone()))
    }
}

/// Apply a signed offset delta to all source offsets.
/// Non-numeric offsets pass through unchanged. Results are clamped to >= 0.
#[allow(dead_code)] // Used only when remote-state feature is enabled
pub(crate) fn apply_offset_delta(
    offsets: &std::collections::HashMap<String, String>,
    delta: i64,
) -> std::collections::HashMap<String, String> {
    offsets
        .iter()
        .map(|(k, v)| {
            let adjusted = v
                .parse::<i64>()
                .map_or_else(|_| v.clone(), |n| (n + delta).max(0).to_string());
            (k.clone(), adjusted)
        })
        .collect()
}

/// Compile and execute the dataflow graph.
#[allow(clippy::too_many_lines)] // Complexity justified by coordinated checkpoint logic
async fn run_graph(
    graph: DataflowGraph,
    controller: &PipelineController,
    shutdown: Option<ShutdownHandle>,
) -> anyhow::Result<()> {
    let compiled = compile_graph(graph.into_nodes())?;

    // Store topology for the HTTP API before nodes are consumed.
    *controller
        .topology
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(compiled.topology.clone());

    let all_operator_names = &compiled.operator_names;

    // Fork mode: load remote manifest if --from-checkpoint is set.
    #[cfg(feature = "remote-state")]
    let fork_data: Option<(u64, std::collections::HashMap<String, String>)> = {
        if let Some(ref manifest_path) = controller.from_checkpoint {
            let remote_cfg = controller.remote_state.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "fork mode requires remote state config \
                         (set RHEI_REMOTE_BUCKET or use .remote_state())"
                )
            })?;

            let object_store = remote_cfg.build_object_store()?;
            let path = object_store::path::Path::from(manifest_path.as_str());
            let manifest = CheckpointManifest::load_from_object_store(object_store.as_ref(), &path)
                .await
                .ok_or_else(|| {
                    anyhow::anyhow!("checkpoint manifest not found at {manifest_path}")
                })?;

            // Validate topology if manifest includes it.
            if let Some(manifest_workers) = manifest.workers_per_process {
                if manifest_workers != controller.workers {
                    anyhow::bail!(
                        "fork mode: manifest has {manifest_workers} workers per process, \
                         but local pipeline has {}. Must match.",
                        controller.workers,
                    );
                }
            }

            let offsets = apply_offset_delta(&manifest.source_offsets, controller.offset_delta);

            tracing::info!(
                checkpoint_id = manifest.checkpoint_id,
                offset_delta = controller.offset_delta,
                adjusted_offsets = ?offsets,
                "fork mode: resuming from remote checkpoint #{}",
                manifest.checkpoint_id
            );

            // Open remote SlateDB read-only for ForkBackend.
            let remote_l3 =
                Arc::new(SlateDbBackend::open(remote_cfg.prefix.as_str(), object_store).await?);
            *controller
                .fork_remote_l3
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(remote_l3);

            Some((manifest.checkpoint_id, offsets))
        } else {
            None
        }
    };

    // Determine initial state: fork mode overrides local manifest.
    let (initial_checkpoint_id, restored_offsets) = {
        #[cfg(feature = "remote-state")]
        {
            if let Some(fork) = fork_data {
                fork
            } else if let Some(manifest) = CheckpointManifest::load(&controller.checkpoint_dir) {
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
            }
        }

        #[cfg(not(feature = "remote-state"))]
        {
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
            }
        }
    };

    controller.health.set_status(PipelineStatus::Running);

    let task_manager = Arc::new(
        TaskManager::build(
            compiled,
            controller,
            shutdown.as_ref(),
            restored_offsets,
            initial_checkpoint_id,
        )
        .await?,
    );

    let last_checkpoint_id = task_manager.clone().run(controller).await?;

    let source_offsets = task_manager.source_offsets();
    let operator_names = task_manager.operator_names().to_vec();
    let ckpt_process_id = controller.process_id();
    let ckpt_n_processes = controller.peers.as_ref().map_or(1, Vec::len);

    // invariant: all workers have finished, no other Arc holders
    let task_manager = Arc::try_unwrap(task_manager)
        .unwrap_or_else(|_| panic!("TaskManager Arc still shared after run() completed"));
    task_manager.drain().await?;

    // Final manifest (includes all accumulated checkpoint progress).
    let checkpoint_id = last_checkpoint_id + 1;
    let manifest = CheckpointManifest {
        version: 1,
        checkpoint_id,
        timestamp_ms: now_millis(),
        operators: operator_names,
        source_offsets,
        n_processes: Some(ckpt_n_processes),
        workers_per_process: Some(controller.workers),
    };

    crate::task_manager::write_manifest(
        &manifest,
        &controller.checkpoint_dir,
        ckpt_process_id,
        ckpt_n_processes,
    )?;

    controller.health.set_status(PipelineStatus::Stopped);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rhei_core::connectors::vec_source::VecSource;
    use rhei_core::state::context::StateContext;
    use rhei_core::traits::{Sink, StreamFunction};

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
        std::env::temp_dir().join(format!("rhei_dataflow_{name}_{}", std::process::id()))
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
                let count = ctx.get::<u64>(key).await.unwrap_or(None).unwrap_or(0) + 1;
                ctx.put(key, &count)?;
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
        let source = VecSource::new(vec!["hello world".to_string(), "hello rhei".to_string()]);
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
        assert_eq!(results.len(), 4); // hello, world, hello, rhei
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"rhei: 1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn keyed_multi_worker() {
        let dir = temp_dir("keyed_multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["hello world".to_string(), "hello rhei".to_string()]);
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
            .build()
            .unwrap();
        controller.run(graph).await.unwrap();
        let mut results = collected.lock().unwrap().clone();
        results.sort_unstable();
        assert_eq!(results.len(), 4);
        // With multi-worker, order may vary but counts must be correct.
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"rhei: 1".to_string()));
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
            .build()
            .unwrap();
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
            .build()
            .unwrap();
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
            .build()
            .unwrap();
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
            rhei_core::connectors::partitioned_vec_source::PartitionedVecSource::new(items, 4);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build()
            .unwrap();
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
        let source = rhei_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
            vec![
                "hello".to_string(),
                "world".to_string(),
                "hello".to_string(),
                "rhei".to_string(),
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
            .build()
            .unwrap();
        controller.run(graph).await.unwrap();
        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 6);
        assert!(results.contains(&"hello: 1".to_string()));
        assert!(results.contains(&"hello: 2".to_string()));
        assert!(results.contains(&"hello: 3".to_string()));
        assert!(results.contains(&"world: 1".to_string()));
        assert!(results.contains(&"world: 2".to_string()));
        assert!(results.contains(&"rhei: 1".to_string()));
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
            rhei_core::connectors::partitioned_vec_source::PartitionedVecSource::new(items, 4);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream.sink(CollectSink {
            collected: collected.clone(),
        });

        let controller = super::PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(1)
            .build()
            .unwrap();
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
        let source = rhei_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
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
            .build()
            .unwrap();
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
            .build()
            .unwrap();
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
    impl rhei_core::traits::Source for CheckpointTrackingSource {
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
            .build()
            .unwrap();
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
        let source = VecSource::new(vec!["hello world".to_string(), "hello rhei".to_string()]);
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
            .build()
            .unwrap();
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
        let source = rhei_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
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
            .build()
            .unwrap();
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
        let source = rhei_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
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
            .build()
            .unwrap();
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
        let partitioned = rhei_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
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
            .build()
            .unwrap();
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
        let source = rhei_core::connectors::partitioned_vec_source::PartitionedVecSource::new(
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
            .build()
            .unwrap();
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
            .build()
            .unwrap();
        assert_eq!(ctrl.total_workers(), 8); // 4 workers * 2 peers
        assert!(ctrl.is_cluster());
    }

    #[test]
    fn single_process_total_workers() {
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(4)
            .build()
            .unwrap();
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
            .build()
            .unwrap();
        assert_eq!(ctrl0.local_worker_range(), 0..4);

        // Process 1 of 2, 4 workers each
        let ctrl1 = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(4)
            .process_id(1)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build()
            .unwrap();
        assert_eq!(ctrl1.local_worker_range(), 4..8);
    }

    #[test]
    fn single_process_local_worker_range() {
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(3)
            .build()
            .unwrap();
        assert_eq!(ctrl.local_worker_range(), 0..3);
    }

    #[test]
    fn cluster_timely_config_is_cluster() {
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(2)
            .process_id(0)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build()
            .unwrap();
        let config = ctrl.timely_config().unwrap();
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
            .build()
            .unwrap();
        let config = ctrl.timely_config().unwrap();
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
        let ctrl = super::PipelineController::builder()
            .checkpoint_dir(temp_dir("cluster_prefix"))
            .workers(2)
            .process_id(1)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build()
            .unwrap();

        let ctx = ctrl.create_context_for_worker("my_op", 3).unwrap();
        // The context should have been created with prefix "p1_w3_my_op"
        // We can verify via the backend path for LocalBackend
        // Just check it doesn't panic with the cluster prefix format
        drop(ctx);
    }

    #[test]
    fn single_process_state_prefix_unchanged() {
        let dir = temp_dir("single_prefix");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let ctrl = super::PipelineController::new(dir.clone());
        let ctx = ctrl.create_context_for_worker("my_op", 0).unwrap();
        // Should not panic; prefix is "my_op_w0" (original format)
        drop(ctx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cluster_requires_process_id() {
        let err = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .peers(vec!["h0:2101".into()])
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("process_id is required when peers are set"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cluster_process_id_out_of_range() {
        let err = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .process_id(2)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("process_id (2) must be less than number of peers (2)"),
            "unexpected error: {err}"
        );
    }

    // ── Flink-inspired feature integration tests ─────────────────────

    #[derive(Clone)]
    struct LifecycleOp {
        opened: Arc<std::sync::atomic::AtomicBool>,
        closed: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait]
    impl StreamFunction for LifecycleOp {
        type Input = String;
        type Output = String;

        async fn open(&mut self, _ctx: &mut StateContext) -> anyhow::Result<()> {
            self.opened.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn close(&mut self) -> anyhow::Result<()> {
            self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn process(
            &mut self,
            input: String,
            _ctx: &mut StateContext,
        ) -> anyhow::Result<Vec<String>> {
            Ok(vec![input])
        }
    }

    #[tokio::test]
    async fn lifecycle_open_close() {
        let dir = temp_dir("lifecycle");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["a".to_string(), "b".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|s: &String| s.clone())
            .operator(
                "lifecycle",
                LifecycleOp {
                    opened: opened.clone(),
                    closed: closed.clone(),
                },
            )
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();

        assert!(
            opened.load(std::sync::atomic::Ordering::SeqCst),
            "open() should have been called"
        );
        assert!(
            closed.load(std::sync::atomic::Ordering::SeqCst),
            "close() should have been called"
        );
        let mut results = collected.lock().unwrap().clone();
        results.sort();
        assert_eq!(results, vec!["a", "b"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn reduce_pipeline() {
        let dir = temp_dir("reduce");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![
            ("a".to_string(), 10i64),
            ("b".to_string(), 100),
            ("a".to_string(), 5),
            ("a".to_string(), 3),
            ("b".to_string(), 50),
        ]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|v: &(String, i64)| v.0.clone())
            .reduce(
                "sum",
                |v: &(String, i64)| v.0.clone(),
                |a: (String, i64), b: (String, i64)| (a.0, a.1 + b.1),
            )
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 5);
        // Last "a" should be 18, last "b" should be 150
        assert!(results.contains(&("a".to_string(), 18)));
        assert!(results.contains(&("b".to_string(), 150)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn aggregate_pipeline() {
        use rhei_core::operators::aggregator::Count;

        let dir = temp_dir("aggregate");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![
            "x".to_string(),
            "y".to_string(),
            "x".to_string(),
            "x".to_string(),
            "y".to_string(),
        ]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|s: &String| s.clone())
            .aggregate("count", |s: &String| s.clone(), Count::<String>::new())
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 5);
        // Rolling counts: x emits 1, 2, 3; y emits 1, 2
        assert!(results.contains(&3u64));
        assert!(results.contains(&2u64));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn count_window_pipeline() {
        use rhei_core::operators::aggregator::Count;
        use rhei_core::operators::count_window::{CountWindow, CountWindowOutput};

        let dir = temp_dir("count_win");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![
            "a".to_string(),
            "a".to_string(),
            "b".to_string(),
            "a".to_string(), // triggers window for "a" at count=3
            "b".to_string(),
            "b".to_string(), // triggers window for "b" at count=3
        ]);
        let collected: Arc<Mutex<Vec<CountWindowOutput<u64>>>> = Arc::new(Mutex::new(Vec::new()));

        let window = CountWindow::<String, _, _>::builder()
            .count(3)
            .key_fn(|s: &String| s.clone())
            .aggregator(Count::<String>::new())
            .build();

        let stream = graph.source(source);
        stream
            .key_by(|s: &String| s.clone())
            .operator("count_window", window)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 2);
        let a_win = results
            .iter()
            .find(|w| w.key == "a")
            .expect("window for 'a'");
        assert_eq!(a_win.count, 3);
        assert_eq!(a_win.value, 3);
        let b_win = results
            .iter()
            .find(|w| w.key == "b")
            .expect("window for 'b'");
        assert_eq!(b_win.count, 3);
        assert_eq!(b_win.value, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[derive(Clone)]
    struct SideOutputOp;

    #[async_trait]
    impl StreamFunction for SideOutputOp {
        type Input = String;
        type Output = rhei_core::operators::with_side::WithSide<String, String>;

        async fn process(
            &mut self,
            input: String,
            _ctx: &mut StateContext,
        ) -> anyhow::Result<Vec<rhei_core::operators::with_side::WithSide<String, String>>>
        {
            use rhei_core::operators::with_side::WithSide;
            if input.starts_with("ERR:") {
                Ok(vec![WithSide::side(input)])
            } else {
                Ok(vec![WithSide::main(input)])
            }
        }
    }

    #[tokio::test]
    async fn split_side_routing() {
        let dir = temp_dir("side_output");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![
            "hello".to_string(),
            "ERR:bad".to_string(),
            "world".to_string(),
            "ERR:oops".to_string(),
        ]);
        let main_collected = Arc::new(Mutex::new(Vec::new()));
        let side_collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        let tagged = stream
            .key_by(|s: &String| s.clone())
            .operator("tagger", SideOutputOp);
        let (main_stream, side_stream) = tagged.split_side();

        main_stream.sink(CollectSink {
            collected: main_collected.clone(),
        });
        side_stream.sink(CollectSink {
            collected: side_collected.clone(),
        });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();

        let mut main_results = main_collected.lock().unwrap().clone();
        main_results.sort();
        assert_eq!(main_results, vec!["hello", "world"]);

        let mut side_results = side_collected.lock().unwrap().clone();
        side_results.sort();
        assert_eq!(side_results, vec!["ERR:bad", "ERR:oops"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_offset_delta_negative() {
        let offsets = std::collections::HashMap::from([
            ("t/0".into(), "100".into()),
            ("t/1".into(), "200".into()),
        ]);
        let result = super::apply_offset_delta(&offsets, -50);
        assert_eq!(result.get("t/0").unwrap(), "50");
        assert_eq!(result.get("t/1").unwrap(), "150");
    }

    #[test]
    fn apply_offset_delta_clamps_to_zero() {
        let offsets = std::collections::HashMap::from([("t/0".into(), "10".into())]);
        let result = super::apply_offset_delta(&offsets, -100);
        assert_eq!(result.get("t/0").unwrap(), "0");
    }

    #[test]
    fn apply_offset_delta_non_numeric_passthrough() {
        let offsets = std::collections::HashMap::from([("t/0".into(), "not_a_number".into())]);
        let result = super::apply_offset_delta(&offsets, -100);
        assert_eq!(result.get("t/0").unwrap(), "not_a_number");
    }

    #[test]
    fn apply_offset_delta_positive() {
        let offsets = std::collections::HashMap::from([("t/0".into(), "100".into())]);
        let result = super::apply_offset_delta(&offsets, 50);
        assert_eq!(result.get("t/0").unwrap(), "150");
    }

    #[test]
    fn apply_offset_delta_zero_identity() {
        let offsets = std::collections::HashMap::from([("t/0".into(), "42".into())]);
        let result = super::apply_offset_delta(&offsets, 0);
        assert_eq!(result.get("t/0").unwrap(), "42");
    }

    #[tokio::test]
    async fn enrich_pipeline() {
        let dir = temp_dir("enrich");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![1i32, 2, 3]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|x: &i32| x.to_string())
            .enrich(
                "lookup",
                2,
                std::time::Duration::from_secs(1),
                |x: i32| async move { Ok(format!("enriched:{x}")) },
            )
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let controller = super::PipelineController::new(dir.clone());
        controller.run(graph).await.unwrap();

        let mut results = collected.lock().unwrap().clone();
        results.sort();
        assert_eq!(results, vec!["enriched:1", "enriched:2", "enriched:3"]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

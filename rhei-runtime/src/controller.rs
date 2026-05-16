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
    /// Custom endpoint URL (for `MinIO`, Azurite, `fake-gcs-server`, etc.).
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
pub struct PipelineController {
    pub(crate) checkpoint_dir: std::path::PathBuf,
    pub(crate) tiered: Option<TieredStorageConfig>,
    pub(crate) workers: usize,
    pub(crate) checkpoint_interval: u64,
    pub(crate) error_policy: ErrorPolicy,
    pub(crate) dlq_sink: std::sync::Mutex<Option<Box<dyn rhei_core::dlq::DlqSink>>>,
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
    /// Remote L3 backend for fork mode (read-only). Populated during `run_graph()`.
    #[cfg(feature = "remote-state")]
    pub(crate) fork_remote_l3: std::sync::Mutex<Option<Arc<SlateDbBackend>>>,
}

impl std::fmt::Debug for PipelineController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineController")
            .field("checkpoint_dir", &self.checkpoint_dir)
            .field("workers", &self.workers)
            .field("error_policy", &self.error_policy)
            .finish_non_exhaustive()
    }
}

/// Builder for [`PipelineController`].
pub struct PipelineControllerBuilder {
    checkpoint_dir: std::path::PathBuf,
    workers: usize,
    checkpoint_interval: u64,
    tiered: Option<TieredStorageConfig>,
    error_policy: ErrorPolicy,
    dlq_sink: Option<Box<dyn rhei_core::dlq::DlqSink>>,
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

impl std::fmt::Debug for PipelineControllerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineControllerBuilder")
            .field("checkpoint_dir", &self.checkpoint_dir)
            .field("workers", &self.workers)
            .field("error_policy", &self.error_policy)
            .field("dlq_sink", &self.dlq_sink.as_ref().map(|_| "..."))
            .finish_non_exhaustive()
    }
}

impl PipelineControllerBuilder {
    /// Apply settings from a [`PipelineConfig`](rhei_core::config::PipelineConfig).
    ///
    /// Core fields (`checkpoint_dir`, `workers`, `checkpoint_interval`) are
    /// set unconditionally from the config. Optional fields (`pipeline_name`,
    /// `metrics_addr`, `process_id`, `peers`) are only set when present in
    /// the config. Call this early in the builder chain so that subsequent
    /// builder methods can override the values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = PipelineConfig::load("pipeline.toml")?.apply_env();
    /// let executor = Executor::builder()
    ///     .apply_config(&config)  // sets all core fields from config
    ///     .workers(8)             // overrides the config value
    ///     .build()?;
    /// ```
    pub fn apply_config(mut self, config: &rhei_core::config::PipelineConfig) -> Self {
        self.checkpoint_dir = std::path::PathBuf::from(&config.pipeline.checkpoint_dir);
        self.workers = config.pipeline.workers;
        self.checkpoint_interval = config.pipeline.checkpoint_interval;
        if let Some(ref name) = config.pipeline.name {
            self.pipeline_name = Some(name.clone());
        }
        if let Some(addr) = config.metrics_addr() {
            self.metrics_addr = Some(addr);
        }
        if let Some(pid) = config.cluster.process_id {
            self.process_id = Some(pid);
        }
        if let Some(ref peers) = config.cluster.peers {
            self.peers = Some(peers.clone());
        }
        self
    }

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

    /// Set the DLQ sink for routing failed records.
    ///
    /// Requires `error_policy(ErrorPolicy::SendToDlq)` to be set.
    pub fn dlq_sink(mut self, sink: impl rhei_core::dlq::DlqSink) -> Self {
        self.dlq_sink = Some(Box::new(sink));
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
        if self.remote_state.is_none()
            && let Ok(bucket) = std::env::var("RHEI_REMOTE_BUCKET")
        {
            self.remote_state = Some(RemoteStateConfig {
                bucket,
                prefix: std::env::var("RHEI_REMOTE_PREFIX").unwrap_or_default(),
                endpoint: std::env::var("RHEI_REMOTE_ENDPOINT").ok(),
                region: std::env::var("RHEI_REMOTE_REGION")
                    .unwrap_or_else(|_| "us-east-1".to_string()),
                allow_http: std::env::var("RHEI_REMOTE_ALLOW_HTTP")
                    .is_ok_and(|v| v == "1" || v == "true"),
            });
        }
        #[cfg(feature = "remote-state")]
        if self.from_checkpoint.is_none()
            && let Ok(val) = std::env::var("RHEI_FROM_CHECKPOINT")
        {
            self.from_checkpoint = Some(val);
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
            dlq_sink: std::sync::Mutex::new(self.dlq_sink),
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
            dlq_sink: None,
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
            dlq_sink: std::sync::Mutex::new(None),
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
            .unwrap_or_else(|e| {
                tracing::warn!("mutex poisoned in topology lock, recovering inner data");
                e.into_inner()
            })
            .clone()
    }

    /// Returns a shared handle to the topology slot for use in the HTTP server.
    pub fn topology_handle(&self) -> Arc<std::sync::Mutex<Option<ApiTopology>>> {
        self.topology.clone()
    }

    /// Set the number of parallel workers.
    ///
    /// When `workers > 1`, batch operators run on parallel Timely worker
    /// threads. Defaults to `1` (single-worker mode).
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
    ///
    /// Validates the graph structure before compilation. Returns a clear
    /// error if any streams do not terminate at a sink.
    pub async fn run(&self, graph: DataflowGraph) -> anyhow::Result<()> {
        graph.validate().map_err(|e| anyhow::anyhow!("{e}"))?;
        run_graph(graph, self, None).await
    }

    /// Initialize telemetry, start the HTTP server (if `metrics_addr` is set),
    /// then compile and execute the [`DataflowGraph`].
    ///
    /// This is the recommended entry point for `#[rhei::pipeline]` generated
    /// code. It handles the full lifecycle: telemetry init, HTTP server start,
    /// graph validation, compilation, and execution.
    pub async fn start(&self, graph: DataflowGraph) -> anyhow::Result<()> {
        graph.validate().map_err(|e| anyhow::anyhow!("{e}"))?;
        let _http_handle = self.maybe_start_http()?;
        run_graph(graph, self, None).await
    }

    /// Compile and execute a [`DataflowGraph`] with graceful shutdown.
    ///
    /// Validates the graph structure before compilation. Returns a clear
    /// error if any streams do not terminate at a sink.
    pub async fn run_with_shutdown(
        &self,
        graph: DataflowGraph,
        shutdown: ShutdownHandle,
    ) -> anyhow::Result<()> {
        graph.validate().map_err(|e| anyhow::anyhow!("{e}"))?;
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
            json_logs: std::env::var("RHEI_JSON_LOGS").is_ok_and(|v| v == "1" || v == "true"),
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
                let fork_l3 = self.fork_remote_l3.lock().unwrap_or_else(|e| {
                    tracing::warn!("mutex poisoned in fork_remote_l3 lock, recovering inner data");
                    e.into_inner()
                });
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
                    let prefixed = PrefixedBackend::new(operator_name, Box::new(fork))?;
                    StateContext::new(Box::new(prefixed))
                } else if let Some(ref tiered) = self.tiered {
                    let tiered_backend = tiered.shared_l2.create_tiered_backend(tiered.l3.clone());
                    let prefixed = PrefixedBackend::new(operator_name, Box::new(tiered_backend))?;
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
                    let prefixed = PrefixedBackend::new(operator_name, Box::new(tiered_backend))?;
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
    *controller.topology.lock().unwrap_or_else(|e| {
        tracing::warn!("mutex poisoned in topology lock (run_graph), recovering inner data");
        e.into_inner()
    }) = Some(compiled.topology.clone());

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
            if let Some(manifest_workers) = manifest.workers_per_process
                && manifest_workers != controller.workers
            {
                anyhow::bail!(
                    "fork mode: manifest has {manifest_workers} workers per process, \
                     but local pipeline has {}. Must match.",
                    controller.workers,
                );
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
            *controller.fork_remote_l3.lock().unwrap_or_else(|e| {
                tracing::warn!(
                    "mutex poisoned in fork_remote_l3 lock (run_graph), recovering inner data"
                );
                e.into_inner()
            }) = Some(remote_l3);

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
    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rhei_dataflow_{name}_{}", std::process::id()))
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

    // ── Offset delta tests ──────────────────────────────────────────

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
}

//! Pipeline configuration, lifecycle orchestration, and checkpointing.
//!
//! [`PipelineController`] (aliased as `Executor` for backward compatibility)
//! owns all configuration and drives the compile → bridge → execute → checkpoint
//! lifecycle via [`PipelineController::run()`].

use std::sync::Arc;

use rhei_core::checkpoint::CheckpointManifest;
use rhei_core::dlq::ErrorPolicy;
use rhei_core::state::context::StateContext;
use rhei_core::state::key_group_addressing::StateAddressing;
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

/// Forward the caller's shutdown signal into one generation's trigger.
///
/// Each generation gets its own [`ShutdownHandle`] so a rescale can stop it
/// without disturbing the caller's; this task bridges the two, and is aborted
/// when the generation ends.
/// Record the start of a pipeline generation, closing out the rescale that
/// preceded it if there was one.
///
/// `rescale_started` is `Some` only when this generation is replacing one that
/// a rescale drained, so the histogram measures real rescale downtime and is
/// not diluted by the initial startup.
fn record_generation_start(
    node_id: &str,
    generation: u64,
    topology: &rhei_core::cluster::ClusterTopology,
    rescale_started: Option<std::time::Instant>,
) {
    tracing::info!(
        node_id,
        generation,
        topology = %topology.summary(),
        "starting pipeline generation"
    );
    metrics::gauge!("rhei_cluster_generation").set(generation as f64);

    if let Some(started) = rescale_started {
        let elapsed = started.elapsed();
        metrics::histogram!("rhei_cluster_rescale_duration_seconds").record(elapsed.as_secs_f64());
        tracing::info!(
            node_id,
            generation,
            downtime_secs = elapsed.as_secs_f64(),
            "rescale complete; pipeline resumed"
        );
    }
}

fn forward_shutdown(
    outer: &ShutdownHandle,
    trigger: Arc<crate::shutdown::ShutdownTrigger>,
) -> tokio::task::JoinHandle<()> {
    let mut rx = outer.subscribe();
    tokio::spawn(async move {
        // Check the current value first: shutdown may already have been
        // requested before this generation started.
        if *rx.borrow_and_update() {
            trigger.shutdown();
            return;
        }
        while rx.changed().await.is_ok() {
            if *rx.borrow_and_update() {
                trigger.shutdown();
                return;
            }
        }
    })
}

/// How one pipeline generation ended under [`PipelineController::run_dynamic`].
enum GenerationOutcome {
    /// The dataflow completed (or failed) on its own.
    Finished(anyhow::Result<()>),
    /// Membership changed; drain this generation and start the next.
    Rescale {
        next: rhei_core::cluster::ClusterTopology,
        moved_key_groups: usize,
    },
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
    /// Number of key groups — the ceiling on parallelism this pipeline can
    /// ever rescale to.
    ///
    /// Fixed for the pipeline's lifetime: it determines which key group every
    /// key hashes into, so changing it invalidates all persisted state. It is
    /// recorded in the checkpoint manifest and validated on restore.
    pub(crate) max_parallelism: usize,
    /// Cluster topology for the current generation, when running under dynamic
    /// discovery.
    ///
    /// When set, this overrides the static `workers` / `process_id` / `peers`
    /// fields — those describe the `--peers` startup configuration, while this
    /// describes the cluster as currently discovered. Interior mutability lets
    /// a rescale install a new generation without rebuilding the controller
    /// (which owns non-cloneable resources like the DLQ sink and L2 cache).
    pub(crate) active_topology: std::sync::Mutex<Option<rhei_core::cluster::ClusterTopology>>,
    /// This node's identifier under dynamic discovery, used to find its own
    /// process ID within [`Self::active_topology`].
    pub(crate) node_id: std::sync::Mutex<Option<String>>,
    /// One `LocalBackend` per operator, shared by every worker in this process.
    ///
    /// Key-group addressing means all workers now derive the *same* physical
    /// keys, so they must also share one backing store. Handing each worker its
    /// own `LocalBackend` over the same file would be silent data loss:
    /// `LocalBackend` holds the whole map in memory and `checkpoint()` writes it
    /// wholesale, so the last worker to flush would erase every other worker's
    /// state.
    ///
    /// Sharing is also what makes in-process rescaling work — a worker that
    /// inherits a key group finds it already present in the shared map.
    local_backends: std::sync::Mutex<std::collections::HashMap<String, Arc<LocalBackend>>>,
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
    max_parallelism: usize,
    metrics_addr: Option<std::net::SocketAddr>,
    pipeline_name: Option<String>,
    #[cfg(feature = "remote-state")]
    remote_state: Option<RemoteStateConfig>,
    #[cfg(feature = "remote-state")]
    from_checkpoint: Option<String>,
    offset_delta: i64,
    /// Environment variables that were set but unparseable.
    ///
    /// Collected rather than ignored so [`PipelineControllerBuilder::build`]
    /// can fail with all of them at once instead of the pipeline starting up
    /// with silently-defaulted settings.
    env_errors: Vec<String>,
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

/// A source of environment values: name in, trimmed value out.
type EnvLookup<'a> = dyn Fn(&str) -> Option<String> + 'a;

/// Normalise a raw environment value, treating empty and whitespace-only
/// values as unset.
///
/// Container orchestrators routinely inject `VAR=""` for an unset optional
/// value; treating that as a real setting would turn every such deployment
/// into a config error.
fn read_env(name: &str, raw: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    let value = raw(name)?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
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
        if let Some(max_p) = config.cluster.max_parallelism {
            self.max_parallelism = max_p;
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

    /// Set the number of key groups (default:
    /// [`DEFAULT_MAX_PARALLELISM`](rhei_core::cluster::DEFAULT_MAX_PARALLELISM)).
    ///
    /// This is the ceiling on the parallelism the pipeline can ever rescale to
    /// — a cluster with more workers than key groups leaves the surplus
    /// workers idle. Pick it once, generously, when the pipeline is created:
    /// every key's group is `hash(key) % max_parallelism`, so changing it later
    /// rehashes the entire key space and invalidates all persisted state.
    pub fn max_parallelism(mut self, n: usize) -> Self {
        self.max_parallelism = n;
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
    /// - `RHEI_CHECKPOINT_DIR`: checkpoint directory
    /// - `RHEI_CHECKPOINT_INTERVAL`: batches between checkpoints
    /// - `RHEI_MAX_PARALLELISM`: key group count (fixed for a pipeline's life)
    /// - `RHEI_LIVENESS_TIMEOUT_SECS`: worker silence tolerated by `/healthz`
    ///
    /// Values already set explicitly on the builder win: the environment fills
    /// gaps, it does not override deliberate code. An unparseable value is
    /// recorded and reported by [`Self::build`] rather than ignored.
    pub fn from_env(self) -> Self {
        self.apply_env_with(&|key| std::env::var(key).ok())
    }

    /// Apply overrides using a caller-supplied raw lookup.
    ///
    /// Lets tests supply a fixed environment without `unsafe` mutation of the
    /// process environment, which would race across parallel tests. Values are
    /// normalised here rather than in the lookup, so an injected environment
    /// goes through exactly the same treatment as the real one.
    fn apply_env_with(mut self, raw: &EnvLookup<'_>) -> Self {
        let env = |key: &str| read_env(key, raw);
        self = self.apply_pipeline_env(&env);
        self = self.apply_cluster_env(&env);
        self.apply_remote_env(&env)
    }

    /// Checkpointing, key groups, and liveness — how this process behaves.
    fn apply_pipeline_env(mut self, env: &EnvLookup<'_>) -> Self {
        if let Some(val) = env("RHEI_CHECKPOINT_DIR") {
            self.checkpoint_dir = std::path::PathBuf::from(val);
        }
        if let Some(val) = env("RHEI_CHECKPOINT_INTERVAL") {
            match val.parse::<u64>() {
                Ok(n) => self.checkpoint_interval = n,
                Err(e) => self.env_error(
                    "RHEI_CHECKPOINT_INTERVAL",
                    &val,
                    &format!("{e}; expected a number of batches"),
                ),
            }
        }
        if let Some(val) = env("RHEI_MAX_PARALLELISM") {
            match val.parse::<usize>() {
                Ok(n) => self.max_parallelism = n,
                Err(e) => self.env_error(
                    "RHEI_MAX_PARALLELISM",
                    &val,
                    &format!("{e}; expected a number of key groups"),
                ),
            }
        }
        if let Some(val) = env("RHEI_LIVENESS_TIMEOUT_SECS") {
            match val.parse::<u64>() {
                Ok(secs) if secs > 0 => {
                    self.health = self
                        .health
                        .with_liveness_timeout(std::time::Duration::from_secs(secs));
                }
                Ok(_) => self.env_error(
                    "RHEI_LIVENESS_TIMEOUT_SECS",
                    &val,
                    "must be at least 1 second; a zero timeout would fail liveness immediately",
                ),
                Err(e) => self.env_error(
                    "RHEI_LIVENESS_TIMEOUT_SECS",
                    &val,
                    &format!("{e}; expected a number of seconds"),
                ),
            }
        }
        self
    }

    /// Parallelism, cluster membership, and the observability endpoint.
    fn apply_cluster_env(mut self, env: &EnvLookup<'_>) -> Self {
        if self.workers == 1
            && let Some(val) = env("RHEI_WORKERS")
        {
            match val.parse::<usize>() {
                Ok(n) => self.workers = n,
                Err(e) => self.env_error("RHEI_WORKERS", &val, &format!("{e}; expected a number")),
            }
        }
        if self.process_id.is_none()
            && let Some(val) = env("RHEI_PROCESS_ID")
        {
            match val.parse::<usize>() {
                Ok(id) => self.process_id = Some(id),
                Err(e) => {
                    self.env_error("RHEI_PROCESS_ID", &val, &format!("{e}; expected a number"));
                }
            }
        }
        if self.peers.is_none()
            && let Some(val) = env("RHEI_PEERS")
        {
            // `"".split(',')` yields one empty element, so an empty variable
            // would otherwise produce a cluster with one nameless peer.
            let peers: Vec<String> = val
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .collect();
            if peers.is_empty() {
                self.env_error(
                    "RHEI_PEERS",
                    &val,
                    "no peer addresses found; expected a comma-separated list of host:port",
                );
            } else {
                self.peers = Some(peers);
            }
        }
        if self.metrics_addr.is_none()
            && let Some(val) = env("RHEI_METRICS_ADDR")
        {
            match val.parse::<std::net::SocketAddr>() {
                Ok(addr) => self.metrics_addr = Some(addr),
                Err(e) => self.env_error(
                    "RHEI_METRICS_ADDR",
                    &val,
                    &format!("{e}; expected host:port, e.g. 0.0.0.0:9090"),
                ),
            }
        }
        if self.pipeline_name.is_none()
            && let Some(val) = env("RHEI_PIPELINE_NAME")
        {
            self.pipeline_name = Some(val);
        }
        self
    }

    /// Object storage for durable state, plus the fork-mode settings.
    #[allow(unused_mut)] // `mut` is only needed with the remote-state feature
    fn apply_remote_env(mut self, env: &EnvLookup<'_>) -> Self {
        #[cfg(feature = "remote-state")]
        if self.remote_state.is_none()
            && let Some(bucket) = env("RHEI_REMOTE_BUCKET")
        {
            self.remote_state = Some(RemoteStateConfig {
                bucket,
                prefix: env("RHEI_REMOTE_PREFIX").unwrap_or_default(),
                endpoint: env("RHEI_REMOTE_ENDPOINT"),
                region: env("RHEI_REMOTE_REGION").unwrap_or_else(|| "us-east-1".to_string()),
                allow_http: env("RHEI_REMOTE_ALLOW_HTTP").is_some_and(|v| v == "1" || v == "true"),
            });
        }
        #[cfg(feature = "remote-state")]
        if self.from_checkpoint.is_none()
            && let Some(val) = env("RHEI_FROM_CHECKPOINT")
        {
            self.from_checkpoint = Some(val);
        }
        if self.offset_delta == 0
            && let Some(val) = env("RHEI_OFFSET_DELTA")
        {
            match val.parse::<i64>() {
                Ok(delta) => self.offset_delta = delta,
                Err(e) => self.env_error(
                    "RHEI_OFFSET_DELTA",
                    &val,
                    &format!("{e}; expected a signed integer"),
                ),
            }
        }
        self
    }

    /// Record a malformed environment variable for [`Self::build`] to report.
    ///
    /// Logged immediately as well, because the error surfaces at `build()` and
    /// the log line is what ties it back to the offending variable in a crash
    /// loop.
    fn env_error(&mut self, var: &str, value: &str, reason: &str) {
        tracing::error!(var, value, reason, "invalid environment variable");
        self.env_errors
            .push(format!("{var}={value:?} is invalid: {reason}"));
    }

    /// Build the controller.
    ///
    /// # Errors
    /// Returns an error if any environment variable read by [`Self::from_env`]
    /// was malformed, or if the resulting configuration is invalid — a zero
    /// worker count, a non-positive checkpoint interval, a `max_parallelism`
    /// below the total worker count, or a `peers`/`process_id` mismatch.
    pub fn build(self) -> anyhow::Result<PipelineController> {
        // Surface malformed environment variables before anything else: a
        // typo'd RHEI_WORKERS must not silently leave the pipeline at its
        // default parallelism.
        if !self.env_errors.is_empty() {
            anyhow::bail!(
                "invalid environment configuration:\n  - {}",
                self.env_errors.join("\n  - ")
            );
        }

        anyhow::ensure!(
            self.workers > 0,
            "workers must be at least 1, got {}. A pipeline with no workers can never \
             process anything.",
            self.workers
        );
        anyhow::ensure!(
            self.checkpoint_interval > 0,
            "checkpoint_interval must be at least 1 batch, got 0. A zero interval would \
             checkpoint on every frontier advance and never let the pipeline make progress."
        );

        if let Some(ref peers) = self.peers {
            anyhow::ensure!(
                !peers.is_empty(),
                "peers was set but is empty. Leave it unset for single-process mode, or list \
                 every process in the cluster (including this one)."
            );
            for peer in peers {
                anyhow::ensure!(
                    !peer.trim().is_empty(),
                    "peers contains an empty address. Check for a stray comma in RHEI_PEERS."
                );
                anyhow::ensure!(
                    peer.contains(':'),
                    "peer address {peer:?} has no port. Cluster peers must be `host:port`."
                );
            }
            let pid = self
                .process_id
                .ok_or_else(|| anyhow::anyhow!("process_id is required when peers are set"))?;
            anyhow::ensure!(
                pid < peers.len(),
                "process_id ({pid}) must be less than number of peers ({})",
                peers.len()
            );
        }

        let total_workers = self.workers * self.peers.as_ref().map_or(1, Vec::len);
        anyhow::ensure!(
            self.max_parallelism >= total_workers,
            "max_parallelism ({}) is below the total worker count ({total_workers}). Key groups \
             are distributed across workers, so workers beyond the key group count would sit \
             permanently idle. Raise max_parallelism — but note it is fixed for a pipeline's \
             lifetime and cannot be changed once state exists.",
            self.max_parallelism
        );

        // Size the liveness heartbeat slots to this process's local workers.
        let health = self.health.with_workers(self.workers);

        Ok(PipelineController {
            checkpoint_dir: self.checkpoint_dir,
            tiered: self.tiered,
            workers: self.workers,
            checkpoint_interval: self.checkpoint_interval,
            error_policy: self.error_policy,
            dlq_sink: std::sync::Mutex::new(self.dlq_sink),
            health,
            process_id: self.process_id,
            peers: self.peers,
            memtable_config: self.memtable_config,
            max_parallelism: self.max_parallelism,
            active_topology: std::sync::Mutex::new(None),
            node_id: std::sync::Mutex::new(None),
            local_backends: std::sync::Mutex::new(std::collections::HashMap::new()),
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
            max_parallelism: rhei_core::cluster::DEFAULT_MAX_PARALLELISM,
            metrics_addr: None,
            pipeline_name: None,
            #[cfg(feature = "remote-state")]
            remote_state: None,
            #[cfg(feature = "remote-state")]
            from_checkpoint: None,
            offset_delta: 0,
            env_errors: Vec::new(),
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
            max_parallelism: rhei_core::cluster::DEFAULT_MAX_PARALLELISM,
            active_topology: std::sync::Mutex::new(None),
            node_id: std::sync::Mutex::new(None),
            local_backends: std::sync::Mutex::new(std::collections::HashMap::new()),
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

    /// The discovered topology for the current generation, if running under
    /// dynamic discovery.
    fn active_topology(&self) -> Option<rhei_core::cluster::ClusterTopology> {
        self.active_topology
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// This node's process ID within the active topology.
    fn active_process_id(&self) -> Option<usize> {
        let topology = self.active_topology()?;
        let node_id = self
            .node_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        topology.process_id_of(&node_id)
    }

    /// Install a discovered topology as the one to execute next.
    ///
    /// # Errors
    /// Returns an error if `node_id` is not a participant in `topology` — this
    /// node would otherwise try to join a Timely cluster that has not reserved
    /// a process slot for it.
    pub fn set_active_topology(
        &self,
        topology: rhei_core::cluster::ClusterTopology,
        node_id: &str,
    ) -> anyhow::Result<()> {
        let process_id = topology.process_id_of(node_id).ok_or_else(|| {
            anyhow::anyhow!(
                "node {node_id:?} is not a participant in the resolved topology \
                 (members: {:?})",
                topology
                    .processes
                    .iter()
                    .map(|m| &m.node_id)
                    .collect::<Vec<_>>()
            )
        })?;
        // How much of the key space this node is responsible for. Summed across
        // nodes this reaches max_parallelism; on its own it is the load share,
        // and comparing it between nodes shows skew that the aggregate
        // `rhei_cluster_workers` cannot.
        let owned_key_groups = rhei_core::cluster::KeyGroupAssignment::new(
            topology.max_parallelism,
            topology.total_workers(),
        )
        .map_or(0, |assignment| {
            let first = process_id * topology.workers_per_process;
            (first..first + topology.workers_per_process)
                .map(|w| assignment.range(w).len())
                .sum::<usize>()
        });
        metrics::gauge!("rhei_cluster_key_groups_owned").set(owned_key_groups as f64);

        tracing::info!(
            node_id,
            process_id,
            topology = %topology.summary(),
            fingerprint = topology.fingerprint(),
            owned_key_groups,
            "installing cluster topology"
        );
        *self
            .node_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(node_id.to_string());
        *self
            .active_topology
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(topology);
        Ok(())
    }

    /// Returns `true` when running in multi-process cluster mode.
    pub fn is_cluster(&self) -> bool {
        self.active_topology().is_some_and(|t| t.n_processes() > 1) || self.peers.is_some()
    }

    /// Returns the configured process ID, if in cluster mode.
    pub fn process_id(&self) -> Option<usize> {
        self.active_process_id().or(self.process_id)
    }

    /// Number of worker threads on this process.
    fn effective_workers(&self) -> usize {
        self.active_topology()
            .map_or(self.workers, |t| t.workers_per_process)
    }

    /// Total number of workers across all processes.
    ///
    /// Under dynamic discovery this is the current topology's worker count; it
    /// changes as nodes join and leave. Otherwise it is `workers * peers.len()`
    /// in cluster mode, or `workers` in single-process mode.
    pub fn total_workers(&self) -> usize {
        if let Some(topology) = self.active_topology() {
            return topology.total_workers();
        }
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
        let workers = self.effective_workers();
        if let Some(pid) = self.process_id() {
            let start = pid * workers;
            start..start + workers
        } else {
            0..workers
        }
    }

    /// Construct a Timely config appropriate for the execution mode.
    ///
    /// # Errors
    /// Returns an error if in cluster mode but `process_id` is not set.
    pub(crate) fn timely_config(&self) -> anyhow::Result<timely::execute::Config> {
        // A discovered topology takes precedence over the static peer list:
        // it reflects the cluster as it is right now, not as it was at startup.
        if let Some(topology) = self.active_topology() {
            let pid = self.active_process_id().ok_or_else(|| {
                anyhow::anyhow!("this node is not a participant in the active topology")
            })?;
            // A single-process topology has no peers to dial, so use the
            // shared-memory allocator rather than standing up a TCP cluster
            // against ourselves.
            if topology.n_processes() == 1 {
                return Ok(timely::execute::Config::process(
                    topology.workers_per_process,
                ));
            }
            return Ok(timely::execute::Config {
                communication: timely::CommunicationConfig::Cluster {
                    threads: topology.workers_per_process,
                    process: pid,
                    addresses: topology.addresses(),
                    report: false,
                    zerocopy: false,
                    log_fn: Arc::new(|_| None),
                },
                worker: timely::WorkerConfig::default(),
            });
        }
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

    /// Run a pipeline under dynamic cluster discovery, rescaling as membership
    /// changes.
    ///
    /// `build_graph` is called once per topology generation: Timely fixes its
    /// worker set when the dataflow starts, so changing the worker count means
    /// building and running a fresh dataflow. Each generation resumes from the
    /// checkpoint the previous one wrote.
    ///
    /// The rescale itself is cheap in the way that matters: because state is
    /// addressed by key group rather than by worker, reassigning ownership
    /// moves no bytes. A worker that gains a key group reads it from shared L3
    /// storage on first access.
    ///
    /// Returns when the pipeline completes on its own (sources exhausted), when
    /// `shutdown` fires, or when the membership provider shuts down.
    ///
    /// # Errors
    /// Returns an error if the initial membership does not resolve to a viable
    /// cluster, if this node is not a participant in the resolved topology, or
    /// if a generation fails to execute.
    ///
    /// # Example
    /// ```ignore
    /// let provider = GossipMembership::join(gossip_config).await?;
    /// let policy = RescalePolicy::new(controller.max_parallelism());
    /// controller
    ///     .run_dynamic(|| build_pipeline(), provider, policy, shutdown)
    ///     .await?;
    /// ```
    pub async fn run_dynamic<F>(
        &self,
        mut build_graph: F,
        provider: Arc<dyn rhei_core::cluster::MembershipProvider>,
        policy: crate::cluster::RescalePolicy,
        shutdown: Option<ShutdownHandle>,
    ) -> anyhow::Result<()>
    where
        F: FnMut() -> DataflowGraph + Send,
    {
        use crate::cluster::{RescaleDecision, RescaleSupervisor};

        anyhow::ensure!(
            policy.max_parallelism == self.max_parallelism,
            "rescale policy max_parallelism ({}) must match the controller's ({}); \
             they determine the same key group space",
            policy.max_parallelism,
            self.max_parallelism
        );

        let node_id = provider.node_id().to_string();
        let mut supervisor = RescaleSupervisor::new(provider.clone(), policy);
        let mut topology = supervisor.initial_topology().await?;
        let mut generation: u64 = 0;
        // Set when a rescale decision is taken, consumed when the replacement
        // generation starts. This spans the drain, the checkpoint and the
        // rebuild — that is, the whole window in which the pipeline is not
        // processing, which is what a rescale actually costs.
        let mut rescale_started: Option<std::time::Instant> = None;

        loop {
            self.set_active_topology(topology.clone(), &node_id)?;
            supervisor.adopt(topology.clone());

            let graph = build_graph();
            graph.validate().map_err(|e| anyhow::anyhow!("{e}"))?;

            // Each generation gets its own shutdown handle so a rescale can stop
            // it without disturbing the caller's handle. The caller's shutdown
            // is forwarded into it by a task that lives only as long as this
            // generation.
            let (gen_shutdown, gen_trigger) = ShutdownHandle::new();
            let gen_trigger = Arc::new(gen_trigger);
            let forwarder = shutdown
                .as_ref()
                .map(|outer| forward_shutdown(outer, gen_trigger.clone()));

            record_generation_start(&node_id, generation, &topology, rescale_started.take());

            let run = run_graph(graph, self, Some(gen_shutdown));
            tokio::pin!(run);

            let outcome = loop {
                tokio::select! {
                    // Bias toward the pipeline so a completed run is observed
                    // before a concurrent membership change starts a rescale
                    // of something that already finished.
                    biased;

                    result = &mut run => break GenerationOutcome::Finished(result),

                    decision = supervisor.next_decision() => match decision {
                        RescaleDecision::Rescale { topology: next, moved_key_groups } => {
                            break GenerationOutcome::Rescale { next: *next, moved_key_groups };
                        }
                        // Keep running this generation and await the next decision.
                        RescaleDecision::NoChange => {}
                        RescaleDecision::NoQuorum(reason) => {
                            // Holding the current topology is the right call: a
                            // total membership loss is far more likely a partition
                            // than a real cluster-wide shutdown.
                            tracing::warn!(
                                node_id = %node_id,
                                reason = %reason,
                                "no viable cluster in latest membership; holding current topology"
                            );
                        }
                        RescaleDecision::ProviderClosed => {
                            tracing::info!("membership provider closed; running to completion");
                            break GenerationOutcome::Finished((&mut run).await);
                        }
                    },
                }
            };

            if let Some(handle) = forwarder {
                handle.abort();
            }

            match outcome {
                GenerationOutcome::Finished(result) => {
                    result?;
                    tracing::info!(generation, "pipeline finished");
                    return Ok(());
                }
                GenerationOutcome::Rescale {
                    next,
                    moved_key_groups,
                } => {
                    tracing::info!(
                        node_id = %node_id,
                        generation,
                        from = %topology.summary(),
                        to = %next.summary(),
                        moved_key_groups,
                        "rescaling: draining current generation to a checkpoint"
                    );
                    metrics::counter!("rhei_cluster_rescales_total").increment(1);
                    metrics::counter!("rhei_cluster_key_groups_moved_total")
                        .increment(moved_key_groups as u64);
                    rescale_started = Some(std::time::Instant::now());

                    // Stop the dataflow gracefully. The executor checkpoints
                    // operator state and commits source offsets on the way out,
                    // so the next generation resumes exactly here.
                    gen_trigger.shutdown();
                    run.await.map_err(|e| {
                        anyhow::anyhow!("failed to drain generation {generation} for rescale: {e}")
                    })?;

                    // If the caller asked to stop while we were draining, honour
                    // that rather than starting another generation.
                    if shutdown.as_ref().is_some_and(ShutdownHandle::is_shutdown) {
                        tracing::info!("shutdown requested during rescale; stopping");
                        return Ok(());
                    }

                    topology = next;
                    generation += 1;
                }
            }
        }
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
            access: crate::http_server::AccessConfig::from_env(),
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

    /// The number of key groups configured for this pipeline.
    pub fn max_parallelism(&self) -> usize {
        self.max_parallelism
    }

    /// Node IDs participating in the active topology, for checkpoint metadata.
    ///
    /// Empty when not running under dynamic discovery.
    pub fn cluster_member_ids(&self) -> Vec<String> {
        self.active_topology().map_or_else(Vec::new, |t| {
            t.processes.iter().map(|m| m.node_id.clone()).collect()
        })
    }

    /// The key-group → worker assignment implied by the current topology.
    ///
    /// # Errors
    /// Returns an error if `max_parallelism` is out of range.
    pub fn key_group_assignment(&self) -> anyhow::Result<rhei_core::cluster::KeyGroupAssignment> {
        let assignment = rhei_core::cluster::KeyGroupAssignment::new(
            self.max_parallelism,
            self.total_workers(),
        )?;
        // A non-zero value means the pipeline has stopped scaling: adding nodes
        // no longer adds keyed throughput. Emitted unconditionally so it can be
        // alerted on, not only logged at the moment it first happens.
        metrics::gauge!("rhei_cluster_idle_workers").set(assignment.idle_workers() as f64);
        if assignment.idle_workers() > 0 {
            tracing::warn!(
                max_parallelism = self.max_parallelism,
                total_workers = self.total_workers(),
                idle_workers = assignment.idle_workers(),
                "cluster has more workers than key groups; surplus workers own no keyed state. \
                 Raising max_parallelism requires a state-incompatible restart."
            );
        }
        Ok(assignment)
    }

    /// Create a per-worker `StateContext` for the given operator.
    ///
    /// State is addressed by **key group**, not by worker: the physical key is
    /// `kg{group}/{operator_name}/{user_key}`. The `worker_index` argument is
    /// therefore deliberately *not* part of the namespace — it only selects
    /// which local L1/L2 caches front the shared store.
    ///
    /// This is what makes rescaling possible. The older scheme
    /// (`p{process_id}/w{worker_index}/{operator_name}`) welded durable state
    /// to the cluster layout, so changing the worker count left every key under
    /// a prefix nobody would look up again. Now a worker that inherits a key
    /// group issues exactly the reads its previous owner issued.
    ///
    /// **This requires shared L3 storage** (`remote_state`) to be useful across
    /// processes: with a purely local backend, a key group that moves to another
    /// *machine* has no path to its bytes. Within a process, moving between
    /// worker threads works with any backend.
    pub fn create_context_for_worker(
        &self,
        operator_name: &str,
        worker_index: usize,
    ) -> anyhow::Result<StateContext> {
        self.create_keyed_context(operator_name, worker_index)
    }

    /// The process-wide `LocalBackend` for `operator_name`, creating it on first
    /// use.
    ///
    /// Every worker in this process shares one instance. See
    /// [`Self::local_backends`] for why separate instances over the same file
    /// would silently lose state.
    fn shared_local_backend(&self, operator_name: &str) -> anyhow::Result<Arc<LocalBackend>> {
        let mut cache = self
            .local_backends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = cache.get(operator_name) {
            return Ok(existing.clone());
        }
        let path = self
            .checkpoint_dir
            .join(format!("{operator_name}.checkpoint.json"));
        let backend = Arc::new(LocalBackend::new(path, None)?);
        cache.insert(operator_name.to_string(), backend.clone());
        Ok(backend)
    }

    /// Create a key-group-addressed `StateContext` for the given operator.
    ///
    /// Identical to [`create_context`](Self::create_context) except that the
    /// context maps every access to a physical key: state keyed by a record key
    /// lands under `kg{group}/{operator_name}/{state_key}`, so ownership can
    /// move between workers without the bytes moving.
    ///
    /// No `PrefixedBackend` wraps the store here — [`StateAddressing`] already
    /// carries the operator name, and it has to, because the key group depends
    /// on the record's partition key and only the layers above the backend
    /// still know what that was.
    pub fn create_keyed_context(
        &self,
        operator_name: &str,
        worker_index: usize,
    ) -> anyhow::Result<StateContext> {
        let addressing = StateAddressing::new(operator_name, self.max_parallelism, worker_index)?;

        let ctx = {
            #[cfg(feature = "remote-state")]
            {
                let fork_l3 = self.fork_remote_l3.lock().unwrap_or_else(|e| {
                    tracing::warn!("mutex poisoned in fork_remote_l3 lock, recovering inner data");
                    e.into_inner()
                });
                if let Some(ref remote_l3) = *fork_l3 {
                    let fork = rhei_core::state::fork_backend::ForkBackend::new(
                        Box::new(self.shared_local_backend(operator_name)?),
                        Box::new(remote_l3.clone()),
                    );
                    StateContext::new(Box::new(fork))
                } else if let Some(ref tiered) = self.tiered {
                    let tiered_backend = tiered.shared_l2.create_tiered_backend(tiered.l3.clone());
                    StateContext::new(Box::new(tiered_backend))
                } else {
                    StateContext::new(Box::new(self.shared_local_backend(operator_name)?))
                }
            }
            #[cfg(not(feature = "remote-state"))]
            {
                if let Some(ref tiered) = self.tiered {
                    let tiered_backend = tiered.shared_l2.create_tiered_backend(tiered.l3.clone());
                    StateContext::new(Box::new(tiered_backend))
                } else {
                    StateContext::new(Box::new(self.shared_local_backend(operator_name)?))
                }
            }
        };

        Ok(ctx
            .with_addressing(addressing)
            .with_memtable_config(self.memtable_config.clone()))
    }

    /// Create a `StateContext` for the given operator.
    ///
    /// When tiered storage is configured, produces a context backed by
    /// `PrefixedBackend(TieredBackend)`. Otherwise falls back to `LocalBackend`.
    ///
    /// This uses flat, operator-only namespacing. Keyed operators should use
    /// [`create_keyed_context`](Self::create_keyed_context) so their state
    /// survives a rescale.
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
            let manifest =
                CheckpointManifest::load_from_object_store_checked(object_store.as_ref(), &path)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!("checkpoint manifest not found at {manifest_path}")
                    })?;

            // A fork inherits the parent's key space, so it must inherit its
            // key group count too — otherwise every key lands in a different
            // group and the forked state reads back empty.
            manifest.validate_compatible(controller.max_parallelism)?;

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
            } else if let Some(manifest) =
                CheckpointManifest::load_checked(&controller.checkpoint_dir)?
            {
                // A different key group count means every key hashes into a
                // different group, so the stored state is unreadable. Fail loudly
                // rather than silently resuming from what looks like empty state.
                manifest.validate_compatible(controller.max_parallelism)?;

                tracing::info!(
                    checkpoint_id = manifest.checkpoint_id,
                    timestamp_ms = manifest.timestamp_ms,
                    operators = ?manifest.operators,
                    source_offsets = ?manifest.source_offsets,
                    max_parallelism = ?manifest.max_parallelism,
                    prev_total_workers = ?manifest.total_workers,
                    "resuming from checkpoint #{}", manifest.checkpoint_id
                );

                // Resuming at a different worker count is normal — that is a
                // rescale — but worth recording, since it is when key groups
                // change owner.
                if let Some(prev_workers) = manifest.total_workers
                    && prev_workers != controller.total_workers()
                {
                    tracing::info!(
                        from_workers = prev_workers,
                        to_workers = controller.total_workers(),
                        "resuming at a different parallelism; key groups will be reassigned"
                    );
                }

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
            if let Some(manifest) = CheckpointManifest::load_checked(&controller.checkpoint_dir)? {
                // A different key group count means every key hashes into a
                // different group, so the stored state is unreadable. Fail loudly
                // rather than silently resuming from what looks like empty state.
                manifest.validate_compatible(controller.max_parallelism)?;

                tracing::info!(
                    checkpoint_id = manifest.checkpoint_id,
                    timestamp_ms = manifest.timestamp_ms,
                    operators = ?manifest.operators,
                    source_offsets = ?manifest.source_offsets,
                    max_parallelism = ?manifest.max_parallelism,
                    prev_total_workers = ?manifest.total_workers,
                    "resuming from checkpoint #{}", manifest.checkpoint_id
                );

                // Resuming at a different worker count is normal — that is a
                // rescale — but worth recording, since it is when key groups
                // change owner.
                if let Some(prev_workers) = manifest.total_workers
                    && prev_workers != controller.total_workers()
                {
                    tracing::info!(
                        from_workers = prev_workers,
                        to_workers = controller.total_workers(),
                        "resuming at a different parallelism; key groups will be reassigned"
                    );
                }

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

    // Flip to Draining the moment a shutdown signal lands, so `/readyz` starts
    // failing while the pipeline is still finishing its work. That is what
    // makes a rolling restart orderly: the orchestrator stops routing to this
    // process and removes it from any endpoint list before it goes away,
    // instead of discovering it is gone after the fact.
    let drain_watcher = shutdown.as_ref().map(|handle| {
        let mut rx = handle.subscribe();
        let health = controller.health.clone();
        tokio::spawn(async move {
            // `changed()` errors only if the sender is dropped, which happens
            // at process teardown — nothing left to mark draining.
            while rx.changed().await.is_ok() {
                if *rx.borrow() {
                    tracing::info!("shutdown signalled; reporting not-ready while draining");
                    health.set_status(PipelineStatus::Draining);
                    return;
                }
            }
        })
    });

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

    if let Some(watcher) = drain_watcher {
        watcher.abort();
    }

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
        max_parallelism: Some(controller.max_parallelism()),
        total_workers: Some(controller.total_workers()),
        cluster_members: controller.cluster_member_ids(),
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

    // ── Configuration validation ────────────────────────────────────

    /// A builder reading from a fixed environment.
    ///
    /// `from_env` assigns fields directly, bypassing the setters' asserts —
    /// which is exactly why these values have to be validated at `build()`.
    fn builder_with_env(vars: &[(&str, &str)]) -> super::PipelineControllerBuilder {
        let owned: Vec<(String, String)> = vars
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .apply_env_with(&|key| owned.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()))
    }

    #[test]
    fn malformed_worker_count_fails_the_build() {
        // Previously this silently ran at default parallelism.
        let err = builder_with_env(&[("RHEI_WORKERS", "four")])
            .build()
            .expect_err("a typo'd worker count must not be ignored");
        let msg = err.to_string();
        assert!(msg.contains("RHEI_WORKERS"), "got: {msg}");
        assert!(msg.contains("four"), "got: {msg}");
    }

    #[test]
    fn malformed_metrics_addr_fails_the_build() {
        // Silently dropping this left the process with no metrics endpoint and
        // no indication that monitoring was dark.
        let err = builder_with_env(&[("RHEI_METRICS_ADDR", "9090")])
            .build()
            .expect_err("a metrics address without a host must not be ignored");
        assert!(err.to_string().contains("RHEI_METRICS_ADDR"));
    }

    #[test]
    fn every_malformed_variable_is_reported_at_once() {
        // One restart should reveal all the problems, not the first one.
        let err = builder_with_env(&[
            ("RHEI_WORKERS", "many"),
            ("RHEI_CHECKPOINT_INTERVAL", "often"),
            ("RHEI_MAX_PARALLELISM", "lots"),
        ])
        .build()
        .expect_err("malformed configuration must fail");
        let msg = err.to_string();
        assert!(msg.contains("RHEI_WORKERS"), "got: {msg}");
        assert!(msg.contains("RHEI_CHECKPOINT_INTERVAL"), "got: {msg}");
        assert!(msg.contains("RHEI_MAX_PARALLELISM"), "got: {msg}");
    }

    #[test]
    fn zero_workers_from_env_is_rejected() {
        let err = builder_with_env(&[("RHEI_WORKERS", "0")])
            .build()
            .expect_err("a pipeline with no workers can never process anything");
        assert!(err.to_string().contains("workers must be at least 1"));
    }

    #[test]
    fn zero_checkpoint_interval_from_env_is_rejected() {
        let err = builder_with_env(&[("RHEI_CHECKPOINT_INTERVAL", "0")])
            .build()
            .expect_err("a zero checkpoint interval never lets the pipeline progress");
        assert!(err.to_string().contains("checkpoint_interval"));
    }

    #[test]
    fn empty_values_are_treated_as_unset() {
        // Orchestrators inject VAR="" for absent optional settings; that must
        // not read as a configuration error.
        let controller = builder_with_env(&[
            ("RHEI_WORKERS", ""),
            ("RHEI_PEERS", "  "),
            ("RHEI_METRICS_ADDR", ""),
        ])
        .build()
        .expect("empty values are simply unset");
        assert_eq!(controller.total_workers(), 1);
        assert!(controller.peers.is_none());
    }

    #[test]
    fn peer_list_ignores_stray_commas() {
        let controller = builder_with_env(&[
            ("RHEI_PEERS", "h0:2101, ,h1:2101,"),
            ("RHEI_PROCESS_ID", "1"),
        ])
        .build()
        .expect("a trailing comma is not a nameless peer");
        assert_eq!(
            controller.peers.as_deref(),
            Some(["h0:2101".to_string(), "h1:2101".to_string()].as_slice())
        );
    }

    #[test]
    fn env_fills_checkpoint_settings_that_previously_only_toml_could() {
        let controller = builder_with_env(&[
            ("RHEI_CHECKPOINT_DIR", "/data/ckpt"),
            ("RHEI_CHECKPOINT_INTERVAL", "250"),
            ("RHEI_MAX_PARALLELISM", "256"),
        ])
        .build()
        .expect("valid configuration");
        assert_eq!(
            controller.checkpoint_dir,
            std::path::PathBuf::from("/data/ckpt")
        );
        assert_eq!(controller.checkpoint_interval, 250);
        assert_eq!(controller.max_parallelism, 256);
    }

    #[test]
    fn liveness_timeout_is_configurable_and_must_be_positive() {
        let controller = builder_with_env(&[("RHEI_LIVENESS_TIMEOUT_SECS", "180")])
            .build()
            .expect("valid configuration");
        assert_eq!(
            controller.health().liveness_timeout(),
            std::time::Duration::from_secs(180)
        );

        let err = builder_with_env(&[("RHEI_LIVENESS_TIMEOUT_SECS", "0")])
            .build()
            .expect_err("a zero timeout would fail liveness immediately");
        assert!(err.to_string().contains("RHEI_LIVENESS_TIMEOUT_SECS"));
    }

    #[test]
    fn max_parallelism_below_worker_count_is_rejected() {
        // Workers beyond the key group count own nothing and sit idle.
        let err = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(64)
            .max_parallelism(16)
            .build()
            .expect_err("more workers than key groups is a misconfiguration");
        assert!(err.to_string().contains("max_parallelism"));
    }

    #[test]
    fn empty_peer_address_is_rejected() {
        let err = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .process_id(0)
            .peers(vec!["host0:2101".into(), String::new()])
            .build()
            .expect_err("an empty peer address is never valid");
        assert!(err.to_string().contains("empty address"));
    }

    #[test]
    fn peer_address_without_a_port_is_rejected() {
        let err = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .process_id(0)
            .peers(vec!["host0".into(), "host1:2101".into()])
            .build()
            .expect_err("peers must carry a port");
        assert!(err.to_string().contains("no port"));
    }

    #[test]
    fn a_valid_configuration_still_builds() {
        let controller = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(4)
            .max_parallelism(128)
            .process_id(1)
            .peers(vec!["h0:2101".into(), "h1:2101".into()])
            .build()
            .expect("this configuration is valid");
        assert_eq!(controller.total_workers(), 8);
    }

    #[test]
    fn build_sizes_heartbeat_slots_to_the_worker_count() {
        // Liveness must watch every local worker, not just the first.
        let controller = super::PipelineController::builder()
            .checkpoint_dir("/tmp/test")
            .workers(6)
            .build()
            .expect("valid configuration");
        assert_eq!(controller.health().heartbeats().len(), 6);
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

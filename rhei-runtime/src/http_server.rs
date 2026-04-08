//! HTTP server for health checks, Prometheus metrics, and JSON API.
//!
//! Provides:
//! - `GET /healthz` — liveness probe (always 200 while the process is alive)
//! - `GET /readyz` — readiness probe (200 when `Running`, 503 otherwise)
//! - `GET /metrics` — Prometheus exposition format via `metrics-exporter-prometheus`
//! - `GET /api/metrics` — structured JSON [`MetricsSnapshot`]
//! - `GET /api/logs?after=<seq>` — buffered log entries as JSON
//! - `GET /api/health` — JSON health status with uptime
//! - `GET /api/topology` — serializable pipeline DAG (nodes + edges)
//! - `GET /api/metrics/history?since=<ms>` — ring buffer of timestamped snapshots
//! - `GET /api/info` — pipeline identity: name, version, workers, uptime

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use rhei_core::checkpoint::{CheckpointManifest, load_operator_state};

use crate::compiler::ApiTopology;
use crate::health::{HealthState, PipelineStatus};
use crate::metrics_snapshot::{MetricsHandle, MetricsSnapshot};
use crate::tracing_capture::LogEntry;

// ─── Log buffer ─────────────────────────────────────────────────────────────

const LOG_BUFFER_CAPACITY: usize = 1000;

/// A serializable log entry for the JSON API.
///
/// Separate from [`LogEntry`] which uses non-serializable `tracing::Level`
/// and `SystemTime`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiLogEntry {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// Epoch milliseconds when the event was recorded.
    pub timestamp_ms: u64,
    /// Severity level string: `"ERROR"`, `"WARN"`, `"INFO"`, etc.
    pub level: String,
    /// The module/target that emitted the event.
    pub target: String,
    /// The formatted message.
    pub message: String,
    /// Worker index, if the event was emitted within a worker span.
    pub worker: Option<usize>,
}

/// Ring buffer of [`ApiLogEntry`] values with a monotonic sequence counter.
#[derive(Debug)]
struct LogBuffer {
    entries: VecDeque<ApiLogEntry>,
    next_seq: u64,
}

impl LogBuffer {
    fn new() -> Self {
        Self {
            entries: VecDeque::with_capacity(LOG_BUFFER_CAPACITY),
            next_seq: 1,
        }
    }

    fn push(&mut self, entry: &LogEntry) {
        #[allow(clippy::cast_possible_truncation)]
        let api_entry = ApiLogEntry {
            seq: self.next_seq,
            timestamp_ms: entry
                .timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            level: entry.level.to_string().to_uppercase(),
            target: entry.target.clone(),
            message: entry.message.clone(),
            worker: entry.worker,
        };
        self.next_seq += 1;
        if self.entries.len() >= LOG_BUFFER_CAPACITY {
            self.entries.pop_front();
        }
        self.entries.push_back(api_entry);
    }

    fn entries_after(&self, after: u64) -> Vec<ApiLogEntry> {
        self.entries
            .iter()
            .filter(|e| e.seq > after)
            .cloned()
            .collect()
    }
}

// ─── Metrics history ring buffer ────────────────────────────────────────────

const METRICS_HISTORY_CAPACITY: usize = 720;

/// Ring buffer of timestamped [`MetricsSnapshot`] values for `/api/metrics/history`.
#[derive(Debug)]
struct MetricsHistory {
    entries: VecDeque<(u64, MetricsSnapshot)>,
}

impl MetricsHistory {
    fn new() -> Self {
        Self {
            entries: VecDeque::with_capacity(METRICS_HISTORY_CAPACITY),
        }
    }

    fn push(&mut self, timestamp_ms: u64, snapshot: MetricsSnapshot) {
        if self.entries.len() >= METRICS_HISTORY_CAPACITY {
            self.entries.pop_front();
        }
        self.entries.push_back((timestamp_ms, snapshot));
    }

    fn entries_since(&self, since_ms: u64) -> Vec<(u64, MetricsSnapshot)> {
        self.entries
            .iter()
            .filter(|(ts, _)| *ts >= since_ms)
            .cloned()
            .collect()
    }
}

// ─── Server configuration ───────────────────────────────────────────────────

/// Configuration for the HTTP server.
#[allow(missing_debug_implementations)]
pub struct HttpServerConfig {
    /// Address to bind the server on.
    pub addr: SocketAddr,
    /// Pipeline health state.
    pub health: HealthState,
    /// Prometheus exporter handle for `/metrics`.
    pub prometheus: PrometheusHandle,
    /// Snapshot metrics handle for `/api/metrics`. If `None`, the endpoint
    /// returns 404.
    pub metrics_handle: Option<MetricsHandle>,
    /// Log entry receiver for the `/api/logs` ring buffer. If `None`, the
    /// endpoint returns an empty array.
    pub log_rx: Option<tokio::sync::mpsc::Receiver<LogEntry>>,
    /// Shared topology slot, populated after graph compilation.
    pub topology: Arc<std::sync::Mutex<Option<ApiTopology>>>,
    /// Pipeline name for `/api/info`.
    pub pipeline_name: Option<String>,
    /// Number of workers for `/api/info`.
    pub workers: usize,
    /// Checkpoint directory for state explorer endpoints.
    pub checkpoint_dir: Option<std::path::PathBuf>,
}

// ─── App state ──────────────────────────────────────────────────────────────

/// Shared state for HTTP handlers.
#[derive(Debug, Clone)]
struct AppState {
    health: HealthState,
    prometheus: PrometheusHandle,
    metrics_handle: Option<MetricsHandle>,
    log_buffer: Arc<Mutex<LogBuffer>>,
    topology: Arc<std::sync::Mutex<Option<ApiTopology>>>,
    metrics_history: Arc<Mutex<MetricsHistory>>,
    pipeline_name: Option<String>,
    workers: usize,
    start_time: std::time::Instant,
    checkpoint_dir: Option<std::path::PathBuf>,
}

/// JSON response body for health endpoints.
#[derive(Serialize, Deserialize)]
struct HealthResponse {
    status: String,
}

/// JSON response body for the `/api/health` endpoint.
#[derive(Serialize, Deserialize)]
struct ApiHealthResponse {
    status: String,
    uptime_secs: f64,
}

/// JSON response body for the `/api/info` endpoint.
#[derive(Serialize, Deserialize)]
struct ApiInfoResponse {
    name: String,
    version: String,
    workers: usize,
    uptime_secs: f64,
}

/// Query parameters for `/api/logs`.
#[derive(Deserialize)]
struct LogQuery {
    after: Option<u64>,
}

/// Query parameters for `/api/metrics/history`.
#[derive(Deserialize)]
struct HistoryQuery {
    since: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct StateOperatorsResponse {
    checkpoint_id: u64,
    timestamp_ms: u64,
    operators: Vec<OperatorStateInfo>,
}

#[derive(Serialize, Deserialize)]
struct OperatorStateInfo {
    name: String,
    entry_count: usize,
}

#[derive(Deserialize)]
struct StateEntriesQuery {
    prefix: Option<String>,
    pattern: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    decode: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct StateEntriesResponse {
    operator: String,
    total: usize,
    entries: Vec<StateEntryItem>,
}

#[derive(Serialize, Deserialize)]
struct StateEntryItem {
    key: String,
    value: serde_json::Value,
    decode: String,
    size_bytes: usize,
}

#[derive(Deserialize)]
struct StateKeyQuery {
    #[allow(dead_code)]
    decode: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct StateKeyResponse {
    key: String,
    size_bytes: usize,
    decodings: std::collections::HashMap<String, serde_json::Value>,
}

// ─── Server start ───────────────────────────────────────────────────────────

/// Start the HTTP server in the background, returning its `JoinHandle`.
///
/// The server runs until the `JoinHandle` is aborted or the process exits.
pub fn start(config: HttpServerConfig) -> tokio::task::JoinHandle<()> {
    let log_buffer = Arc::new(Mutex::new(LogBuffer::new()));
    let metrics_history = Arc::new(Mutex::new(MetricsHistory::new()));

    // Spawn background task to drain log receiver into ring buffer
    if let Some(mut log_rx) = config.log_rx {
        let buf = log_buffer.clone();
        tokio::spawn(async move {
            while let Some(entry) = log_rx.recv().await {
                buf.lock().await.push(&entry);
            }
        });
    }

    // Spawn background task to record metrics history every 500ms
    if let Some(ref handle) = config.metrics_handle {
        let handle = handle.clone();
        let history = metrics_history.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                ticker.tick().await;
                let snapshot = handle.snapshot();
                #[allow(clippy::cast_possible_truncation)]
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                history.lock().await.push(now_ms, snapshot);
            }
        });
    }

    let state = Arc::new(AppState {
        health: config.health,
        prometheus: config.prometheus,
        metrics_handle: config.metrics_handle,
        log_buffer,
        topology: config.topology,
        metrics_history,
        pipeline_name: config.pipeline_name,
        workers: config.workers,
        start_time: std::time::Instant::now(),
        checkpoint_dir: config.checkpoint_dir,
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/api/metrics", get(api_metrics))
        .route("/api/metrics/history", get(api_metrics_history))
        .route("/api/logs", get(api_logs))
        .route("/api/health", get(api_health))
        .route("/api/topology", get(api_topology))
        .route("/api/info", get(api_info))
        .route("/api/state/operators", get(api_state_operators))
        .route("/api/state/operators/{name}", get(api_state_entries))
        .route(
            "/api/state/operators/{name}/keys/{*key}",
            get(api_state_key),
        )
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = config.addr;
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, %addr, "failed to bind HTTP server");
                return;
            }
        };
        tracing::info!(%addr, "HTTP server started");
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "HTTP server error");
        }
    })
}

// ─── Existing handlers ──────────────────────────────────────────────────────

/// Liveness probe — always 200 while the process is alive.
async fn healthz(State(state): State<Arc<AppState>>) -> axum::Json<HealthResponse> {
    axum::Json(HealthResponse {
        status: state.health.status().to_string(),
    })
}

/// Readiness probe — 200 when running, 503 otherwise.
async fn readyz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let status = state.health.status();
    let code = if status == PipelineStatus::Running {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        axum::Json(HealthResponse {
            status: status.to_string(),
        }),
    )
}

/// Prometheus exposition format rendered by `metrics-exporter-prometheus`.
async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let body = state.prometheus.render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

// ─── API handlers ───────────────────────────────────────────────────────────

/// `GET /api/metrics` — returns the current [`MetricsSnapshot`] as JSON.
async fn api_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match &state.metrics_handle {
        Some(handle) => {
            let snapshot = handle.snapshot();
            (StatusCode::OK, axum::Json(Some(snapshot)))
        }
        None => (StatusCode::NOT_FOUND, axum::Json(None)),
    }
}

/// `GET /api/metrics/history?since=<ms>` — returns timestamped metrics snapshots.
async fn api_metrics_history(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HistoryQuery>,
) -> axum::Json<Vec<(u64, MetricsSnapshot)>> {
    let since = query.since.unwrap_or(0);
    let history = state.metrics_history.lock().await;
    axum::Json(history.entries_since(since))
}

/// `GET /api/logs?after=<seq>` — returns buffered log entries with `seq > after`.
async fn api_logs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LogQuery>,
) -> axum::Json<Vec<ApiLogEntry>> {
    let after = query.after.unwrap_or(0);
    let buf = state.log_buffer.lock().await;
    axum::Json(buf.entries_after(after))
}

/// `GET /api/health` — JSON health status with uptime.
async fn api_health(State(state): State<Arc<AppState>>) -> axum::Json<ApiHealthResponse> {
    let uptime_secs = state
        .metrics_handle
        .as_ref()
        .map_or(0.0, |h| h.snapshot().uptime.as_secs_f64());
    axum::Json(ApiHealthResponse {
        status: state.health.status().to_string(),
        uptime_secs,
    })
}

/// `GET /api/topology` — returns the pipeline DAG as JSON.
async fn api_topology(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let topo = state
        .topology
        .lock()
        .unwrap_or_else(|e| {
            tracing::warn!("mutex poisoned in topology lock (api_topology), recovering inner data");
            e.into_inner()
        })
        .clone();
    match topo {
        Some(topology) => (StatusCode::OK, axum::Json(Some(topology))),
        None => (StatusCode::NOT_FOUND, axum::Json(None)),
    }
}

/// `GET /api/info` — returns pipeline identity information.
async fn api_info(State(state): State<Arc<AppState>>) -> axum::Json<ApiInfoResponse> {
    let uptime_secs = state.start_time.elapsed().as_secs_f64();
    axum::Json(ApiInfoResponse {
        name: state
            .pipeline_name
            .clone()
            .unwrap_or_else(|| "unnamed".to_string()),
        version: env!("CARGO_PKG_VERSION").to_string(),
        workers: state.workers,
        uptime_secs,
    })
}

// ─── State explorer helpers ─────────────────────────────────────────────────

fn decode_value(raw: &[u8], format: &str) -> (serde_json::Value, String) {
    match format {
        "json" => {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw) {
                (v, "json".to_string())
            } else {
                (
                    serde_json::Value::String(hex::encode(raw)),
                    "hex".to_string(),
                )
            }
        }
        "utf8" => match std::str::from_utf8(raw) {
            Ok(s) => (serde_json::Value::String(s.to_string()), "utf8".to_string()),
            Err(_) => (
                serde_json::Value::String(hex::encode(raw)),
                "hex".to_string(),
            ),
        },
        "hex" => (
            serde_json::Value::String(hex::encode(raw)),
            "hex".to_string(),
        ),
        _ => {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw) {
                (v, "json".to_string())
            } else if let Ok(s) = std::str::from_utf8(raw) {
                (serde_json::Value::String(s.to_string()), "utf8".to_string())
            } else {
                (
                    serde_json::Value::String(hex::encode(raw)),
                    "hex".to_string(),
                )
            }
        }
    }
}

// ─── State explorer handlers ────────────────────────────────────────────────

/// `GET /api/state/operators` — list operators from last checkpoint.
async fn api_state_operators(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let Some(ref dir) = state.checkpoint_dir else {
        return (StatusCode::NOT_FOUND, axum::Json(None));
    };

    let Some(manifest) = CheckpointManifest::load(dir) else {
        return (StatusCode::NOT_FOUND, axum::Json(None));
    };

    let operators: Vec<OperatorStateInfo> = manifest
        .operators
        .iter()
        .map(|name| {
            let entry_count = load_operator_state(dir, name)
                .map(|entries| entries.len())
                .unwrap_or(0);
            OperatorStateInfo {
                name: name.clone(),
                entry_count,
            }
        })
        .collect();

    (
        StatusCode::OK,
        axum::Json(Some(StateOperatorsResponse {
            checkpoint_id: manifest.checkpoint_id,
            timestamp_ms: manifest.timestamp_ms,
            operators,
        })),
    )
}

/// `GET /api/state/operators/:name` — paginated key-value entries.
async fn api_state_entries(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(operator_name): axum::extract::Path<String>,
    Query(query): Query<StateEntriesQuery>,
) -> impl IntoResponse {
    let Some(ref dir) = state.checkpoint_dir else {
        return (StatusCode::NOT_FOUND, axum::Json(None));
    };

    let Ok(entries) = load_operator_state(dir, &operator_name) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(None));
    };

    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|(key, _)| {
            if let Some(ref prefix) = query.prefix {
                key.starts_with(prefix)
            } else {
                true
            }
        })
        .filter(|(key, _)| {
            if let Some(ref pattern) = query.pattern {
                regex::Regex::new(pattern)
                    .map(|re| re.is_match(key))
                    .unwrap_or(false)
            } else {
                true
            }
        })
        .collect();

    let total = filtered.len();
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(50).min(500);
    let decode_format = query.decode.as_deref().unwrap_or("json");

    let page: Vec<StateEntryItem> = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(key, value_bytes)| {
            let size_bytes = value_bytes.len();
            let (value, decode) = decode_value(&value_bytes, decode_format);
            StateEntryItem {
                key,
                value,
                decode,
                size_bytes,
            }
        })
        .collect();

    (
        StatusCode::OK,
        axum::Json(Some(StateEntriesResponse {
            operator: operator_name,
            total,
            entries: page,
        })),
    )
}

/// `GET /api/state/operators/:name/keys/:key` — single key with all decodings.
async fn api_state_key(
    State(state): State<Arc<AppState>>,
    axum::extract::Path((operator_name, key)): axum::extract::Path<(String, String)>,
    Query(_query): Query<StateKeyQuery>,
) -> impl IntoResponse {
    let Some(ref dir) = state.checkpoint_dir else {
        return (StatusCode::NOT_FOUND, axum::Json(None));
    };

    let Ok(entries) = load_operator_state(dir, &operator_name) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(None));
    };

    let decoded_key = urlencoding::decode(&key).unwrap_or(std::borrow::Cow::Borrowed(&key));
    let found = entries.into_iter().find(|(k, _)| k == decoded_key.as_ref());

    let Some((found_key, value_bytes)) = found else {
        return (StatusCode::NOT_FOUND, axum::Json(None));
    };

    let size_bytes = value_bytes.len();
    let mut decodings = std::collections::HashMap::new();

    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&value_bytes) {
        decodings.insert("json".to_string(), v);
    }
    if let Ok(s) = std::str::from_utf8(&value_bytes) {
        decodings.insert("utf8".to_string(), serde_json::Value::String(s.to_string()));
    }
    decodings.insert(
        "hex".to_string(),
        serde_json::Value::String(hex::encode(&value_bytes)),
    );

    (
        StatusCode::OK,
        axum::Json(Some(StateKeyResponse {
            key: found_key,
            size_bytes,
            decodings,
        })),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    fn test_prometheus_handle() -> PrometheusHandle {
        // Build a recorder without spawning an HTTP listener or installing globally.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        recorder.handle()
    }

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState {
            health: HealthState::new(),
            prometheus: test_prometheus_handle(),
            metrics_handle: None,
            log_buffer: Arc::new(Mutex::new(LogBuffer::new())),
            topology: Arc::new(std::sync::Mutex::new(None)),
            metrics_history: Arc::new(Mutex::new(MetricsHistory::new())),
            pipeline_name: None,
            workers: 1,
            start_time: std::time::Instant::now(),
            checkpoint_dir: None,
        })
    }

    #[tokio::test]
    async fn healthz_returns_200_json() {
        let state = test_state();
        let response = healthz(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let parsed: HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.status, "starting");
    }

    #[tokio::test]
    async fn readyz_returns_503_when_starting() {
        let state = test_state();
        let response = readyz(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let parsed: HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.status, "starting");
    }

    #[tokio::test]
    async fn readyz_returns_200_when_running() {
        let health = HealthState::new();
        health.set_status(PipelineStatus::Running);
        let state = Arc::new(AppState {
            health,
            prometheus: test_prometheus_handle(),
            metrics_handle: None,
            log_buffer: Arc::new(Mutex::new(LogBuffer::new())),
            topology: Arc::new(std::sync::Mutex::new(None)),
            metrics_history: Arc::new(Mutex::new(MetricsHistory::new())),
            pipeline_name: None,
            workers: 1,
            start_time: std::time::Instant::now(),
            checkpoint_dir: None,
        });
        let response = readyz(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let parsed: HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.status, "running");
    }

    #[tokio::test]
    async fn log_buffer_evicts_oldest() {
        let mut buf = LogBuffer::new();
        for i in 0..1005 {
            let entry = LogEntry {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: tracing::Level::INFO,
                target: "test".to_string(),
                message: format!("msg {i}"),
                worker: None,
            };
            buf.push(&entry);
        }
        assert_eq!(buf.entries.len(), LOG_BUFFER_CAPACITY);
        // First entry should have seq = 6 (entries 1..5 were evicted)
        assert_eq!(buf.entries.front().unwrap().seq, 6);
    }

    #[tokio::test]
    async fn api_logs_filters_by_seq() {
        let mut buf = LogBuffer::new();
        for _ in 0..5 {
            let entry = LogEntry {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: tracing::Level::INFO,
                target: "test".to_string(),
                message: "msg".to_string(),
                worker: None,
            };
            buf.push(&entry);
        }
        let after_3 = buf.entries_after(3);
        assert_eq!(after_3.len(), 2);
        assert_eq!(after_3[0].seq, 4);
        assert_eq!(after_3[1].seq, 5);
    }

    #[tokio::test]
    async fn topology_returns_404_when_not_set() {
        let state = test_state();
        let response = api_topology(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn topology_returns_200_when_set() {
        let state = test_state();
        {
            let mut topo = state.topology.lock().unwrap();
            *topo = Some(crate::compiler::ApiTopology {
                nodes: vec![crate::compiler::ApiTopologyNode {
                    id: 0,
                    kind: "source".to_string(),
                    name: "Source_0".to_string(),
                }],
                edges: vec![],
            });
        }
        let response = api_topology(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let parsed: crate::compiler::ApiTopology = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.nodes.len(), 1);
        assert_eq!(parsed.nodes[0].kind, "source");
    }

    #[tokio::test]
    async fn info_returns_pipeline_info() {
        let state = Arc::new(AppState {
            health: HealthState::new(),
            prometheus: test_prometheus_handle(),
            metrics_handle: None,
            log_buffer: Arc::new(Mutex::new(LogBuffer::new())),
            topology: Arc::new(std::sync::Mutex::new(None)),
            metrics_history: Arc::new(Mutex::new(MetricsHistory::new())),
            pipeline_name: Some("test-pipeline".to_string()),
            workers: 4,
            start_time: std::time::Instant::now(),
            checkpoint_dir: None,
        });
        let response = api_info(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let parsed: ApiInfoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.name, "test-pipeline");
        assert_eq!(parsed.workers, 4);
    }

    #[tokio::test]
    async fn metrics_history_returns_entries_since() {
        let mut history = MetricsHistory::new();
        history.push(1000, MetricsSnapshot::default());
        history.push(2000, MetricsSnapshot::default());
        history.push(3000, MetricsSnapshot::default());

        let since_2000 = history.entries_since(2000);
        assert_eq!(since_2000.len(), 2);
        assert_eq!(since_2000[0].0, 2000);
        assert_eq!(since_2000[1].0, 3000);
    }

    #[tokio::test]
    async fn metrics_history_evicts_oldest() {
        let mut history = MetricsHistory::new();
        for i in 0..730 {
            history.push(i, MetricsSnapshot::default());
        }
        assert_eq!(history.entries.len(), METRICS_HISTORY_CAPACITY);
        assert_eq!(history.entries.front().unwrap().0, 10);
    }
}

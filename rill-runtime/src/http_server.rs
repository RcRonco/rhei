//! HTTP server for health checks, Prometheus metrics, and JSON API.
//!
//! Provides:
//! - `GET /healthz` — liveness probe (always 200 while the process is alive)
//! - `GET /readyz` — readiness probe (200 when `Running`, 503 otherwise)
//! - `GET /metrics` — Prometheus exposition format via `metrics-exporter-prometheus`
//! - `GET /api/metrics` — structured JSON [`MetricsSnapshot`]
//! - `GET /api/logs?after=<seq>` — buffered log entries as JSON
//! - `GET /api/health` — JSON health status with uptime

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

use crate::health::{HealthState, PipelineStatus};
use crate::metrics_snapshot::MetricsHandle;
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
}

// ─── App state ──────────────────────────────────────────────────────────────

/// Shared state for HTTP handlers.
#[derive(Debug, Clone)]
struct AppState {
    health: HealthState,
    prometheus: PrometheusHandle,
    metrics_handle: Option<MetricsHandle>,
    log_buffer: Arc<Mutex<LogBuffer>>,
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

/// Query parameters for `/api/logs`.
#[derive(Deserialize)]
struct LogQuery {
    after: Option<u64>,
}

// ─── Server start ───────────────────────────────────────────────────────────

/// Start the HTTP server in the background, returning its `JoinHandle`.
///
/// The server runs until the `JoinHandle` is aborted or the process exits.
pub fn start(config: HttpServerConfig) -> tokio::task::JoinHandle<()> {
    let log_buffer = Arc::new(Mutex::new(LogBuffer::new()));

    // Spawn background task to drain log receiver into ring buffer
    if let Some(mut log_rx) = config.log_rx {
        let buf = log_buffer.clone();
        tokio::spawn(async move {
            while let Some(entry) = log_rx.recv().await {
                buf.lock().await.push(&entry);
            }
        });
    }

    let state = Arc::new(AppState {
        health: config.health,
        prometheus: config.prometheus,
        metrics_handle: config.metrics_handle,
        log_buffer,
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/api/metrics", get(api_metrics))
        .route("/api/logs", get(api_logs))
        .route("/api/health", get(api_health))
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

// ─── New API handlers ───────────────────────────────────────────────────────

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

#[cfg(test)]
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
}

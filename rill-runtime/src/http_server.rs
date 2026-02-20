//! HTTP server for health checks and Prometheus metrics.
//!
//! Provides:
//! - `GET /healthz` — liveness probe (always 200 while the process is alive)
//! - `GET /readyz` — readiness probe (200 when `Running`, 503 otherwise)
//! - `GET /metrics` — Prometheus exposition format via `metrics-exporter-prometheus`

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};

use crate::health::{HealthState, PipelineStatus};

/// Shared state for HTTP handlers.
#[derive(Debug, Clone)]
struct AppState {
    health: HealthState,
    prometheus: PrometheusHandle,
}

/// JSON response body for health endpoints.
#[derive(Serialize, Deserialize)]
struct HealthResponse {
    status: String,
}

/// Start the HTTP server in the background, returning its `JoinHandle`.
///
/// The server runs until the `JoinHandle` is aborted or the process exits.
pub fn start(
    addr: SocketAddr,
    health: HealthState,
    prometheus: PrometheusHandle,
) -> tokio::task::JoinHandle<()> {
    let state = Arc::new(AppState { health, prometheus });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state);

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    fn test_prometheus_handle() -> PrometheusHandle {
        // Build a recorder without spawning an HTTP listener or installing globally.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        recorder.handle()
    }

    #[tokio::test]
    async fn healthz_returns_200_json() {
        let state = Arc::new(AppState {
            health: HealthState::new(),
            prometheus: test_prometheus_handle(),
        });
        let response = healthz(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let parsed: HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.status, "starting");
    }

    #[tokio::test]
    async fn readyz_returns_503_when_starting() {
        let state = Arc::new(AppState {
            health: HealthState::new(),
            prometheus: test_prometheus_handle(),
        });
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
        });
        let response = readyz(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let parsed: HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.status, "running");
    }
}

use std::net::SocketAddr;
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusHandle;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::fanout_recorder::FanoutRecorder;
use crate::metrics_snapshot::{
    MetricsHandle, MetricsSnapshot, SnapshotRecorder, start_snapshot_publisher,
};
use crate::tracing_capture::{CapturingLayer, LogEntry};

/// Configuration for the observability stack.
#[derive(Debug)]
pub struct TelemetryConfig {
    /// If set, start an HTTP server on this address with `/healthz`, `/readyz`,
    /// and `/metrics` endpoints.
    pub metrics_addr: Option<SocketAddr>,
    /// `tracing` env-filter string (e.g. `"info"`, `"rhei_core=debug"`).
    pub log_filter: String,
    /// Emit logs as JSON instead of human-readable format.
    pub json_logs: bool,
    /// Enable TUI dashboard mode (installs snapshot recorder + capturing layer).
    pub tui: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            metrics_addr: None,
            log_filter: "info".to_string(),
            json_logs: false,
            tui: false,
        }
    }
}

/// Handles returned from telemetry initialization for TUI consumers.
#[derive(Debug)]
pub struct TelemetryHandles {
    /// Receiver for periodic metrics snapshots (only when `tui == true`).
    pub metrics_rx: Option<tokio::sync::watch::Receiver<MetricsSnapshot>>,
    /// Receiver for captured log entries (only when `tui == true` or `metrics_addr` is set).
    pub log_rx: Option<tokio::sync::mpsc::Receiver<LogEntry>>,
    /// Prometheus exporter handle for the HTTP `/metrics` endpoint.
    pub prometheus_handle: Option<PrometheusHandle>,
    /// Snapshot metrics handle for the `/api/metrics` JSON endpoint.
    pub metrics_handle: Option<MetricsHandle>,
}

/// Initialize the tracing subscriber and (optionally) the Prometheus metrics exporter.
///
/// When `config.tui` is `true`, installs a [`SnapshotRecorder`] and
/// [`CapturingLayer`], returning handles for the TUI to consume. Otherwise
/// behaves like the previous `init()` — Prometheus-only recorder and fmt
/// subscriber.
///
/// When `metrics_addr` is set in non-TUI mode, installs a [`FanoutRecorder`]
/// that delegates to both Prometheus and Snapshot recorders, and also installs
/// a [`CapturingLayer`] alongside the fmt layer for log capture via HTTP API.
///
/// This should be called once at process startup.
pub fn init(config: TelemetryConfig) -> anyhow::Result<TelemetryHandles> {
    let env_filter =
        EnvFilter::try_new(&config.log_filter).unwrap_or_else(|_| EnvFilter::new("info"));

    if config.tui {
        // --- TUI mode: snapshot recorder + capturing layer ---
        let (recorder, metrics_handle) = SnapshotRecorder::new();

        // Install as the global metrics recorder
        metrics::set_global_recorder(recorder)
            .map_err(|e| anyhow::anyhow!("failed to set metrics recorder: {e}"))?;

        let metrics_rx = start_snapshot_publisher(metrics_handle, Duration::from_millis(500));

        let (capturing_layer, log_rx) = CapturingLayer::new(1000);

        // Build subscriber: env filter + capturing layer (no fmt output in TUI mode)
        tracing_subscriber::registry()
            .with(env_filter)
            .with(capturing_layer)
            .init();

        Ok(TelemetryHandles {
            metrics_rx: Some(metrics_rx),
            log_rx: Some(log_rx),
            prometheus_handle: None,
            metrics_handle: None,
        })
    } else if config.metrics_addr.is_some() {
        // --- Standard mode with HTTP server: fanout recorder + fmt + capturing layer ---
        let prometheus_recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let prometheus_handle = prometheus_recorder.handle();

        let (snapshot_recorder, metrics_handle) = SnapshotRecorder::new();

        let fanout = FanoutRecorder::new(prometheus_recorder, snapshot_recorder);
        metrics::set_global_recorder(fanout)
            .map_err(|e| anyhow::anyhow!("failed to set metrics recorder: {e}"))?;

        let (capturing_layer, log_rx) = CapturingLayer::new(1000);

        if config.json_logs {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .with(capturing_layer)
                .init();
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .with(capturing_layer)
                .init();
        }

        Ok(TelemetryHandles {
            metrics_rx: None,
            log_rx: Some(log_rx),
            prometheus_handle: Some(prometheus_handle),
            metrics_handle: Some(metrics_handle),
        })
    } else {
        // --- Standard mode without HTTP server ---
        if config.json_logs {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .init();
        }

        Ok(TelemetryHandles {
            metrics_rx: None,
            log_rx: None,
            prometheus_handle: None,
            metrics_handle: None,
        })
    }
}

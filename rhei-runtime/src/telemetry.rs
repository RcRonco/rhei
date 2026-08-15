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

/// Buckets for the fast state-access histograms.
///
/// Spans the whole tier hierarchy — an L1 hit lands in the microsecond
/// buckets, an L2 `NVMe` read in the millisecond ones, an L3 object-storage read
/// in the tail — so a single histogram shows which tier reads are actually
/// coming from.
const STATE_LATENCY_BUCKETS: &[f64] = &[
    0.000_001, 0.000_005, 0.000_010, 0.000_050, 0.000_100, 0.000_500, 0.001, 0.005, 0.010, 0.050,
    0.100, 0.500, 1.0, 5.0,
];

/// Buckets for checkpoint durations, which run from milliseconds to minutes.
const CHECKPOINT_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.010, 0.050, 0.100, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// Build the Prometheus recorder with explicit histogram buckets.
///
/// Without buckets the exporter renders histograms as summaries, whose
/// quantiles are computed per-process and cannot be aggregated: in a cluster
/// you could read one pod's p99 but never the pipeline's. Buckets are
/// additive across pods, so `histogram_quantile` over a `sum by (le)` gives a
/// real cluster-wide figure.
fn build_prometheus_recorder() -> anyhow::Result<metrics_exporter_prometheus::PrometheusRecorder> {
    use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

    let builder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("state_get_duration_seconds".to_string()),
            STATE_LATENCY_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Suffix("checkpoint_duration_seconds".to_string()),
            CHECKPOINT_DURATION_BUCKETS,
        )?
        // Anything added later without an explicit rule still gets buckets
        // rather than silently becoming an unaggregatable summary.
        .set_buckets(CHECKPOINT_DURATION_BUCKETS)?;
    Ok(builder.build_recorder())
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
        let prometheus_recorder = build_prometheus_recorder()?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn histograms_export_as_buckets_not_summaries() {
        let recorder = build_prometheus_recorder().unwrap();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("state_checkpoint_duration_seconds").record(0.25);
        });

        let rendered = handle.render();

        // Buckets aggregate across pods; summaries do not. A cluster-wide p95
        // is only computable from the former, and the shipped alert rules
        // depend on `_bucket` existing.
        assert!(
            rendered.contains("state_checkpoint_duration_seconds_bucket"),
            "expected histogram buckets, got:\n{rendered}"
        );
        assert!(
            rendered.contains("state_checkpoint_duration_seconds_sum"),
            "expected histogram sum, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("quantile="),
            "histogram rendered as a summary:\n{rendered}"
        );
    }

    #[test]
    fn state_reads_get_microsecond_resolution_buckets() {
        // An L1 hit resolves in microseconds. Without buckets below 1ms every
        // hot read collapses into the first bucket and the tier hierarchy
        // becomes invisible.
        assert!(STATE_LATENCY_BUCKETS[0] <= 0.000_001);
        assert!(STATE_LATENCY_BUCKETS.iter().any(|&b| b >= 1.0));
        assert!(
            STATE_LATENCY_BUCKETS.windows(2).all(|w| w[0] < w[1]),
            "buckets must be strictly increasing"
        );
    }

    #[test]
    fn checkpoint_buckets_cover_slow_object_storage() {
        // Checkpoints against S3 can take minutes; a histogram topping out at
        // one second would report every slow checkpoint identically.
        assert!(CHECKPOINT_DURATION_BUCKETS.iter().any(|&b| b >= 300.0));
        assert!(
            CHECKPOINT_DURATION_BUCKETS.windows(2).all(|w| w[0] < w[1]),
            "buckets must be strictly increasing"
        );
    }
}

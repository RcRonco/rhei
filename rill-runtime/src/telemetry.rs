use std::net::SocketAddr;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Configuration for the observability stack.
#[derive(Debug)]
pub struct TelemetryConfig {
    /// If set, start a Prometheus HTTP exporter on this address.
    pub metrics_addr: Option<SocketAddr>,
    /// `tracing` env-filter string (e.g. `"info"`, `"rill_core=debug"`).
    pub log_filter: String,
    /// Emit logs as JSON instead of human-readable format.
    pub json_logs: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            metrics_addr: None,
            log_filter: "info".to_string(),
            json_logs: false,
        }
    }
}

/// Initialize the tracing subscriber and (optionally) the Prometheus metrics exporter.
///
/// This should be called once at process startup. Calling it more than once will
/// result in an error (the global subscriber can only be set once).
pub fn init(config: TelemetryConfig) -> anyhow::Result<()> {
    let env_filter =
        EnvFilter::try_new(&config.log_filter).unwrap_or_else(|_| EnvFilter::new("info"));

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

    // Set up Prometheus exporter if configured
    if let Some(addr) = config.metrics_addr {
        let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
        builder
            .with_http_listener(addr)
            .install()
            .map_err(|e| anyhow::anyhow!("failed to install Prometheus exporter: {e}"))?;
        tracing::info!(%addr, "Prometheus metrics exporter started");
    }

    Ok(())
}

//! TOML-based pipeline configuration with environment variable overrides.
//!
//! Load pipeline settings from a TOML file via [`PipelineConfig::load`],
//! then apply environment variable overrides with [`PipelineConfig::apply_env`].
//!
//! # File format
//!
//! ```toml
//! [pipeline]
//! name = "clickstream-ingest"
//! workers = 4
//! checkpoint_dir = "/data/checkpoints"
//! checkpoint_interval = 100
//!
//! [metrics]
//! addr = "0.0.0.0:9090"
//! log_level = "info"
//! json_logs = false
//!
//! [cluster]
//! process_id = 0
//! peers = ["host1:2101", "host2:2101"]
//! ```
//!
//! # Environment variable overrides
//!
//! Environment variables take precedence over TOML values:
//!
//! | TOML field                 | Environment variable     |
//! |----------------------------|--------------------------|
//! | `pipeline.name`            | `RHEI_PIPELINE_NAME`     |
//! | `pipeline.workers`         | `RHEI_WORKERS`           |
//! | `pipeline.checkpoint_dir`  | `RHEI_CHECKPOINT_DIR`    |
//! | `pipeline.checkpoint_interval` | `RHEI_CHECKPOINT_INTERVAL` |
//! | `metrics.addr`             | `RHEI_METRICS_ADDR`      |
//! | `metrics.log_level`        | `RHEI_LOG_LEVEL`         |
//! | `metrics.json_logs`        | `RHEI_JSON_LOGS`         |
//! | `cluster.process_id`       | `RHEI_PROCESS_ID`        |
//! | `cluster.peers`            | `RHEI_PEERS`             |
//!
//! # Example
//!
//! ```ignore
//! let config = PipelineConfig::load("pipeline.toml")?
//!     .apply_env();
//!
//! let executor = Executor::builder()
//!     .apply_config(&config)
//!     .build()?;
//! ```

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Top-level pipeline configuration loaded from TOML.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PipelineConfig {
    /// Core pipeline settings.
    #[serde(default)]
    pub pipeline: PipelineSection,

    /// Metrics and observability settings.
    #[serde(default)]
    pub metrics: MetricsSection,

    /// Cluster mode settings.
    #[serde(default)]
    pub cluster: ClusterSection,
}

/// Core pipeline execution settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSection {
    /// Human-readable pipeline name.
    pub name: Option<String>,

    /// Number of parallel worker threads.
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// Directory for checkpoint state.
    #[serde(default = "default_checkpoint_dir")]
    pub checkpoint_dir: String,

    /// Checkpoint interval in batches.
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: u64,
}

impl Default for PipelineSection {
    fn default() -> Self {
        Self {
            name: None,
            workers: default_workers(),
            checkpoint_dir: default_checkpoint_dir(),
            checkpoint_interval: default_checkpoint_interval(),
        }
    }
}

fn default_workers() -> usize {
    1
}
fn default_checkpoint_dir() -> String {
    "./checkpoints".to_string()
}
fn default_checkpoint_interval() -> u64 {
    100
}

/// Metrics and observability settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSection {
    /// HTTP bind address for metrics/health server (e.g. `"0.0.0.0:9090"`).
    pub addr: Option<String>,

    /// Log level filter (e.g. `"info"`, `"debug"`, `"rhei_core=trace"`).
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Emit logs as JSON instead of plain text.
    #[serde(default)]
    pub json_logs: bool,
}

impl Default for MetricsSection {
    fn default() -> Self {
        Self {
            addr: None,
            log_level: default_log_level(),
            json_logs: false,
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

/// Cluster mode settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterSection {
    /// Process ID for multi-process mode (0-based).
    pub process_id: Option<usize>,

    /// Peer addresses for multi-process cluster mode.
    pub peers: Option<Vec<String>>,
}

impl PipelineConfig {
    /// Load configuration from a TOML file.
    ///
    /// Returns a helpful error message if the file is not found or
    /// contains invalid TOML.
    ///
    /// # Errors
    ///
    /// - File not found: suggests checking the path
    /// - Parse error: includes the TOML parse error with line/column
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!(
                "failed to read config file '{}': {}\n\
                 hint: create a pipeline.toml or specify --config <path>",
                path.display(),
                e
            )
        })?;

        Self::from_toml(&contents).map_err(|e| {
            anyhow::anyhow!(
                "failed to parse config file '{}': {}\n\
                 hint: check the TOML syntax at the indicated location",
                path.display(),
                e
            )
        })
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml(toml_str: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(toml_str)?)
    }

    /// Apply environment variable overrides to this configuration.
    ///
    /// Environment variables take precedence over TOML values.
    /// Call this after [`load()`](Self::load) to allow deployment-time
    /// overrides without modifying the config file.
    pub fn apply_env(self) -> Self {
        self.apply_env_with(|key| std::env::var(key))
    }

    /// Internal: apply overrides using a custom env-lookup function.
    ///
    /// This lets tests supply a mock environment without requiring
    /// `unsafe` calls to `std::env::set_var`.
    fn apply_env_with<F>(mut self, env: F) -> Self
    where
        F: Fn(&str) -> Result<String, std::env::VarError>,
    {
        if let Ok(val) = env("RHEI_PIPELINE_NAME") {
            self.pipeline.name = Some(val);
        }
        if let Ok(val) = env("RHEI_WORKERS")
            && let Ok(n) = val.parse::<usize>()
        {
            self.pipeline.workers = n;
        }
        if let Ok(val) = env("RHEI_CHECKPOINT_DIR") {
            self.pipeline.checkpoint_dir = val;
        }
        if let Ok(val) = env("RHEI_CHECKPOINT_INTERVAL")
            && let Ok(n) = val.parse::<u64>()
        {
            self.pipeline.checkpoint_interval = n;
        }
        if let Ok(val) = env("RHEI_METRICS_ADDR") {
            self.metrics.addr = Some(val);
        }
        if let Ok(val) = env("RHEI_LOG_LEVEL") {
            self.metrics.log_level = val;
        }
        if let Ok(val) = env("RHEI_JSON_LOGS") {
            self.metrics.json_logs = val == "1" || val == "true";
        }
        if let Ok(val) = env("RHEI_PROCESS_ID")
            && let Ok(id) = val.parse::<usize>()
        {
            self.cluster.process_id = Some(id);
        }
        if let Ok(val) = env("RHEI_PEERS") {
            let peers: Vec<String> = val.split(',').map(|s| s.trim().to_string()).collect();
            if !peers.is_empty() {
                self.cluster.peers = Some(peers);
            }
        }
        self
    }

    /// Returns the metrics address parsed as a `SocketAddr`, if set.
    pub fn metrics_addr(&self) -> Option<std::net::SocketAddr> {
        self.metrics.addr.as_ref()?.parse().ok()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let config = PipelineConfig::from_toml("").unwrap();
        assert_eq!(config.pipeline.workers, 1);
        assert_eq!(config.pipeline.checkpoint_dir, "./checkpoints");
        assert_eq!(config.pipeline.checkpoint_interval, 100);
        assert!(config.pipeline.name.is_none());
    }

    #[test]
    fn parse_full_config() {
        let toml = r#"
[pipeline]
name = "fraud-detector"
workers = 8
checkpoint_dir = "/data/checkpoints"
checkpoint_interval = 50

[metrics]
addr = "0.0.0.0:9090"
log_level = "debug"
json_logs = true

[cluster]
process_id = 1
peers = ["host1:2101", "host2:2101", "host3:2101"]
"#;
        let config = PipelineConfig::from_toml(toml).unwrap();
        assert_eq!(config.pipeline.name.as_deref(), Some("fraud-detector"));
        assert_eq!(config.pipeline.workers, 8);
        assert_eq!(config.pipeline.checkpoint_dir, "/data/checkpoints");
        assert_eq!(config.pipeline.checkpoint_interval, 50);
        assert_eq!(config.metrics.addr.as_deref(), Some("0.0.0.0:9090"));
        assert_eq!(config.metrics.log_level, "debug");
        assert!(config.metrics.json_logs);
        assert_eq!(config.cluster.process_id, Some(1));
        assert_eq!(
            config.cluster.peers.as_deref(),
            Some(
                vec!["host1:2101", "host2:2101", "host3:2101"]
                    .into_iter()
                    .map(String::from)
                    .collect::<Vec<_>>()
                    .as_slice()
            )
        );
    }

    #[test]
    fn partial_config() {
        let toml = r"
[pipeline]
workers = 4
";
        let config = PipelineConfig::from_toml(toml).unwrap();
        assert_eq!(config.pipeline.workers, 4);
        assert_eq!(config.pipeline.checkpoint_dir, "./checkpoints"); // default
        assert!(config.metrics.addr.is_none()); // default
    }

    #[test]
    fn invalid_toml_gives_clear_error() {
        let result = PipelineConfig::from_toml("[pipeline\nworkers = ???");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Should contain parse error info
        assert!(
            err_msg.contains("expected") || err_msg.contains("TOML"),
            "error should describe parse failure: {err_msg}"
        );
    }

    #[test]
    fn load_nonexistent_file_gives_hint() {
        let result = PipelineConfig::load("/tmp/rhei_nonexistent_config_file.toml");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("hint"),
            "error should contain a hint: {err_msg}"
        );
    }

    #[test]
    fn metrics_addr_parsing() {
        let config = PipelineConfig::from_toml(
            r#"
[metrics]
addr = "0.0.0.0:9090"
"#,
        )
        .unwrap();
        let addr = config.metrics_addr().unwrap();
        assert_eq!(addr.port(), 9090);
    }

    #[test]
    fn default_config() {
        let config = PipelineConfig::default();
        assert_eq!(config.pipeline.workers, 1);
        assert!(config.pipeline.name.is_none());
        assert_eq!(config.metrics.log_level, "info");
        assert!(!config.metrics.json_logs);
    }

    /// Build a mock env-lookup function from key-value pairs.
    fn mock_env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Result<String, std::env::VarError> {
        let map: std::collections::HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key: &str| map.get(key).cloned().ok_or(std::env::VarError::NotPresent)
    }

    #[test]
    fn apply_env_numeric_workers_override() {
        let env = mock_env(&[("RHEI_WORKERS", "16")]);
        let config = PipelineConfig::default().apply_env_with(env);
        assert_eq!(config.pipeline.workers, 16);
    }

    #[test]
    fn apply_env_non_numeric_workers_silently_skipped() {
        let env = mock_env(&[("RHEI_WORKERS", "not-a-number")]);
        let config = PipelineConfig::default().apply_env_with(env);
        // Should keep the default value since the env var is not a valid number
        assert_eq!(config.pipeline.workers, 1);
    }

    #[test]
    fn apply_env_json_logs_boolean() {
        let env = mock_env(&[("RHEI_JSON_LOGS", "true")]);
        let config = PipelineConfig::default().apply_env_with(env);
        assert!(config.metrics.json_logs);

        // Also test "1"
        let env = mock_env(&[("RHEI_JSON_LOGS", "1")]);
        let config = PipelineConfig::default().apply_env_with(env);
        assert!(config.metrics.json_logs);

        // "false" or any other value should set json_logs to false
        let env = mock_env(&[("RHEI_JSON_LOGS", "false")]);
        let config = PipelineConfig::default().apply_env_with(env);
        assert!(!config.metrics.json_logs);
    }

    #[test]
    fn apply_env_comma_separated_peers() {
        let env = mock_env(&[("RHEI_PEERS", "host1:2101, host2:2101, host3:2101")]);
        let config = PipelineConfig::default().apply_env_with(env);
        let peers = config.cluster.peers.unwrap();
        assert_eq!(
            peers,
            vec![
                "host1:2101".to_string(),
                "host2:2101".to_string(),
                "host3:2101".to_string(),
            ]
        );
    }

    #[test]
    fn apply_env_checkpoint_dir_and_interval() {
        let env = mock_env(&[
            ("RHEI_CHECKPOINT_DIR", "/tmp/my-checkpoints"),
            ("RHEI_CHECKPOINT_INTERVAL", "500"),
        ]);
        let config = PipelineConfig::default().apply_env_with(env);
        assert_eq!(config.pipeline.checkpoint_dir, "/tmp/my-checkpoints");
        assert_eq!(config.pipeline.checkpoint_interval, 500);
    }
}

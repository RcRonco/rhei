use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, UNIX_EPOCH};

use clap::{Parser, Subcommand};

mod scaffold;
mod tui;

#[derive(Parser)]
#[command(name = "rhei", about = "Rhei stream processing CLI")]
struct Cli {
    /// Emit logs as JSON
    #[arg(long, global = true)]
    json_logs: bool,

    /// Log level filter (e.g. `"info"`, `"debug"`, `"rhei_core=trace"`)
    #[arg(long, global = true, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

// `Run` carries far more options than the other subcommands, so the variants
// differ in size. Boxing would buy nothing: this enum is constructed exactly
// once, at startup, from parsed argv.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
enum Commands {
    /// Run the current Rhei project
    Run {
        /// Launch the TUI dashboard instead of shelling out to cargo
        #[arg(long)]
        tui: bool,

        /// Number of parallel Timely worker threads per process
        #[arg(long, default_value = "1")]
        workers: usize,

        /// Bind address for the HTTP health/metrics server (e.g. `0.0.0.0:9090`)
        #[arg(long)]
        metrics_addr: Option<std::net::SocketAddr>,

        /// Process ID for multi-process cluster mode (0-based)
        #[arg(long)]
        process_id: Option<usize>,

        /// Comma-separated peer addresses (host:port) for cluster mode
        #[arg(long, value_delimiter = ',')]
        peers: Option<Vec<String>>,

        /// Path to hostfile (one host:port per line), alternative to --peers
        #[arg(long, conflicts_with = "peers")]
        hostfile: Option<std::path::PathBuf>,

        #[command(flatten)]
        discovery: DiscoveryArgs,

        /// Path to TOML config file (env vars override config values)
        #[arg(long)]
        config: Option<std::path::PathBuf>,
    },
    /// Attach to a running pipeline's HTTP server and display the TUI dashboard
    Attach {
        /// Address of the running pipeline's HTTP server (e.g. `127.0.0.1:9090`)
        addr: String,
    },
    /// Create a new Rhei pipeline project
    New {
        /// Project name (used as directory name and Cargo package name)
        name: String,

        /// Parent directory for the project (defaults to current directory)
        #[arg(long)]
        path: Option<PathBuf>,
    },
    /// Run the built-in demo pipeline with the HTTP server for the web dashboard
    Demo {
        /// Number of parallel Timely worker threads
        #[arg(long, default_value = "1")]
        workers: usize,

        /// Bind address for the HTTP server (default: 0.0.0.0:9090)
        #[arg(long, default_value = "0.0.0.0:9090")]
        addr: std::net::SocketAddr,
    },
}

/// Cluster discovery flags for `rhei run`.
///
/// Grouped into their own `Args` struct so the `Run` variant stays small and the
/// gossip options read as one coherent block in `--help`.
#[derive(clap::Args, Debug, Default)]
struct DiscoveryArgs {
    /// Discovery backend: `static` (fixed --peers) or `gossip` (dynamic).
    ///
    /// With `gossip`, nodes find each other at runtime and the pipeline
    /// rescales as membership changes; --process-id and --peers are ignored.
    #[arg(long, value_parser = ["static", "gossip"])]
    discovery: Option<String>,

    /// Stable node identifier for gossip discovery (defaults to hostname).
    ///
    /// Must be stable across restarts — a changing ID looks like permanent
    /// join/leave churn to the rest of the cluster.
    #[arg(long)]
    node_id: Option<String>,

    /// Cluster name; only nodes sharing it gossip with each other
    #[arg(long)]
    cluster_id: Option<String>,

    /// Bind address for the gossip socket (e.g. `0.0.0.0:2201`)
    #[arg(long)]
    gossip_addr: Option<String>,

    /// Address peers should gossip with, if different from --gossip-addr
    #[arg(long)]
    gossip_advertise_addr: Option<String>,

    /// Timely data-plane address advertised to peers (e.g. `10.0.0.5:2101`)
    #[arg(long)]
    data_addr: Option<String>,

    /// Comma-separated gossip seed nodes; only one needs to be reachable
    #[arg(long, value_delimiter = ',')]
    seeds: Option<Vec<String>>,

    /// Number of key groups — the ceiling on parallelism this pipeline can
    /// ever rescale to.
    ///
    /// Fixed for the pipeline's lifetime: changing it re-partitions the key
    /// space and invalidates every existing checkpoint.
    #[arg(long)]
    max_parallelism: Option<usize>,

    /// Seconds membership must be stable before rescaling (default: 5)
    #[arg(long)]
    rescale_debounce_secs: Option<u64>,

    /// Report membership changes but never rescale automatically
    #[arg(long)]
    no_auto_rescale: bool,
}

/// Cluster discovery settings resolved from CLI flags and the config file.
///
/// Forwarded to the pipeline process as environment variables, matching how the
/// CLI passes every other setting through to `cargo run`.
#[derive(Debug, Default)]
struct DiscoverySettings {
    discovery: Option<String>,
    node_id: Option<String>,
    cluster_id: Option<String>,
    gossip_addr: Option<String>,
    gossip_advertise_addr: Option<String>,
    data_addr: Option<String>,
    seeds: Option<Vec<String>>,
    max_parallelism: Option<usize>,
    rescale_debounce_secs: Option<u64>,
    auto_rescale: Option<bool>,
}

impl DiscoverySettings {
    /// Merge CLI flags with the config file's `[cluster]` section.
    ///
    /// CLI flags win; the config file fills the gaps.
    fn resolve(args: DiscoveryArgs, cfg: rhei_core::config::ClusterSection) -> Self {
        Self {
            discovery: args.discovery.or(cfg.discovery),
            node_id: args.node_id.or(cfg.node_id),
            cluster_id: args.cluster_id.or(cfg.cluster_id),
            gossip_addr: args.gossip_addr.or(cfg.gossip_addr),
            gossip_advertise_addr: args.gossip_advertise_addr.or(cfg.gossip_advertise_addr),
            data_addr: args.data_addr.or(cfg.data_addr),
            seeds: args.seeds.or(cfg.seeds),
            max_parallelism: args.max_parallelism.or(cfg.max_parallelism),
            rescale_debounce_secs: args.rescale_debounce_secs.or(cfg.rescale_debounce_secs),
            // `--no-auto-rescale` is a flag, so it can only turn the behaviour
            // off; when absent, the config file decides.
            auto_rescale: if args.no_auto_rescale {
                Some(false)
            } else {
                cfg.auto_rescale
            },
        }
    }

    /// Whether gossip discovery is requested.
    fn is_gossip(&self) -> bool {
        self.discovery
            .as_deref()
            .is_some_and(|d| d.eq_ignore_ascii_case("gossip"))
    }

    /// Check the settings are self-consistent before launching the pipeline.
    ///
    /// Catching these here gives a clear message at the CLI instead of an
    /// obscure bind failure or a node that silently never joins the cluster.
    fn validate(&self) -> anyhow::Result<()> {
        if let Some(max_p) = self.max_parallelism {
            anyhow::ensure!(max_p > 0, "--max-parallelism must be at least 1");
        }

        if !self.is_gossip() {
            // Gossip-only options are meaningless without gossip discovery;
            // silently ignoring them would hide a misconfigured deployment.
            let stray = [
                ("--gossip-addr", self.gossip_addr.is_some()),
                (
                    "--gossip-advertise-addr",
                    self.gossip_advertise_addr.is_some(),
                ),
                ("--seeds", self.seeds.is_some()),
                ("--data-addr", self.data_addr.is_some()),
            ];
            if let Some((flag, _)) = stray.iter().find(|(_, set)| *set) {
                anyhow::bail!(
                    "{flag} requires --discovery gossip \
                     (without it, the cluster shape comes from --peers)"
                );
            }
            return Ok(());
        }

        // Gossip needs somewhere to listen and an address to hand peers for the
        // data plane; neither has a safe default.
        anyhow::ensure!(
            self.gossip_addr.is_some(),
            "--discovery gossip requires --gossip-addr (e.g. 0.0.0.0:2201)"
        );
        anyhow::ensure!(
            self.data_addr.is_some(),
            "--discovery gossip requires --data-addr — the Timely address peers \
             will connect to (e.g. 10.0.0.5:2101)"
        );

        if self.seeds.as_ref().is_none_or(Vec::is_empty) {
            // Legal — this is how the first node of a new cluster starts — but
            // a silent typo here leaves a node gossiping alone forever.
            tracing::warn!(
                "no --seeds given; this node will form a new cluster of its own. \
                 Pass --seeds with an existing node's gossip address to join one."
            );
        }
        Ok(())
    }

    /// Forward these settings to the pipeline process as environment variables.
    fn apply_env(&self, cmd: &mut Command) {
        if let Some(ref v) = self.discovery {
            cmd.env("RHEI_DISCOVERY", v);
        }
        if let Some(ref v) = self.node_id {
            cmd.env("RHEI_NODE_ID", v);
        }
        if let Some(ref v) = self.cluster_id {
            cmd.env("RHEI_CLUSTER_ID", v);
        }
        if let Some(ref v) = self.gossip_addr {
            cmd.env("RHEI_GOSSIP_ADDR", v);
        }
        if let Some(ref v) = self.gossip_advertise_addr {
            cmd.env("RHEI_GOSSIP_ADVERTISE_ADDR", v);
        }
        if let Some(ref v) = self.data_addr {
            cmd.env("RHEI_DATA_ADDR", v);
        }
        if let Some(ref v) = self.seeds {
            cmd.env("RHEI_SEEDS", v.join(","));
        }
        if let Some(v) = self.max_parallelism {
            cmd.env("RHEI_MAX_PARALLELISM", v.to_string());
        }
        if let Some(v) = self.rescale_debounce_secs {
            cmd.env("RHEI_RESCALE_DEBOUNCE_SECS", v.to_string());
        }
        if let Some(v) = self.auto_rescale {
            cmd.env("RHEI_AUTO_RESCALE", if v { "1" } else { "0" });
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            tui: true, workers, ..
        } => cmd_run_tui(cli.log_level, workers),
        Commands::Run {
            tui: false,
            workers,
            metrics_addr,
            process_id,
            peers,
            hostfile,
            discovery,
            config,
        } => {
            // Load TOML config if --config is specified, then apply env overrides.
            let file_config = if let Some(ref config_path) = config {
                Some(rhei_core::config::PipelineConfig::load(config_path)?.apply_env())
            } else {
                None
            };

            // CLI flags override config file values; config overrides defaults.
            let effective_workers = if workers > 1 {
                workers
            } else {
                file_config.as_ref().map_or(workers, |c| c.pipeline.workers)
            };
            let effective_metrics_addr = metrics_addr.or_else(|| {
                file_config
                    .as_ref()
                    .and_then(rhei_core::config::PipelineConfig::metrics_addr)
            });
            let effective_log_level = if cli.log_level == "info" {
                file_config
                    .as_ref()
                    .map_or(cli.log_level.clone(), |c| c.metrics.log_level.clone())
            } else {
                cli.log_level.clone()
            };
            let effective_json_logs =
                cli.json_logs || file_config.as_ref().is_some_and(|c| c.metrics.json_logs);
            let effective_process_id =
                process_id.or_else(|| file_config.as_ref().and_then(|c| c.cluster.process_id));

            let _telemetry =
                rhei_runtime::telemetry::init(rhei_runtime::telemetry::TelemetryConfig {
                    metrics_addr: effective_metrics_addr,
                    log_filter: effective_log_level,
                    json_logs: effective_json_logs,
                    tui: false,
                })?;

            let file_cluster = file_config.as_ref().map(|c| c.cluster.clone());

            // Resolve peers from --peers, --hostfile, or config file.
            let resolved_peers = if let Some(peers) = peers {
                Some(peers)
            } else if let Some(ref path) = hostfile {
                let contents = std::fs::read_to_string(path)?;
                let peers: Vec<String> = contents
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .map(String::from)
                    .collect();
                if peers.is_empty() {
                    anyhow::bail!("hostfile is empty: {}", path.display());
                }
                Some(peers)
            } else {
                file_config.and_then(|c| c.cluster.peers)
            };

            let discovery = DiscoverySettings::resolve(discovery, file_cluster.unwrap_or_default());
            discovery.validate()?;

            cmd_run(
                effective_workers,
                effective_metrics_addr,
                effective_process_id,
                resolved_peers,
                &discovery,
            )
        }
        Commands::New { name, path } => scaffold::create_project(&name, path.as_deref()),
        Commands::Attach { addr } => cmd_attach(addr),
        Commands::Demo { workers, addr } => cmd_demo(workers, addr),
    }
}

fn cmd_run(
    workers: usize,
    metrics_addr: Option<std::net::SocketAddr>,
    process_id: Option<usize>,
    peers: Option<Vec<String>>,
    discovery: &DiscoverySettings,
) -> anyhow::Result<()> {
    if !Path::new("Cargo.toml").exists() {
        anyhow::bail!("No Cargo.toml found. Are you in a Rhei project directory?");
    }

    let mut cmd = Command::new("cargo");
    cmd.arg("run");
    cmd.env("RHEI_WORKERS", workers.to_string());

    if let Some(addr) = metrics_addr {
        cmd.env("RHEI_METRICS_ADDR", addr.to_string());
    }
    if let Some(pid) = process_id {
        cmd.env("RHEI_PROCESS_ID", pid.to_string());
    }
    if let Some(ref peers) = peers {
        cmd.env("RHEI_PEERS", peers.join(","));
    }
    discovery.apply_env(&mut cmd);

    let status = cmd.status()?;

    if !status.success() {
        anyhow::bail!("cargo run failed with status: {status}");
    }

    Ok(())
}

fn cmd_run_tui(log_level: String, workers: usize) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        // Initialize telemetry inside the runtime so tokio::spawn works
        let handles = rhei_runtime::telemetry::init(rhei_runtime::telemetry::TelemetryConfig {
            metrics_addr: None,
            log_filter: log_level,
            json_logs: false,
            tui: true,
        })?;

        let metrics_rx = handles
            .metrics_rx
            .ok_or_else(|| anyhow::anyhow!("TUI mode should provide metrics_rx"))?;
        let log_rx = handles
            .log_rx
            .ok_or_else(|| anyhow::anyhow!("TUI mode should provide log_rx"))?;

        // Build the logical plan for the demo pipeline
        let plan = rhei_core::graph::StreamGraph::new()
            .source("SensorSource")
            .filter("RangeFilter")
            .key_by("BySensorId")
            .operator("TumblingWindow")
            .map("FormatAvg")
            .sink("LogSink")
            .build();

        // Spawn a demo pipeline in the background
        let pipeline_handle = tokio::spawn(async move { run_demo_pipeline(workers).await });

        // Run the TUI on the main task
        let app = tui::TuiApp::new(metrics_rx, log_rx, Some(plan));
        let tui_result = app.run().await;

        // Cancel pipeline on TUI exit
        pipeline_handle.abort();

        tui_result
    })
}

fn cmd_attach(addr: String) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        let base_url = if addr.starts_with("http") {
            addr.clone()
        } else {
            format!("http://{addr}")
        };

        // Verify connectivity before launching TUI
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;

        let health_url = format!("{base_url}/api/health");
        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                anyhow::bail!(
                    "Pipeline at {addr} returned HTTP {}: is the server running?",
                    resp.status()
                );
            }
            Err(e) => {
                anyhow::bail!("Failed to connect to pipeline at {addr}: {e}");
            }
        }

        let (metrics_tx, metrics_rx) =
            tokio::sync::watch::channel(rhei_runtime::metrics_snapshot::MetricsSnapshot::default());
        let (log_tx, log_rx) =
            tokio::sync::mpsc::channel::<rhei_runtime::tracing_capture::LogEntry>(1000);

        // Spawn metrics poller: every 500ms, GET /api/metrics
        let metrics_client = client.clone();
        let metrics_url = format!("{base_url}/api/metrics");
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                let Ok(resp) = metrics_client.get(&metrics_url).send().await else {
                    continue;
                };
                if let Ok(snapshot) = resp
                    .json::<rhei_runtime::metrics_snapshot::MetricsSnapshot>()
                    .await
                    && metrics_tx.send(snapshot).is_err()
                {
                    break;
                }
            }
        });

        // Spawn log poller: every 250ms, GET /api/logs?after=<last_seq>
        let log_client = client;
        let logs_url = format!("{base_url}/api/logs");
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            let mut last_seq: u64 = 0;
            loop {
                interval.tick().await;
                let url = format!("{logs_url}?after={last_seq}");
                let Ok(resp) = log_client.get(&url).send().await else {
                    continue;
                };
                let Ok(entries) = resp
                    .json::<Vec<rhei_runtime::http_server::ApiLogEntry>>()
                    .await
                else {
                    continue;
                };
                for entry in entries {
                    if entry.seq > last_seq {
                        last_seq = entry.seq;
                    }
                    let log_entry = api_log_to_log_entry(&entry);
                    if log_tx.send(log_entry).await.is_err() {
                        return;
                    }
                }
            }
        });

        // Run TUI with no graph view (remote pipeline graph is unknown)
        let app = tui::TuiApp::new(metrics_rx, log_rx, None);
        app.run().await
    })
}

/// Convert an [`ApiLogEntry`] to a [`LogEntry`] for the TUI.
fn api_log_to_log_entry(
    api: &rhei_runtime::http_server::ApiLogEntry,
) -> rhei_runtime::tracing_capture::LogEntry {
    let level = match api.level.as_str() {
        "ERROR" => tracing::Level::ERROR,
        "WARN" => tracing::Level::WARN,
        "DEBUG" => tracing::Level::DEBUG,
        "TRACE" => tracing::Level::TRACE,
        _ => tracing::Level::INFO,
    };
    let timestamp = UNIX_EPOCH + Duration::from_millis(api.timestamp_ms);
    rhei_runtime::tracing_capture::LogEntry {
        timestamp,
        level,
        target: api.target.clone(),
        message: api.message.clone(),
        worker: api.worker,
    }
}

fn cmd_demo(workers: usize, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        let handles = rhei_runtime::telemetry::init(rhei_runtime::telemetry::TelemetryConfig {
            metrics_addr: Some(addr),
            log_filter: "info".to_string(),
            json_logs: false,
            tui: false,
        })?;

        let health = rhei_runtime::health::HealthState::new();

        let executor = rhei_runtime::executor::Executor::builder()
            .checkpoint_dir(std::env::temp_dir().join("rhei_demo"))
            .workers(workers)
            .health(health.clone())
            .build()?;

        let _http = rhei_runtime::http_server::start(rhei_runtime::http_server::HttpServerConfig {
            addr,
            health,
            prometheus: handles
                .prometheus_handle
                .ok_or_else(|| anyhow::anyhow!("prometheus handle should exist"))?,
            metrics_handle: handles.metrics_handle,
            log_rx: handles.log_rx,
            topology: executor.topology_handle(),
            pipeline_name: Some("sensor-demo".to_string()),
            workers,
            checkpoint_dir: None,
            // `rhei demo` exists to drive the local web dashboard, so allow
            // the Vite dev server's origin alongside anything set in the
            // environment. Everything else stays at the closed default.
            access: demo_access_config(),
        });

        eprintln!("Rhei demo pipeline starting on http://{addr}");
        eprintln!("Open the dashboard at http://localhost:5173 and add {addr}");

        run_demo_pipeline_with(executor).await
    })
}

/// Built-in demo pipeline for TUI mode.
///
/// Simulates a real-time sensor aggregation pipeline: 5 sensors emit readings
/// every 150 ms, aggregated into 10-second tumbling windows using the
/// operators library.
async fn run_demo_pipeline(workers: usize) -> anyhow::Result<()> {
    use rhei_runtime::executor::Executor;

    let dir = std::env::temp_dir().join("rhei_tui_demo");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let executor = Executor::builder()
        .checkpoint_dir(dir)
        .workers(workers)
        .from_env()
        .build()?;

    run_demo_with_executor(&executor).await
}

/// Run the demo pipeline with a pre-configured executor (for `rhei demo`).
async fn run_demo_pipeline_with(
    executor: rhei_runtime::controller::PipelineController,
) -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rhei_demo");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    run_demo_with_executor(&executor).await?;

    // Keep alive so the HTTP server stays up
    loop {
        tokio::time::sleep(std::time::Duration::from_mins(1)).await;
    }
}

/// Shared demo pipeline logic: build graph and run on the given executor.
async fn run_demo_with_executor(
    _executor: &rhei_runtime::controller::PipelineController,
) -> anyhow::Result<()> {
    tracing::info!("demo pipeline started (placeholder — define a RheiSchema type for real use)");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    tracing::info!("pipeline completed");
    Ok(())
}

/// Access settings for the `rhei demo` HTTP server.
///
/// The demo's whole purpose is to back the local web dashboard, which runs on
/// the Vite dev server at a different origin, so those two localhost origins
/// are allowed in addition to anything set via `RHEI_ALLOWED_ORIGINS`.
/// Authentication and the state explorer keep their closed defaults.
fn demo_access_config() -> rhei_runtime::http_server::AccessConfig {
    let mut access = rhei_runtime::http_server::AccessConfig::from_env();
    for origin in [
        "http://localhost:5173".to_string(),
        "http://127.0.0.1:5173".to_string(),
    ] {
        if !access.allowed_origins.contains(&origin) {
            access.allowed_origins.push(origin);
        }
    }
    access
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn gossip() -> DiscoverySettings {
        DiscoverySettings {
            discovery: Some("gossip".to_string()),
            gossip_addr: Some("0.0.0.0:2201".to_string()),
            data_addr: Some("10.0.0.5:2101".to_string()),
            seeds: Some(vec!["10.0.0.4:2201".to_string()]),
            ..Default::default()
        }
    }

    #[test]
    fn cli_definition_is_valid() {
        // Catches conflicting flag names and bad value parsers at test time
        // rather than on first run.
        Cli::command().debug_assert();
    }

    #[test]
    fn static_discovery_is_the_default() {
        let settings = DiscoverySettings::default();
        assert!(!settings.is_gossip());
        settings.validate().unwrap();
    }

    #[test]
    fn gossip_settings_validate() {
        assert!(gossip().is_gossip());
        gossip().validate().unwrap();
    }

    #[test]
    fn gossip_requires_a_gossip_address() {
        let settings = DiscoverySettings {
            gossip_addr: None,
            ..gossip()
        };
        let err = settings.validate().unwrap_err().to_string();
        assert!(err.contains("--gossip-addr"), "unexpected error: {err}");
    }

    #[test]
    fn gossip_requires_a_data_address() {
        // Without it, peers have no address to open the Timely connection to.
        let settings = DiscoverySettings {
            data_addr: None,
            ..gossip()
        };
        let err = settings.validate().unwrap_err().to_string();
        assert!(err.contains("--data-addr"), "unexpected error: {err}");
    }

    #[test]
    fn seedless_gossip_is_allowed() {
        // This is how the first node of a brand-new cluster starts.
        let settings = DiscoverySettings {
            seeds: None,
            ..gossip()
        };
        settings.validate().unwrap();
    }

    #[test]
    fn gossip_flags_without_gossip_discovery_are_rejected() {
        // Silently ignoring them would hide a misconfigured deployment that
        // never actually forms a cluster.
        for settings in [
            DiscoverySettings {
                gossip_addr: Some("0.0.0.0:2201".into()),
                ..Default::default()
            },
            DiscoverySettings {
                seeds: Some(vec!["h:2201".into()]),
                ..Default::default()
            },
            DiscoverySettings {
                data_addr: Some("h:2101".into()),
                ..Default::default()
            },
        ] {
            let err = settings.validate().unwrap_err().to_string();
            assert!(
                err.contains("--discovery gossip"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn zero_max_parallelism_is_rejected() {
        let settings = DiscoverySettings {
            max_parallelism: Some(0),
            ..Default::default()
        };
        assert!(settings.validate().is_err());
    }

    #[test]
    fn settings_are_forwarded_as_env_vars() {
        let mut cmd = Command::new("true");
        let settings = DiscoverySettings {
            node_id: Some("node-a".into()),
            cluster_id: Some("prod".into()),
            max_parallelism: Some(256),
            rescale_debounce_secs: Some(30),
            auto_rescale: Some(false),
            ..gossip()
        };
        settings.apply_env(&mut cmd);

        let envs: std::collections::HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();

        assert_eq!(envs["RHEI_DISCOVERY"].as_deref(), Some("gossip"));
        assert_eq!(envs["RHEI_NODE_ID"].as_deref(), Some("node-a"));
        assert_eq!(envs["RHEI_CLUSTER_ID"].as_deref(), Some("prod"));
        assert_eq!(envs["RHEI_GOSSIP_ADDR"].as_deref(), Some("0.0.0.0:2201"));
        assert_eq!(envs["RHEI_DATA_ADDR"].as_deref(), Some("10.0.0.5:2101"));
        assert_eq!(envs["RHEI_SEEDS"].as_deref(), Some("10.0.0.4:2201"));
        assert_eq!(envs["RHEI_MAX_PARALLELISM"].as_deref(), Some("256"));
        assert_eq!(envs["RHEI_RESCALE_DEBOUNCE_SECS"].as_deref(), Some("30"));
        assert_eq!(envs["RHEI_AUTO_RESCALE"].as_deref(), Some("0"));
    }

    #[test]
    fn unset_settings_are_not_forwarded() {
        let mut cmd = Command::new("true");
        DiscoverySettings::default().apply_env(&mut cmd);
        assert_eq!(cmd.get_envs().count(), 0);
    }
}

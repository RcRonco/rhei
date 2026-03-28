use std::path::Path;
use std::process::Command;
use std::time::{Duration, UNIX_EPOCH};

use clap::{Parser, Subcommand};

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
    },
    /// Attach to a running pipeline's HTTP server and display the TUI dashboard
    Attach {
        /// Address of the running pipeline's HTTP server (e.g. `127.0.0.1:9090`)
        addr: String,
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
        } => {
            let _telemetry =
                rhei_runtime::telemetry::init(rhei_runtime::telemetry::TelemetryConfig {
                    metrics_addr,
                    log_filter: cli.log_level.clone(),
                    json_logs: cli.json_logs,
                    tui: false,
                })?;

            // Resolve peers from --peers or --hostfile.
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
                None
            };

            cmd_run(workers, metrics_addr, process_id, resolved_peers)
        }
        Commands::Attach { addr } => cmd_attach(addr),
        Commands::Demo { workers, addr } => cmd_demo(workers, addr),
    }
}

fn cmd_run(
    workers: usize,
    metrics_addr: Option<std::net::SocketAddr>,
    process_id: Option<usize>,
    peers: Option<Vec<String>>,
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
            .build();

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
        .build();

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
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

/// Shared demo pipeline logic: build graph and run on the given executor.
async fn run_demo_with_executor(
    executor: &rhei_runtime::controller::PipelineController,
) -> anyhow::Result<()> {
    use std::fmt;
    use std::marker::PhantomData;

    use async_trait::async_trait;
    use rhei_core::operators::{Avg, TumblingWindow, WindowOutput};
    use rhei_core::traits::{Sink, Source};
    use rhei_runtime::dataflow::DataflowGraph;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct SensorReading {
        sensor_id: String,
        value: f64,
        timestamp: u64,
    }

    /// Generates sensor readings in real time, one batch per tick.
    struct SimulatedSensorSource {
        tick: u64,
        max_ticks: u64,
    }

    #[async_trait]
    impl Source for SimulatedSensorSource {
        type Output = SensorReading;

        async fn next_batch(&mut self) -> Option<Vec<SensorReading>> {
            if self.tick >= self.max_ticks {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;

            let sensors = [
                ("temp-1", 22.0),
                ("temp-2", 18.0),
                ("pressure-1", 1013.0),
                ("humidity-1", 65.0),
                ("temp-3", 25.0),
            ];

            let t = self.tick as f64;
            let readings = sensors
                .iter()
                .map(|&(id, base)| {
                    let variation = (t * 0.7).sin() * 2.0 + (t * 1.3).cos() * 1.5;
                    SensorReading {
                        sensor_id: id.to_string(),
                        value: base + variation,
                        timestamp: self.tick,
                    }
                })
                .collect();

            self.tick += 1;
            Some(readings)
        }
    }

    /// Routes operator output via `tracing::info!`.
    struct LoggingSink<T>(PhantomData<fn(T)>);

    #[async_trait]
    impl<T: Send + Sync + fmt::Display + 'static> Sink for LoggingSink<T> {
        type Input = T;

        async fn write(&mut self, input: T) -> anyhow::Result<()> {
            tracing::info!("{input}");
            Ok(())
        }
    }

    let op = TumblingWindow::builder()
        .window_size(10)
        .key_fn(|r: &SensorReading| r.sensor_id.clone())
        .time_fn(|r: &SensorReading| r.timestamp)
        .aggregator(Avg::new(|r: &SensorReading| r.value))
        .build();

    let graph = DataflowGraph::new();
    let source = SimulatedSensorSource {
        tick: 0,
        max_ticks: 500,
    };

    graph
        .source(source)
        .key_by(|r: &SensorReading| r.sensor_id.clone())
        .operator("tumbling_window", op)
        .map(|w: WindowOutput<f64>| format!("{w}"))
        .sink(LoggingSink::<String>(PhantomData));

    // Let the TUI render before data starts flowing
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tracing::info!("sensor aggregation pipeline started — 5 sensors, 10s tumbling windows");
    executor.run(graph).await?;
    tracing::info!("pipeline completed");

    Ok(())
}

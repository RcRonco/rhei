use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, UNIX_EPOCH};

use clap::{Parser, Subcommand};

mod tui;

#[derive(Parser)]
#[command(name = "rill", about = "Rill stream processing CLI")]
struct Cli {
    /// Emit logs as JSON
    #[arg(long, global = true)]
    json_logs: bool,

    /// Log level filter (e.g. `"info"`, `"debug"`, `"rill_core=trace"`)
    #[arg(long, global = true, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate a new Rill project
    New {
        /// Project name
        name: String,
    },
    /// Run the current Rill project
    Run {
        /// Launch the TUI dashboard instead of shelling out to cargo
        #[arg(long)]
        tui: bool,

        /// Number of parallel Timely worker threads
        #[arg(long, default_value = "1")]
        workers: usize,

        /// Bind address for the HTTP health/metrics server (e.g. `0.0.0.0:9090`)
        #[arg(long)]
        metrics_addr: Option<std::net::SocketAddr>,
    },
    /// Attach to a running pipeline's HTTP server and display the TUI dashboard
    Attach {
        /// Address of the running pipeline's HTTP server (e.g. `127.0.0.1:9090`)
        addr: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::New { name } => {
            let _telemetry =
                rill_runtime::telemetry::init(rill_runtime::telemetry::TelemetryConfig {
                    metrics_addr: None,
                    log_filter: cli.log_level.clone(),
                    json_logs: cli.json_logs,
                    tui: false,
                })?;
            cmd_new(&name)
        }
        Commands::Run {
            tui: true, workers, ..
        } => cmd_run_tui(cli.log_level, workers),
        Commands::Run {
            tui: false,
            workers,
            metrics_addr,
        } => {
            let _telemetry =
                rill_runtime::telemetry::init(rill_runtime::telemetry::TelemetryConfig {
                    metrics_addr,
                    log_filter: cli.log_level.clone(),
                    json_logs: cli.json_logs,
                    tui: false,
                })?;
            cmd_run(workers)
        }
        Commands::Attach { addr } => cmd_attach(addr),
    }
}

fn cmd_new(name: &str) -> anyhow::Result<()> {
    let project_dir = Path::new(name);
    if project_dir.exists() {
        anyhow::bail!("Directory '{name}' already exists");
    }

    fs::create_dir_all(project_dir.join("src"))?;

    let cargo_toml = format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2024"

[dependencies]
anyhow = "1.0"
async-trait = "0.1"
rill-core = {{ git = "https://github.com/your-org/rill", package = "rill-core" }}
rill-runtime = {{ git = "https://github.com/your-org/rill", package = "rill-runtime" }}
tokio = {{ version = "1", features = ["full"] }}
serde = {{ version = "1", features = ["derive"] }}
"#
    );

    let main_rs = r#"use async_trait::async_trait;
use rill_core::traits::StreamFunction;
use rill_core::state::context::StateContext;
use rill_core::connectors::vec_source::VecSource;
use rill_core::connectors::print_sink::PrintSink;
use rill_runtime::dataflow::DataflowGraph;
use rill_runtime::executor::Executor;

#[derive(Clone)]
struct MyOperator;

#[async_trait]
impl StreamFunction for MyOperator {
    type Input = String;
    type Output = String;

    async fn process(&mut self, input: String, ctx: &mut StateContext) -> anyhow::Result<Vec<String>> {
        // Your processing logic here
        Ok(vec![input])
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let graph = DataflowGraph::new();
    let source = VecSource::new(vec![
        "hello world".to_string(),
        "hello rill".to_string(),
    ]);

    graph.source(source)
        .key_by(|s: &String| s.clone())
        .operator("my_operator", MyOperator)
        .sink(PrintSink::new());

    let executor = Executor::new("./checkpoints".into());
    executor.run(graph).await?;
    Ok(())
}
"#;

    fs::write(project_dir.join("Cargo.toml"), cargo_toml)?;
    fs::write(project_dir.join("src/main.rs"), main_rs)?;

    println!("Created new Rill project: {name}");
    println!("  cd {name}");
    println!("  cargo run");

    Ok(())
}

fn cmd_run(workers: usize) -> anyhow::Result<()> {
    if !Path::new("Cargo.toml").exists() {
        anyhow::bail!("No Cargo.toml found. Are you in a Rill project directory?");
    }

    let status = Command::new("cargo")
        .arg("run")
        .env("RILL_WORKERS", workers.to_string())
        .status()?;

    if !status.success() {
        anyhow::bail!("cargo run failed with status: {status}");
    }

    Ok(())
}

fn cmd_run_tui(log_level: String, workers: usize) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        // Initialize telemetry inside the runtime so tokio::spawn works
        let handles = rill_runtime::telemetry::init(rill_runtime::telemetry::TelemetryConfig {
            metrics_addr: None,
            log_filter: log_level,
            json_logs: false,
            tui: true,
        })?;

        let metrics_rx = handles
            .metrics_rx
            .expect("TUI mode should provide metrics_rx");
        let log_rx = handles.log_rx.expect("TUI mode should provide log_rx");

        // Build the logical plan for the demo pipeline
        let plan = rill_core::graph::StreamGraph::new()
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
            tokio::sync::watch::channel(rill_runtime::metrics_snapshot::MetricsSnapshot::default());
        let (log_tx, log_rx) =
            tokio::sync::mpsc::channel::<rill_runtime::tracing_capture::LogEntry>(1000);

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
                    .json::<rill_runtime::metrics_snapshot::MetricsSnapshot>()
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
                    .json::<Vec<rill_runtime::http_server::ApiLogEntry>>()
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
    api: &rill_runtime::http_server::ApiLogEntry,
) -> rill_runtime::tracing_capture::LogEntry {
    let level = match api.level.as_str() {
        "ERROR" => tracing::Level::ERROR,
        "WARN" => tracing::Level::WARN,
        "DEBUG" => tracing::Level::DEBUG,
        "TRACE" => tracing::Level::TRACE,
        _ => tracing::Level::INFO,
    };
    let timestamp = UNIX_EPOCH + Duration::from_millis(api.timestamp_ms);
    rill_runtime::tracing_capture::LogEntry {
        timestamp,
        level,
        target: api.target.clone(),
        message: api.message.clone(),
        worker: api.worker,
    }
}

/// Built-in demo pipeline for TUI mode.
///
/// Simulates a real-time sensor aggregation pipeline: 5 sensors emit readings
/// every 150 ms, aggregated into 10-second tumbling windows using the
/// operators library.
async fn run_demo_pipeline(workers: usize) -> anyhow::Result<()> {
    use std::fmt;
    use std::marker::PhantomData;

    use async_trait::async_trait;
    use rill_core::operators::{Avg, TumblingWindow, WindowOutput};
    use rill_core::traits::{Sink, Source};
    use rill_runtime::dataflow::DataflowGraph;
    use rill_runtime::executor::Executor;
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
            // Pace the data so the TUI has something to watch
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

    /// Routes operator output into the TUI log viewer via `tracing::info!`.
    struct LoggingSink<T>(PhantomData<fn(T)>);

    #[async_trait]
    impl<T: Send + Sync + fmt::Display + 'static> Sink for LoggingSink<T> {
        type Input = T;

        async fn write(&mut self, input: T) -> anyhow::Result<()> {
            tracing::info!("{input}");
            Ok(())
        }
    }

    let dir = std::env::temp_dir().join("rill_tui_demo");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let op = TumblingWindow::builder()
        .window_size(10)
        .key_fn(|r: &SensorReading| r.sensor_id.clone())
        .time_fn(|r: &SensorReading| r.timestamp)
        .aggregator(Avg::new(|r: &SensorReading| r.value))
        .build();

    let graph = DataflowGraph::new();
    // 500 ticks × 150 ms ≈ 75 seconds of simulated sensor data
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

    let executor = Executor::builder()
        .checkpoint_dir(dir.clone())
        .workers(workers)
        .build();

    // Let the TUI render before data starts flowing
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tracing::info!("sensor aggregation pipeline started — 5 sensors, 10s tumbling windows");
    executor.run(graph).await?;
    tracing::info!("pipeline completed");

    // Keep alive so TUI stays up until user quits
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

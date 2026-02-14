use std::fs;
use std::path::Path;
use std::process::Command;

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
        Commands::Run { tui: true } => {
            let handles =
                rill_runtime::telemetry::init(rill_runtime::telemetry::TelemetryConfig {
                    metrics_addr: None,
                    log_filter: cli.log_level.clone(),
                    json_logs: false,
                    tui: true,
                })?;
            cmd_run_tui(handles)
        }
        Commands::Run { tui: false } => {
            let _telemetry =
                rill_runtime::telemetry::init(rill_runtime::telemetry::TelemetryConfig {
                    metrics_addr: None,
                    log_filter: cli.log_level.clone(),
                    json_logs: cli.json_logs,
                    tui: false,
                })?;
            cmd_run()
        }
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
use rill_runtime::executor::{Executor, OperatorSlot};

struct MyOperator;

#[async_trait]
impl StreamFunction for MyOperator {
    type Input = String;
    type Output = String;

    async fn process(&mut self, input: String, ctx: &mut StateContext) -> Vec<String> {
        // Your processing logic here
        vec![input]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let executor = Executor::new("./checkpoints".into());
    let ctx = executor.create_context("my_operator").await?;

    let mut source = VecSource::new(vec![
        "hello world".to_string(),
        "hello rill".to_string(),
    ]);

    let mut operators = vec![
        OperatorSlot::new("my_operator", MyOperator, ctx, Some(tokio::runtime::Handle::current())),
    ];

    let mut sink: PrintSink<String> = PrintSink::new();

    executor.run_linear(&mut source, &mut operators, &mut sink).await?;
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

fn cmd_run() -> anyhow::Result<()> {
    if !Path::new("Cargo.toml").exists() {
        anyhow::bail!("No Cargo.toml found. Are you in a Rill project directory?");
    }

    let status = Command::new("cargo").arg("run").status()?;

    if !status.success() {
        anyhow::bail!("cargo run failed with status: {status}");
    }

    Ok(())
}

fn cmd_run_tui(handles: rill_runtime::telemetry::TelemetryHandles) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        let metrics_rx = handles
            .metrics_rx
            .expect("TUI mode should provide metrics_rx");
        let log_rx = handles.log_rx.expect("TUI mode should provide log_rx");

        // Spawn a demo pipeline in the background
        let pipeline_handle = tokio::spawn(async {
            run_demo_pipeline().await
        });

        // Run the TUI on the main task
        let app = tui::TuiApp::new(metrics_rx, log_rx);
        let tui_result = app.run().await;

        // Cancel pipeline on TUI exit
        pipeline_handle.abort();

        tui_result
    })
}

/// Built-in demo pipeline for TUI mode (word count).
async fn run_demo_pipeline() -> anyhow::Result<()> {
    use async_trait::async_trait;
    use rill_core::connectors::print_sink::PrintSink;
    use rill_core::connectors::vec_source::VecSource;
    use rill_core::state::context::StateContext;
    use rill_core::traits::StreamFunction;
    use rill_runtime::executor::{Executor, OperatorSlot};

    struct WordCounter;

    #[async_trait]
    impl StreamFunction for WordCounter {
        type Input = String;
        type Output = String;

        async fn process(&mut self, input: String, ctx: &mut StateContext) -> Vec<String> {
            let mut outputs = Vec::new();
            for word in input.split_whitespace() {
                let key = word.as_bytes();
                let count = match ctx.get(key).await.unwrap_or(None) {
                    Some(bytes) => {
                        let n = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                        n + 1
                    }
                    None => 1,
                };
                ctx.put(key, &count.to_le_bytes());
                outputs.push(format!("{word}: {count}"));
            }
            outputs
        }
    }

    let dir = std::env::temp_dir().join("rill_tui_demo");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let executor = Executor::new(dir.clone());
    let ctx = executor.create_context("word_counter").await?;

    // Generate many batches so the pipeline runs for a while
    let mut data = Vec::new();
    for i in 0..100 {
        data.push(format!("hello world batch {i}"));
        data.push(format!("rill stream processing round {i}"));
    }

    let mut source = VecSource::new(data);
    let mut operators = vec![OperatorSlot::new(
        "word_counter",
        WordCounter,
        ctx,
        Some(tokio::runtime::Handle::current()),
    )];
    let mut sink: PrintSink<String> = PrintSink::new();

    // Small delay so TUI renders before pipeline starts
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    tracing::info!("demo pipeline starting");
    executor
        .run_linear(&mut source, &mut operators, &mut sink)
        .await?;
    tracing::info!("demo pipeline completed");

    // Keep alive so TUI stays up until user quits
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

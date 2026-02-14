use std::fs;
use std::path::Path;
use std::process::Command;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "rill", about = "Rill stream processing CLI")]
struct Cli {
    /// Emit logs as JSON
    #[arg(long, global = true)]
    json_logs: bool,

    /// Log level filter (e.g. "info", "debug", "rill_core=trace")
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
    Run,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    rill_runtime::telemetry::init(rill_runtime::telemetry::TelemetryConfig {
        metrics_addr: None,
        log_filter: cli.log_level.clone(),
        json_logs: cli.json_logs,
    })?;

    match cli.command {
        Commands::New { name } => cmd_new(&name),
        Commands::Run => cmd_run(),
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
        OperatorSlot::new("my_operator", MyOperator, ctx),
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

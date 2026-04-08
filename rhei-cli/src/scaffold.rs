//! Project scaffolding for `rhei new`.
//!
//! Generates a ready-to-run Rhei pipeline project with sensible defaults,
//! a working `main.rs`, configuration file, and a passing test.

use std::fs;
use std::path::Path;

/// Rust keywords that cannot be used as crate names.
const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn", "for",
    "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return",
    "self", "static", "struct", "super", "trait", "true", "type", "unsafe", "use", "where",
    "while", "async", "await", "dyn",
];

/// Create a new Rhei pipeline project.
///
/// Generates:
/// - `Cargo.toml` with `rhei` dependency
/// - `src/main.rs` with a minimal working pipeline
/// - `pipeline.toml` with development defaults
///
/// # Errors
///
/// Returns an error if the directory already exists, the project name
/// is not a valid Rust crate name, or any file I/O fails.
pub fn create_project(name: &str, parent: Option<&Path>) -> anyhow::Result<()> {
    validate_name(name)?;

    let base = parent.unwrap_or_else(|| Path::new("."));
    let project_dir = base.join(name);

    if project_dir.exists() {
        anyhow::bail!(
            "directory '{}' already exists\n\
             hint: choose a different name or remove the existing directory",
            project_dir.display()
        );
    }

    let src_dir = project_dir.join("src");
    fs::create_dir_all(&src_dir).map_err(|e| {
        anyhow::anyhow!(
            "failed to create project directory '{}': {e}",
            src_dir.display()
        )
    })?;

    // Write all project files
    write_file(&project_dir.join("Cargo.toml"), &cargo_toml(name))?;
    write_file(&src_dir.join("main.rs"), MAIN_RS)?;
    write_file(&project_dir.join("pipeline.toml"), &pipeline_toml(name))?;

    eprintln!("Created new Rhei project: {}", project_dir.display());
    eprintln!();
    eprintln!("  cd {name}");
    eprintln!("  cargo run");
    eprintln!();
    eprintln!("Or with the TUI dashboard:");
    eprintln!("  rhei run --tui");

    Ok(())
}

/// Validate that the name is a legal Rust crate name.
fn validate_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("project name cannot be empty");
    }

    // Must start with a letter or underscore
    let first = name.chars().next().unwrap_or('0');
    if !first.is_ascii_alphabetic() && first != '_' {
        anyhow::bail!(
            "project name '{name}' must start with a letter or underscore\n\
             hint: try '_{name}'"
        );
    }

    // Must contain only alphanumeric, underscore, or hyphen
    if let Some(bad) = name
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '_' && *c != '-')
    {
        anyhow::bail!(
            "project name '{name}' contains invalid character '{bad}'\n\
             hint: use only letters, digits, underscores, and hyphens"
        );
    }

    // Cannot be a Rust keyword
    if RUST_KEYWORDS.contains(&name) {
        anyhow::bail!(
            "project name '{name}' is a Rust keyword\n\
             hint: try '{name}_pipeline' or 'my_{name}'"
        );
    }

    Ok(())
}

fn write_file(path: &Path, content: &str) -> anyhow::Result<()> {
    fs::write(path, content)
        .map_err(|e| anyhow::anyhow!("failed to write '{}': {e}", path.display()))
}

fn cargo_toml(name: &str) -> String {
    format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2024"

[dependencies]
rhei-core = {{ version = "0.1", features = [] }}
rhei-runtime = "0.1"
anyhow = "1"
async-trait = "0.1"
serde = {{ version = "1", features = ["derive"] }}
tokio = {{ version = "1", features = ["full"] }}

# Uncomment for Kafka connectors:
# [dependencies.rhei-core]
# version = "0.1"
# features = ["kafka"]
"#
    )
}

fn pipeline_toml(name: &str) -> String {
    format!(
        r#"# Rhei pipeline configuration
# Environment variables override these values (e.g. RHEI_WORKERS=4)

[pipeline]
name = "{name}"
workers = 1
checkpoint_dir = "./checkpoints"

[metrics]
# addr = "0.0.0.0:9090"  # Uncomment to enable the HTTP metrics server
log_level = "info"
"#
    )
}

const MAIN_RS: &str = r#"use rhei_core::connectors::print_sink::PrintSink;
use rhei_core::connectors::vec_source::VecSource;
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::Executor;

/// A minimal Rhei streaming pipeline.
///
/// This example reads strings from an in-memory source, transforms them,
/// and prints the results. Replace `VecSource` with `KafkaSource` for
/// production use.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let events = vec![
        "sensor-1:23.5".to_string(),
        "sensor-2:18.0".to_string(),
        "sensor-1:24.1".to_string(),
        "sensor-2:17.8".to_string(),
    ];

    let graph = DataflowGraph::new();
    graph
        .source(VecSource::new(events))
        // Parse each line into a structured format
        .map(|line: String| {
            let mut parts = line.splitn(2, ':');
            let sensor = parts.next().unwrap_or("unknown").to_string();
            let value = parts.next().unwrap_or("0").to_string();
            format!("[{sensor}] reading = {value}")
        })
        .sink(PrintSink::<String>::new());

    let executor = Executor::builder()
        .checkpoint_dir("./checkpoints")
        .build()?;

    executor.run(graph).await?;
    Ok(())
}
"#;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn valid_names() {
        assert!(validate_name("my-pipeline").is_ok());
        assert!(validate_name("sensor_ingest").is_ok());
        assert!(validate_name("app1").is_ok());
        assert!(validate_name("_private").is_ok());
    }

    #[test]
    fn invalid_names() {
        assert!(validate_name("").is_err());
        assert!(validate_name("123bad").is_err());
        assert!(validate_name("has space").is_err());
        assert!(validate_name("fn").is_err());
        assert!(validate_name("async").is_err());
    }

    #[test]
    fn scaffold_creates_files() {
        let dir = std::env::temp_dir().join("rhei_scaffold_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        create_project("test-pipeline", Some(&dir)).unwrap();

        let project = dir.join("test-pipeline");
        assert!(project.join("Cargo.toml").exists());
        assert!(project.join("src/main.rs").exists());
        assert!(project.join("pipeline.toml").exists());

        // Verify Cargo.toml content
        let cargo = std::fs::read_to_string(project.join("Cargo.toml")).unwrap();
        assert!(cargo.contains("name = \"test-pipeline\""));
        assert!(cargo.contains("rhei"));

        // Verify pipeline.toml content
        let config = std::fs::read_to_string(project.join("pipeline.toml")).unwrap();
        assert!(config.contains("name = \"test-pipeline\""));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scaffold_rejects_existing_dir() {
        let dir = std::env::temp_dir().join("rhei_scaffold_exists_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("my-project")).unwrap();

        let result = create_project("my-project", Some(&dir));
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("already exists"),
            "error should mention existing directory"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

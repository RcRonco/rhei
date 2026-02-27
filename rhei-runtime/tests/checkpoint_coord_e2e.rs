//! Cross-process checkpoint coordination E2E test.
//!
//! Validates that coordinated checkpointing works across 2 OS processes, each
//! running 2 Timely workers (4 workers total), connected over TCP.
//!
//! Pipeline topology:
//! ```text
//! VecSource(words) → key_by(first_char) → StatefulCounter → FileSink
//! ```
//!
//! Self-spawning pattern: the test binary re-invokes itself. When
//! `RHEI_COORD_WORKER` is not set, the orchestrator runs; otherwise the
//! worker process runs.

use std::collections::HashMap;
use std::time::Duration;

use rill_core::checkpoint::CheckpointManifest;

// ── Port allocation ─────────────────────────────────────────────────

/// Allocate `n` free TCP ports by binding to :0, collecting ports, then
/// dropping all listeners. TOCTOU race is acceptable for CI.
fn allocate_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port"))
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    drop(listeners);
    ports
}

// ── Test entry point ────────────────────────────────────────────────

#[tokio::test]
async fn checkpoint_coord_e2e() {
    if std::env::var("RHEI_COORD_WORKER").is_ok() {
        worker_main().await;
    } else {
        orchestrator_main();
    }
}

// ── Orchestrator ────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn orchestrator_main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // ── Allocate TCP ports for Timely cluster ───────────────────────
    let ports = allocate_ports(2);
    let peers = format!("127.0.0.1:{},127.0.0.1:{}", ports[0], ports[1]);

    // ── Prepare output directory ────────────────────────────────────
    let output_dir = tempfile::tempdir().unwrap();
    let output_p0 = output_dir.path().join("output_p0.jsonl");
    let output_p1 = output_dir.path().join("output_p1.jsonl");
    let checkpoint_dir = tempfile::tempdir().unwrap();

    // ── Spawn 2 child processes ─────────────────────────────────────
    let test_exe = std::env::current_exe().expect("failed to get current exe");

    let mut children = Vec::new();
    for pid in 0..2u32 {
        let output_path = if pid == 0 { &output_p0 } else { &output_p1 };

        let child = std::process::Command::new(&test_exe)
            .args(["--exact", "checkpoint_coord_e2e", "--nocapture"])
            .env("RHEI_COORD_WORKER", "1")
            .env("RHEI_PROCESS_ID", pid.to_string())
            .env("RHEI_PEERS", &peers)
            .env("RHEI_WORKERS", "2")
            .env("RHEI_OUTPUT_PATH", output_path)
            .env("RHEI_CHECKPOINT_DIR", checkpoint_dir.path())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn child process {pid}: {e}"));

        children.push((pid, child));
    }

    // ── Wait for children with timeout ──────────────────────────────
    let timeout = Duration::from_secs(30);
    let deadline = std::time::Instant::now() + timeout;

    for (pid, mut child) in children {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .unwrap_or(Duration::ZERO);

        let status = wait_with_timeout(&mut child, remaining);
        match status {
            Some(s) if s.success() => {
                eprintln!("child process {pid} exited successfully");
            }
            Some(s) => {
                panic!("child process {pid} exited with status: {s}");
            }
            None => {
                let _ = child.kill();
                panic!("child process {pid} timed out after {timeout:?}");
            }
        }
    }

    // ── Verify output ───────────────────────────────────────────────
    let p0_output = std::fs::read_to_string(&output_p0).unwrap_or_default();
    let p1_output = std::fs::read_to_string(&output_p1).unwrap_or_default();

    // FileSink<String> writes JSON-encoded strings, so parse each line as JSON.
    let p0_lines: Vec<String> = p0_output
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|line| serde_json::from_str::<String>(line).ok())
        .collect();
    let p1_lines: Vec<String> = p1_output
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|line| serde_json::from_str::<String>(line).ok())
        .collect();

    let all_lines: Vec<&str> = p0_lines
        .iter()
        .chain(p1_lines.iter())
        .map(String::as_str)
        .collect();

    // 1. Both processes produced output (proves cross-process exchange worked)
    assert!(
        !p0_lines.is_empty(),
        "process 0 produced no output — exchange may not be working"
    );
    assert!(
        !p1_lines.is_empty(),
        "process 1 produced no output — exchange may not be working"
    );

    // 2. Parse output as "key:count" and verify counts
    let mut final_counts: HashMap<String, u64> = HashMap::new();
    for line in &all_lines {
        if let Some((key, count_str)) = line.rsplit_once(':')
            && let Ok(count) = count_str.parse::<u64>()
        {
            let entry = final_counts.entry(key.to_string()).or_insert(0);
            *entry = (*entry).max(count);
        }
    }
    assert!(
        !final_counts.is_empty(),
        "no valid key:count output found in combined output"
    );

    // 3. Both partial manifests exist
    let partial_p0 = CheckpointManifest::load_partial(checkpoint_dir.path(), 0);
    let partial_p1 = CheckpointManifest::load_partial(checkpoint_dir.path(), 1);

    assert!(
        partial_p0.is_some(),
        "process 0 partial manifest should exist"
    );
    assert!(
        partial_p1.is_some(),
        "process 1 partial manifest should exist"
    );

    // 4. Merged manifest exists with correct data
    let merged = CheckpointManifest::load(checkpoint_dir.path());
    assert!(merged.is_some(), "merged manifest should exist");
    let merged = merged.unwrap();
    assert!(merged.checkpoint_id > 0, "checkpoint_id should be positive");
    assert!(
        !merged.operators.is_empty(),
        "operators list should not be empty"
    );

    eprintln!(
        "Coord E2E passed: p0={} lines, p1={} lines, {} unique keys, checkpoint_id={}",
        p0_lines.len(),
        p1_lines.len(),
        final_counts.len(),
        merged.checkpoint_id,
    );
}

/// Wait for a child process with a timeout. Returns `None` on timeout.
fn wait_with_timeout(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Option<std::process::ExitStatus> {
    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {
                if start.elapsed() >= timeout {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("error waiting for child process: {e}"),
        }
    }
}

// ── Worker process ──────────────────────────────────────────────────

// ── Stateful counter operator ──────────────────────────────────

#[derive(Clone)]
struct CharCounter;

#[async_trait::async_trait]
impl rill_core::traits::StreamFunction for CharCounter {
    type Input = String;
    type Output = String;

    async fn process(
        &mut self,
        input: String,
        ctx: &mut rill_core::state::context::StateContext,
    ) -> anyhow::Result<Vec<String>> {
        let key = input.as_bytes();
        let count = match ctx.get(key).await? {
            Some(bytes) => {
                let n = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                n + 1
            }
            None => 1,
        };
        ctx.put(key, &count.to_le_bytes());
        Ok(vec![format!("{input}:{count}")])
    }
}

async fn worker_main() {
    use rill_core::connectors::file_sink::FileSink;
    use rill_core::connectors::partitioned_vec_source::PartitionedVecSource;
    use rill_runtime::dataflow::DataflowGraph;
    use rill_runtime::executor::Executor;

    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let output_path = std::path::PathBuf::from(
        std::env::var("RHEI_OUTPUT_PATH").expect("RHEI_OUTPUT_PATH not set"),
    );
    let checkpoint_dir = std::path::PathBuf::from(
        std::env::var("RHEI_CHECKPOINT_DIR").expect("RHEI_CHECKPOINT_DIR not set"),
    );
    let process_id: usize = std::env::var("RHEI_PROCESS_ID")
        .expect("RHEI_PROCESS_ID not set")
        .parse()
        .expect("RHEI_PROCESS_ID not a number");

    eprintln!("coord worker process {process_id} starting");

    // ── Build pipeline ─────────────────────────────────────────────
    // Generate words keyed by first character (26 distinct keys).
    // Use PartitionedVecSource with 4 partitions (matching total workers)
    // so that all processes produce data and register the AnyItem type.
    let words: Vec<String> = ('a'..='z')
        .flat_map(|c| (0..5).map(move |i| format!("{c}{i}")))
        .collect();

    let source = PartitionedVecSource::new(words, 4);
    let file_sink = FileSink::<String>::new(&output_path).unwrap();

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .key_by(|w: &String| w.chars().next().unwrap_or('_').to_string())
        .operator("char_counter", CharCounter)
        .sink(file_sink);

    // ── Run ─────────────────────────────────────────────────────────
    // All processes share the same checkpoint directory so that partial
    // manifests (manifest_p0.json, manifest_p1.json) are co-located for merging.
    std::fs::create_dir_all(&checkpoint_dir).ok();

    let executor = Executor::builder()
        .checkpoint_dir(&checkpoint_dir)
        .from_env()
        .build();

    eprintln!(
        "coord worker process {process_id}: cluster={}, total_workers={}, local_range={:?}",
        executor.is_cluster(),
        executor.total_workers(),
        executor.local_worker_range(),
    );

    executor.run(graph).await.unwrap();
    eprintln!("coord worker process {process_id} finished");
}

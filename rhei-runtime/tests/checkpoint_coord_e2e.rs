#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Cross-process checkpoint coordination E2E test.
//!
//! Validates that coordinated checkpointing works across 2 OS processes, each
//! running 2 Timely workers (4 workers total), connected over TCP.
//!
//! Pipeline topology:
//! ```text
//! PartitionedVecSource(words) → key_by(first_char) → CharCounter → JsonFileSink
//! ```
//!
//! Self-spawning pattern: the test binary re-invokes itself. When
//! `RHEI_COORD_WORKER` is not set, the orchestrator runs; otherwise the
//! worker process runs.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use rhei_core::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, Sink, StreamFunction,
};
use rhei_core::checkpoint::CheckpointManifest;
use rhei_core::connectors::batch::PartitionedVecSource;
use rhei_core::operators::keyed_state::KeyedState;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Schema: WordEvent ───────────────────────────────────────────────

#[derive(Clone)]
struct WordEvent {
    word: String,
}

struct WordEventBuilder {
    word: arrow_array::builder::StringBuilder,
}

struct WordEventView<'a> {
    word: &'a str,
}

struct WordEventColumns<'a> {
    #[allow(dead_code)]
    word: &'a arrow_array::StringArray,
}

impl RheiBuilder for WordEventBuilder {
    type Item = WordEvent;

    fn append(&mut self, item: WordEvent) {
        self.word.append_value(&item.word);
    }

    fn append_null(&mut self) {
        self.word.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.word)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        #[allow(clippy::expect_used)]
        arrow_array::RecordBatch::try_new(
            WordEvent::arrow_schema(),
            vec![Arc::new(self.word.finish())],
        )
        .expect("infallible schema")
    }
}

impl RheiSchema for WordEvent {
    type Builder = WordEventBuilder;
    type View<'a> = WordEventView<'a>;
    type Columns<'a> = WordEventColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "word",
            arrow_schema::DataType::Utf8,
            false,
        )]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        WordEventBuilder {
            word: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        WordEventView {
            word: batch.column(0).as_string::<i32>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        WordEventColumns {
            word: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Schema: WordCount output ────────────────────────────────────────

struct WordCount {
    text: String,
}

struct WordCountBuilder {
    text: arrow_array::builder::StringBuilder,
}

struct WordCountView<'a> {
    text: &'a str,
}

struct WordCountColumns<'a> {
    #[allow(dead_code)]
    text: &'a arrow_array::StringArray,
}

impl RheiBuilder for WordCountBuilder {
    type Item = WordCount;

    fn append(&mut self, item: WordCount) {
        self.text.append_value(&item.text);
    }

    fn append_null(&mut self) {
        self.text.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.text)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        #[allow(clippy::expect_used)]
        arrow_array::RecordBatch::try_new(
            WordCount::arrow_schema(),
            vec![Arc::new(self.text.finish())],
        )
        .expect("infallible schema")
    }
}

impl RheiSchema for WordCount {
    type Builder = WordCountBuilder;
    type View<'a> = WordCountView<'a>;
    type Columns<'a> = WordCountColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            arrow_schema::DataType::Utf8,
            false,
        )]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        WordCountBuilder {
            text: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        WordCountView {
            text: batch.column(0).as_string::<i32>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        WordCountColumns {
            text: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Stateful operator: CharCounter ─────────────────────────────────

#[derive(Clone)]
struct CharCounter;

#[async_trait]
impl StreamFunction for CharCounter {
    type Input = WordEvent;
    type Output = WordCount;

    async fn process(
        &mut self,
        input: RheiBuffer<WordEvent>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<WordCount>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }

        let words: Vec<String> = input.iter().map(|v| v.word.to_string()).collect();
        let mut builder = WordCount::builder(words.len());

        for word in words {
            let count: u64 = {
                let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "counts");
                state.get(&word).await?.unwrap_or(0)
            };
            let new_count = count + 1;
            {
                let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "counts");
                state.put(&word, &new_count)?;
            }
            builder.append(WordCount {
                text: format!("{word}:{new_count}"),
            });
        }

        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── File-writing sink (replaces the removed FileSink<String>) ───────

struct JsonFileSink {
    path: std::path::PathBuf,
    file: Option<std::fs::File>,
}

impl JsonFileSink {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path, file: None }
    }
}

#[async_trait]
impl Sink for JsonFileSink {
    type Input = WordCount;

    async fn write_batch(&mut self, input: RheiBuffer<WordCount>) -> anyhow::Result<()> {
        use std::io::Write;
        let file = self.file.get_or_insert_with(|| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .expect("failed to open output file")
        });
        for view in &input {
            writeln!(file, "{}", serde_json::to_string(view.text).unwrap())?;
        }
        Ok(())
    }
}

// ── Port allocation ─────────────────────────────────────────────────

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

    let ports = allocate_ports(2);
    let peers = format!("127.0.0.1:{},127.0.0.1:{}", ports[0], ports[1]);

    let output_dir = tempfile::tempdir().unwrap();
    let output_p0 = output_dir.path().join("output_p0.jsonl");
    let output_p1 = output_dir.path().join("output_p1.jsonl");
    let checkpoint_dir = tempfile::tempdir().unwrap();

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

    // Wait for children with timeout
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

    assert!(
        !p0_lines.is_empty(),
        "process 0 produced no output — exchange may not be working"
    );
    assert!(
        !p1_lines.is_empty(),
        "process 1 produced no output — exchange may not be working"
    );

    // Parse output as "key:count" and verify counts
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

    // Both partial manifests exist
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

    // Merged manifest exists
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

async fn worker_main() {
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

    // Generate words keyed by first character (26 distinct keys).
    // Use PartitionedVecSource with 4 partitions (matching total workers).
    let words: Vec<WordEvent> = ('a'..='z')
        .flat_map(|c| (0..5).map(move |i| WordEvent { word: format!("{c}{i}") }))
        .collect();

    let source = PartitionedVecSource::new(words, 4);
    let file_sink = JsonFileSink::new(output_path);

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .key_by(|w: &WordEventView<'_>| w.word.chars().next().unwrap_or('_').to_string())
        .operator("char_counter", CharCounter)
        .sink(file_sink);

    std::fs::create_dir_all(&checkpoint_dir).ok();

    let ctrl = PipelineController::builder()
        .checkpoint_dir(&checkpoint_dir)
        .from_env()
        .build()
        .unwrap();

    eprintln!(
        "coord worker process {process_id}: cluster={}, total_workers={}, local_range={:?}",
        ctrl.is_cluster(),
        ctrl.total_workers(),
        ctrl.local_worker_range(),
    );

    ctrl.run(graph).await.unwrap();
    eprintln!("coord worker process {process_id} finished");
}

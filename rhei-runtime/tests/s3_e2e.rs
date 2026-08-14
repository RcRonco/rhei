#![cfg(feature = "remote-state")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! End-to-end S3 tiered storage test (batch API).
//!
//! Exercises the full L1 memtable → L2 Foyer → L3 SlateDB/S3 storage path
//! against a real S3-compatible service (`MinIO`).
//!
//! Pipeline topology:
//!
//! ```text
//! VecSource(words) → WordCounter(KeyedState) → CollectSink
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use rhei_core::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, Sink, StreamFunction,
};
use rhei_core::cluster::DEFAULT_MAX_PARALLELISM;
use rhei_core::connectors::batch::VecSource;
use rhei_core::operators::keyed_state::KeyedState;
use rhei_core::state::backend::{BatchOp, StateBackend};
use rhei_core::state::key_group_addressing::keyed_physical_key;
use rhei_core::state::slatedb_backend::SlateDbBackend;
use rhei_core::state::tiered_backend::TieredBackendConfig;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Helpers ────────────────────────────────────────────────────────

fn env_or(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

fn unique_path(prefix: &str) -> String {
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    )
}

fn build_s3_store() -> Arc<dyn ObjectStore> {
    let s3 = AmazonS3Builder::new()
        .with_endpoint(env_or("S3_ENDPOINT", "http://localhost:9000"))
        .with_bucket_name(env_or("S3_BUCKET", "rhei-test"))
        .with_access_key_id(env_or("S3_ACCESS_KEY", "minioadmin"))
        .with_secret_access_key(env_or("S3_SECRET_KEY", "minioadmin"))
        .with_region(env_or("S3_REGION", "us-east-1"))
        .with_allow_http(true)
        .build()
        .expect("failed to build S3 object store");
    Arc::new(s3)
}

// ── Schema types ───────────────────────────────────────────────────

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
        arrow_array::RecordBatch::try_new(
            WordEvent::arrow_schema(),
            vec![Arc::new(self.word.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for WordEvent {
    type Builder = WordEventBuilder;
    type View<'a> = WordEventView<'a>;
    type Columns<'a> = WordEventColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
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

struct WordCount {
    word: String,
    count: u64,
}

struct WordCountBuilder {
    word: arrow_array::builder::StringBuilder,
    count: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
}

#[derive(Debug, Clone)]
struct WordCountView<'a> {
    word: &'a str,
    count: u64,
}

struct WordCountColumns<'a> {
    #[allow(dead_code)]
    word: &'a arrow_array::StringArray,
    #[allow(dead_code)]
    count: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
}

impl RheiBuilder for WordCountBuilder {
    type Item = WordCount;

    fn append(&mut self, item: WordCount) {
        self.word.append_value(&item.word);
        self.count.append_value(item.count);
    }

    fn append_null(&mut self) {
        self.word.append_null();
        self.count.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.word)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            WordCount::arrow_schema(),
            vec![Arc::new(self.word.finish()), Arc::new(self.count.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for WordCount {
    type Builder = WordCountBuilder;
    type View<'a> = WordCountView<'a>;
    type Columns<'a> = WordCountColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("word", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("count", arrow_schema::DataType::UInt64, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        WordCountBuilder {
            word: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            count: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        WordCountView {
            word: batch.column(0).as_string::<i32>().value(index),
            count: batch.column(1).as_primitive::<UInt64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        WordCountColumns {
            word: batch.column(0).as_string::<i32>(),
            count: batch.column(1).as_primitive::<UInt64Type>(),
        }
    }
}

// ── Stateful operator ──────────────────────────────────────────────

#[derive(Clone)]
struct WordCounter;

#[async_trait]
impl StreamFunction for WordCounter {
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
        let mut outputs = Vec::with_capacity(words.len());

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
            outputs.push(WordCount {
                word,
                count: new_count,
            });
        }

        let mut builder = WordCount::builder(outputs.len());
        for item in outputs {
            builder.append(item);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── Collecting sink ────────────────────────────────────────────────

struct CollectSink {
    collected: Arc<Mutex<Vec<(String, u64)>>>,
}

#[async_trait]
impl Sink for CollectSink {
    type Input = WordCount;

    async fn write_batch(&mut self, input: RheiBuffer<WordCount>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.word.to_string(), view.count));
        }
        Ok(())
    }
}

// ── Word generation ────────────────────────────────────────────────

fn generate_words() -> Vec<WordEvent> {
    let base_words = [
        "alpha", "arden", "arrow", "azure", "apex", "beta", "blaze", "brook", "bright", "brine",
        "cedar", "cliff", "coral", "crest", "crane", "delta", "drift", "dusk", "dawn", "depth",
        "ember", "echo", "edge", "elm", "earth", "frost", "fern", "flame", "field", "forge",
        "gale", "grove", "gleam", "grain", "gate", "haze", "helm", "haven", "husk", "hilt", "iris",
        "isle", "iron", "ivory", "inlet", "jade", "jest", "jetty", "jewel", "junco", "knoll",
        "kelp", "kern", "kite", "knot", "lark", "leaf", "lodge", "lumen", "lyric",
    ];
    let mut words = Vec::new();
    for &w in &base_words {
        for _ in 0..5 {
            words.push(WordEvent {
                word: w.to_string(),
            });
        }
    }
    words
}

fn expected_counts(words: &[WordEvent]) -> HashMap<String, u64> {
    let mut counts = HashMap::new();
    for w in words {
        *counts.entry(w.word.clone()).or_insert(0u64) += 1;
    }
    counts
}

// ── Test ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn s3_tiered_storage_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let s3_store = build_s3_store();
    let slate_path = unique_path("s3_e2e");

    // ── 1. Open SlateDB on S3 ──────────────────────────────────────
    let l3 = Arc::new(
        SlateDbBackend::open(slate_path.as_str(), s3_store.clone())
            .await
            .expect("failed to open SlateDB on S3"),
    );

    // ── 2. Configure tiered storage ────────────────────────────────
    let foyer_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();

    let foyer_config = TieredBackendConfig {
        foyer_dir: foyer_dir.path().to_path_buf(),
        foyer_memory_capacity: 4 * 1024 * 1024,
        foyer_disk_capacity: 16 * 1024 * 1024,
        foyer_block_size: 256 * 1024,
    };

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf())
        .with_workers(1)
        .with_tiered_storage(
            checkpoint_dir.path().to_path_buf(),
            l3.clone(),
            foyer_config,
        )
        .await
        .unwrap();

    // ── 3. Build pipeline ──────────────────────────────────────────
    let words = generate_words();
    let expected = expected_counts(&words);
    let collected = Arc::new(Mutex::new(Vec::<(String, u64)>::new()));

    let source = VecSource::new(words).with_batch_size(50);

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("word_counter", WordCounter)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    // ── 4. Run pipeline ────────────────────────────────────────────
    ctrl.run(graph).await.expect("pipeline execution failed");

    // ── 5. Verify output ───────────────────────────────────────────
    let results = collected.lock().unwrap().clone();
    assert!(!results.is_empty(), "pipeline produced no output");

    // Find the final (max) count per word from all emitted outputs.
    let mut final_counts: HashMap<String, u64> = HashMap::new();
    for (word, count) in &results {
        let entry = final_counts.entry(word.clone()).or_insert(0);
        *entry = (*entry).max(*count);
    }

    for (word, expected_count) in &expected {
        let actual = final_counts.get(word).copied().unwrap_or(0);
        assert_eq!(
            actual, *expected_count,
            "count mismatch for {word}: actual={actual}, expected={expected_count}"
        );
    }

    eprintln!(
        "Output verified: {} results, {} distinct words",
        results.len(),
        final_counts.len()
    );

    // ── 6. Close L3 to flush pending writes ────────────────────────
    l3.close().await.expect("failed to close SlateDB");

    // ── 7. Verify S3 persistence ───────────────────────────────────
    let l3_verify = SlateDbBackend::open(slate_path.as_str(), s3_store.clone())
        .await
        .expect("failed to reopen SlateDB on S3");

    let verify_words = ["alpha", "beta", "cedar", "delta", "ember", "frost"];
    for word in &verify_words {
        let val = l3_verify
            .get(&state_key(word))
            .await
            .unwrap_or_else(|e| panic!("failed to read key for {word}: {e}"));

        assert!(
            val.is_some(),
            "key for '{word}' not found in S3 — state not persisted"
        );

        let bytes = val.unwrap();
        let count: u64 =
            serde_json::from_slice(&bytes).expect("failed to deserialize persisted count");
        assert_eq!(
            count, 5,
            "persisted count for '{word}': actual={count}, expected=5"
        );
    }

    eprintln!("S3 persistence verified for {} words", verify_words.len());
    l3_verify.close().await.expect("failed to close verify DB");

    // ── 8. Verify S3 objects exist ─────────────────────────────────
    let obj_prefix = object_store::path::Path::from(slate_path.clone());
    let objects: Vec<_> = s3_store
        .list(Some(&obj_prefix))
        .try_collect()
        .await
        .expect("failed to list S3 objects");

    assert!(
        !objects.is_empty(),
        "no objects found in S3 under '{slate_path}' — SlateDB did not write to S3"
    );

    eprintln!(
        "S3 objects verified: {} objects under '{slate_path}'",
        objects.len()
    );
}

// ════════════════════════════════════════════════════════════════════
//  Extended coverage: restart recovery, multi-worker, spill, direct ops
// ════════════════════════════════════════════════════════════════════

// ── Shared helpers ──────────────────────────────────────────────────

/// Compute the raw `SlateDB` key under which `KeyedState<String, u64>` (in the
/// `"counts"` namespace) stores `word`.
///
/// The `KeyedState` namespace (`counts:` + JSON-encoded key) is the *storage*
/// key; the word itself is the partition key, and it alone decides the key
/// group — the same bytes `key_by` hashed to route the row here. Deriving the
/// physical layout here by hand is what made this helper go stale once before,
/// so it defers to the single definition in `rhei-core`.
///
/// Note there is no worker index: keyed state is worker-independent by design,
/// which is exactly what lets ownership move on a rescale without relocating
/// bytes.
fn state_key(word: &str) -> Vec<u8> {
    let storage_key = format!("counts:{}", serde_json::to_string(word).unwrap());
    keyed_physical_key(
        "word_counter",
        DEFAULT_MAX_PARALLELISM,
        word.as_bytes(),
        storage_key.as_bytes(),
    )
}

/// Read the persisted count for `word` directly from L3.
async fn read_count(l3: &SlateDbBackend, word: &str) -> Option<u64> {
    l3.get(&state_key(word))
        .await
        .unwrap()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
}

/// Final (max) count per word across every emitted output row.
fn final_counts(results: &[(String, u64)]) -> HashMap<String, u64> {
    let mut counts: HashMap<String, u64> = HashMap::new();
    for (word, count) in results {
        let entry = counts.entry(word.clone()).or_insert(0);
        *entry = (*entry).max(*count);
    }
    counts
}

/// `n_keys` distinct keys, each repeated `reps` times and interleaved so the
/// same key recurs throughout the stream — forcing reads back through L2/L3
/// after the small in-memory tier evicts them.
fn generate_repeated_unique(n_keys: usize, reps: usize) -> Vec<WordEvent> {
    let mut words = Vec::with_capacity(n_keys * reps);
    for _ in 0..reps {
        for i in 0..n_keys {
            words.push(WordEvent {
                word: format!("key_{i:06}"),
            });
        }
    }
    words
}

/// Run `VecSource(words) → [key_by] → WordCounter → CollectSink` over a tiered
/// backend rooted at `slate_path` on S3, then close L3 and return every emitted
/// `(word, running_count)` pair.
///
/// Reopening the same `slate_path` across calls exercises L3 recovery: a fresh
/// process has empty L1/L2 tiers and must read prior state back from S3.
async fn run_word_count_tiered(
    s3: &Arc<dyn ObjectStore>,
    slate_path: &str,
    words: Vec<WordEvent>,
    workers: usize,
    foyer_memory_capacity: usize,
) -> Vec<(String, u64)> {
    let l3 = Arc::new(
        SlateDbBackend::open(slate_path, s3.clone())
            .await
            .expect("failed to open SlateDB on S3"),
    );

    let foyer_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let foyer_config = TieredBackendConfig {
        foyer_dir: foyer_dir.path().to_path_buf(),
        foyer_memory_capacity,
        foyer_disk_capacity: 16 * 1024 * 1024,
        foyer_block_size: 256 * 1024,
    };

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf())
        .with_workers(workers)
        .with_tiered_storage(
            checkpoint_dir.path().to_path_buf(),
            l3.clone(),
            foyer_config,
        )
        .await
        .unwrap();

    let collected = Arc::new(Mutex::new(Vec::<(String, u64)>::new()));
    let source = VecSource::new(words).with_batch_size(50);
    let graph = DataflowGraph::new();
    let counted = if workers > 1 {
        graph
            .source(source)
            .key_by(|v: &WordEventView<'_>| v.word.to_string())
            .operator("word_counter", WordCounter)
    } else {
        graph.source(source).operator("word_counter", WordCounter)
    };
    counted.sink(CollectSink {
        collected: collected.clone(),
    });

    ctrl.run(graph).await.expect("pipeline execution failed");
    l3.close().await.expect("failed to close SlateDB");

    collected.lock().unwrap().clone()
}

// ── Test: state recovers across a restart (L3 read-through) ──────────

/// Two consecutive pipeline runs sharing one `SlateDB` path must accumulate
/// state: run 2 starts with empty L1/L2 tiers and reads run 1's counts back
/// from S3, so every word ends at twice its single-pass count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_state_recovers_across_restart() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let s3 = build_s3_store();
    let slate_path = unique_path("s3_restart");

    // Run 1 establishes counts of 5 per word in S3.
    let _ = run_word_count_tiered(&s3, &slate_path, generate_words(), 1, 4 * 1024 * 1024).await;

    // Run 2 reopens the same path; counts must continue from L3.
    let run2 = run_word_count_tiered(&s3, &slate_path, generate_words(), 1, 4 * 1024 * 1024).await;
    assert!(!run2.is_empty(), "run 2 produced no output");

    let finals = final_counts(&run2);
    let expected = expected_counts(&generate_words());
    for (word, base) in &expected {
        let actual = finals.get(word).copied().unwrap_or(0);
        assert_eq!(
            actual,
            base * 2,
            "word {word} should accumulate across restart: actual={actual}, expected={}",
            base * 2
        );
    }

    // Verify the durable S3 state directly.
    let l3 = SlateDbBackend::open(slate_path.as_str(), s3.clone())
        .await
        .expect("reopen for verification");
    for word in ["alpha", "beta", "cedar", "delta"] {
        assert_eq!(
            read_count(&l3, word).await,
            Some(10),
            "S3 count for '{word}' after restart"
        );
    }
    l3.close().await.expect("close verify DB");

    eprintln!(
        "S3 restart recovery verified: {} words at 2x count",
        expected.len()
    );
}

// ── Test: multi-worker tiered persistence ───────────────────────────

/// With 2 workers and a `key_by`, each word is owned by exactly one worker, and
/// every worker's slice of state must persist to S3 under its own prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_tiered_multi_worker_persistence() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let s3 = build_s3_store();
    let slate_path = unique_path("s3_mw");

    let results =
        run_word_count_tiered(&s3, &slate_path, generate_words(), 2, 4 * 1024 * 1024).await;
    assert!(
        !results.is_empty(),
        "multi-worker pipeline produced no output"
    );

    let finals = final_counts(&results);
    let expected = expected_counts(&generate_words());
    for (word, count) in &expected {
        assert_eq!(
            finals.get(word).copied().unwrap_or(0),
            *count,
            "final count mismatch for {word}"
        );
    }

    // Each word is persisted exactly once, under its key group rather than
    // under the index of whichever worker happened to process it. Both workers
    // therefore derive the same physical key, so the count landing there is the
    // proof that a single owner accumulated it — a split across workers would
    // show up as a count below the expected total.
    let l3 = SlateDbBackend::open(slate_path.as_str(), s3.clone())
        .await
        .expect("reopen for verification");
    for (word, count) in &expected {
        assert_eq!(
            read_count(&l3, word).await,
            Some(*count),
            "persisted count for '{word}' (key-group addressed, worker-independent)"
        );
    }
    l3.close().await.expect("close verify DB");

    eprintln!(
        "S3 multi-worker persistence verified: {} words across 2 workers",
        expected.len()
    );
}

// ── Test: large state spills L1 → L2 → L3 ───────────────────────────

/// A tiny in-memory L2 capacity with many distinct keys forces eviction to
/// disk and read-through to S3 mid-run. Counts must remain correct and all
/// state must land in S3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_tiered_large_state_spills_to_l3() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let s3 = build_s3_store();
    let slate_path = unique_path("s3_spill");

    let n_keys = 1500usize;
    let reps = 3usize;
    let words = generate_repeated_unique(n_keys, reps);

    // 1 MiB L2 memory cannot hold all keys → forces eviction + L3 read-through.
    let results = run_word_count_tiered(&s3, &slate_path, words, 1, 1024 * 1024).await;

    let finals = final_counts(&results);
    assert_eq!(
        finals.len(),
        n_keys,
        "every distinct key must be counted exactly once in the output set"
    );
    for (word, count) in &finals {
        assert_eq!(
            *count, reps as u64,
            "count for {word} must equal repetition count even after eviction"
        );
    }

    // Spot-check durable S3 state across the key range.
    let l3 = SlateDbBackend::open(slate_path.as_str(), s3.clone())
        .await
        .expect("reopen for verification");
    for i in [0usize, n_keys / 2, n_keys - 1] {
        let word = format!("key_{i:06}");
        assert_eq!(
            read_count(&l3, &word).await,
            Some(reps as u64),
            "S3 count for spilled key '{word}'"
        );
    }
    l3.close().await.expect("close verify DB");

    eprintln!("S3 spill test verified: {n_keys} keys × {reps} reps through L1→L2→L3");
}

// ── Test: SlateDbBackend direct ops against real S3 ─────────────────

/// Exercises `SlateDbBackend`'s `StateBackend` surface (put/get/delete/batch)
/// and durability against a live S3 service — the unit tests only cover the
/// in-memory object store.
#[tokio::test]
async fn s3_slatedb_backend_direct_ops() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let s3 = build_s3_store();
    let path = unique_path("s3_direct");

    let backend = SlateDbBackend::open(path.as_str(), s3.clone())
        .await
        .expect("open SlateDB on S3");

    // put / get
    backend.put(b"k1", b"v1").await.unwrap();
    assert_eq!(
        backend.get(b"k1").await.unwrap().as_deref(),
        Some(b"v1".as_slice())
    );

    // delete
    backend.delete(b"k1").await.unwrap();
    assert!(backend.get(b"k1").await.unwrap().is_none());

    // atomic batch: put k2, put k3, delete k2 → only k3 survives
    backend
        .put_batch(vec![
            BatchOp::Put {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
            },
            BatchOp::Put {
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
            },
            BatchOp::Delete {
                key: b"k2".to_vec(),
            },
        ])
        .await
        .unwrap();
    assert!(backend.get(b"k2").await.unwrap().is_none());
    assert_eq!(
        backend.get(b"k3").await.unwrap().as_deref(),
        Some(b"v3".as_slice())
    );

    backend.close().await.expect("close");

    // Durability: reopen from S3 and confirm k3 survived.
    let reopened = SlateDbBackend::open(path.as_str(), s3.clone())
        .await
        .expect("reopen SlateDB on S3");
    assert_eq!(
        reopened.get(b"k3").await.unwrap().as_deref(),
        Some(b"v3".as_slice())
    );
    reopened.close().await.expect("close reopened");

    eprintln!("SlateDB direct S3 ops verified");
}

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
use rhei_core::connectors::batch::VecSource;
use rhei_core::operators::keyed_state::KeyedState;
use rhei_core::state::backend::StateBackend;
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
struct BatchWordCounter;

#[async_trait]
impl StreamFunction for BatchWordCounter {
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
        .operator("word_counter", BatchWordCounter)
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

    let prefix = b"word_counter_w0/";
    let verify_words = ["alpha", "beta", "cedar", "delta", "ember", "frost"];
    for word in &verify_words {
        let mut key = prefix.to_vec();
        key.extend_from_slice(word.as_bytes());

        let val = l3_verify
            .get(&key)
            .await
            .unwrap_or_else(|e| panic!("failed to read key for {word}: {e}"));

        assert!(
            val.is_some(),
            "key for '{word}' not found in S3 — state not persisted"
        );

        let bytes = val.unwrap();
        let count: u64 =
            bincode::deserialize(&bytes).expect("failed to deserialize persisted count");
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

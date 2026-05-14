#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: merge combines two streams into one.
//!
//! Pipeline:
//! ```text
//! source1 ("a","b","c") --\
//!                           merge → map(add prefix) → CollectSink
//! source2 ("x","y","z") --/
//! ```
//! Verify: all rows from both sources reach the sink.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::arrow::{BatchSink, RheiBuffer, RheiBuilder, RheiSchema};
use rhei_core::connectors::batch::BatchVecSource;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Schema types ────────────────────────────────────────────────────

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

// ── Collecting sink ────────────────────────────────────────────────────

struct CollectSink {
    collected: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl BatchSink for CollectSink {
    type Input = WordEvent;

    async fn write_batch(&mut self, input: RheiBuffer<WordEvent>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push(view.word.to_string());
        }
        Ok(())
    }
}

// ── Test ─────────────────────────────────────────────────────────────

fn make_events(words: &[&str]) -> Vec<WordEvent> {
    words
        .iter()
        .map(|w| WordEvent {
            word: w.to_string(),
        })
        .collect()
}

#[tokio::test]
async fn merge_combines_two_sources() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<String>::new()));

    let source1 = BatchVecSource::new(make_events(&["a", "b", "c"])).with_batch_size(10);
    let source2 = BatchVecSource::new(make_events(&["x", "y", "z"])).with_batch_size(10);

    let graph = DataflowGraph::new();
    let stream1 = graph.batch_source(source1);
    let stream2 = graph.batch_source(source2);

    stream1.merge(stream2).sink(CollectSink {
        collected: collected.clone(),
    });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort();

    assert_eq!(results.len(), 6, "merge should produce 6 total items");
    assert_eq!(
        results,
        vec!["a", "b", "c", "x", "y", "z"],
        "all items from both sources should be present"
    );
}

#[tokio::test]
async fn merge_works_with_multiple_workers() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<String>::new()));

    let words1: Vec<&str> = (0..20)
        .map(|i| if i % 2 == 0 { "even" } else { "odd" })
        .collect();
    let words2: Vec<&str> = (0..10).map(|_| "extra").collect();

    let source1 = BatchVecSource::new(make_events(&words1)).with_batch_size(5);
    let source2 = BatchVecSource::new(make_events(&words2)).with_batch_size(5);

    let graph = DataflowGraph::new();
    let stream1 = graph.batch_source(source1);
    let stream2 = graph.batch_source(source2);

    stream1.merge(stream2).sink(CollectSink {
        collected: collected.clone(),
    });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(2);
    ctrl.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();
    assert_eq!(
        results.len(),
        30,
        "merge should produce all 30 items with 2 workers"
    );

    let even_count = results.iter().filter(|w| *w == "even").count();
    let odd_count = results.iter().filter(|w| *w == "odd").count();
    let extra_count = results.iter().filter(|w| *w == "extra").count();
    assert_eq!(even_count, 10);
    assert_eq!(odd_count, 10);
    assert_eq!(extra_count, 10);
}

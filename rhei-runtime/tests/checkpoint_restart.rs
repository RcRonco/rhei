#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: stateful operator state persists across pipeline restarts.
//!
//! Run 1: process `["a","b","c","a","b"]` through a stateful word counter.
//! Run 2: process `["a","c","d"]` with the same checkpoint directory.
//!        Verify that counts accumulated across runs.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::arrow::{
    BatchSink, BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer, RheiBuilder,
    RheiSchema,
};
use rhei_core::connectors::batch::BatchVecSource;
use rhei_core::operators::batch::keyed_state::KeyedState;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Schema types (manual RheiSchema impl — no derive macro dep) ─────

struct WordEvent {
    word: String,
}

struct WordEventBuilder {
    word: arrow_array::builder::StringBuilder,
}

#[derive(Debug, Clone)]
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

// ── Output type: word + count ───────────────────────────────────────

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

// ── Stateful operator: BatchWordCounter ─────────────────────────────

#[derive(Clone)]
struct BatchWordCounter;

#[async_trait]
impl BatchStreamFunction for BatchWordCounter {
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

        let mut outputs = Vec::with_capacity(input.len());

        // Collect words first to avoid borrow issues with views across await.
        let words: Vec<String> = input.iter().map(|v| v.word.to_string()).collect();

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

// ── Collecting sink ─────────────────────────────────────────────────

struct WordCountSink {
    collected: Arc<Mutex<Vec<(String, u64)>>>,
}

#[async_trait]
impl BatchSink for WordCountSink {
    type Input = WordCount;

    async fn write_batch(&mut self, input: RheiBuffer<WordCount>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.word.to_string(), view.count));
        }
        Ok(())
    }
}

// ── Test ─────────────────────────────────────────────────────────────

fn make_word_events(words: &[&str]) -> Vec<WordEvent> {
    words
        .iter()
        .map(|w| WordEvent {
            word: w.to_string(),
        })
        .collect()
}

#[tokio::test]
async fn checkpoint_restart_preserves_state() {
    let dir = std::env::temp_dir().join(format!("rhei_ckpt_restart_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // ── Run 1: process ["a", "b", "c", "a", "b"] ───────────────────
    let collected_run1: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let events = make_word_events(&["a", "b", "c", "a", "b"]);
        let source = BatchVecSource::new(events).with_batch_size(10);

        let graph = DataflowGraph::new();
        graph
            .batch_source(source)
            .operator("word_counter", BatchWordCounter)
            .sink(WordCountSink {
                collected: collected_run1.clone(),
            });

        let ctrl = PipelineController::new(dir.clone()).with_workers(1);
        ctrl.run(graph).await.unwrap();
    }

    // Verify run 1 output: a=1, b=1, c=1, a=2, b=2
    let results_run1 = collected_run1.lock().unwrap().clone();
    assert_eq!(results_run1.len(), 5, "run 1 should emit 5 outputs");

    // Final counts after run 1: a=2, b=2, c=1
    let last_a = results_run1.iter().rev().find(|(w, _)| w == "a").unwrap();
    let last_b = results_run1.iter().rev().find(|(w, _)| w == "b").unwrap();
    let last_c = results_run1.iter().rev().find(|(w, _)| w == "c").unwrap();
    assert_eq!(last_a.1, 2, "run 1: final count for 'a' should be 2");
    assert_eq!(last_b.1, 2, "run 1: final count for 'b' should be 2");
    assert_eq!(last_c.1, 1, "run 1: final count for 'c' should be 1");

    // ── Run 2: process ["a", "c", "d"] with same checkpoint dir ─────
    let collected_run2: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let events = make_word_events(&["a", "c", "d"]);
        let source = BatchVecSource::new(events).with_batch_size(10);

        let graph = DataflowGraph::new();
        graph
            .batch_source(source)
            .operator("word_counter", BatchWordCounter)
            .sink(WordCountSink {
                collected: collected_run2.clone(),
            });

        let ctrl = PipelineController::new(dir.clone()).with_workers(1);
        ctrl.run(graph).await.unwrap();
    }

    // Verify run 2 output: counts should accumulate from checkpoint.
    // Expected: a=3 (was 2, +1), c=2 (was 1, +1), d=1 (new)
    let results_run2 = collected_run2.lock().unwrap().clone();
    assert_eq!(results_run2.len(), 3, "run 2 should emit 3 outputs");

    let a_count = results_run2.iter().find(|(w, _)| w == "a").unwrap().1;
    let c_count = results_run2.iter().find(|(w, _)| w == "c").unwrap().1;
    let d_count = results_run2.iter().find(|(w, _)| w == "d").unwrap().1;

    assert_eq!(a_count, 3, "run 2: 'a' should accumulate to 3 (2 + 1)");
    assert_eq!(c_count, 2, "run 2: 'c' should accumulate to 2 (1 + 1)");
    assert_eq!(d_count, 1, "run 2: 'd' should be 1 (new key)");

    let _ = std::fs::remove_dir_all(&dir);
}

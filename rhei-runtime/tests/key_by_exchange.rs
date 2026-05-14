#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: `key_by` exchange routes same-key events to the same worker.
//!
//! Pipeline with 2 workers:
//! ```text
//! BatchVecSource(events) → key_by(|v| v.key) → WordCounter(KeyedState) → CollectSink
//! ```
//! Verify: all events for the same key are processed by a single worker,
//! producing correct cumulative counts.

use std::collections::HashMap;
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

// ── Schema types ────────────────────────────────────────────────────

struct KeyedEvent {
    key: String,
}

struct KeyedEventBuilder {
    key: arrow_array::builder::StringBuilder,
}

struct KeyedEventView<'a> {
    key: &'a str,
}

struct KeyedEventColumns<'a> {
    #[allow(dead_code)]
    key: &'a arrow_array::StringArray,
}

impl RheiBuilder for KeyedEventBuilder {
    type Item = KeyedEvent;

    fn append(&mut self, item: KeyedEvent) {
        self.key.append_value(&item.key);
    }

    fn append_null(&mut self) {
        self.key.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.key)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            KeyedEvent::arrow_schema(),
            vec![Arc::new(self.key.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for KeyedEvent {
    type Builder = KeyedEventBuilder;
    type View<'a> = KeyedEventView<'a>;
    type Columns<'a> = KeyedEventColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "key",
            arrow_schema::DataType::Utf8,
            false,
        )]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        KeyedEventBuilder {
            key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        KeyedEventView {
            key: batch.column(0).as_string::<i32>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        KeyedEventColumns {
            key: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Output type ─────────────────────────────────────────────────────

struct KeyCount {
    key: String,
    count: u64,
}

struct KeyCountBuilder {
    key: arrow_array::builder::StringBuilder,
    count: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
}

#[derive(Debug, Clone)]
struct KeyCountView<'a> {
    key: &'a str,
    count: u64,
}

struct KeyCountColumns<'a> {
    #[allow(dead_code)]
    key: &'a arrow_array::StringArray,
}

impl RheiBuilder for KeyCountBuilder {
    type Item = KeyCount;

    fn append(&mut self, item: KeyCount) {
        self.key.append_value(&item.key);
        self.count.append_value(item.count);
    }

    fn append_null(&mut self) {
        self.key.append_null();
        self.count.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.key)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            KeyCount::arrow_schema(),
            vec![Arc::new(self.key.finish()), Arc::new(self.count.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for KeyCount {
    type Builder = KeyCountBuilder;
    type View<'a> = KeyCountView<'a>;
    type Columns<'a> = KeyCountColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("count", arrow_schema::DataType::UInt64, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        KeyCountBuilder {
            key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            count: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        KeyCountView {
            key: batch.column(0).as_string::<i32>().value(index),
            count: batch.column(1).as_primitive::<UInt64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        KeyCountColumns {
            key: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Stateful operator ─────────────────────────────────────────────────

#[derive(Clone)]
struct KeyCounter;

#[async_trait]
impl BatchStreamFunction for KeyCounter {
    type Input = KeyedEvent;
    type Output = KeyCount;

    async fn process(
        &mut self,
        input: RheiBuffer<KeyedEvent>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<KeyCount>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }

        let keys: Vec<String> = input.iter().map(|v| v.key.to_string()).collect();
        let mut outputs = Vec::with_capacity(keys.len());

        for key in keys {
            let count: u64 = {
                let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "counts");
                state.get(&key).await?.unwrap_or(0)
            };
            let new_count = count + 1;
            {
                let mut state = KeyedState::<String, u64>::new(&mut ctx.state, "counts");
                state.put(&key, &new_count)?;
            }
            outputs.push(KeyCount {
                key,
                count: new_count,
            });
        }

        let mut builder = KeyCount::builder(outputs.len());
        for item in outputs {
            builder.append(item);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── Collecting sink ────────────────────────────────────────────────────

struct CollectSink {
    collected: Arc<Mutex<Vec<(String, u64)>>>,
}

#[async_trait]
impl BatchSink for CollectSink {
    type Input = KeyCount;

    async fn write_batch(&mut self, input: RheiBuffer<KeyCount>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.key.to_string(), view.count));
        }
        Ok(())
    }
}

// ── Test ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn key_by_exchange_routes_same_key_to_same_worker() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // Generate events: 10 occurrences each of keys "alpha", "beta", "gamma", "delta".
    let keys = ["alpha", "beta", "gamma", "delta"];
    let events: Vec<KeyedEvent> = keys
        .iter()
        .cycle()
        .take(40)
        .map(|k| KeyedEvent { key: k.to_string() })
        .collect();

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<(String, u64)>::new()));

    let source = BatchVecSource::new(events).with_batch_size(8);

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .key_by(|view: &<KeyedEvent as RheiSchema>::View<'_>| view.key.to_string())
        .operator("key_counter", KeyCounter)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    // Run with 2 workers to exercise the exchange.
    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(2);
    ctrl.run(graph).await.unwrap();

    // Verify: final count for each key should be 10.
    let results = collected.lock().unwrap().clone();
    assert!(!results.is_empty(), "pipeline produced no output");

    let mut final_counts: HashMap<String, u64> = HashMap::new();
    for (key, count) in &results {
        let entry = final_counts.entry(key.clone()).or_insert(0);
        *entry = (*entry).max(*count);
    }

    for key in &keys {
        let actual = final_counts.get(*key).copied().unwrap_or(0);
        assert_eq!(
            actual, 10,
            "key '{key}' should have count 10 (got {actual}) — key_by exchange \
             should route all same-key events to the same worker"
        );
    }

    // Also verify monotonically increasing counts per key (no resets).
    let mut per_key_sequence: HashMap<String, Vec<u64>> = HashMap::new();
    for (key, count) in &results {
        per_key_sequence
            .entry(key.clone())
            .or_default()
            .push(*count);
    }
    for (key, counts) in &per_key_sequence {
        for window in counts.windows(2) {
            assert!(
                window[1] >= window[0],
                "key '{key}' has non-monotonic counts: {counts:?}"
            );
        }
    }
}

#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration tests for convenience stream methods:
//! `inspect`, `name`, `limit`, `batch`, `distinct_by`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::arrow::{RheiBuffer, RheiBuilder, RheiSchema, Sink};
use rhei_core::connectors::batch::VecSource;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Schema types ────────────────────────────────────────────────────

struct NumEvent {
    val: i64,
}

struct NumEventBuilder {
    val: arrow_array::builder::Int64Builder,
}

struct NumEventView<'a> {
    val: i64,
    #[allow(dead_code)]
    _lt: std::marker::PhantomData<&'a ()>,
}

struct NumEventColumns<'a> {
    #[allow(dead_code)]
    val: &'a arrow_array::Int64Array,
}

impl RheiBuilder for NumEventBuilder {
    type Item = NumEvent;

    fn append(&mut self, item: NumEvent) {
        self.val.append_value(item.val);
    }

    fn append_null(&mut self) {
        self.val.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.val)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            NumEvent::arrow_schema(),
            vec![Arc::new(self.val.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for NumEvent {
    type Builder = NumEventBuilder;
    type View<'a> = NumEventView<'a>;
    type Columns<'a> = NumEventColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "val",
            arrow_schema::DataType::Int64,
            false,
        )]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        NumEventBuilder {
            val: arrow_array::builder::Int64Builder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;
        NumEventView {
            val: batch.column(0).as_primitive::<Int64Type>().value(index),
            _lt: std::marker::PhantomData,
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;
        NumEventColumns {
            val: batch.column(0).as_primitive::<Int64Type>(),
        }
    }
}

// ── Collecting sink ────────────────────────────────────────────────────

struct CollectSink {
    collected: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl Sink for CollectSink {
    type Input = NumEvent;

    async fn write_batch(&mut self, input: RheiBuffer<NumEvent>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push(view.val);
        }
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

fn make_events(vals: &[i64]) -> Vec<NumEvent> {
    vals.iter().map(|&v| NumEvent { val: v }).collect()
}

#[tokio::test]
async fn inspect_does_not_modify_stream() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<i64>::new()));
    let inspected = Arc::new(Mutex::new(Vec::<i64>::new()));
    let inspected_clone = inspected.clone();

    let source = VecSource::new(make_events(&[1, 2, 3, 4, 5])).with_batch_size(10);
    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .inspect(move |v| {
            inspected_clone.lock().unwrap().push(v.val);
        })
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_unstable();
    assert_eq!(results, vec![1, 2, 3, 4, 5]);

    let mut inspected_results = inspected.lock().unwrap().clone();
    inspected_results.sort_unstable();
    assert_eq!(inspected_results, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn limit_caps_output() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<i64>::new()));

    let source = VecSource::new(make_events(&[10, 20, 30, 40, 50])).with_batch_size(10);
    let graph = DataflowGraph::new();
    graph.batch_source(source).limit(3).sink(CollectSink {
        collected: collected.clone(),
    });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();
    assert_eq!(results.len(), 3, "limit(3) should produce exactly 3 items");
    // Should be the first 3 items
    assert_eq!(results, vec![10, 20, 30]);
}

#[tokio::test]
async fn distinct_by_deduplicates() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<i64>::new()));

    // Values: 1, 2, 2, 3, 3, 3 — distinct_by key = val → should get 1, 2, 3
    let source = VecSource::new(make_events(&[1, 2, 2, 3, 3, 3])).with_batch_size(10);
    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .distinct_by(|v| v.val.to_string())
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_unstable();
    assert_eq!(results, vec![1, 2, 3]);
}

#[tokio::test]
async fn name_does_not_affect_data() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<i64>::new()));

    let source = VecSource::new(make_events(&[7, 8, 9])).with_batch_size(10);
    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .name("my_stream")
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_unstable();
    assert_eq!(results, vec![7, 8, 9]);
}

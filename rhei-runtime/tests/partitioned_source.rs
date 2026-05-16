#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: partitioned sources distribute data across workers.
//!
//! Pipeline with 2 workers:
//! ```text
//! PartitionedVecSource(items, 4 partitions) → CollectSink
//! ```
//! Verify: all items reach the sink, proving that partition assignment
//! distributes work to all workers (not just worker 0).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::arrow::{RheiBuffer, RheiBuilder, RheiSchema, Sink};
use rhei_core::connectors::batch::PartitionedVecSource;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Schema types ────────────────────────────────────────────────────

#[derive(Clone)]
struct IndexEvent {
    idx: i64,
}

struct IndexEventBuilder {
    idx: arrow_array::builder::Int64Builder,
}

struct IndexEventView<'a> {
    idx: i64,
    #[allow(dead_code)]
    _lt: std::marker::PhantomData<&'a ()>,
}

struct IndexEventColumns<'a> {
    #[allow(dead_code)]
    idx: &'a arrow_array::Int64Array,
}

impl RheiBuilder for IndexEventBuilder {
    type Item = IndexEvent;

    fn append(&mut self, item: IndexEvent) {
        self.idx.append_value(item.idx);
    }

    fn append_null(&mut self) {
        self.idx.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.idx)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            IndexEvent::arrow_schema(),
            vec![Arc::new(self.idx.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for IndexEvent {
    type Builder = IndexEventBuilder;
    type View<'a> = IndexEventView<'a>;
    type Columns<'a> = IndexEventColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "idx",
            arrow_schema::DataType::Int64,
            false,
        )]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        IndexEventBuilder {
            idx: arrow_array::builder::Int64Builder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;
        IndexEventView {
            idx: batch.column(0).as_primitive::<Int64Type>().value(index),
            _lt: std::marker::PhantomData,
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;
        IndexEventColumns {
            idx: batch.column(0).as_primitive::<Int64Type>(),
        }
    }
}

// ── Collecting sink ────────────────────────────────────────────────────

struct CollectSink {
    collected: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl Sink for CollectSink {
    type Input = IndexEvent;

    async fn write_batch(&mut self, input: RheiBuffer<IndexEvent>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push(view.idx);
        }
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn partitioned_source_distributes_to_workers() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<i64>::new()));

    let items: Vec<IndexEvent> = (0..20).map(|i| IndexEvent { idx: i }).collect();
    let source = PartitionedVecSource::new(items, 4).with_batch_size(5);

    let graph = DataflowGraph::new();
    graph.source(source).sink(CollectSink {
        collected: collected.clone(),
    });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(2);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_unstable();
    assert_eq!(
        results,
        (0..20).collect::<Vec<i64>>(),
        "all 20 items should reach the sink via partitioned reading"
    );
}

#[tokio::test]
async fn partitioned_source_with_more_workers_than_partitions() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<i64>::new()));

    let items: Vec<IndexEvent> = (0..6).map(|i| IndexEvent { idx: i }).collect();
    // 2 partitions, 4 workers — some workers will have no partitions assigned
    let source = PartitionedVecSource::new(items, 2).with_batch_size(10);

    let graph = DataflowGraph::new();
    graph.source(source).sink(CollectSink {
        collected: collected.clone(),
    });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(4);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_unstable();
    assert_eq!(
        results,
        (0..6).collect::<Vec<i64>>(),
        "all items should reach the sink even with more workers than partitions"
    );
}

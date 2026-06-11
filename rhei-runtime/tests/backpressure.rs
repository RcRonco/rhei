#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Resilience test: backpressure must not drop data.
//!
//! A fast source feeds a deliberately slow sink through the runtime's bounded
//! bridge channels. The bounded channels apply backpressure (the source blocks
//! when they fill) rather than dropping batches, so every emitted row must be
//! delivered exactly once. Directly exercises the KI-18 gap ("backpressure
//! behaviour when channels fill up").

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::erased_batch::{ErasedSink, ErasedSource};
use rhei_runtime::erased_buffer::{ErasedBuffer, schema_hash_of};

const ROWS: i64 = 500;

fn n_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

fn one_row(value: i64) -> ErasedBuffer {
    let schema = n_schema();
    let col = Int64Array::from(vec![value]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).unwrap();
    ErasedBuffer::from_parts(batch, None, schema_hash_of(&schema))
}

fn values_of(buf: &ErasedBuffer) -> Vec<i64> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    let batch = buf.as_record_batch();
    let col = batch.column(0).as_primitive::<Int64Type>();
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// Fast source: emits `ROWS` single-row batches as fast as it is polled.
struct FastSource {
    batches: VecDeque<ErasedBuffer>,
}

#[async_trait]
impl ErasedSource for FastSource {
    async fn next_batch(&mut self) -> Option<ErasedBuffer> {
        self.batches.pop_front()
    }
    async fn on_checkpoint_complete(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn current_offsets(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }
    async fn restore_offsets(
        &mut self,
        _: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn partition_count(&self) -> Option<usize> {
        None
    }
    fn create_partition_source(&self, _: &[usize]) -> Option<Box<dyn ErasedSource>> {
        None
    }
    fn current_watermark(&self) -> Option<u64> {
        None
    }
}

/// Slow sink: sleeps briefly on every batch so the bridge channel fills up and
/// the source is forced to wait (backpressure).
struct SlowSink {
    out: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl ErasedSink for SlowSink {
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_micros(200)).await;
        self.out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(values_of(&buf));
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn slow_sink_applies_backpressure_without_dropping_rows() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let delivered = Arc::new(Mutex::new(Vec::<i64>::new()));

    let source = FastSource {
        batches: (0..ROWS).map(one_row).collect(),
    };

    let graph = DataflowGraph::new();
    let src = graph.add_erased_source(Box::new(source));
    graph.add_erased_sink(
        src,
        Box::new(SlowSink {
            out: delivered.clone(),
        }),
    );

    let ctrl = PipelineController::builder()
        .checkpoint_dir(checkpoint_dir.path().to_path_buf())
        .workers(1)
        .build()
        .unwrap();

    ctrl.run(graph).await.unwrap();

    let mut got = delivered
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    got.sort_unstable();

    // Every row delivered exactly once — nothing dropped under backpressure.
    assert_eq!(
        got.len(),
        usize::try_from(ROWS).unwrap(),
        "row count must be preserved"
    );
    assert_eq!(
        got,
        (0..ROWS).collect::<Vec<_>>(),
        "no rows lost or duplicated"
    );
}

#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Proof that a dataflow graph can be built and run entirely through the
//! public erased-builder API — no `RheiSchema`-typed code at all. This is the
//! foundation the Python bindings target.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::{BatchTransformFn, DataflowGraph};
use rhei_runtime::erased_batch::{ErasedSink, ErasedSource};
use rhei_runtime::erased_buffer::{ErasedBuffer, schema_hash_of};

/// A single-Int64-column schema: `{ n: Int64 }`.
fn n_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

/// Build a one-column `ErasedBuffer` from a slice of i64s.
fn erased_from(values: &[i64]) -> ErasedBuffer {
    let schema = n_schema();
    let col = Int64Array::from(values.to_vec());
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).unwrap();
    ErasedBuffer::from_parts(batch, None, schema_hash_of(&schema))
}

/// Read the single Int64 column out of an `ErasedBuffer`.
fn values_of(buf: &ErasedBuffer) -> Vec<i64> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    let batch = buf.as_record_batch();
    let col = batch.column(0).as_primitive::<Int64Type>();
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// A source that emits a fixed list of batches, then `None`.
struct ListSource {
    batches: std::collections::VecDeque<ErasedBuffer>,
}

#[async_trait]
impl ErasedSource for ListSource {
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
        _offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn partition_count(&self) -> Option<usize> {
        None
    }
    fn create_partition_source(&self, _assigned: &[usize]) -> Option<Box<dyn ErasedSource>> {
        None
    }
    fn current_watermark(&self) -> Option<u64> {
        None
    }
}

/// A sink that collects every batch's values into a shared Vec.
struct CollectSink {
    out: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl ErasedSink for CollectSink {
    async fn write_batch(&mut self, buf: ErasedBuffer) -> anyhow::Result<()> {
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
async fn erased_source_transform_sink_runs() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<i64>::new()));

    // Source: two batches of i64s.
    let source = ListSource {
        batches: std::collections::VecDeque::from(vec![
            erased_from(&[1, 2, 3]),
            erased_from(&[4, 5]),
        ]),
    };

    // Transform: double every value. Rebuilds the buffer with the same schema id.
    let schema = n_schema();
    let schema_id = schema_hash_of(&schema);
    let transform: BatchTransformFn = Arc::new(move |buf: ErasedBuffer| {
        let doubled: Vec<i64> = values_of(&buf).into_iter().map(|v| v * 2).collect();
        let col = Int64Array::from(doubled);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).unwrap();
        vec![ErasedBuffer::from_parts(batch, None, schema_id)]
    });

    let sink = CollectSink {
        out: collected.clone(),
    };

    // Build the graph purely through the erased API.
    let graph = DataflowGraph::new();
    let src = graph.add_erased_source(Box::new(source));
    let mapped = graph.add_erased_transform(src, transform);
    graph.add_erased_sink(mapped, Box::new(sink));

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut got = collected
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    got.sort_unstable();
    assert_eq!(got, vec![2, 4, 6, 8, 10]);
}

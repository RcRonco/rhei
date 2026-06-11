#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Resilience test: dead-letter-queue routing under sustained operator errors.
//!
//! Builds a pipeline whose operator fails on "bad" rows and verifies that, with
//! `ErrorPolicy::SendToDlq`, every failed batch is routed to the DLQ sink while
//! the good rows still reach the output sink. Directly exercises the KI-18 gap
//! ("DLQ routing under sustained error rates").

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

use rhei_core::arrow::OperatorContext;
use rhei_core::dlq::{DeadLetterRecord, DlqSink, ErrorPolicy};
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::erased_batch::{ErasedBatchOperator, ErasedSink, ErasedSource};
use rhei_runtime::erased_buffer::{ErasedBuffer, schema_hash_of};

fn n_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

/// One single-row `ErasedBuffer` per value, so each "bad" value fails its own
/// batch (one DLQ record per failure) instead of poisoning a whole batch.
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

/// Source emitting one row per batch.
struct RowAtATimeSource {
    batches: VecDeque<ErasedBuffer>,
}

#[async_trait]
impl ErasedSource for RowAtATimeSource {
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

/// Operator that errors on any batch containing a negative value, and passes
/// non-negative batches through unchanged.
struct RejectNegativesOp;

#[async_trait]
impl ErasedBatchOperator for RejectNegativesOp {
    async fn process(
        &mut self,
        input: ErasedBuffer,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        if values_of(&input).iter().any(|&v| v < 0) {
            anyhow::bail!("negative value rejected");
        }
        Ok(vec![input])
    }
    async fn on_watermark(
        &mut self,
        _: u64,
        _: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        Ok(vec![])
    }
    async fn open(&mut self, _: &mut OperatorContext) -> anyhow::Result<()> {
        Ok(())
    }
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_timer(
        &mut self,
        _: u64,
        _: &str,
        _: &mut OperatorContext,
    ) -> anyhow::Result<Vec<ErasedBuffer>> {
        Ok(vec![])
    }
    fn clone_erased(&self) -> Box<dyn ErasedBatchOperator> {
        Box::new(RejectNegativesOp)
    }
}

/// Sink collecting all delivered values.
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

/// DLQ sink collecting every dead-letter record in memory for assertions.
struct CollectingDlqSink {
    records: Arc<Mutex<Vec<DeadLetterRecord>>>,
}

#[async_trait]
impl DlqSink for CollectingDlqSink {
    async fn write(&mut self, record: DeadLetterRecord) -> anyhow::Result<()> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(record);
        Ok(())
    }
}

#[tokio::test]
async fn failed_records_route_to_dlq_while_good_ones_reach_the_sink() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let delivered = Arc::new(Mutex::new(Vec::<i64>::new()));
    let dlq_records = Arc::new(Mutex::new(Vec::<DeadLetterRecord>::new()));

    // Interleave good and bad rows: 1, -1, 2, -2, 3, -3 → three failures.
    let source = RowAtATimeSource {
        batches: VecDeque::from(vec![
            one_row(1),
            one_row(-1),
            one_row(2),
            one_row(-2),
            one_row(3),
            one_row(-3),
        ]),
    };

    let graph = DataflowGraph::new();
    let src = graph.add_erased_source(Box::new(source));
    let checked = graph.add_erased_operator(src, "reject_negatives", Box::new(RejectNegativesOp));
    graph.add_erased_sink(
        checked,
        Box::new(CollectSink {
            out: delivered.clone(),
        }),
    );

    let ctrl = PipelineController::builder()
        .checkpoint_dir(checkpoint_dir.path().to_path_buf())
        .workers(1)
        .error_policy(ErrorPolicy::SendToDlq)
        .dlq_sink(CollectingDlqSink {
            records: dlq_records.clone(),
        })
        .build()
        .unwrap();

    ctrl.run(graph).await.unwrap();

    // Good rows passed through, in order.
    let mut got = delivered
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3], "non-negative rows must reach the sink");

    // Every failure produced exactly one DLQ record from the right operator.
    let records = dlq_records
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(records.len(), 3, "one DLQ record per failed batch");
    for record in &records {
        assert_eq!(record.operator_name, "reject_negatives");
        assert!(
            record.error.contains("negative value rejected"),
            "unexpected DLQ error text: {}",
            record.error
        );
    }
}

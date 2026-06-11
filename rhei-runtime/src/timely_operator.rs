//! Timely-aware operator wrappers with capability management (batch/Arrow path).

use rhei_core::arrow::OperatorContext;

use crate::erased_batch::ErasedBatchOperator;
use crate::erased_buffer::ErasedBuffer;

/// Wraps a type-erased [`ErasedBatchOperator`] + [`OperatorContext`] with
/// frontier-based checkpoint tracking for the Arrow columnar execution path.
pub(crate) struct TimelyBatchOperator {
    op: Box<dyn ErasedBatchOperator>,
    ctx: OperatorContext,
    last_checkpoint_epoch: Option<u64>,
}

impl TimelyBatchOperator {
    /// Create a new batch operator wrapper.
    pub fn new(op: Box<dyn ErasedBatchOperator>, ctx: OperatorContext) -> Self {
        Self {
            op,
            ctx,
            last_checkpoint_epoch: None,
        }
    }

    /// Process a type-erased input buffer. Blocks on the Tokio runtime.
    ///
    /// Returns `(output_buffers, errors)`.
    pub fn process(
        &mut self,
        input: ErasedBuffer,
        rt: &tokio::runtime::Handle,
    ) -> (Vec<ErasedBuffer>, Vec<anyhow::Error>) {
        match rt.block_on(self.op.process(input, &mut self.ctx)) {
            Ok(results) => (results, vec![]),
            Err(e) => (vec![], vec![e]),
        }
    }

    /// Checkpoint state when frontier advances past last checkpoint epoch.
    ///
    /// Returns `Some(epoch)` if a checkpoint was performed, `None` otherwise.
    pub fn maybe_checkpoint(
        &mut self,
        frontier: &[u64],
        rt: &tokio::runtime::Handle,
    ) -> anyhow::Result<Option<u64>> {
        let min_frontier = frontier.iter().copied().min();

        let should_checkpoint = match (min_frontier, self.last_checkpoint_epoch) {
            (Some(current), Some(last)) => current > last,
            (Some(_), None) | (None, _) => true,
        };

        if should_checkpoint {
            let ckpt_start = std::time::Instant::now();
            rt.block_on(self.ctx.state.checkpoint())?;
            metrics::gauge!("executor_checkpoint_duration_seconds")
                .set(ckpt_start.elapsed().as_secs_f64());
            self.last_checkpoint_epoch = min_frontier;
            Ok(min_frontier)
        } else {
            Ok(None)
        }
    }

    /// Process a watermark advancement. Returns any outputs produced.
    pub fn process_watermark(
        &mut self,
        watermark: u64,
        rt: &tokio::runtime::Handle,
    ) -> Vec<ErasedBuffer> {
        match rt.block_on(self.op.on_watermark(watermark, &mut self.ctx)) {
            Ok(results) => results,
            Err(e) => {
                tracing::error!("batch watermark processing failed: {e}");
                vec![]
            }
        }
    }

    /// Call the operator's `open` lifecycle hook.
    /// Also restores any persisted timer state from the backend.
    pub fn open(&mut self, rt: &tokio::runtime::Handle) -> anyhow::Result<()> {
        rt.block_on(self.ctx.state.restore_timers())
            .map_err(|e| anyhow::anyhow!("timer restore failed: {e}"))?;
        rt.block_on(self.op.open(&mut self.ctx))
            .map_err(|e| anyhow::anyhow!("batch operator open failed: {e}"))?;
        Ok(())
    }

    /// Call the operator's `close` lifecycle hook.
    pub fn close(&mut self, rt: &tokio::runtime::Handle) -> anyhow::Result<()> {
        rt.block_on(self.op.close())
            .map_err(|e| anyhow::anyhow!("batch operator close failed: {e}"))
    }

    /// Drain fired timers and call `on_timer` for each.
    /// Returns any outputs produced by timer callbacks.
    pub fn process_timers(
        &mut self,
        watermark: u64,
        rt: &tokio::runtime::Handle,
    ) -> Vec<ErasedBuffer> {
        if !self.ctx.state.has_timers() {
            return vec![];
        }
        let fired = self.ctx.state.timers().drain_fired(watermark);
        let mut outputs = Vec::new();
        for (ts, key) in fired {
            match rt.block_on(self.op.on_timer(ts, &key, &mut self.ctx)) {
                Ok(results) => outputs.extend(results),
                Err(e) => tracing::error!("batch on_timer failed: {e}"),
            }
        }
        outputs
    }

    /// Advance the watermark and process fired timers in one call.
    pub fn advance_time(
        &mut self,
        frontier_wm: u64,
        last_watermark: &mut u64,
        rt: &tokio::runtime::Handle,
    ) -> Vec<ErasedBuffer> {
        let mut results = Vec::new();
        if frontier_wm > *last_watermark {
            *last_watermark = frontier_wm;
            results.extend(self.process_watermark(frontier_wm, rt));
        }
        results.extend(self.process_timers(frontier_wm, rt));
        results
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::cast_possible_wrap)]
mod tests {
    use super::*;
    use crate::erased_buffer::{ErasedBuffer, schema_hash_of};
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use rhei_core::state::context::StateContext;
    use rhei_core::state::local_backend::LocalBackend;
    use std::sync::{Arc, Mutex};

    fn n_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    fn erased_from(values: &[i64]) -> ErasedBuffer {
        let schema = n_schema();
        let col = Int64Array::from(values.to_vec());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).unwrap();
        ErasedBuffer::from_parts(batch, None, schema_hash_of(&schema))
    }

    fn values_of(buf: &ErasedBuffer) -> Vec<i64> {
        let batch = buf.as_record_batch();
        let col = batch.column(0).as_primitive::<Int64Type>();
        (0..col.len()).map(|i| col.value(i)).collect()
    }

    /// Records lifecycle calls so tests can assert what the wrapper drove.
    #[derive(Default)]
    struct Calls {
        watermarks: Vec<u64>,
        timers: Vec<(u64, String)>,
        opened: bool,
        closed: bool,
    }

    struct StubOp {
        calls: Arc<Mutex<Calls>>,
        fail_process: bool,
        register_timer_at: Option<u64>,
    }

    impl StubOp {
        fn new(calls: Arc<Mutex<Calls>>) -> Self {
            Self {
                calls,
                fail_process: false,
                register_timer_at: None,
            }
        }
    }

    #[async_trait]
    impl ErasedBatchOperator for StubOp {
        async fn process(
            &mut self,
            input: ErasedBuffer,
            ctx: &mut OperatorContext,
        ) -> anyhow::Result<Vec<ErasedBuffer>> {
            if self.fail_process {
                anyhow::bail!("stub process failure");
            }
            if let Some(ts) = self.register_timer_at {
                ctx.state.timers().register(ts, "k".to_string());
            }
            // Echo each input value offset by 100 so output is distinguishable.
            let out: Vec<i64> = values_of(&input).iter().map(|v| v + 100).collect();
            Ok(vec![erased_from(&out)])
        }

        async fn on_watermark(
            &mut self,
            watermark: u64,
            _ctx: &mut OperatorContext,
        ) -> anyhow::Result<Vec<ErasedBuffer>> {
            self.calls.lock().unwrap().watermarks.push(watermark);
            Ok(vec![erased_from(&[watermark as i64])])
        }

        async fn open(&mut self, _ctx: &mut OperatorContext) -> anyhow::Result<()> {
            self.calls.lock().unwrap().opened = true;
            Ok(())
        }

        async fn close(&mut self) -> anyhow::Result<()> {
            self.calls.lock().unwrap().closed = true;
            Ok(())
        }

        async fn on_timer(
            &mut self,
            timestamp: u64,
            key: &str,
            _ctx: &mut OperatorContext,
        ) -> anyhow::Result<Vec<ErasedBuffer>> {
            self.calls
                .lock()
                .unwrap()
                .timers
                .push((timestamp, key.to_string()));
            Ok(vec![])
        }

        fn clone_erased(&self) -> Box<dyn ErasedBatchOperator> {
            unreachable!("clone_erased is not exercised by these tests")
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    /// Builds a wrapper around `stub`; the returned `TempDir` must be kept alive
    /// for the duration of the test so the backing state file stays valid.
    fn wrap(stub: StubOp) -> (TimelyBatchOperator, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().join("state.json"), None).unwrap();
        let ctx = OperatorContext::new(StateContext::new(Box::new(backend)));
        (TimelyBatchOperator::new(Box::new(stub), ctx), dir)
    }

    #[test]
    fn process_forwards_operator_output() {
        let rt = rt();
        let (mut op, _dir) = wrap(StubOp::new(Arc::default()));

        let (out, errs) = op.process(erased_from(&[1, 2, 3]), rt.handle());
        assert!(errs.is_empty());
        assert_eq!(out.len(), 1);
        assert_eq!(values_of(&out[0]), vec![101, 102, 103]);
    }

    #[test]
    fn process_captures_operator_errors() {
        let rt = rt();
        let mut stub = StubOp::new(Arc::default());
        stub.fail_process = true;
        let (mut op, _dir) = wrap(stub);

        let (out, errs) = op.process(erased_from(&[1]), rt.handle());
        assert!(out.is_empty());
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("stub process failure"));
    }

    #[test]
    fn checkpoint_happens_first_time_then_only_on_advance() {
        let rt = rt();
        let (mut op, _dir) = wrap(StubOp::new(Arc::default()));

        // First checkpoint with no prior epoch always fires.
        assert_eq!(op.maybe_checkpoint(&[5], rt.handle()).unwrap(), Some(5));
        // Same frontier: nothing to do.
        assert_eq!(op.maybe_checkpoint(&[5], rt.handle()).unwrap(), None);
        // Advance: checkpoints at the new epoch.
        assert_eq!(op.maybe_checkpoint(&[8], rt.handle()).unwrap(), Some(8));
        // A regressed frontier must not re-checkpoint.
        assert_eq!(op.maybe_checkpoint(&[3], rt.handle()).unwrap(), None);
    }

    #[test]
    fn checkpoint_uses_minimum_of_multi_input_frontier() {
        let rt = rt();
        let (mut op, _dir) = wrap(StubOp::new(Arc::default()));
        // The smallest frontier value governs the checkpoint epoch.
        assert_eq!(
            op.maybe_checkpoint(&[9, 4, 7], rt.handle()).unwrap(),
            Some(4)
        );
    }

    #[test]
    fn process_watermark_records_and_emits() {
        let rt = rt();
        let calls = Arc::new(Mutex::new(Calls::default()));
        let (mut op, _dir) = wrap(StubOp::new(calls.clone()));

        let out = op.process_watermark(7, rt.handle());
        assert_eq!(values_of(&out[0]), vec![7]);
        assert_eq!(calls.lock().unwrap().watermarks, vec![7]);
    }

    #[test]
    fn advance_time_emits_watermark_output_only_on_increase() {
        let rt = rt();
        let calls = Arc::new(Mutex::new(Calls::default()));
        let (mut op, _dir) = wrap(StubOp::new(calls.clone()));
        let mut last = 0u64;

        let first = op.advance_time(5, &mut last, rt.handle());
        assert_eq!(values_of(&first[0]), vec![5]);
        assert_eq!(last, 5);

        // Re-advancing to the same watermark must not re-fire on_watermark.
        let second = op.advance_time(5, &mut last, rt.handle());
        assert!(second.is_empty());
        assert_eq!(calls.lock().unwrap().watermarks, vec![5]);
    }

    #[test]
    fn open_and_close_invoke_lifecycle_hooks() {
        let rt = rt();
        let calls = Arc::new(Mutex::new(Calls::default()));
        let (mut op, _dir) = wrap(StubOp::new(calls.clone()));

        op.open(rt.handle()).unwrap();
        assert!(calls.lock().unwrap().opened);

        op.close(rt.handle()).unwrap();
        assert!(calls.lock().unwrap().closed);
    }

    #[test]
    fn process_timers_is_noop_without_registered_timers() {
        let rt = rt();
        let calls = Arc::new(Mutex::new(Calls::default()));
        let (mut op, _dir) = wrap(StubOp::new(calls.clone()));

        let out = op.process_timers(100, rt.handle());
        assert!(out.is_empty());
        assert!(calls.lock().unwrap().timers.is_empty());
    }

    #[test]
    fn process_timers_fires_only_due_timers() {
        let rt = rt();
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut stub = StubOp::new(calls.clone());
        stub.register_timer_at = Some(10);
        let (mut op, _dir) = wrap(stub);

        // Registers a timer at t=10.
        let _ = op.process(erased_from(&[1]), rt.handle());

        // Watermark below the timer: nothing fires.
        op.process_timers(5, rt.handle());
        assert!(calls.lock().unwrap().timers.is_empty());

        // Watermark reaches the timer: it fires exactly once.
        op.process_timers(10, rt.handle());
        assert_eq!(calls.lock().unwrap().timers, vec![(10, "k".to_string())]);
    }
}

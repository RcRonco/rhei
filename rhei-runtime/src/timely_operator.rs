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

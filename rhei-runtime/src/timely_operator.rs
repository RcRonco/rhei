use std::collections::HashMap;

use rill_core::state::context::StateContext;
use rill_core::traits::StreamFunction;

use crate::async_operator::AsyncOperator;
use crate::dataflow::{AnyItem, ErasedOperator};

/// Wraps `AsyncOperator<F>` with Timely capability management.
///
/// Retains capabilities for epochs that have pending work, preventing Timely
/// from advancing the frontier past stashed elements' epochs. When processing
/// completes and caps are dropped, Timely progresses.
///
/// NOTE: This type is NOT `Send` because `Capability<u64>` uses `Rc` internally.
/// It must be constructed inside the Timely worker thread.
pub struct TimelyAsyncOperator<F: StreamFunction + 'static> {
    inner: AsyncOperator<F>,
    /// Capabilities retained per epoch. Dropped capabilities are tracked
    /// via a `ChangeBatch` so Timely sees frontier updates.
    retained_caps: HashMap<u64, CapabilityToken>,
    last_checkpoint_epoch: Option<u64>,
}

impl<F: StreamFunction + 'static> std::fmt::Debug for TimelyAsyncOperator<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimelyAsyncOperator")
            .field("retained_caps", &self.retained_caps)
            .field("last_checkpoint_epoch", &self.last_checkpoint_epoch)
            .finish_non_exhaustive()
    }
}

/// Lightweight token tracking a retained epoch. We don't store an actual
/// `timely::progress::Capability` here because `Capability` is `!Send`
/// (contains `Rc`). Instead the caller manages capabilities externally
/// and this struct just tracks which epochs are logically retained.
#[derive(Debug)]
pub struct CapabilityToken {
    /// The epoch (logical timestamp) this token retains.
    pub epoch: u64,
}

impl<F: StreamFunction + 'static> TimelyAsyncOperator<F> {
    /// Wraps the given `AsyncOperator` with Timely capability tracking.
    pub fn new(inner: AsyncOperator<F>) -> Self {
        Self {
            inner,
            retained_caps: HashMap::new(),
            last_checkpoint_epoch: None,
        }
    }

    /// Process an input element, marking the epoch as retained.
    /// Returns immediately-completed outputs and errors.
    pub fn process(&mut self, input: F::Input, epoch: u64) -> (Vec<F::Output>, Vec<anyhow::Error>) {
        self.retained_caps
            .entry(epoch)
            .or_insert(CapabilityToken { epoch });
        self.inner.process_element(input, Some(epoch))
    }

    /// Poll pending futures and collect completed results.
    pub fn poll_pending(&mut self) -> (Vec<F::Output>, Vec<anyhow::Error>) {
        self.inner.poll_pending()
    }

    /// Returns true if there are pending futures or stashed elements.
    pub fn has_pending(&self) -> bool {
        self.inner.has_pending()
    }

    /// Release epoch tokens for epochs that the frontier has passed
    /// and that have no pending work.
    pub fn release_finished_epochs(&mut self, frontier: &[u64]) {
        if !self.inner.has_pending() {
            // No pending work — safe to release tokens for passed epochs
            self.retained_caps.retain(|epoch, _| {
                // Keep if frontier hasn't fully passed this epoch
                frontier.iter().any(|f| f <= epoch)
            });
        }
    }

    /// Checkpoint state when frontier advances past last checkpoint epoch.
    pub fn maybe_checkpoint(&mut self, frontier: &[u64], rt: &tokio::runtime::Handle) {
        let min_frontier = frontier.iter().copied().min();

        let should_checkpoint = match (min_frontier, self.last_checkpoint_epoch) {
            (Some(current), Some(last)) => current > last,
            (Some(_), None) | (None, _) => true, // new or computation done
        };

        if should_checkpoint && !self.inner.has_pending() {
            let ctx = self.inner.context_mut();
            if let Err(e) = rt.block_on(ctx.checkpoint()) {
                tracing::error!("checkpoint failed: {e}");
            }
            self.last_checkpoint_epoch = min_frontier;
        }
    }

    /// Get mutable reference to the state context (for final checkpoint).
    pub fn context_mut(&mut self) -> &mut StateContext {
        self.inner.context_mut()
    }
}

/// Wraps a type-erased `ErasedOperator` + `StateContext` with frontier-based
/// checkpoint tracking for the dataflow graph execution path.
///
/// Analogous to [`TimelyAsyncOperator`] but for `Box<dyn ErasedOperator>`.
pub(crate) struct TimelyErasedOperator {
    op: Box<dyn ErasedOperator>,
    ctx: StateContext,
    last_checkpoint_epoch: Option<u64>,
}

impl TimelyErasedOperator {
    /// Create a new erased operator wrapper.
    pub fn new(op: Box<dyn ErasedOperator>, ctx: StateContext) -> Self {
        Self {
            op,
            ctx,
            last_checkpoint_epoch: None,
        }
    }

    /// Process a type-erased input item. Blocks on the Tokio runtime since
    /// we're running on a Timely worker thread (not a Tokio thread).
    ///
    /// Returns `(outputs, errors)`.
    pub fn process(
        &mut self,
        input: AnyItem,
        rt: &tokio::runtime::Handle,
    ) -> (Vec<AnyItem>, Vec<anyhow::Error>) {
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
    ) -> Option<u64> {
        let min_frontier = frontier.iter().copied().min();

        let should_checkpoint = match (min_frontier, self.last_checkpoint_epoch) {
            (Some(current), Some(last)) => current > last,
            (Some(_), None) | (None, _) => true,
        };

        if should_checkpoint {
            let ckpt_start = std::time::Instant::now();
            if let Err(e) = rt.block_on(self.ctx.checkpoint()) {
                tracing::error!("checkpoint failed: {e}");
            }
            metrics::gauge!("executor_checkpoint_duration_seconds")
                .set(ckpt_start.elapsed().as_secs_f64());
            self.last_checkpoint_epoch = min_frontier;
            Some(min_frontier.unwrap_or(0))
        } else {
            None
        }
    }

    /// Process a watermark advancement. Delegates to the operator's `on_watermark`.
    /// Returns any outputs produced (e.g. closed windows).
    pub fn process_watermark(
        &mut self,
        watermark: u64,
        rt: &tokio::runtime::Handle,
    ) -> Vec<AnyItem> {
        match rt.block_on(self.op.on_watermark(watermark, &mut self.ctx)) {
            Ok(results) => results,
            Err(e) => {
                tracing::error!("watermark processing failed: {e}");
                vec![]
            }
        }
    }

    /// Force a checkpoint (for final flush).
    #[allow(dead_code)]
    pub fn checkpoint(&mut self, rt: &tokio::runtime::Handle) {
        let ckpt_start = std::time::Instant::now();
        if let Err(e) = rt.block_on(self.ctx.checkpoint()) {
            tracing::error!("final checkpoint failed: {e}");
        }
        metrics::gauge!("executor_checkpoint_duration_seconds")
            .set(ckpt_start.elapsed().as_secs_f64());
    }
}

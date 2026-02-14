use std::collections::HashMap;

use rill_core::state::context::StateContext;
use rill_core::traits::StreamFunction;

use crate::async_operator::AsyncOperator;

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
    /// Returns immediately-completed outputs.
    pub fn process(&mut self, input: F::Input, epoch: u64) -> Vec<F::Output> {
        self.retained_caps
            .entry(epoch)
            .or_insert(CapabilityToken { epoch });
        self.inner.process_element(input, Some(epoch))
    }

    /// Poll pending futures and collect completed results.
    pub fn poll_pending(&mut self) -> Vec<F::Output> {
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

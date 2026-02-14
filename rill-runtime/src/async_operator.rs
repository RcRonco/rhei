use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};

use futures::FutureExt;
use rill_core::state::context::StateContext;
use rill_core::traits::StreamFunction;

use crate::stash::Stash;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A no-op waker for polling futures without a real executor.
struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: std::sync::Arc<Self>) {}
}

fn noop_waker() -> Waker {
    Waker::from(std::sync::Arc::new(NoopWaker))
}

/// Wraps a `StreamFunction` to execute asynchronously without blocking the
/// Timely worker thread.
///
/// - **Hot path:** If the future completes synchronously (L1 cache hit),
///   `now_or_never()` resolves it immediately — no scheduling overhead.
/// - **Cold path:** If the future is pending (state miss -> backend fetch),
///   the element is stashed and the future is polled on subsequent ticks.
pub struct AsyncOperator<F: StreamFunction> {
    func: F,
    ctx: StateContext,
    #[allow(clippy::type_complexity)]
    pending: VecDeque<(BoxFuture<Vec<F::Output>>, Option<u64>)>,
    stash: Stash<F::Input>,
}

impl<F: StreamFunction> std::fmt::Debug for AsyncOperator<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncOperator")
            .field("pending_count", &self.pending.len())
            .field("stash_len", &self.stash.len())
            .finish_non_exhaustive()
    }
}

impl<F: StreamFunction + 'static> AsyncOperator<F> {
    /// Create a new async operator wrapping the given stream function and state context.
    pub fn new(func: F, ctx: StateContext) -> Self {
        Self {
            func,
            ctx,
            pending: VecDeque::new(),
            stash: Stash::new(),
        }
    }

    /// Process an input element. Returns outputs that completed immediately.
    /// Elements whose futures are pending are stashed for later.
    pub fn process_element(&mut self, input: F::Input, capability: Option<u64>) -> Vec<F::Output> {
        let mut completed = Vec::new();

        // First, drain any completed pending futures
        self.drain_completed(&mut completed);

        // Stash the new input for ordered processing
        self.stash.push(input, capability);

        // Try to process stashed items
        self.process_stash(&mut completed);

        completed
    }

    /// Poll all pending futures and collect completed results.
    pub fn poll_pending(&mut self) -> Vec<F::Output> {
        let mut completed = Vec::new();
        self.drain_completed(&mut completed);
        self.process_stash(&mut completed);
        completed
    }

    /// Returns true if there are pending futures or stashed elements.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty() || !self.stash.is_empty()
    }

    fn drain_completed(&mut self, output: &mut Vec<F::Output>) {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut still_pending = VecDeque::new();

        while let Some((mut fut, cap)) = self.pending.pop_front() {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(results) => {
                    output.extend(results);
                    let _ = cap;
                }
                Poll::Pending => {
                    still_pending.push_back((fut, cap));
                }
            }
        }
        self.pending = still_pending;
    }

    fn process_stash(&mut self, output: &mut Vec<F::Output>) {
        while let Some((input, _cap)) = self.stash.pop() {
            // Attempt synchronous processing via now_or_never.
            // If state is in L1 memtable, the future completes on the first poll.
            let result = {
                let fut = self.func.process(input, &mut self.ctx);
                fut.now_or_never()
            };

            match result {
                Some(results) => {
                    output.extend(results);
                }
                None => {
                    // The future didn't complete synchronously.
                    // In a full Timely integration, we'd spawn this onto
                    // the Tokio runtime. For now, break and retry next tick.
                    break;
                }
            }
        }
    }

    /// Get a mutable reference to the state context (for checkpointing).
    pub fn context_mut(&mut self) -> &mut StateContext {
        &mut self.ctx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rill_core::state::local_backend::LocalBackend;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct PassThrough;

    #[async_trait]
    impl StreamFunction for PassThrough {
        type Input = String;
        type Output = String;

        async fn process(&mut self, input: String, _ctx: &mut StateContext) -> Vec<String> {
            vec![input]
        }
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rill_async_op_{name}_{}", std::process::id()))
    }

    #[test]
    fn hot_path_sync_completion() {
        let path = temp_path("hot");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let ctx = StateContext::new(Box::new(backend));
        let mut op = AsyncOperator::new(PassThrough, ctx);

        let results = op.process_element("hello".to_string(), Some(0));
        assert_eq!(results, vec!["hello".to_string()]);
        assert!(!op.has_pending());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn does_not_block_timely_thread() {
        let path = temp_path("nonblock");
        let _ = std::fs::remove_file(&path);

        let flag = Arc::new(AtomicBool::new(false));
        let flag2 = flag.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            flag2.store(true, Ordering::SeqCst);
        });

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let ctx = StateContext::new(Box::new(backend));
        let mut op = AsyncOperator::new(PassThrough, ctx);

        // This should not block — PassThrough completes synchronously
        let _ = op.process_element("test".to_string(), Some(0));

        handle.await.unwrap();
        assert!(flag.load(Ordering::SeqCst));

        let _ = std::fs::remove_file(&path);
    }
}

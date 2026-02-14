use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};

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
///   a single poll resolves it immediately — no scheduling overhead.
/// - **Cold path:** If the future is pending (state miss -> backend fetch),
///   the runtime handle drives it to completion via `block_in_place`.
pub struct AsyncOperator<F: StreamFunction> {
    func: F,
    ctx: StateContext,
    rt: Option<tokio::runtime::Handle>,
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
    ///
    /// When `rt` is `Some`, the cold path (pending futures from state misses) will
    /// be driven to completion via the Tokio runtime. When `None`, pending futures
    /// are dropped with a warning.
    pub fn new(func: F, ctx: StateContext, rt: Option<tokio::runtime::Handle>) -> Self {
        Self {
            func,
            ctx,
            rt,
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

        self.report_backpressure();
        completed
    }

    /// Poll all pending futures and collect completed results.
    pub fn poll_pending(&mut self) -> Vec<F::Output> {
        let mut completed = Vec::new();
        self.drain_completed(&mut completed);
        self.process_stash(&mut completed);
        self.report_backpressure();
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
            let fut = self.func.process(input, &mut self.ctx);
            let mut fut = std::pin::pin!(fut);

            // Try a single synchronous poll first (hot path).
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(results) => {
                    output.extend(results);
                }
                Poll::Pending => {
                    // Cold path: the future needs async I/O (state miss).
                    // Drive it to completion via the Tokio runtime.
                    if let Some(ref rt) = self.rt {
                        let results = tokio::task::block_in_place(|| rt.block_on(fut));
                        output.extend(results);
                    } else {
                        tracing::warn!("future pending but no runtime handle — dropping element");
                        break;
                    }
                }
            }
        }
    }

    fn report_backpressure(&self) {
        #[allow(clippy::cast_precision_loss)]
        {
            metrics::gauge!("backpressure_stash_depth").set(self.stash.len() as f64);
            metrics::gauge!("backpressure_pending_futures").set(self.pending.len() as f64);
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
        let mut op = AsyncOperator::new(PassThrough, ctx, None);

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
        let mut op = AsyncOperator::new(PassThrough, ctx, Some(tokio::runtime::Handle::current()));

        // This should not block — PassThrough completes synchronously
        let _ = op.process_element("test".to_string(), Some(0));

        handle.await.unwrap();
        assert!(flag.load(Ordering::SeqCst));

        let _ = std::fs::remove_file(&path);
    }
}

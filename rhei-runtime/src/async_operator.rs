use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};

use rhei_core::state::context::StateContext;
use rhei_core::traits::StreamFunction;

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
    /// Scaffolding for a future async cold path. Currently unused because
    /// `process_stash` drives pending futures to completion via `block_in_place`.
    /// A true async cold path would require an API redesign (e.g., `'static`
    /// futures or split prepare/complete) since `StreamFunction::process` borrows
    /// `&mut self` + `&mut StateContext`.
    #[allow(clippy::type_complexity)]
    pending: VecDeque<(BoxFuture<anyhow::Result<Vec<F::Output>>>, Option<u64>)>,
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

    /// Process an input element. Returns outputs that completed immediately and
    /// any errors from operator processing.
    pub fn process_element(
        &mut self,
        input: F::Input,
        capability: Option<u64>,
    ) -> (Vec<F::Output>, Vec<anyhow::Error>) {
        let mut completed = Vec::new();
        let mut errors = Vec::new();

        // First, drain any completed pending futures
        self.drain_completed(&mut completed, &mut errors);

        // Stash the new input for ordered processing
        self.stash.push(input, capability);

        // Try to process stashed items
        self.process_stash(&mut completed, &mut errors);

        self.report_backpressure();
        (completed, errors)
    }

    /// Poll all pending futures and collect completed results.
    ///
    /// Note: Currently the `pending` queue is always empty because `process_stash`
    /// uses `block_in_place` to drive futures synchronously. This method is
    /// scaffolding for a future async cold path (see KI-11).
    pub fn poll_pending(&mut self) -> (Vec<F::Output>, Vec<anyhow::Error>) {
        let mut completed = Vec::new();
        let mut errors = Vec::new();
        self.drain_completed(&mut completed, &mut errors);
        self.process_stash(&mut completed, &mut errors);
        self.report_backpressure();
        (completed, errors)
    }

    /// Returns true if there are pending futures or stashed elements.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty() || !self.stash.is_empty()
    }

    /// Drains completed futures from the `pending` queue.
    ///
    /// Note: Currently the `pending` queue is always empty because `process_stash`
    /// uses `block_in_place` to drive futures synchronously. This method is
    /// scaffolding for a future async cold path (see KI-11).
    fn drain_completed(&mut self, output: &mut Vec<F::Output>, errors: &mut Vec<anyhow::Error>) {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut still_pending = VecDeque::new();

        while let Some((mut fut, cap)) = self.pending.pop_front() {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(results)) => {
                    output.extend(results);
                    // Capability is intentionally unused — not retained for frontier
                    // tracking. Retaining caps for a future async cold path requires
                    // an API redesign (see KI-11).
                    let _ = cap;
                }
                Poll::Ready(Err(e)) => {
                    errors.push(e);
                    let _ = cap;
                }
                Poll::Pending => {
                    still_pending.push_back((fut, cap));
                }
            }
        }
        self.pending = still_pending;
    }

    fn process_stash(&mut self, output: &mut Vec<F::Output>, errors: &mut Vec<anyhow::Error>) {
        while let Some((input, _cap)) = self.stash.pop() {
            let fut = self.func.process(input, &mut self.ctx);
            let mut fut = std::pin::pin!(fut);

            // Try a single synchronous poll first (hot path).
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(results)) => {
                    output.extend(results);
                }
                Poll::Ready(Err(e)) => {
                    errors.push(e);
                }
                Poll::Pending => {
                    // Cold path: the future needs async I/O (state miss).
                    // Drive it to completion via the Tokio runtime.
                    //
                    // block_in_place panics on a current_thread runtime.
                    // The runtime must be multi-threaded for the cold path
                    // to work. Assert this in debug builds to catch
                    // misconfiguration early.
                    if let Some(ref rt) = self.rt {
                        debug_assert!(
                            rt.runtime_flavor() != tokio::runtime::RuntimeFlavor::CurrentThread,
                            "block_in_place requires a multi-threaded Tokio \
                             runtime, but a current_thread runtime was provided. \
                             Ensure the executor is configured with a \
                             multi-threaded runtime for async state backends."
                        );
                        match tokio::task::block_in_place(|| rt.block_on(fut)) {
                            Ok(results) => output.extend(results),
                            Err(e) => errors.push(e),
                        }
                    } else {
                        tracing::error!(
                            "future pending but no runtime handle — element dropped. \
                             Async state backends (L2/L3) require a runtime handle."
                        );
                        metrics::counter!("async_operator_dropped_elements_total").increment(1);
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rhei_core::state::local_backend::LocalBackend;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct PassThrough;

    #[async_trait]
    impl StreamFunction for PassThrough {
        type Input = String;
        type Output = String;

        async fn process(
            &mut self,
            input: String,
            _ctx: &mut StateContext,
        ) -> anyhow::Result<Vec<String>> {
            Ok(vec![input])
        }
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rhei_async_op_{name}_{}", std::process::id()))
    }

    #[test]
    fn hot_path_sync_completion() {
        let path = temp_path("hot");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        let ctx = StateContext::new(Box::new(backend));
        let mut op = AsyncOperator::new(PassThrough, ctx, None);

        let (results, errors) = op.process_element("hello".to_string(), Some(0));
        assert_eq!(results, vec!["hello".to_string()]);
        assert!(errors.is_empty());
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

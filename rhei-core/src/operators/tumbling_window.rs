//! Tumbling window operator with pluggable aggregation.
//!
//! Groups elements into fixed-size, non-overlapping time windows keyed by a
//! grouping function. When an element arrives in a new window, the previous
//! window is closed and its aggregate is emitted.

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::time::{TimeProvider, WallClockProvider};

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

use super::aggregator::Aggregator;
use super::keyed_state::KeyedState;

/// The output emitted when a window closes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowOutput<V> {
    /// The grouping key for this window.
    pub key: String,
    /// Inclusive start of the window (timestamp).
    pub window_start: u64,
    /// Exclusive end of the window (timestamp).
    pub window_end: u64,
    /// The aggregated value.
    pub value: V,
}

impl<V: fmt::Display> fmt::Display for WindowOutput<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "window: {} [{}..{}) value={}",
            self.key, self.window_start, self.window_end, self.value
        )
    }
}

/// A tumbling (fixed-size, non-overlapping) window operator.
///
/// # Type Parameters
///
/// - `T` — input element type
/// - `A` — aggregator
/// - `KF` — key extraction function `Fn(&T) -> String`
/// - `TF` — timestamp extraction function `Fn(&T) -> u64`
pub struct TumblingWindow<T, A, KF, TF> {
    window_size: u64,
    key_fn: KF,
    time_fn: TF,
    aggregator: A,
    /// In-memory set of keys with open windows (for watermark-driven closure).
    active_keys: HashSet<String>,
    /// Configurable allowed lateness for late event detection (default: 0).
    allowed_lateness: u64,
    /// Last seen watermark for late event detection.
    last_watermark: u64,
    _phantom: PhantomData<T>,
}

impl<T, A: Clone, KF: Clone, TF: Clone> Clone for TumblingWindow<T, A, KF, TF> {
    fn clone(&self) -> Self {
        Self {
            window_size: self.window_size,
            key_fn: self.key_fn.clone(),
            time_fn: self.time_fn.clone(),
            aggregator: self.aggregator.clone(),
            active_keys: HashSet::new(),
            allowed_lateness: self.allowed_lateness,
            last_watermark: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF, TF> fmt::Debug for TumblingWindow<T, A, KF, TF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TumblingWindow")
            .field("window_size", &self.window_size)
            .finish_non_exhaustive()
    }
}

impl<T, A, KF, TF> TumblingWindow<T, A, KF, TF> {
    /// Creates a new tumbling window operator.
    pub fn new(window_size: u64, key_fn: KF, time_fn: TF, aggregator: A) -> Self {
        Self {
            window_size,
            key_fn,
            time_fn,
            aggregator,
            active_keys: HashSet::new(),
            allowed_lateness: 0,
            last_watermark: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T> TumblingWindow<T, (), (), ()> {
    /// Returns a builder for constructing a `TumblingWindow`.
    pub fn builder() -> TumblingWindowBuilder<T> {
        TumblingWindowBuilder {
            window_size: 0,
            key_fn: (),
            time_fn: (),
            aggregator: (),
            allowed_lateness: 0,
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`TumblingWindow`].
pub struct TumblingWindowBuilder<T, A = (), KF = (), TF = ()> {
    window_size: u64,
    key_fn: KF,
    time_fn: TF,
    aggregator: A,
    allowed_lateness: u64,
    _phantom: PhantomData<T>,
}

impl<T, A, KF, TF> fmt::Debug for TumblingWindowBuilder<T, A, KF, TF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TumblingWindowBuilder")
            .field("window_size", &self.window_size)
            .finish_non_exhaustive()
    }
}

impl<T, A, KF, TF> TumblingWindowBuilder<T, A, KF, TF> {
    /// Sets the window size in timestamp units.
    pub fn window_size(mut self, size: u64) -> Self {
        self.window_size = size;
        self
    }

    /// Sets the allowed lateness in timestamp units (default: 0).
    pub fn allowed_lateness(mut self, lateness: u64) -> Self {
        self.allowed_lateness = lateness;
        self
    }

    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> TumblingWindowBuilder<T, A, KF2, TF> {
        TumblingWindowBuilder {
            window_size: self.window_size,
            key_fn: kf,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
            allowed_lateness: self.allowed_lateness,
            _phantom: PhantomData,
        }
    }

    /// Sets the timestamp extraction function.
    pub fn time_fn<TF2>(self, tf: TF2) -> TumblingWindowBuilder<T, A, KF, TF2> {
        TumblingWindowBuilder {
            window_size: self.window_size,
            key_fn: self.key_fn,
            time_fn: tf,
            aggregator: self.aggregator,
            allowed_lateness: self.allowed_lateness,
            _phantom: PhantomData,
        }
    }

    /// Sets the aggregator.
    pub fn aggregator<A2>(self, agg: A2) -> TumblingWindowBuilder<T, A2, KF, TF> {
        TumblingWindowBuilder {
            window_size: self.window_size,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            aggregator: agg,
            allowed_lateness: self.allowed_lateness,
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF> TumblingWindowBuilder<T, A, KF, ()> {
    /// Uses wall-clock (processing) time instead of event time.
    ///
    /// The timestamp extraction function is replaced by a closure that calls
    /// [`WallClockProvider::current_time()`].
    pub fn proc_time_fn(
        self,
    ) -> TumblingWindowBuilder<T, A, KF, impl Fn(&T) -> u64 + Send + Sync + Clone> {
        let provider = WallClockProvider;
        self.time_fn(move |_: &T| provider.current_time())
    }

    /// Uses a custom [`TimeProvider`] for processing-time semantics.
    ///
    /// Pass a [`FixedClockProvider`](crate::time::FixedClockProvider) for
    /// deterministic testing or replay.
    pub fn proc_time_fn_with(
        self,
        provider: Arc<dyn TimeProvider>,
    ) -> TumblingWindowBuilder<T, A, KF, impl Fn(&T) -> u64 + Send + Sync + Clone> {
        self.time_fn(move |_: &T| provider.current_time())
    }
}

impl<T, A, KF, TF> TumblingWindowBuilder<T, A, KF, TF>
where
    T: Send + Sync,
    A: Aggregator<Input = T>,
    A::Output: Send,
    KF: Fn(&T) -> String + Send + Sync,
    TF: Fn(&T) -> u64 + Send + Sync,
{
    /// Builds the `TumblingWindow` operator.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` is zero.
    pub fn build(self) -> TumblingWindow<T, A, KF, TF> {
        assert!(self.window_size > 0, "window_size must be > 0");
        TumblingWindow {
            window_size: self.window_size,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
            active_keys: HashSet::new(),
            allowed_lateness: self.allowed_lateness,
            last_watermark: 0,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, A, KF, TF> StreamFunction for TumblingWindow<T, A, KF, TF>
where
    T: Clone + Send + Sync + std::fmt::Debug,
    A: Aggregator<Input = T>,
    A::Output: Clone + Send + std::fmt::Debug,
    KF: Fn(&T) -> String + Send + Sync,
    TF: Fn(&T) -> u64 + Send + Sync,
{
    type Input = T;
    type Output = WindowOutput<A::Output>;

    async fn process(
        &mut self,
        input: T,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<WindowOutput<A::Output>>> {
        let key = (self.key_fn)(&input);
        let timestamp = (self.time_fn)(&input);
        let window_start = timestamp - (timestamp % self.window_size);
        let window_end = window_start + self.window_size;

        // Late event detection: drop if window is past watermark + allowed_lateness.
        if window_end + self.allowed_lateness <= self.last_watermark {
            metrics::counter!("late_events_dropped_total").increment(1);
            return Ok(vec![]);
        }

        let mut outputs = Vec::new();

        // Track this key as active for watermark-driven closure.
        self.active_keys.insert(key.clone());

        // Load the active window for this key
        let active_window: Option<u64> = {
            let mut state = KeyedState::<String, u64>::new(ctx, "win");
            state.get(&key).await.unwrap_or(None)
        };

        // If the active window differs, close the old one and emit its aggregate
        if let Some(old_start) = active_window
            && old_start != window_start
        {
            let acc_key = format!("{key}:{old_start}");
            let old_acc: Option<A::Accumulator> = {
                let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "acc");
                state.get(&acc_key).await.unwrap_or(None)
            };
            if let Some(acc) = old_acc {
                outputs.push(WindowOutput {
                    key: key.clone(),
                    window_start: old_start,
                    window_end: old_start + self.window_size,
                    value: self.aggregator.finish(&acc),
                });
            }
            // Delete old accumulator
            {
                let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "acc");
                state.delete(&acc_key);
            }
        }

        // Load or create accumulator for the current window
        let acc_key = format!("{key}:{window_start}");
        let mut acc: A::Accumulator = {
            let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "acc");
            state
                .get(&acc_key)
                .await
                .unwrap_or(None)
                .unwrap_or_default()
        };

        // Accumulate
        self.aggregator.accumulate(&mut acc, &input);

        // Store updated accumulator
        {
            let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "acc");
            state.put(&acc_key, &acc);
        }

        // Update active window tracker
        {
            let mut state = KeyedState::<String, u64>::new(ctx, "win");
            state.put(&key, &window_start);
        }

        Ok(outputs)
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<WindowOutput<A::Output>>> {
        self.last_watermark = watermark;
        let mut outputs = Vec::new();
        let mut closed_keys = Vec::new();

        for key in &self.active_keys {
            let active_window: Option<u64> = {
                let mut state = KeyedState::<String, u64>::new(ctx, "win");
                state.get(key).await.unwrap_or(None)
            };

            if let Some(win_start) = active_window
                && win_start + self.window_size + self.allowed_lateness <= watermark
            {
                // Close this window.
                let acc_key = format!("{key}:{win_start}");
                let acc: Option<A::Accumulator> = {
                    let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "acc");
                    state.get(&acc_key).await.unwrap_or(None)
                };
                if let Some(acc) = acc {
                    outputs.push(WindowOutput {
                        key: key.clone(),
                        window_start: win_start,
                        window_end: win_start + self.window_size,
                        value: self.aggregator.finish(&acc),
                    });
                }
                // Clean up state.
                {
                    let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "acc");
                    state.delete(&acc_key);
                }
                {
                    let mut state = KeyedState::<String, u64>::new(ctx, "win");
                    state.delete(key);
                }
                closed_keys.push(key.clone());
            }
        }

        for key in &closed_keys {
            self.active_keys.remove(key);
        }

        Ok(outputs)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::operators::aggregator::Count;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;
    use crate::time::FixedClockProvider;

    fn test_ctx(name: &str) -> StateContext {
        let path = std::env::temp_dir().join(format!("rhei_tw_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[allow(clippy::type_complexity)]
    fn make_window() -> TumblingWindow<
        (String, u64),
        Count<(String, u64)>,
        fn(&(String, u64)) -> String,
        fn(&(String, u64)) -> u64,
    > {
        TumblingWindow::new(
            10,
            (|e: &(String, u64)| e.0.clone()) as fn(&(String, u64)) -> String,
            (|e: &(String, u64)| e.1) as fn(&(String, u64)) -> u64,
            Count::new(),
        )
    }

    #[tokio::test]
    async fn tumbling_watermark_closes_window() {
        let mut ctx = test_ctx("wm_close");
        let mut win = make_window();

        // Process elements in window [0, 10)
        let r = win.process(("a".into(), 1), &mut ctx).await.unwrap();
        assert!(r.is_empty());
        let r = win.process(("a".into(), 5), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Watermark at 10 should close window [0, 10)
        let r = win.on_watermark(10, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, "a");
        assert_eq!(r[0].window_start, 0);
        assert_eq!(r[0].window_end, 10);
        assert_eq!(r[0].value, 2);
    }

    #[tokio::test]
    async fn tumbling_watermark_no_close_before_end() {
        let mut ctx = test_ctx("wm_no_close");
        let mut win = make_window();

        // Process element in window [0, 10)
        let r = win.process(("a".into(), 3), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Watermark at 5 — window [0, 10) not complete yet
        let r = win.on_watermark(5, &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn tumbling_late_event_dropped() {
        let mut ctx = test_ctx("late_drop");
        let mut win = make_window();

        // Process element in window [0, 10)
        win.process(("a".into(), 3), &mut ctx).await.unwrap();

        // Advance watermark past window [0, 10)
        win.on_watermark(10, &mut ctx).await.unwrap();

        // Late event for window [0, 10) should be dropped
        let r = win.process(("a".into(), 2), &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn tumbling_allowed_lateness_grace_period() {
        let mut ctx = test_ctx("lateness_grace");
        let mut win = TumblingWindow::<(String, u64), _, _, _>::builder()
            .window_size(10)
            .allowed_lateness(5)
            .key_fn(|e: &(String, u64)| e.0.clone())
            .time_fn(|e: &(String, u64)| e.1)
            .aggregator(Count::new())
            .build();

        // Process element in window [0, 10)
        win.process(("a".into(), 3), &mut ctx).await.unwrap();

        // Watermark at 10 — window end (10) + allowed_lateness (5) = 15 > 10, so no close
        let r = win.on_watermark(10, &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Watermark at 15 — now 0 + 10 + 5 <= 15, so close
        let r = win.on_watermark(15, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].value, 1);
    }

    #[tokio::test]
    async fn tumbling_proc_time_with_fixed_clock() {
        let mut ctx = test_ctx("proc_fixed");
        let clock = FixedClockProvider::new(5);

        let mut win = TumblingWindow::<String, _, _, _>::builder()
            .window_size(10)
            .key_fn(|e: &String| e.clone())
            .proc_time_fn_with(Arc::new(clock.clone()))
            .aggregator(Count::new())
            .build();

        // Clock at 5 → window [0, 10)
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Advance clock to 15 → window [10, 20), closes [0, 10)
        clock.set(15);
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].window_start, 0);
        assert_eq!(r[0].window_end, 10);
        assert_eq!(r[0].value, 1);
    }

    #[tokio::test]
    async fn tumbling_proc_time_fn_smoke() {
        let mut ctx = test_ctx("proc_wall");
        let mut win = TumblingWindow::<String, _, _, _>::builder()
            .window_size(60_000)
            .key_fn(|e: &String| e.clone())
            .proc_time_fn()
            .aggregator(Count::new())
            .build();

        // Just verify it compiles and runs without panic
        let r = win.process("x".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }
}

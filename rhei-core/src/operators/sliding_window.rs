//! Sliding window operator with pluggable aggregation.
//!
//! Groups elements into overlapping time windows defined by `window_size` and
//! `slide`. Each element belongs to every open window whose range contains
//! its timestamp. When an element arrives whose timestamp is past a window's
//! end, that window is closed and its aggregate is emitted.

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
use super::tumbling_window::WindowOutput;

/// Tracks which window start values are currently active for a given key.
#[derive(Serialize, Deserialize, Default)]
struct ActiveWindows {
    starts: Vec<u64>,
}

/// A sliding (overlapping) window operator.
///
/// # Type Parameters
///
/// - `T` — input element type
/// - `A` — aggregator
/// - `KF` — key extraction function `Fn(&T) -> String`
/// - `TF` — timestamp extraction function `Fn(&T) -> u64`
pub struct SlidingWindow<T, A, KF, TF> {
    window_size: u64,
    slide: u64,
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

impl<T, A: Clone, KF: Clone, TF: Clone> Clone for SlidingWindow<T, A, KF, TF> {
    fn clone(&self) -> Self {
        Self {
            window_size: self.window_size,
            slide: self.slide,
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

impl<T, A, KF, TF> fmt::Debug for SlidingWindow<T, A, KF, TF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlidingWindow")
            .field("window_size", &self.window_size)
            .field("slide", &self.slide)
            .finish_non_exhaustive()
    }
}

impl<T, A, KF, TF> SlidingWindow<T, A, KF, TF> {
    /// Creates a new sliding window operator.
    pub fn new(window_size: u64, slide: u64, key_fn: KF, time_fn: TF, aggregator: A) -> Self {
        Self {
            window_size,
            slide,
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

impl<T> SlidingWindow<T, (), (), ()> {
    /// Returns a builder for constructing a `SlidingWindow`.
    pub fn builder() -> SlidingWindowBuilder<T> {
        SlidingWindowBuilder {
            window_size: 0,
            slide: 0,
            key_fn: (),
            time_fn: (),
            aggregator: (),
            allowed_lateness: 0,
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`SlidingWindow`].
pub struct SlidingWindowBuilder<T, A = (), KF = (), TF = ()> {
    window_size: u64,
    slide: u64,
    key_fn: KF,
    time_fn: TF,
    aggregator: A,
    allowed_lateness: u64,
    _phantom: PhantomData<T>,
}

impl<T, A, KF, TF> fmt::Debug for SlidingWindowBuilder<T, A, KF, TF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlidingWindowBuilder")
            .field("window_size", &self.window_size)
            .field("slide", &self.slide)
            .finish_non_exhaustive()
    }
}

impl<T, A, KF, TF> SlidingWindowBuilder<T, A, KF, TF> {
    /// Sets the window size in timestamp units.
    pub fn window_size(mut self, size: u64) -> Self {
        self.window_size = size;
        self
    }

    /// Sets the slide interval in timestamp units.
    pub fn slide(mut self, slide: u64) -> Self {
        self.slide = slide;
        self
    }

    /// Sets the allowed lateness in timestamp units (default: 0).
    pub fn allowed_lateness(mut self, lateness: u64) -> Self {
        self.allowed_lateness = lateness;
        self
    }

    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> SlidingWindowBuilder<T, A, KF2, TF> {
        SlidingWindowBuilder {
            window_size: self.window_size,
            slide: self.slide,
            key_fn: kf,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
            allowed_lateness: self.allowed_lateness,
            _phantom: PhantomData,
        }
    }

    /// Sets the timestamp extraction function.
    pub fn time_fn<TF2>(self, tf: TF2) -> SlidingWindowBuilder<T, A, KF, TF2> {
        SlidingWindowBuilder {
            window_size: self.window_size,
            slide: self.slide,
            key_fn: self.key_fn,
            time_fn: tf,
            aggregator: self.aggregator,
            allowed_lateness: self.allowed_lateness,
            _phantom: PhantomData,
        }
    }

    /// Sets the aggregator.
    pub fn aggregator<A2>(self, agg: A2) -> SlidingWindowBuilder<T, A2, KF, TF> {
        SlidingWindowBuilder {
            window_size: self.window_size,
            slide: self.slide,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            aggregator: agg,
            allowed_lateness: self.allowed_lateness,
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF> SlidingWindowBuilder<T, A, KF, ()> {
    /// Uses wall-clock (processing) time instead of event time.
    ///
    /// The timestamp extraction function is replaced by a closure that calls
    /// [`WallClockProvider::current_time()`].
    pub fn proc_time_fn(
        self,
    ) -> SlidingWindowBuilder<T, A, KF, impl Fn(&T) -> u64 + Send + Sync + Clone> {
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
    ) -> SlidingWindowBuilder<T, A, KF, impl Fn(&T) -> u64 + Send + Sync + Clone> {
        self.time_fn(move |_: &T| provider.current_time())
    }
}

impl<T, A, KF, TF> SlidingWindowBuilder<T, A, KF, TF>
where
    T: Send + Sync,
    A: Aggregator<Input = T>,
    A::Output: Send,
    KF: Fn(&T) -> String + Send + Sync,
    TF: Fn(&T) -> u64 + Send + Sync,
{
    /// Builds the `SlidingWindow` operator.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` or `slide` is zero, or if `slide > window_size`.
    pub fn build(self) -> SlidingWindow<T, A, KF, TF> {
        assert!(self.window_size > 0, "window_size must be > 0");
        assert!(self.slide > 0, "slide must be > 0");
        assert!(
            self.slide <= self.window_size,
            "slide must be <= window_size"
        );
        SlidingWindow {
            window_size: self.window_size,
            slide: self.slide,
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

/// Compute all window starts that contain the given timestamp.
///
/// A window `[start, start + window_size)` contains `timestamp` iff
/// `start <= timestamp < start + window_size`, where `start` is a multiple
/// of `slide`.
fn window_starts_for(timestamp: u64, window_size: u64, slide: u64) -> Vec<u64> {
    let latest_slide = timestamp - (timestamp % slide);
    let mut starts = Vec::new();
    let mut start = latest_slide;
    loop {
        if start + window_size > timestamp {
            starts.push(start);
        }
        if start < slide {
            break;
        }
        start -= slide;
        if start + window_size <= timestamp {
            break;
        }
    }
    starts.sort_unstable();
    starts
}

#[async_trait]
impl<T, A, KF, TF> StreamFunction for SlidingWindow<T, A, KF, TF>
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
        let mut outputs = Vec::new();

        // Late event detection: check if ALL windows this element would belong to
        // are past the watermark + allowed_lateness.
        let candidate_starts = window_starts_for(timestamp, self.window_size, self.slide);
        let all_late = !candidate_starts.is_empty()
            && candidate_starts
                .iter()
                .all(|&ws| ws + self.window_size + self.allowed_lateness <= self.last_watermark);
        if all_late {
            metrics::counter!("late_events_dropped_total").increment(1);
            return Ok(vec![]);
        }

        // Track this key as active for watermark-driven closure.
        self.active_keys.insert(key.clone());

        // Load current active windows for this key
        let mut active: ActiveWindows = {
            let mut state = KeyedState::<String, ActiveWindows>::new(ctx, "sw_active");
            state.get(&key).await.unwrap_or(None).unwrap_or_default()
        };

        // Close any windows whose end <= timestamp (the element has advanced past them)
        let mut still_active = Vec::new();
        for &win_start in &active.starts {
            let win_end = win_start + self.window_size;
            if win_end <= timestamp {
                // Window closed — emit its aggregate
                let acc_key = format!("{key}:{win_start}");
                let acc: Option<A::Accumulator> = {
                    let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "sw_acc");
                    state.get(&acc_key).await.unwrap_or(None)
                };
                if let Some(acc) = acc {
                    outputs.push(WindowOutput {
                        key: key.clone(),
                        window_start: win_start,
                        window_end: win_end,
                        value: self.aggregator.finish(&acc),
                    });
                }
                // Clean up accumulator
                let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "sw_acc");
                state.delete(&acc_key)?;
            } else {
                still_active.push(win_start);
            }
        }
        active.starts = still_active;

        // Determine which windows this element belongs to
        let window_starts = window_starts_for(timestamp, self.window_size, self.slide);

        // Accumulate into each window
        for win_start in &window_starts {
            let acc_key = format!("{key}:{win_start}");
            let mut acc: A::Accumulator = {
                let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "sw_acc");
                state
                    .get(&acc_key)
                    .await
                    .unwrap_or(None)
                    .unwrap_or_default()
            };

            self.aggregator.accumulate(&mut acc, &input);

            {
                let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "sw_acc");
                state.put(&acc_key, &acc)?;
            }

            // Track this window as active if not already
            if !active.starts.contains(win_start) {
                active.starts.push(*win_start);
            }
        }

        // Store updated active windows
        active.starts.sort_unstable();
        {
            let mut state = KeyedState::<String, ActiveWindows>::new(ctx, "sw_active");
            state.put(&key, &active)?;
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
        let mut keys_to_remove = Vec::new();

        for key in &self.active_keys {
            let mut active: ActiveWindows = {
                let mut state = KeyedState::<String, ActiveWindows>::new(ctx, "sw_active");
                state.get(key).await.unwrap_or(None).unwrap_or_default()
            };

            let mut still_active = Vec::new();
            for &win_start in &active.starts {
                if win_start + self.window_size + self.allowed_lateness <= watermark {
                    // Close this window.
                    let acc_key = format!("{key}:{win_start}");
                    let acc: Option<A::Accumulator> = {
                        let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "sw_acc");
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
                    let mut state = KeyedState::<String, A::Accumulator>::new(ctx, "sw_acc");
                    state.delete(&acc_key)?;
                } else {
                    still_active.push(win_start);
                }
            }

            active.starts = still_active;
            if active.starts.is_empty() {
                let mut state = KeyedState::<String, ActiveWindows>::new(ctx, "sw_active");
                state.delete(key)?;
                keys_to_remove.push(key.clone());
            } else {
                active.starts.sort_unstable();
                let mut state = KeyedState::<String, ActiveWindows>::new(ctx, "sw_active");
                state.put(key, &active)?;
            }
        }

        for key in &keys_to_remove {
            self.active_keys.remove(key);
        }

        Ok(outputs)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::operators::aggregator::Count;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;
    use crate::time::FixedClockProvider;

    fn test_ctx(name: &str) -> StateContext {
        let path = std::env::temp_dir().join(format!("rhei_sw_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[test]
    fn window_starts_basic() {
        // window_size=10, slide=5
        // timestamp=7 → windows [5,15) and [0,10)
        let starts = window_starts_for(7, 10, 5);
        assert_eq!(starts, vec![0, 5]);
    }

    #[test]
    fn window_starts_on_boundary() {
        // timestamp=10 → windows [5,15) and [10,20)
        let starts = window_starts_for(10, 10, 5);
        assert_eq!(starts, vec![5, 10]);
    }

    #[test]
    fn window_starts_slide_equals_size() {
        // Degenerate case: slide == window_size → tumbling behavior
        let starts = window_starts_for(7, 10, 10);
        assert_eq!(starts, vec![0]);
    }

    #[test]
    fn window_starts_small_timestamp() {
        // timestamp=2, window_size=10, slide=5
        // Only window [0,10) contains it
        let starts = window_starts_for(2, 10, 5);
        assert_eq!(starts, vec![0]);
    }

    #[allow(clippy::type_complexity)]
    fn make_sliding() -> SlidingWindow<
        (String, u64),
        Count<(String, u64)>,
        fn(&(String, u64)) -> String,
        fn(&(String, u64)) -> u64,
    > {
        SlidingWindow::new(
            10,
            5,
            (|e: &(String, u64)| e.0.clone()) as fn(&(String, u64)) -> String,
            (|e: &(String, u64)| e.1) as fn(&(String, u64)) -> u64,
            Count::new(),
        )
    }

    #[tokio::test]
    async fn sliding_watermark_closes_windows() {
        let mut ctx = test_ctx("wm_close");
        let mut win = make_sliding();

        // Element at ts=3 belongs to window [0,10)
        let r = win.process(("a".into(), 3), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Watermark at 10 closes window [0,10) but not [0+5=5, 15)
        // Actually window [0,10): 0+10+0 <= 10 → closed
        let r = win.on_watermark(10, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].window_start, 0);
        assert_eq!(r[0].window_end, 10);
        assert_eq!(r[0].value, 1);
    }

    #[tokio::test]
    async fn sliding_watermark_no_close_before_end() {
        let mut ctx = test_ctx("wm_no_close");
        let mut win = make_sliding();

        // Element at ts=3 belongs to window [0,10)
        win.process(("a".into(), 3), &mut ctx).await.unwrap();

        // Watermark at 5 — window [0,10) not complete yet (0+10 > 5)
        let r = win.on_watermark(5, &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn sliding_late_event_dropped() {
        let mut ctx = test_ctx("late_drop");
        let mut win = make_sliding();

        // Process element to establish windows
        win.process(("a".into(), 3), &mut ctx).await.unwrap();

        // Advance watermark well past all windows containing ts=2
        // ts=2 belongs to [0, 10). 0+10+0 <= 15 → all late
        win.on_watermark(15, &mut ctx).await.unwrap();

        // Late element
        let r = win.process(("a".into(), 2), &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn sliding_proc_time_with_fixed_clock() {
        let mut ctx = test_ctx("proc_fixed");
        let clock = FixedClockProvider::new(3);

        let mut win = SlidingWindow::<String, _, _, _>::builder()
            .window_size(10)
            .slide(5)
            .key_fn(|e: &String| e.clone())
            .proc_time_fn_with(Arc::new(clock.clone()))
            .aggregator(Count::new())
            .build();

        // Clock at 3 → belongs to window [0,10)
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Advance clock to 12 → belongs to [10,20) and [5,15); closes [0,10)
        clock.set(12);
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].window_start, 0);
        assert_eq!(r[0].window_end, 10);
        assert_eq!(r[0].value, 1);
    }

    #[tokio::test]
    async fn sliding_proc_time_fn_smoke() {
        let mut ctx = test_ctx("proc_wall");
        let mut win = SlidingWindow::<String, _, _, _>::builder()
            .window_size(60_000)
            .slide(10_000)
            .key_fn(|e: &String| e.clone())
            .proc_time_fn()
            .aggregator(Count::new())
            .build();

        let r = win.process("x".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }
}

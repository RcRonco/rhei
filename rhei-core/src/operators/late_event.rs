//! Late-event re-emission operator for updating windows on late arrivals.
//!
//! When a late event arrives (after the window has been closed and emitted),
//! [`LateEventWindow`] re-opens the window, incorporates the late event into
//! the accumulator, and re-emits an updated [`WindowOutput`]. This is useful
//! for pipelines that prefer eventual correctness over strict once-per-window
//! semantics.
//!
//! # Semantics
//!
//! - Elements within the current watermark window are accumulated normally
//!   (delegated to the inner aggregator).
//! - Elements whose window has already closed but arrive within the
//!   `late_deadline` are re-accumulated and trigger a re-emission.
//! - Elements arriving after the `late_deadline` are dropped (counted via
//!   metrics).
//!
//! # Analogy
//!
//! Flink's allowed lateness with accumulating/retracting firing modes.

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

use super::aggregator::Aggregator;
use super::keyed_state::KeyedState;
use super::tumbling_window::WindowOutput;

/// Tracks the state of a window that may be re-opened by late events.
#[derive(Serialize, Deserialize)]
struct WindowState<Acc> {
    accumulator: Acc,
    /// Whether the window has been closed/emitted at least once.
    closed: bool,
    /// How many times this window has been re-emitted.
    version: u64,
}

/// A tumbling window operator that re-emits updated results on late arrivals.
///
/// # Type Parameters
///
/// - `T` -- input element type
/// - `A` -- aggregator
/// - `KF` -- key extraction function `Fn(&T) -> String`
/// - `TF` -- timestamp extraction function `Fn(&T) -> u64`
pub struct LateEventWindow<T, A, KF, TF> {
    window_size: u64,
    key_fn: KF,
    time_fn: TF,
    aggregator: A,
    /// How far past window close we still accept late events (timestamp units).
    late_deadline: u64,
    /// Current watermark.
    last_watermark: u64,
    /// Keys with open or re-openable windows.
    active_keys: HashSet<String>,
    _phantom: PhantomData<T>,
}

impl<T, A: Clone, KF: Clone, TF: Clone> Clone for LateEventWindow<T, A, KF, TF> {
    fn clone(&self) -> Self {
        Self {
            window_size: self.window_size,
            key_fn: self.key_fn.clone(),
            time_fn: self.time_fn.clone(),
            aggregator: self.aggregator.clone(),
            late_deadline: self.late_deadline,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF, TF> fmt::Debug for LateEventWindow<T, A, KF, TF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LateEventWindow")
            .field("window_size", &self.window_size)
            .field("late_deadline", &self.late_deadline)
            .finish_non_exhaustive()
    }
}

impl<T> LateEventWindow<T, (), (), ()> {
    /// Returns a builder for constructing a `LateEventWindow`.
    pub fn builder() -> LateEventWindowBuilder<T> {
        LateEventWindowBuilder {
            window_size: 0,
            key_fn: (),
            time_fn: (),
            aggregator: (),
            late_deadline: 0,
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`LateEventWindow`].
pub struct LateEventWindowBuilder<T, A = (), KF = (), TF = ()> {
    window_size: u64,
    key_fn: KF,
    time_fn: TF,
    aggregator: A,
    late_deadline: u64,
    _phantom: PhantomData<T>,
}

impl<T, A, KF, TF> fmt::Debug for LateEventWindowBuilder<T, A, KF, TF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LateEventWindowBuilder")
            .field("window_size", &self.window_size)
            .field("late_deadline", &self.late_deadline)
            .finish_non_exhaustive()
    }
}

impl<T, A, KF, TF> LateEventWindowBuilder<T, A, KF, TF> {
    /// Sets the window size in timestamp units.
    pub fn window_size(mut self, size: u64) -> Self {
        self.window_size = size;
        self
    }

    /// Sets how far past window close late events are still accepted.
    pub fn late_deadline(mut self, deadline: u64) -> Self {
        self.late_deadline = deadline;
        self
    }

    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> LateEventWindowBuilder<T, A, KF2, TF> {
        LateEventWindowBuilder {
            window_size: self.window_size,
            key_fn: kf,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
            late_deadline: self.late_deadline,
            _phantom: PhantomData,
        }
    }

    /// Sets the timestamp extraction function.
    pub fn time_fn<TF2>(self, tf: TF2) -> LateEventWindowBuilder<T, A, KF, TF2> {
        LateEventWindowBuilder {
            window_size: self.window_size,
            key_fn: self.key_fn,
            time_fn: tf,
            aggregator: self.aggregator,
            late_deadline: self.late_deadline,
            _phantom: PhantomData,
        }
    }

    /// Sets the aggregator.
    pub fn aggregator<A2>(self, agg: A2) -> LateEventWindowBuilder<T, A2, KF, TF> {
        LateEventWindowBuilder {
            window_size: self.window_size,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            aggregator: agg,
            late_deadline: self.late_deadline,
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF, TF> LateEventWindowBuilder<T, A, KF, TF>
where
    T: Send + Sync,
    A: Aggregator<Input = T>,
    A::Output: Send,
    KF: Fn(&T) -> String + Send + Sync,
    TF: Fn(&T) -> u64 + Send + Sync,
{
    /// Builds the `LateEventWindow` operator.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` is zero.
    pub fn build(self) -> LateEventWindow<T, A, KF, TF> {
        assert!(self.window_size > 0, "window_size must be > 0");
        LateEventWindow {
            window_size: self.window_size,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
            late_deadline: self.late_deadline,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, A, KF, TF> StreamFunction for LateEventWindow<T, A, KF, TF>
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

        // Check if completely past the late deadline (drop the event).
        if window_end + self.late_deadline < self.last_watermark {
            metrics::counter!("late_events_dropped_total").increment(1);
            return Ok(vec![]);
        }

        self.active_keys.insert(key.clone());

        let state_key = format!("{key}:{window_start}");

        // Load or create window state.
        let mut ws: WindowState<A::Accumulator> = {
            let mut state = KeyedState::<String, WindowState<A::Accumulator>>::new(ctx, "lew");
            state
                .get(&state_key)
                .await
                .unwrap_or(None)
                .unwrap_or(WindowState {
                    accumulator: A::Accumulator::default(),
                    closed: false,
                    version: 0,
                })
        };

        // Accumulate the input.
        self.aggregator.accumulate(&mut ws.accumulator, &input);

        let mut outputs = Vec::new();

        // Determine if this is a late event (window already closed).
        let is_late = window_end <= self.last_watermark;
        if is_late && ws.closed {
            // Re-emit with updated aggregate.
            ws.version += 1;
            metrics::counter!("late_events_re_emitted_total").increment(1);
            outputs.push(WindowOutput {
                key: key.clone(),
                window_start,
                window_end,
                value: self.aggregator.finish(&ws.accumulator),
            });
        }

        // Store updated state.
        {
            let mut state = KeyedState::<String, WindowState<A::Accumulator>>::new(ctx, "lew");
            state.put(&state_key, &ws)?;
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

        // We need to track which window each key is in. We iterate active keys
        // and check their windows. We use a simple scan approach: for each key,
        // check if there are windows ready to close.
        //
        // Note: In this simplified implementation, we track one active window per
        // key. A production implementation would use a window index.
        for key in &self.active_keys {
            // Look for unclosed windows for this key by checking the current
            // window based on the watermark.
            // We need to find windows that should close. Since we don't track
            // all window starts per key, we scan a reasonable range.
            // For simplicity, check windows that could close at this watermark.

            // Find all windows ending at or before the watermark.
            // We check the window that ends at the latest slide before watermark.
            let mut any_remaining = false;

            // Check windows ending in the range [0, watermark]
            // We only need to check windows that could have data.
            // Use a scan approach: check windows starting from 0 up to watermark.
            // In practice, we'd have an index. Here we check the window
            // corresponding to watermark - window_size.
            let candidate_start = watermark.saturating_sub(self.window_size);
            // Check a range of recent windows (current and a few past).
            let num_check = (self.late_deadline / self.window_size)
                .saturating_add(2)
                .min(20);
            for i in 0..num_check {
                let ws_start = if candidate_start >= i * self.window_size {
                    candidate_start - i * self.window_size
                } else {
                    break;
                };
                // Align to window boundary.
                let aligned_start = ws_start - (ws_start % self.window_size);
                let state_key = format!("{key}:{aligned_start}");
                let ws_opt: Option<WindowState<A::Accumulator>> = {
                    let mut state =
                        KeyedState::<String, WindowState<A::Accumulator>>::new(ctx, "lew");
                    state.get(&state_key).await.unwrap_or(None)
                };

                if let Some(mut ws) = ws_opt {
                    let win_end = aligned_start + self.window_size;
                    if win_end <= watermark && !ws.closed {
                        // Close and emit.
                        ws.closed = true;
                        outputs.push(WindowOutput {
                            key: key.clone(),
                            window_start: aligned_start,
                            window_end: win_end,
                            value: self.aggregator.finish(&ws.accumulator),
                        });
                        let mut state =
                            KeyedState::<String, WindowState<A::Accumulator>>::new(ctx, "lew");
                        state.put(&state_key, &ws)?;
                    }

                    // Clean up if past late deadline.
                    if win_end + self.late_deadline < watermark {
                        let mut state =
                            KeyedState::<String, WindowState<A::Accumulator>>::new(ctx, "lew");
                        state.delete(&state_key)?;
                    } else {
                        any_remaining = true;
                    }
                }
            }

            if !any_remaining {
                keys_to_remove.push(key.clone());
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
    use super::*;
    use crate::operators::aggregator::Count;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path =
            std::env::temp_dir().join(format!("rhei_lew_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[allow(clippy::type_complexity)]
    fn make_window() -> LateEventWindow<
        (String, u64),
        Count<(String, u64)>,
        fn(&(String, u64)) -> String,
        fn(&(String, u64)) -> u64,
    > {
        LateEventWindow::builder()
            .window_size(10)
            .late_deadline(20)
            .key_fn((|e: &(String, u64)| e.0.clone()) as fn(&(String, u64)) -> String)
            .time_fn((|e: &(String, u64)| e.1) as fn(&(String, u64)) -> u64)
            .aggregator(Count::new())
            .build()
    }

    #[tokio::test]
    async fn normal_window_close_on_watermark() {
        let mut ctx = test_ctx("normal_close");
        let mut win = make_window();

        // Elements in window [0, 10)
        win.process(("a".into(), 3), &mut ctx).await.unwrap();
        win.process(("a".into(), 7), &mut ctx).await.unwrap();

        // Watermark at 10 closes window [0, 10)
        let r = win.on_watermark(10, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, "a");
        assert_eq!(r[0].window_start, 0);
        assert_eq!(r[0].window_end, 10);
        assert_eq!(r[0].value, 2);
    }

    #[tokio::test]
    async fn late_event_triggers_re_emission() {
        let mut ctx = test_ctx("re_emit");
        let mut win = make_window();

        // Element in window [0, 10)
        win.process(("a".into(), 5), &mut ctx).await.unwrap();

        // Close window [0, 10)
        let r = win.on_watermark(10, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].value, 1);

        // Late event for window [0, 10) — within late_deadline (10 + 20 = 30 > 10)
        let r = win.process(("a".into(), 3), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].window_start, 0);
        assert_eq!(r[0].window_end, 10);
        // Updated count: original 1 + late 1 = 2
        assert_eq!(r[0].value, 2);
    }

    #[tokio::test]
    async fn event_past_late_deadline_is_dropped() {
        let mut ctx = test_ctx("past_deadline");
        let mut win = make_window();

        // Element in window [0, 10)
        win.process(("a".into(), 5), &mut ctx).await.unwrap();

        // Advance watermark well past late deadline (10 + 20 = 30 < 35)
        win.on_watermark(35, &mut ctx).await.unwrap();

        // Late event for window [0, 10) — past deadline
        let r = win.process(("a".into(), 3), &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn multiple_late_events_accumulate() {
        let mut ctx = test_ctx("multi_late");
        let mut win = make_window();

        // Two elements in window [0, 10)
        win.process(("a".into(), 2), &mut ctx).await.unwrap();
        win.process(("a".into(), 8), &mut ctx).await.unwrap();

        // Close window
        let r = win.on_watermark(10, &mut ctx).await.unwrap();
        assert_eq!(r[0].value, 2);

        // First late event
        let r = win.process(("a".into(), 4), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].value, 3);

        // Second late event
        let r = win.process(("a".into(), 6), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].value, 4);
    }

    #[tokio::test]
    async fn on_time_event_does_not_re_emit() {
        let mut ctx = test_ctx("on_time");
        let mut win = make_window();

        // Element in window [10, 20)
        let r = win.process(("a".into(), 15), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Another element in same window — no re-emission (window not closed yet)
        let r = win.process(("a".into(), 17), &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }
}

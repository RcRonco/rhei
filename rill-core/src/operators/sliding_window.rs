//! Sliding window operator with pluggable aggregation.
//!
//! Groups elements into overlapping time windows defined by `window_size` and
//! `slide`. Each element belongs to every open window whose range contains
//! its timestamp. When an element arrives whose timestamp is past a window's
//! end, that window is closed and its aggregate is emitted.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
    _phantom: PhantomData<T>,
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

    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> SlidingWindowBuilder<T, A, KF2, TF> {
        SlidingWindowBuilder {
            window_size: self.window_size,
            slide: self.slide,
            key_fn: kf,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
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
            _phantom: PhantomData,
        }
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
    T: Send + Sync,
    A: Aggregator<Input = T>,
    A::Output: Send,
    KF: Fn(&T) -> String + Send + Sync,
    TF: Fn(&T) -> u64 + Send + Sync,
{
    type Input = T;
    type Output = WindowOutput<A::Output>;

    async fn process(&mut self, input: T, ctx: &mut StateContext) -> Vec<WindowOutput<A::Output>> {
        let key = (self.key_fn)(&input);
        let timestamp = (self.time_fn)(&input);
        let mut outputs = Vec::new();

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
                state.delete(&acc_key);
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
                state.put(&acc_key, &acc);
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
            state.put(&key, &active);
        }

        outputs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

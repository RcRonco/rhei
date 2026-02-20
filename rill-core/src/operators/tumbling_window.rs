//! Tumbling window operator with pluggable aggregation.
//!
//! Groups elements into fixed-size, non-overlapping time windows keyed by a
//! grouping function. When an element arrives in a new window, the previous
//! window is closed and its aggregate is emitted.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
    _phantom: PhantomData<T>,
}

impl<T, A: Clone, KF: Clone, TF: Clone> Clone for TumblingWindow<T, A, KF, TF> {
    fn clone(&self) -> Self {
        Self {
            window_size: self.window_size,
            key_fn: self.key_fn.clone(),
            time_fn: self.time_fn.clone(),
            aggregator: self.aggregator.clone(),
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

    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> TumblingWindowBuilder<T, A, KF2, TF> {
        TumblingWindowBuilder {
            window_size: self.window_size,
            key_fn: kf,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
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
            _phantom: PhantomData,
        }
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
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, A, KF, TF> StreamFunction for TumblingWindow<T, A, KF, TF>
where
    T: Clone + Send + Sync,
    A: Aggregator<Input = T>,
    A::Output: Clone + Send,
    KF: Fn(&T) -> String + Send + Sync,
    TF: Fn(&T) -> u64 + Send + Sync,
{
    type Input = T;
    type Output = WindowOutput<A::Output>;

    async fn process(&mut self, input: T, ctx: &mut StateContext) -> Vec<WindowOutput<A::Output>> {
        let key = (self.key_fn)(&input);
        let timestamp = (self.time_fn)(&input);
        let window_start = timestamp - (timestamp % self.window_size);
        let mut outputs = Vec::new();

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

        outputs
    }
}

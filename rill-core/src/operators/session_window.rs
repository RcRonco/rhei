//! Session window operator with pluggable aggregation.
//!
//! Groups elements into sessions separated by an inactivity gap. When an
//! element arrives with a timestamp more than `gap` units after the last event
//! in the current session, the session is closed and its aggregate is emitted.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

use super::aggregator::Aggregator;
use super::keyed_state::KeyedState;
use super::tumbling_window::WindowOutput;

/// Internal session state stored per key.
#[derive(Serialize, Deserialize)]
struct SessionState<Acc> {
    session_start: u64,
    last_event_time: u64,
    accumulator: Acc,
}

/// A session window operator that groups events by inactivity gap.
///
/// # Type Parameters
///
/// - `T` — input element type
/// - `A` — aggregator
/// - `KF` — key extraction function `Fn(&T) -> String`
/// - `TF` — timestamp extraction function `Fn(&T) -> u64`
pub struct SessionWindow<T, A, KF, TF> {
    gap: u64,
    key_fn: KF,
    time_fn: TF,
    aggregator: A,
    _phantom: PhantomData<T>,
}

impl<T, A, KF, TF> fmt::Debug for SessionWindow<T, A, KF, TF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionWindow")
            .field("gap", &self.gap)
            .finish_non_exhaustive()
    }
}

impl<T, A, KF, TF> SessionWindow<T, A, KF, TF> {
    /// Creates a new session window operator.
    pub fn new(gap: u64, key_fn: KF, time_fn: TF, aggregator: A) -> Self {
        Self {
            gap,
            key_fn,
            time_fn,
            aggregator,
            _phantom: PhantomData,
        }
    }
}

impl<T> SessionWindow<T, (), (), ()> {
    /// Returns a builder for constructing a `SessionWindow`.
    pub fn builder() -> SessionWindowBuilder<T> {
        SessionWindowBuilder {
            gap: 0,
            key_fn: (),
            time_fn: (),
            aggregator: (),
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`SessionWindow`].
pub struct SessionWindowBuilder<T, A = (), KF = (), TF = ()> {
    gap: u64,
    key_fn: KF,
    time_fn: TF,
    aggregator: A,
    _phantom: PhantomData<T>,
}

impl<T, A, KF, TF> fmt::Debug for SessionWindowBuilder<T, A, KF, TF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionWindowBuilder")
            .field("gap", &self.gap)
            .finish_non_exhaustive()
    }
}

impl<T, A, KF, TF> SessionWindowBuilder<T, A, KF, TF> {
    /// Sets the maximum inactivity gap before a session closes.
    pub fn gap(mut self, gap: u64) -> Self {
        self.gap = gap;
        self
    }

    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> SessionWindowBuilder<T, A, KF2, TF> {
        SessionWindowBuilder {
            gap: self.gap,
            key_fn: kf,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
            _phantom: PhantomData,
        }
    }

    /// Sets the timestamp extraction function.
    pub fn time_fn<TF2>(self, tf: TF2) -> SessionWindowBuilder<T, A, KF, TF2> {
        SessionWindowBuilder {
            gap: self.gap,
            key_fn: self.key_fn,
            time_fn: tf,
            aggregator: self.aggregator,
            _phantom: PhantomData,
        }
    }

    /// Sets the aggregator.
    pub fn aggregator<A2>(self, agg: A2) -> SessionWindowBuilder<T, A2, KF, TF> {
        SessionWindowBuilder {
            gap: self.gap,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            aggregator: agg,
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF, TF> SessionWindowBuilder<T, A, KF, TF>
where
    T: Send + Sync,
    A: Aggregator<Input = T>,
    A::Output: Send,
    KF: Fn(&T) -> String + Send + Sync,
    TF: Fn(&T) -> u64 + Send + Sync,
{
    /// Builds the `SessionWindow` operator.
    ///
    /// # Panics
    ///
    /// Panics if `gap` is zero.
    pub fn build(self) -> SessionWindow<T, A, KF, TF> {
        assert!(self.gap > 0, "gap must be > 0");
        SessionWindow {
            gap: self.gap,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, A, KF, TF> StreamFunction for SessionWindow<T, A, KF, TF>
where
    T: Send + Sync,
    A: Aggregator<Input = T>,
    A::Output: Send,
    KF: Fn(&T) -> String + Send + Sync,
    TF: Fn(&T) -> u64 + Send + Sync,
{
    type Input = T;
    type Output = WindowOutput<A::Output>;

    async fn process(
        &mut self,
        input: T,
        ctx: &mut StateContext,
    ) -> Vec<WindowOutput<A::Output>> {
        let key = (self.key_fn)(&input);
        let timestamp = (self.time_fn)(&input);
        let mut outputs = Vec::new();

        // Load existing session state for this key
        let session: Option<SessionState<A::Accumulator>> = {
            let mut state =
                KeyedState::<String, SessionState<A::Accumulator>>::new(ctx, "session");
            state.get(&key).await.unwrap_or(None)
        };

        let new_session = match session {
            Some(s) if timestamp - s.last_event_time > self.gap => {
                // Gap exceeded — close the old session and emit its result
                outputs.push(WindowOutput {
                    key: key.clone(),
                    window_start: s.session_start,
                    window_end: s.last_event_time,
                    value: self.aggregator.finish(&s.accumulator),
                });
                // Start a new session
                let mut acc = A::Accumulator::default();
                self.aggregator.accumulate(&mut acc, &input);
                SessionState {
                    session_start: timestamp,
                    last_event_time: timestamp,
                    accumulator: acc,
                }
            }
            Some(mut s) => {
                // Within the gap — extend the session
                s.last_event_time = timestamp;
                self.aggregator.accumulate(&mut s.accumulator, &input);
                s
            }
            None => {
                // First event for this key — start a new session
                let mut acc = A::Accumulator::default();
                self.aggregator.accumulate(&mut acc, &input);
                SessionState {
                    session_start: timestamp,
                    last_event_time: timestamp,
                    accumulator: acc,
                }
            }
        };

        // Store updated session state
        {
            let mut state =
                KeyedState::<String, SessionState<A::Accumulator>>::new(ctx, "session");
            state.put(&key, &new_session);
        }

        outputs
    }
}

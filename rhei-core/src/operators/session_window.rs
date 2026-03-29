//! Session window operator with pluggable aggregation.
//!
//! Groups elements into sessions separated by an inactivity gap. When an
//! element arrives with a timestamp more than `gap` units after the last event
//! in the current session, the session is closed and its aggregate is emitted.

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
    /// In-memory set of keys with open sessions (for watermark-driven closure).
    active_keys: HashSet<String>,
    /// Configurable allowed lateness for late event detection (default: 0).
    allowed_lateness: u64,
    /// Last seen watermark for late event detection.
    last_watermark: u64,
    _phantom: PhantomData<T>,
}

impl<T, A: Clone, KF: Clone, TF: Clone> Clone for SessionWindow<T, A, KF, TF> {
    fn clone(&self) -> Self {
        Self {
            gap: self.gap,
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
            active_keys: HashSet::new(),
            allowed_lateness: 0,
            last_watermark: 0,
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
            allowed_lateness: 0,
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
    allowed_lateness: u64,
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

    /// Sets the allowed lateness in timestamp units (default: 0).
    pub fn allowed_lateness(mut self, lateness: u64) -> Self {
        self.allowed_lateness = lateness;
        self
    }

    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> SessionWindowBuilder<T, A, KF2, TF> {
        SessionWindowBuilder {
            gap: self.gap,
            key_fn: kf,
            time_fn: self.time_fn,
            aggregator: self.aggregator,
            allowed_lateness: self.allowed_lateness,
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
            allowed_lateness: self.allowed_lateness,
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
            allowed_lateness: self.allowed_lateness,
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF> SessionWindowBuilder<T, A, KF, ()> {
    /// Uses wall-clock (processing) time instead of event time.
    ///
    /// The timestamp extraction function is replaced by a closure that calls
    /// [`WallClockProvider::current_time()`].
    pub fn proc_time_fn(
        self,
    ) -> SessionWindowBuilder<T, A, KF, impl Fn(&T) -> u64 + Send + Sync + Clone> {
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
    ) -> SessionWindowBuilder<T, A, KF, impl Fn(&T) -> u64 + Send + Sync + Clone> {
        self.time_fn(move |_: &T| provider.current_time())
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
            active_keys: HashSet::new(),
            allowed_lateness: self.allowed_lateness,
            last_watermark: 0,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, A, KF, TF> StreamFunction for SessionWindow<T, A, KF, TF>
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

        // Track this key as active for watermark-driven closure.
        self.active_keys.insert(key.clone());

        // Load existing session state for this key
        let session: Option<SessionState<A::Accumulator>> = {
            let mut state = KeyedState::<String, SessionState<A::Accumulator>>::new(ctx, "session");
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
            let mut state = KeyedState::<String, SessionState<A::Accumulator>>::new(ctx, "session");
            state.put(&key, &new_session)?;
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
            let session: Option<SessionState<A::Accumulator>> = {
                let mut state =
                    KeyedState::<String, SessionState<A::Accumulator>>::new(ctx, "session");
                state.get(key).await.unwrap_or(None)
            };

            if let Some(s) = session
                && s.last_event_time + self.gap + self.allowed_lateness <= watermark
            {
                // Session timed out — close it.
                outputs.push(WindowOutput {
                    key: key.clone(),
                    window_start: s.session_start,
                    window_end: s.last_event_time,
                    value: self.aggregator.finish(&s.accumulator),
                });
                let mut state =
                    KeyedState::<String, SessionState<A::Accumulator>>::new(ctx, "session");
                state.delete(key)?;
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::operators::aggregator::Count;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;
    use crate::time::FixedClockProvider;

    fn test_ctx(name: &str) -> StateContext {
        let path =
            std::env::temp_dir().join(format!("rhei_sess_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[allow(clippy::type_complexity)]
    fn make_session() -> SessionWindow<
        (String, u64),
        Count<(String, u64)>,
        fn(&(String, u64)) -> String,
        fn(&(String, u64)) -> u64,
    > {
        SessionWindow::new(
            10,
            (|e: &(String, u64)| e.0.clone()) as fn(&(String, u64)) -> String,
            (|e: &(String, u64)| e.1) as fn(&(String, u64)) -> u64,
            Count::new(),
        )
    }

    #[tokio::test]
    async fn session_watermark_closes_session() {
        let mut ctx = test_ctx("wm_close");
        let mut win = make_session();

        // Two events for key "a", last at ts=5
        win.process(("a".into(), 1), &mut ctx).await.unwrap();
        win.process(("a".into(), 5), &mut ctx).await.unwrap();

        // Watermark at 16: last_event_time(5) + gap(10) + allowed_lateness(0) = 15 <= 16
        let r = win.on_watermark(16, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, "a");
        assert_eq!(r[0].window_start, 1);
        assert_eq!(r[0].window_end, 5);
        assert_eq!(r[0].value, 2);
    }

    #[tokio::test]
    async fn session_watermark_no_close_before_gap() {
        let mut ctx = test_ctx("wm_no_close");
        let mut win = make_session();

        // Event at ts=5
        win.process(("a".into(), 5), &mut ctx).await.unwrap();

        // Watermark at 10: 5 + 10 + 0 = 15 > 10, no close
        let r = win.on_watermark(10, &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn session_multiple_keys_watermark() {
        let mut ctx = test_ctx("multi_key");
        let mut win = make_session();

        // Key "a" events
        win.process(("a".into(), 1), &mut ctx).await.unwrap();
        // Key "b" events at later time
        win.process(("b".into(), 10), &mut ctx).await.unwrap();

        // Watermark at 12: "a" session: 1+10+0 = 11 <= 12 → close
        // "b" session: 10+10+0 = 20 > 12 → stay open
        let r = win.on_watermark(12, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, "a");
    }

    #[tokio::test]
    async fn session_proc_time_with_fixed_clock() {
        let mut ctx = test_ctx("proc_fixed");
        let clock = FixedClockProvider::new(100);

        let mut win = SessionWindow::<String, _, _, _>::builder()
            .gap(10)
            .key_fn(|e: &String| e.clone())
            .proc_time_fn_with(Arc::new(clock.clone()))
            .aggregator(Count::new())
            .build();

        // First event at clock=100
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Second event within gap (clock=105)
        clock.set(105);
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Third event past gap (clock=120, gap=10, last_event=105 → 120 - 105 > 10)
        clock.set(120);
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].window_start, 100);
        assert_eq!(r[0].window_end, 105);
        assert_eq!(r[0].value, 2);
    }

    #[tokio::test]
    async fn session_proc_time_fn_smoke() {
        let mut ctx = test_ctx("proc_wall");
        let mut win = SessionWindow::<String, _, _, _>::builder()
            .gap(60_000)
            .key_fn(|e: &String| e.clone())
            .proc_time_fn()
            .aggregator(Count::new())
            .build();

        let r = win.process("x".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }
}

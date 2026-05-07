//! Interval join operator for time-bounded correlation of two streams.
//!
//! Matches events from two sides where the left event's timestamp falls within
//! a configurable time interval relative to the right event's timestamp (and
//! vice versa). This is equivalent to Flink's `IntervalJoin`:
//!
//! ```text
//! left.timestamp + lower_bound <= right.timestamp <= left.timestamp + upper_bound
//! ```
//!
//! Events are buffered in state until they can no longer participate in any
//! future join (determined by watermark + bounds). Stale events are evicted
//! during `on_watermark`.
//!
//! # Analogy
//!
//! Flink `IntervalJoin`, Kafka Streams `JoinWindows`.

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

use super::keyed_state::KeyedState;

/// Input event tagged with its side of the interval join, including a timestamp.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IntervalSide<L, R> {
    /// A left-side event with its event timestamp.
    Left(L, u64),
    /// A right-side event with its event timestamp.
    Right(R, u64),
}

/// A buffered event with its timestamp and a unique sequence number.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BufferedEvent<V> {
    value: V,
    timestamp: u64,
    seq: u64,
}

/// Buffer of events for one side of the join, per key.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EventBuffer<V> {
    events: Vec<BufferedEvent<V>>,
    next_seq: u64,
}

/// Manual `Default` implementation to avoid requiring `V: Default`.
impl<V> Default for EventBuffer<V> {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            next_seq: 0,
        }
    }
}

/// An interval join operator that correlates events within a time window.
///
/// # Type Parameters
///
/// - `L` -- left-side event type
/// - `R` -- right-side event type
/// - `KF` -- key extraction function `Fn(&IntervalSide<L, R>) -> String`
/// - `JF` -- join function `Fn(&L, &R) -> O`
pub struct IntervalJoin<L, R, KF, JF> {
    key_fn: KF,
    join_fn: JF,
    /// Lower bound: `left.ts + lower_bound <= right.ts`
    lower_bound: i64,
    /// Upper bound: `right.ts <= left.ts + upper_bound`
    upper_bound: i64,
    /// Current watermark for eviction.
    last_watermark: u64,
    /// Keys with buffered events.
    active_keys: HashSet<String>,
    _phantom: PhantomData<(L, R)>,
}

impl<L, R, KF: Clone, JF: Clone> Clone for IntervalJoin<L, R, KF, JF> {
    fn clone(&self) -> Self {
        Self {
            key_fn: self.key_fn.clone(),
            join_fn: self.join_fn.clone(),
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<L, R, KF, JF> fmt::Debug for IntervalJoin<L, R, KF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IntervalJoin")
            .field("lower_bound", &self.lower_bound)
            .field("upper_bound", &self.upper_bound)
            .finish_non_exhaustive()
    }
}

impl<L, R> IntervalJoin<L, R, (), ()> {
    /// Returns a builder for constructing an `IntervalJoin`.
    pub fn builder() -> IntervalJoinBuilder<L, R> {
        IntervalJoinBuilder {
            key_fn: (),
            join_fn: (),
            lower_bound: 0,
            upper_bound: 0,
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`IntervalJoin`].
pub struct IntervalJoinBuilder<L, R, KF = (), JF = ()> {
    key_fn: KF,
    join_fn: JF,
    lower_bound: i64,
    upper_bound: i64,
    _phantom: PhantomData<(L, R)>,
}

impl<L, R, KF, JF> fmt::Debug for IntervalJoinBuilder<L, R, KF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IntervalJoinBuilder")
            .field("lower_bound", &self.lower_bound)
            .field("upper_bound", &self.upper_bound)
            .finish_non_exhaustive()
    }
}

impl<L, R, KF, JF> IntervalJoinBuilder<L, R, KF, JF> {
    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> IntervalJoinBuilder<L, R, KF2, JF> {
        IntervalJoinBuilder {
            key_fn: kf,
            join_fn: self.join_fn,
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            _phantom: PhantomData,
        }
    }

    /// Sets the join function called for each matching pair.
    pub fn join_fn<JF2>(self, jf: JF2) -> IntervalJoinBuilder<L, R, KF, JF2> {
        IntervalJoinBuilder {
            key_fn: self.key_fn,
            join_fn: jf,
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            _phantom: PhantomData,
        }
    }

    /// Sets the lower bound (may be negative).
    ///
    /// The join condition is: `left.ts + lower_bound <= right.ts`
    pub fn lower_bound(mut self, bound: i64) -> Self {
        self.lower_bound = bound;
        self
    }

    /// Sets the upper bound (may be negative, but typically positive).
    ///
    /// The join condition is: `right.ts <= left.ts + upper_bound`
    pub fn upper_bound(mut self, bound: i64) -> Self {
        self.upper_bound = bound;
        self
    }
}

impl<L, R, O, KF, JF> IntervalJoinBuilder<L, R, KF, JF>
where
    L: Serialize + DeserializeOwned + Send + Sync,
    R: Serialize + DeserializeOwned + Send + Sync,
    O: Send,
    KF: Fn(&IntervalSide<L, R>) -> String + Send + Sync,
    JF: Fn(&L, &R) -> O + Send + Sync,
{
    /// Builds the `IntervalJoin` operator.
    ///
    /// # Panics
    ///
    /// Panics if `lower_bound > upper_bound`.
    pub fn build(self) -> IntervalJoin<L, R, KF, JF> {
        assert!(
            self.lower_bound <= self.upper_bound,
            "lower_bound must be <= upper_bound"
        );
        IntervalJoin {
            key_fn: self.key_fn,
            join_fn: self.join_fn,
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

/// Helper: add a signed offset to a u64 timestamp, clamping to 0.
fn ts_add(ts: u64, offset: i64) -> u64 {
    if offset >= 0 {
        ts.saturating_add(offset.unsigned_abs())
    } else {
        ts.saturating_sub(offset.unsigned_abs())
    }
}

/// Opportunistically evict stale events from a buffer.
///
/// Retains only events where `watermark <= ts_add(ev.timestamp, bound)`.
/// Returns the number of evicted events.
fn evict_stale<V>(buf: &mut EventBuffer<V>, watermark: u64, bound: i64) -> usize {
    let before = buf.events.len();
    buf.events
        .retain(|ev| watermark <= ts_add(ev.timestamp, bound));
    before - buf.events.len()
}

#[async_trait]
impl<L, R, O, KF, JF> StreamFunction for IntervalJoin<L, R, KF, JF>
where
    L: Clone + Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug,
    R: Clone + Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug,
    O: Clone + Send + std::fmt::Debug,
    KF: Fn(&IntervalSide<L, R>) -> String + Send + Sync,
    JF: Fn(&L, &R) -> O + Send + Sync,
{
    type Input = IntervalSide<L, R>;
    type Output = O;

    async fn process(
        &mut self,
        input: IntervalSide<L, R>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<O>> {
        let key = (self.key_fn)(&input);
        self.active_keys.insert(key.clone());

        match input {
            IntervalSide::Left(l, l_ts) => {
                // Probe right buffer: match where l_ts + lower <= r_ts <= l_ts + upper
                let mut right_buf: EventBuffer<R> = {
                    let mut state = KeyedState::<String, EventBuffer<R>>::new(ctx, "ij_right");
                    state.get(&key).await.unwrap_or(None).unwrap_or_default()
                };

                let min_r = ts_add(l_ts, self.lower_bound);
                let max_r = ts_add(l_ts, self.upper_bound);

                let mut outputs = Vec::new();
                for ev in &right_buf.events {
                    if ev.timestamp >= min_r && ev.timestamp <= max_r {
                        outputs.push((self.join_fn)(&l, &ev.value));
                    }
                }

                // Opportunistic eviction of stale right events.
                if self.last_watermark > 0 {
                    let evicted =
                        evict_stale(&mut right_buf, self.last_watermark, -self.lower_bound);
                    if evicted > 0 {
                        metrics::counter!("interval_join_right_evicted_total")
                            .increment(u64::try_from(evicted).unwrap_or(u64::MAX));
                    }
                    let mut state = KeyedState::<String, EventBuffer<R>>::new(ctx, "ij_right");
                    state.put(&key, &right_buf)?;
                }

                // Buffer the left event.
                let mut left_buf: EventBuffer<L> = {
                    let mut state = KeyedState::<String, EventBuffer<L>>::new(ctx, "ij_left");
                    state.get(&key).await.unwrap_or(None).unwrap_or_default()
                };
                let seq = left_buf.next_seq;
                left_buf.next_seq += 1;
                left_buf.events.push(BufferedEvent {
                    value: l,
                    timestamp: l_ts,
                    seq,
                });

                // Opportunistic eviction of stale left events.
                if self.last_watermark > 0 {
                    let evicted = evict_stale(&mut left_buf, self.last_watermark, self.upper_bound);
                    if evicted > 0 {
                        metrics::counter!("interval_join_left_evicted_total")
                            .increment(u64::try_from(evicted).unwrap_or(u64::MAX));
                    }
                }

                {
                    let mut state = KeyedState::<String, EventBuffer<L>>::new(ctx, "ij_left");
                    state.put(&key, &left_buf)?;
                }

                Ok(outputs)
            }
            IntervalSide::Right(r, r_ts) => {
                // Probe left buffer: match where l_ts + lower <= r_ts <= l_ts + upper
                // Rearranged: r_ts - upper <= l_ts <= r_ts - lower
                let mut left_buf: EventBuffer<L> = {
                    let mut state = KeyedState::<String, EventBuffer<L>>::new(ctx, "ij_left");
                    state.get(&key).await.unwrap_or(None).unwrap_or_default()
                };

                let min_l = ts_add(r_ts, -self.upper_bound);
                let max_l = ts_add(r_ts, -self.lower_bound);

                let mut outputs = Vec::new();
                for ev in &left_buf.events {
                    if ev.timestamp >= min_l && ev.timestamp <= max_l {
                        outputs.push((self.join_fn)(&ev.value, &r));
                    }
                }

                // Opportunistic eviction of stale left events.
                if self.last_watermark > 0 {
                    let evicted = evict_stale(&mut left_buf, self.last_watermark, self.upper_bound);
                    if evicted > 0 {
                        metrics::counter!("interval_join_left_evicted_total")
                            .increment(u64::try_from(evicted).unwrap_or(u64::MAX));
                    }
                    let mut state = KeyedState::<String, EventBuffer<L>>::new(ctx, "ij_left");
                    state.put(&key, &left_buf)?;
                }

                // Buffer the right event.
                let mut right_buf: EventBuffer<R> = {
                    let mut state = KeyedState::<String, EventBuffer<R>>::new(ctx, "ij_right");
                    state.get(&key).await.unwrap_or(None).unwrap_or_default()
                };
                let seq = right_buf.next_seq;
                right_buf.next_seq += 1;
                right_buf.events.push(BufferedEvent {
                    value: r,
                    timestamp: r_ts,
                    seq,
                });

                // Opportunistic eviction of stale right events.
                if self.last_watermark > 0 {
                    let evicted =
                        evict_stale(&mut right_buf, self.last_watermark, -self.lower_bound);
                    if evicted > 0 {
                        metrics::counter!("interval_join_right_evicted_total")
                            .increment(u64::try_from(evicted).unwrap_or(u64::MAX));
                    }
                }

                {
                    let mut state = KeyedState::<String, EventBuffer<R>>::new(ctx, "ij_right");
                    state.put(&key, &right_buf)?;
                }

                Ok(outputs)
            }
        }
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<O>> {
        self.last_watermark = watermark;

        let mut keys_to_remove = Vec::new();

        for key in &self.active_keys {
            let mut modified = false;

            // Evict left events that can no longer match any future right event.
            {
                let mut left_buf: EventBuffer<L> = {
                    let mut state = KeyedState::<String, EventBuffer<L>>::new(ctx, "ij_left");
                    state.get(key).await.unwrap_or(None).unwrap_or_default()
                };
                let evicted = evict_stale(&mut left_buf, watermark, self.upper_bound);
                if evicted > 0 {
                    modified = true;
                    metrics::counter!("interval_join_left_evicted_total")
                        .increment(u64::try_from(evicted).unwrap_or(u64::MAX));
                    let mut state = KeyedState::<String, EventBuffer<L>>::new(ctx, "ij_left");
                    if left_buf.events.is_empty() {
                        state.delete(key)?;
                    } else {
                        state.put(key, &left_buf)?;
                    }
                }
            }

            // Evict right events that can no longer match any future left event.
            {
                let mut right_buf: EventBuffer<R> = {
                    let mut state = KeyedState::<String, EventBuffer<R>>::new(ctx, "ij_right");
                    state.get(key).await.unwrap_or(None).unwrap_or_default()
                };
                let evicted = evict_stale(&mut right_buf, watermark, -self.lower_bound);
                if evicted > 0 {
                    modified = true;
                    metrics::counter!("interval_join_right_evicted_total")
                        .increment(u64::try_from(evicted).unwrap_or(u64::MAX));
                    let mut state = KeyedState::<String, EventBuffer<R>>::new(ctx, "ij_right");
                    if right_buf.events.is_empty() {
                        state.delete(key)?;
                    } else {
                        state.put(key, &right_buf)?;
                    }
                }
            }

            // Check if both buffers are now empty for this key.
            if modified {
                let left_empty = {
                    let mut state = KeyedState::<String, EventBuffer<L>>::new(ctx, "ij_left");
                    state.get(key).await.unwrap_or(None).is_none()
                };
                let right_empty = {
                    let mut state = KeyedState::<String, EventBuffer<R>>::new(ctx, "ij_right");
                    state.get(key).await.unwrap_or(None).is_none()
                };
                if left_empty && right_empty {
                    keys_to_remove.push(key.clone());
                }
            }
        }

        for key in &keys_to_remove {
            self.active_keys.remove(key);
        }

        Ok(vec![])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path = std::env::temp_dir().join(format!("rhei_ij_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    fn key_fn(side: &IntervalSide<String, String>) -> String {
        match side {
            IntervalSide::Left(s, _) | IntervalSide::Right(s, _) => {
                s.split(':').next().unwrap().to_string()
            }
        }
    }

    fn join_fn(l: &String, r: &String) -> String {
        format!("{l}+{r}")
    }

    #[tokio::test]
    async fn left_then_right_within_bounds() {
        let mut ctx = test_ctx("lr_within");
        let mut join = IntervalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .lower_bound(-5)
            .upper_bound(5)
            .build();

        // Left at ts=10
        let r = join
            .process(IntervalSide::Left("k1:left".into(), 10), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty()); // no right events yet

        // Right at ts=12 (within [10-5, 10+5] = [5, 15])
        let r = join
            .process(IntervalSide::Right("k1:right".into(), 12), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], "k1:left+k1:right");
    }

    #[tokio::test]
    async fn right_then_left_within_bounds() {
        let mut ctx = test_ctx("rl_within");
        let mut join = IntervalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .lower_bound(-5)
            .upper_bound(5)
            .build();

        // Right at ts=12
        let r = join
            .process(IntervalSide::Right("k1:right".into(), 12), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());

        // Left at ts=10, right at 12 is within [10-5, 10+5] = [5, 15]
        let r = join
            .process(IntervalSide::Left("k1:left".into(), 10), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], "k1:left+k1:right");
    }

    #[tokio::test]
    async fn out_of_bounds_no_match() {
        let mut ctx = test_ctx("out_bounds");
        let mut join = IntervalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .lower_bound(-2)
            .upper_bound(2)
            .build();

        // Left at ts=10
        join.process(IntervalSide::Left("k1:left".into(), 10), &mut ctx)
            .await
            .unwrap();

        // Right at ts=20 (way outside [8, 12])
        let r = join
            .process(IntervalSide::Right("k1:right".into(), 20), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn multiple_matches() {
        let mut ctx = test_ctx("multi_match");
        let mut join = IntervalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .lower_bound(-5)
            .upper_bound(5)
            .build();

        // Two left events
        join.process(IntervalSide::Left("k1:L1".into(), 10), &mut ctx)
            .await
            .unwrap();
        join.process(IntervalSide::Left("k1:L2".into(), 12), &mut ctx)
            .await
            .unwrap();

        // Right at ts=11, matches both L1 (11 in [5,15]) and L2 (11 in [7,17])
        let r = join
            .process(IntervalSide::Right("k1:R1".into(), 11), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&"k1:L1+k1:R1".to_string()));
        assert!(r.contains(&"k1:L2+k1:R1".to_string()));
    }

    #[tokio::test]
    async fn watermark_evicts_stale_events() {
        let mut ctx = test_ctx("wm_evict");
        let mut join = IntervalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .lower_bound(-5)
            .upper_bound(5)
            .build();

        // Left at ts=10, can match right events up to ts=15 (10+5)
        join.process(IntervalSide::Left("k1:left".into(), 10), &mut ctx)
            .await
            .unwrap();

        // Watermark at 16 > 10+5=15, so left event should be evicted
        join.on_watermark(16, &mut ctx).await.unwrap();

        // Right at ts=12 — would match if left was still buffered, but it was evicted
        let r = join
            .process(IntervalSide::Right("k1:right".into(), 12), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn watermark_does_not_evict_live_events() {
        let mut ctx = test_ctx("wm_live");
        let mut join = IntervalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .lower_bound(-5)
            .upper_bound(10)
            .build();

        // Left at ts=10, can match right events up to ts=20 (10+10)
        join.process(IntervalSide::Left("k1:left".into(), 10), &mut ctx)
            .await
            .unwrap();

        // Watermark at 15 <= 20, left event should NOT be evicted
        join.on_watermark(15, &mut ctx).await.unwrap();

        // Right at ts=14 — within [5, 20]
        let r = join
            .process(IntervalSide::Right("k1:right".into(), 14), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
    }

    #[tokio::test]
    async fn different_keys_independent() {
        let mut ctx = test_ctx("diff_keys");
        let mut join = IntervalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .lower_bound(-5)
            .upper_bound(5)
            .build();

        join.process(IntervalSide::Left("k1:L".into(), 10), &mut ctx)
            .await
            .unwrap();
        join.process(IntervalSide::Left("k2:L".into(), 10), &mut ctx)
            .await
            .unwrap();

        // k1 right matches k1 left only
        let r = join
            .process(IntervalSide::Right("k1:R".into(), 12), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], "k1:L+k1:R");
    }

    #[test]
    fn ts_add_handles_negative() {
        assert_eq!(ts_add(10, -5), 5);
        assert_eq!(ts_add(3, -5), 0); // clamped
        assert_eq!(ts_add(10, 5), 15);
        assert_eq!(ts_add(0, 0), 0);
    }

    #[tokio::test]
    async fn boundary_exact_match() {
        let mut ctx = test_ctx("boundary");
        let mut join = IntervalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .lower_bound(0)
            .upper_bound(5)
            .build();

        // Left at ts=10, bounds [10, 15]
        join.process(IntervalSide::Left("k1:L".into(), 10), &mut ctx)
            .await
            .unwrap();

        // Right at exactly ts=10 (lower bound, should match)
        let r = join
            .process(IntervalSide::Right("k1:R10".into(), 10), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);

        // Right at exactly ts=15 (upper bound, should match)
        let r = join
            .process(IntervalSide::Right("k1:R15".into(), 15), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);

        // Right at ts=16 (beyond upper bound, no match)
        let r = join
            .process(IntervalSide::Right("k1:R16".into(), 16), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());
    }
}

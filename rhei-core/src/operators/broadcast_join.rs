//! Broadcast join operator for enriching a high-volume stream with a small table.
//!
//! The small ("broadcast") side is buffered in state keyed by a join key. When a
//! large-side event arrives, it probes the broadcast table and, if a match is
//! found, calls the join function to produce an output.
//!
//! Unlike [`TemporalJoin`](super::temporal_join::TemporalJoin), the broadcast
//! side is expected to be much smaller and is never evicted by default. An
//! optional TTL can be configured to expire stale broadcast entries via
//! watermark-driven eviction.
//!
//! # Analogy
//!
//! This is equivalent to a Flink *broadcast state pattern* or Kafka Streams
//! `GlobalKTable` join.

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

use super::keyed_state::KeyedState;

/// Input event tagged with its side of the broadcast join.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BroadcastSide<B, S> {
    /// A broadcast (small-table) event that is buffered for lookup.
    Broadcast(B),
    /// A stream (large-side) event that probes the broadcast table.
    Stream(S),
}

/// Timestamped wrapper for broadcast entries (supports TTL eviction).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Stamped<V> {
    value: V,
    watermark_at_buffer: u64,
}

/// A broadcast join operator that enriches stream events from a small table.
///
/// # Type Parameters
///
/// - `B` -- broadcast (small-table) event type
/// - `S` -- stream (large-side) event type
/// - `KF` -- key extraction function `Fn(&BroadcastSide<B, S>) -> String`
/// - `JF` -- join function `Fn(S, &B) -> O`
pub struct BroadcastJoin<B, S, KF, JF> {
    key_fn: KF,
    join_fn: JF,
    /// Watermark units before a broadcast entry expires (`None` = no expiry).
    ttl: Option<u64>,
    /// Current watermark for timestamping broadcast entries.
    last_watermark: u64,
    /// Keys with buffered broadcast entries (for efficient TTL scans).
    active_keys: HashSet<String>,
    _phantom: PhantomData<(B, S)>,
}

impl<B, S, KF: Clone, JF: Clone> Clone for BroadcastJoin<B, S, KF, JF> {
    fn clone(&self) -> Self {
        Self {
            key_fn: self.key_fn.clone(),
            join_fn: self.join_fn.clone(),
            ttl: self.ttl,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<B, S, KF, JF> fmt::Debug for BroadcastJoin<B, S, KF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastJoin")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<B, S> BroadcastJoin<B, S, (), ()> {
    /// Returns a builder for constructing a `BroadcastJoin`.
    pub fn builder() -> BroadcastJoinBuilder<B, S> {
        BroadcastJoinBuilder {
            key_fn: (),
            join_fn: (),
            ttl: None,
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`BroadcastJoin`].
pub struct BroadcastJoinBuilder<B, S, KF = (), JF = ()> {
    key_fn: KF,
    join_fn: JF,
    ttl: Option<u64>,
    _phantom: PhantomData<(B, S)>,
}

impl<B, S, KF, JF> fmt::Debug for BroadcastJoinBuilder<B, S, KF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastJoinBuilder")
            .finish_non_exhaustive()
    }
}

impl<B, S, KF, JF> BroadcastJoinBuilder<B, S, KF, JF> {
    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> BroadcastJoinBuilder<B, S, KF2, JF> {
        BroadcastJoinBuilder {
            key_fn: kf,
            join_fn: self.join_fn,
            ttl: self.ttl,
            _phantom: PhantomData,
        }
    }

    /// Sets the join function called when a stream event matches a broadcast entry.
    ///
    /// The join function receives the stream event by value and the broadcast
    /// entry by reference (since it may be probed multiple times).
    pub fn join_fn<JF2>(self, jf: JF2) -> BroadcastJoinBuilder<B, S, KF, JF2> {
        BroadcastJoinBuilder {
            key_fn: self.key_fn,
            join_fn: jf,
            ttl: self.ttl,
            _phantom: PhantomData,
        }
    }

    /// Sets the TTL in watermark units. Broadcast entries older than
    /// `watermark - ttl` will be evicted during `on_watermark`.
    pub fn ttl(mut self, ttl: u64) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl<B, S, O, KF, JF> BroadcastJoinBuilder<B, S, KF, JF>
where
    B: Serialize + DeserializeOwned + Send + Sync,
    S: Serialize + DeserializeOwned + Send + Sync,
    O: Send,
    KF: Fn(&BroadcastSide<B, S>) -> String + Send + Sync,
    JF: Fn(S, &B) -> O + Send + Sync,
{
    /// Builds the `BroadcastJoin` operator.
    pub fn build(self) -> BroadcastJoin<B, S, KF, JF> {
        BroadcastJoin {
            key_fn: self.key_fn,
            join_fn: self.join_fn,
            ttl: self.ttl,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<B, S, O, KF, JF> StreamFunction for BroadcastJoin<B, S, KF, JF>
where
    B: Clone + Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug,
    S: Clone + Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug,
    O: Clone + Send + std::fmt::Debug,
    KF: Fn(&BroadcastSide<B, S>) -> String + Send + Sync,
    JF: Fn(S, &B) -> O + Send + Sync,
{
    type Input = BroadcastSide<B, S>;
    type Output = O;

    async fn process(
        &mut self,
        input: BroadcastSide<B, S>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<O>> {
        let key = (self.key_fn)(&input);

        match input {
            BroadcastSide::Broadcast(b) => {
                // Upsert the broadcast entry with a watermark timestamp.
                let mut state = KeyedState::<String, Stamped<B>>::new(ctx, "broadcast");
                state.put(
                    &key,
                    &Stamped {
                        value: b,
                        watermark_at_buffer: self.last_watermark,
                    },
                )?;
                self.active_keys.insert(key);
                Ok(vec![])
            }
            BroadcastSide::Stream(s) => {
                // Probe the broadcast table.
                let mut state = KeyedState::<String, Stamped<B>>::new(ctx, "broadcast");
                let entry = state.get(&key).await.unwrap_or(None);

                if let Some(stamped) = entry {
                    Ok(vec![(self.join_fn)(s, &stamped.value)])
                } else {
                    metrics::counter!("broadcast_join_misses_total").increment(1);
                    Ok(vec![])
                }
            }
        }
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<O>> {
        self.last_watermark = watermark;
        let Some(ttl) = self.ttl else {
            return Ok(vec![]);
        };

        let mut evicted_keys = Vec::new();
        for key in &self.active_keys {
            let mut state = KeyedState::<String, Stamped<B>>::new(ctx, "broadcast");
            if let Some(stamped) = state.get(key).await.unwrap_or(None)
                && watermark >= stamped.watermark_at_buffer + ttl
            {
                state.delete(key)?;
                evicted_keys.push(key.clone());
                metrics::counter!("broadcast_join_evicted_total").increment(1);
            }
        }

        for key in &evicted_keys {
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
        let path = std::env::temp_dir().join(format!("rhei_bj_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    fn key_fn(side: &BroadcastSide<String, String>) -> String {
        match side {
            BroadcastSide::Broadcast(s) | BroadcastSide::Stream(s) => {
                s.split(':').next().unwrap().to_string()
            }
        }
    }

    fn join_fn(stream: String, broadcast: &String) -> String {
        format!("{stream}+{broadcast}")
    }

    #[tokio::test]
    async fn broadcast_then_stream_matches() {
        let mut ctx = test_ctx("b_then_s");
        let mut join = BroadcastJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .build();

        // Broadcast arrives first
        let r = join
            .process(BroadcastSide::Broadcast("k1:ref_data".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());

        // Stream event probes broadcast table
        let r = join
            .process(BroadcastSide::Stream("k1:event".into()), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], "k1:event+k1:ref_data");
    }

    #[tokio::test]
    async fn stream_before_broadcast_misses() {
        let mut ctx = test_ctx("s_before_b");
        let mut join = BroadcastJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .build();

        // Stream event arrives before broadcast — no match
        let r = join
            .process(BroadcastSide::Stream("k1:event".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn broadcast_update_overwrites() {
        let mut ctx = test_ctx("update");
        let mut join = BroadcastJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .build();

        // First broadcast
        join.process(BroadcastSide::Broadcast("k1:v1".into()), &mut ctx)
            .await
            .unwrap();

        // Update broadcast
        join.process(BroadcastSide::Broadcast("k1:v2".into()), &mut ctx)
            .await
            .unwrap();

        // Stream should see updated value
        let r = join
            .process(BroadcastSide::Stream("k1:event".into()), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], "k1:event+k1:v2");
    }

    #[tokio::test]
    async fn multiple_stream_events_probe_same_broadcast() {
        let mut ctx = test_ctx("multi_probe");
        let mut join = BroadcastJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .build();

        join.process(BroadcastSide::Broadcast("k1:ref".into()), &mut ctx)
            .await
            .unwrap();

        // Multiple stream events should all match
        for i in 0..3 {
            let r = join
                .process(BroadcastSide::Stream(format!("k1:event{i}")), &mut ctx)
                .await
                .unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r[0], format!("k1:event{i}+k1:ref"));
        }
    }

    #[tokio::test]
    async fn ttl_evicts_stale_broadcast_entries() {
        let mut ctx = test_ctx("ttl_evict");
        let mut join = BroadcastJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .ttl(50)
            .build();

        // Broadcast at watermark 0
        join.process(BroadcastSide::Broadcast("k1:ref".into()), &mut ctx)
            .await
            .unwrap();

        // Advance watermark past TTL (0 + 50 = 50)
        join.on_watermark(50, &mut ctx).await.unwrap();

        // Stream event should miss because broadcast was evicted
        let r = join
            .process(BroadcastSide::Stream("k1:event".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn no_ttl_keeps_entries_forever() {
        let mut ctx = test_ctx("no_ttl");
        let mut join = BroadcastJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .build(); // no TTL

        join.process(BroadcastSide::Broadcast("k1:ref".into()), &mut ctx)
            .await
            .unwrap();

        // Advance watermark very far
        join.on_watermark(1_000_000, &mut ctx).await.unwrap();

        // Should still match
        let r = join
            .process(BroadcastSide::Stream("k1:event".into()), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
    }

    #[tokio::test]
    async fn different_keys_independent() {
        let mut ctx = test_ctx("diff_keys");
        let mut join = BroadcastJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .build();

        join.process(BroadcastSide::Broadcast("k1:ref1".into()), &mut ctx)
            .await
            .unwrap();
        join.process(BroadcastSide::Broadcast("k2:ref2".into()), &mut ctx)
            .await
            .unwrap();

        // k1 matches k1 broadcast
        let r = join
            .process(BroadcastSide::Stream("k1:ev".into()), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r[0], "k1:ev+k1:ref1");

        // k2 matches k2 broadcast
        let r = join
            .process(BroadcastSide::Stream("k2:ev".into()), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r[0], "k2:ev+k2:ref2");

        // k3 misses
        let r = join
            .process(BroadcastSide::Stream("k3:ev".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());
    }
}

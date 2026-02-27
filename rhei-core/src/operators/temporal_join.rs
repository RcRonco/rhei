//! Temporal join operator for correlating events from two sides by key.
//!
//! Events arrive as [`JoinSide::Left`] or [`JoinSide::Right`]. When both sides
//! for a given key have arrived, the join function is called and the result is
//! emitted. Unmatched events are buffered in operator state until their
//! counterpart appears.
//!
//! An optional `timeout` (in watermark units) triggers eviction of stale
//! buffered events when the watermark advances past their buffered timestamp
//! plus the timeout. This prevents unbounded state growth from unmatched keys.

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

use super::keyed_state::KeyedState;

/// An input event tagged with its side of the join.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JoinSide<L, R> {
    /// An event from the left side.
    Left(L),
    /// An event from the right side.
    Right(R),
}

/// A temporal join operator that matches events from two sides by key.
///
/// # Type Parameters
///
/// - `L` — left-side event type
/// - `R` — right-side event type
/// - `KF` — key extraction function `Fn(&JoinSide<L, R>) -> String`
/// - `JF` — join function `Fn(L, R) -> O`
pub struct TemporalJoin<L, R, KF, JF> {
    key_fn: KF,
    join_fn: JF,
    /// Watermark units before an unmatched event is evicted (`None` = no eviction).
    timeout: Option<u64>,
    /// Current watermark for timestamping buffered events.
    last_watermark: u64,
    /// Keys with buffered (unmatched) events, for efficient eviction scans.
    active_keys: HashSet<String>,
    _phantom: PhantomData<(L, R)>,
}

impl<L, R, KF: Clone, JF: Clone> Clone for TemporalJoin<L, R, KF, JF> {
    fn clone(&self) -> Self {
        Self {
            key_fn: self.key_fn.clone(),
            join_fn: self.join_fn.clone(),
            timeout: self.timeout,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<L, R, KF, JF> fmt::Debug for TemporalJoin<L, R, KF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TemporalJoin")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl<L, R, KF, JF> TemporalJoin<L, R, KF, JF> {
    /// Creates a new temporal join operator with no timeout (unbounded buffering).
    pub fn new(key_fn: KF, join_fn: JF) -> Self {
        Self {
            key_fn,
            join_fn,
            timeout: None,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<L, R> TemporalJoin<L, R, (), ()> {
    /// Returns a builder for constructing a `TemporalJoin`.
    pub fn builder() -> TemporalJoinBuilder<L, R> {
        TemporalJoinBuilder {
            key_fn: (),
            join_fn: (),
            timeout: None,
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`TemporalJoin`].
pub struct TemporalJoinBuilder<L, R, KF = (), JF = ()> {
    key_fn: KF,
    join_fn: JF,
    timeout: Option<u64>,
    _phantom: PhantomData<(L, R)>,
}

impl<L, R, KF, JF> fmt::Debug for TemporalJoinBuilder<L, R, KF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TemporalJoinBuilder")
            .finish_non_exhaustive()
    }
}

impl<L, R, KF, JF> TemporalJoinBuilder<L, R, KF, JF> {
    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> TemporalJoinBuilder<L, R, KF2, JF> {
        TemporalJoinBuilder {
            key_fn: kf,
            join_fn: self.join_fn,
            timeout: self.timeout,
            _phantom: PhantomData,
        }
    }

    /// Sets the join function called when both sides match.
    pub fn join_fn<JF2>(self, jf: JF2) -> TemporalJoinBuilder<L, R, KF, JF2> {
        TemporalJoinBuilder {
            key_fn: self.key_fn,
            join_fn: jf,
            timeout: self.timeout,
            _phantom: PhantomData,
        }
    }

    /// Sets the timeout in watermark units. Unmatched events older than
    /// `watermark - timeout` will be evicted during `on_watermark`.
    pub fn timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<L, R, O, KF, JF> TemporalJoinBuilder<L, R, KF, JF>
where
    L: Serialize + DeserializeOwned + Send + Sync,
    R: Serialize + DeserializeOwned + Send + Sync,
    O: Send,
    KF: Fn(&JoinSide<L, R>) -> String + Send + Sync,
    JF: Fn(L, R) -> O + Send + Sync,
{
    /// Builds the `TemporalJoin` operator.
    pub fn build(self) -> TemporalJoin<L, R, KF, JF> {
        TemporalJoin {
            key_fn: self.key_fn,
            join_fn: self.join_fn,
            timeout: self.timeout,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

/// Timestamped wrapper for buffered join events.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Stamped<V> {
    value: V,
    watermark_at_buffer: u64,
}

#[async_trait]
impl<L, R, O, KF, JF> StreamFunction for TemporalJoin<L, R, KF, JF>
where
    L: Clone + Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug,
    R: Clone + Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug,
    O: Clone + Send + std::fmt::Debug,
    KF: Fn(&JoinSide<L, R>) -> String + Send + Sync,
    JF: Fn(L, R) -> O + Send + Sync,
{
    type Input = JoinSide<L, R>;
    type Output = O;

    async fn process(
        &mut self,
        input: JoinSide<L, R>,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<O>> {
        let key = (self.key_fn)(&input);

        match input {
            JoinSide::Left(l) => {
                // Check for a buffered right-side event
                let right_value: Option<R> = {
                    let mut state = KeyedState::<String, Stamped<R>>::new(ctx, "right");
                    let val = state.get(&key).await.unwrap_or(None);
                    if val.is_some() {
                        state.delete(&key);
                    }
                    val.map(|s| s.value)
                };

                if let Some(r) = right_value {
                    self.active_keys.remove(&key);
                    Ok(vec![(self.join_fn)(l, r)])
                } else {
                    // Buffer the left-side event with current watermark timestamp
                    let mut state = KeyedState::<String, Stamped<L>>::new(ctx, "left");
                    state.put(
                        &key,
                        &Stamped {
                            value: l,
                            watermark_at_buffer: self.last_watermark,
                        },
                    );
                    self.active_keys.insert(key);
                    Ok(vec![])
                }
            }
            JoinSide::Right(r) => {
                // Check for a buffered left-side event
                let left_value: Option<L> = {
                    let mut state = KeyedState::<String, Stamped<L>>::new(ctx, "left");
                    let val = state.get(&key).await.unwrap_or(None);
                    if val.is_some() {
                        state.delete(&key);
                    }
                    val.map(|s| s.value)
                };

                if let Some(l) = left_value {
                    self.active_keys.remove(&key);
                    Ok(vec![(self.join_fn)(l, r)])
                } else {
                    // Buffer the right-side event with current watermark timestamp
                    let mut state = KeyedState::<String, Stamped<R>>::new(ctx, "right");
                    state.put(
                        &key,
                        &Stamped {
                            value: r,
                            watermark_at_buffer: self.last_watermark,
                        },
                    );
                    self.active_keys.insert(key);
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
        let Some(timeout) = self.timeout else {
            return Ok(vec![]);
        };

        let mut evicted_keys = Vec::new();
        for key in &self.active_keys {
            let mut did_evict = false;

            // Check left side
            {
                let mut state = KeyedState::<String, Stamped<L>>::new(ctx, "left");
                if let Some(stamped) = state.get(key).await.unwrap_or(None)
                    && watermark >= stamped.watermark_at_buffer + timeout
                {
                    state.delete(key);
                    did_evict = true;
                }
            }

            // Check right side
            {
                let mut state = KeyedState::<String, Stamped<R>>::new(ctx, "right");
                if let Some(stamped) = state.get(key).await.unwrap_or(None)
                    && watermark >= stamped.watermark_at_buffer + timeout
                {
                    state.delete(key);
                    did_evict = true;
                }
            }

            if did_evict {
                evicted_keys.push(key.clone());
                metrics::counter!("temporal_join_evicted_total").increment(1);
            }
        }

        for key in &evicted_keys {
            self.active_keys.remove(key);
        }

        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path = std::env::temp_dir().join(format!("rhei_tj_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    fn key_fn(side: &JoinSide<String, String>) -> String {
        match side {
            JoinSide::Left(s) | JoinSide::Right(s) => s.split(':').next().unwrap().to_string(),
        }
    }

    fn join_fn(l: String, r: String) -> String {
        format!("{l}+{r}")
    }

    #[tokio::test]
    async fn join_matches_left_then_right() {
        let mut ctx = test_ctx("match_lr");
        let mut join = TemporalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .timeout(100)
            .build();

        // Left arrives first — buffered
        let r = join
            .process(JoinSide::Left("k1:left_val".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());

        // Right arrives — match emitted
        let r = join
            .process(JoinSide::Right("k1:right_val".into()), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], "k1:left_val+k1:right_val");
    }

    #[tokio::test]
    async fn eviction_on_watermark_timeout() {
        let mut ctx = test_ctx("evict_wm");
        let mut join = TemporalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .timeout(50)
            .build();

        // Left arrives at watermark 0 — buffered
        let r = join
            .process(JoinSide::Left("k1:left_val".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());

        // Advance watermark past timeout (0 + 50 = 50)
        let r = join.on_watermark(50, &mut ctx).await.unwrap();
        assert!(r.is_empty()); // evicted events are silently dropped

        // Right arrives — no match because left was evicted
        let r = join
            .process(JoinSide::Right("k1:right_val".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty()); // buffered, no match
    }

    #[tokio::test]
    async fn no_eviction_without_timeout() {
        let mut ctx = test_ctx("no_evict");
        let mut join = TemporalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .build(); // no timeout

        // Left arrives — buffered
        let r = join
            .process(JoinSide::Left("k1:left_val".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());

        // Advance watermark far ahead — no eviction without timeout
        let r = join.on_watermark(1_000_000, &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Right arrives — match still works
        let r = join
            .process(JoinSide::Right("k1:right_val".into()), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], "k1:left_val+k1:right_val");
    }

    #[tokio::test]
    async fn join_matches_right_then_left() {
        let mut ctx = test_ctx("match_rl");
        let mut join = TemporalJoin::builder()
            .key_fn(key_fn)
            .join_fn(join_fn)
            .timeout(100)
            .build();

        // Right arrives first — buffered
        let r = join
            .process(JoinSide::Right("k1:right_val".into()), &mut ctx)
            .await
            .unwrap();
        assert!(r.is_empty());

        // Left arrives — match emitted
        let r = join
            .process(JoinSide::Left("k1:left_val".into()), &mut ctx)
            .await
            .unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], "k1:left_val+k1:right_val");
    }
}

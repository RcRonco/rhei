//! Temporal join operator for correlating events from two sides by key.
//!
//! Events arrive as [`JoinSide::Left`] or [`JoinSide::Right`]. When both sides
//! for a given key have arrived, the join function is called and the result is
//! emitted. Unmatched events are buffered in operator state until their
//! counterpart appears.

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
    _phantom: PhantomData<(L, R)>,
}

impl<L, R, KF, JF> fmt::Debug for TemporalJoin<L, R, KF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TemporalJoin").finish_non_exhaustive()
    }
}

impl<L, R, KF, JF> TemporalJoin<L, R, KF, JF> {
    /// Creates a new temporal join operator.
    pub fn new(key_fn: KF, join_fn: JF) -> Self {
        Self {
            key_fn,
            join_fn,
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
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`TemporalJoin`].
pub struct TemporalJoinBuilder<L, R, KF = (), JF = ()> {
    key_fn: KF,
    join_fn: JF,
    _phantom: PhantomData<(L, R)>,
}

impl<L, R, KF, JF> fmt::Debug for TemporalJoinBuilder<L, R, KF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TemporalJoinBuilder").finish_non_exhaustive()
    }
}

impl<L, R, KF, JF> TemporalJoinBuilder<L, R, KF, JF> {
    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> TemporalJoinBuilder<L, R, KF2, JF> {
        TemporalJoinBuilder {
            key_fn: kf,
            join_fn: self.join_fn,
            _phantom: PhantomData,
        }
    }

    /// Sets the join function called when both sides match.
    pub fn join_fn<JF2>(self, jf: JF2) -> TemporalJoinBuilder<L, R, KF, JF2> {
        TemporalJoinBuilder {
            key_fn: self.key_fn,
            join_fn: jf,
            _phantom: PhantomData,
        }
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
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<L, R, O, KF, JF> StreamFunction for TemporalJoin<L, R, KF, JF>
where
    L: Serialize + DeserializeOwned + Send + Sync,
    R: Serialize + DeserializeOwned + Send + Sync,
    O: Send,
    KF: Fn(&JoinSide<L, R>) -> String + Send + Sync,
    JF: Fn(L, R) -> O + Send + Sync,
{
    type Input = JoinSide<L, R>;
    type Output = O;

    async fn process(&mut self, input: JoinSide<L, R>, ctx: &mut StateContext) -> Vec<O> {
        let key = (self.key_fn)(&input);

        match input {
            JoinSide::Left(l) => {
                // Check for a buffered right-side event
                let right_value: Option<R> = {
                    let mut state = KeyedState::<String, R>::new(ctx, "right");
                    let val = state.get(&key).await.unwrap_or(None);
                    if val.is_some() {
                        state.delete(&key);
                    }
                    val
                };

                if let Some(r) = right_value {
                    vec![(self.join_fn)(l, r)]
                } else {
                    // Buffer the left-side event
                    let mut state = KeyedState::<String, L>::new(ctx, "left");
                    state.put(&key, &l);
                    vec![]
                }
            }
            JoinSide::Right(r) => {
                // Check for a buffered left-side event
                let left_value: Option<L> = {
                    let mut state = KeyedState::<String, L>::new(ctx, "left");
                    let val = state.get(&key).await.unwrap_or(None);
                    if val.is_some() {
                        state.delete(&key);
                    }
                    val
                };

                if let Some(l) = left_value {
                    vec![(self.join_fn)(l, r)]
                } else {
                    // Buffer the right-side event
                    let mut state = KeyedState::<String, R>::new(ctx, "right");
                    state.put(&key, &r);
                    vec![]
                }
            }
        }
    }
}

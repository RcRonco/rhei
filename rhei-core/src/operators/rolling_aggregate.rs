//! Rolling aggregation operator.
//!
//! Per-key stateful aggregation using the [`Aggregator`] trait.
//! Emits the current aggregate value on every input.

use std::fmt;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

use super::aggregator::Aggregator;

/// A per-key rolling aggregate operator.
///
/// For each input, loads the accumulator for the key, calls `accumulate`,
/// stores the updated accumulator, and emits `finish(&acc)`.
pub struct RollingAggregateOp<A, KF> {
    aggregator: A,
    key_fn: KF,
}

impl<A: Clone, KF: Clone> Clone for RollingAggregateOp<A, KF> {
    fn clone(&self) -> Self {
        Self {
            aggregator: self.aggregator.clone(),
            key_fn: self.key_fn.clone(),
        }
    }
}

impl<A, KF> fmt::Debug for RollingAggregateOp<A, KF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RollingAggregateOp").finish_non_exhaustive()
    }
}

impl<A, KF> RollingAggregateOp<A, KF> {
    /// Creates a new `RollingAggregateOp`.
    ///
    /// - `key_fn`: extracts the grouping key from each input.
    /// - `aggregator`: the [`Aggregator`] implementation.
    pub fn new(key_fn: KF, aggregator: A) -> Self {
        Self { aggregator, key_fn }
    }
}

#[async_trait]
impl<A, KF> StreamFunction for RollingAggregateOp<A, KF>
where
    A: Aggregator + Send + Sync,
    A::Input: Clone + Send + Sync + std::fmt::Debug,
    A::Accumulator: Serialize + DeserializeOwned,
    A::Output: Clone + Send + Sync + std::fmt::Debug + Serialize + DeserializeOwned,
    KF: Fn(&A::Input) -> String + Send + Sync,
{
    type Input = A::Input;
    type Output = A::Output;

    async fn process(
        &mut self,
        input: A::Input,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<A::Output>> {
        let key = (self.key_fn)(&input);
        let state_key = format!("agg:{key}");
        let key_bytes = state_key.as_bytes();

        let mut acc: A::Accumulator = ctx.get(key_bytes).await?.unwrap_or_default();
        self.aggregator.accumulate(&mut acc, &input);
        ctx.put(key_bytes, &acc);

        Ok(vec![self.aggregator.finish(&acc)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::aggregator::Count;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path =
            std::env::temp_dir().join(format!("rhei_rollagg_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[tokio::test]
    async fn count_per_key() {
        let mut ctx = test_ctx("count");
        let mut op = RollingAggregateOp::new(
            |v: &(String, i64)| v.0.clone(),
            Count::<(String, i64)>::new(),
        );

        let r = op.process(("a".into(), 1), &mut ctx).await.unwrap();
        assert_eq!(r, vec![1]);

        let r = op.process(("a".into(), 2), &mut ctx).await.unwrap();
        assert_eq!(r, vec![2]);

        let r = op.process(("b".into(), 1), &mut ctx).await.unwrap();
        assert_eq!(r, vec![1]);

        let r = op.process(("a".into(), 3), &mut ctx).await.unwrap();
        assert_eq!(r, vec![3]);
    }
}

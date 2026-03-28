//! Count-based window operator.
//!
//! Emits after exactly N elements per key, then resets.
//! No watermark dependency — purely count-driven.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

use super::aggregator::Aggregator;

/// Internal state for a count window: count of elements and accumulator.
#[derive(Serialize, Deserialize)]
struct CountState<Acc> {
    count: u64,
    accumulator: Acc,
}

impl<Acc: Default> Default for CountState<Acc> {
    fn default() -> Self {
        Self {
            count: 0,
            accumulator: Acc::default(),
        }
    }
}

/// The output emitted when a count window fires.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CountWindowOutput<V> {
    /// The grouping key.
    pub key: String,
    /// The total element count in this window.
    pub count: u64,
    /// The aggregated value.
    pub value: V,
}

impl<V: fmt::Display> fmt::Display for CountWindowOutput<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "count_window: {} count={} value={}",
            self.key, self.count, self.value
        )
    }
}

/// A count-based window operator.
///
/// Accumulates elements per key. When the count reaches `threshold`,
/// emits the aggregate and resets.
pub struct CountWindow<T, A, KF> {
    threshold: u64,
    key_fn: KF,
    aggregator: A,
    _phantom: PhantomData<T>,
}

impl<T, A: Clone, KF: Clone> Clone for CountWindow<T, A, KF> {
    fn clone(&self) -> Self {
        Self {
            threshold: self.threshold,
            key_fn: self.key_fn.clone(),
            aggregator: self.aggregator.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF> fmt::Debug for CountWindow<T, A, KF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountWindow")
            .field("threshold", &self.threshold)
            .finish_non_exhaustive()
    }
}

impl<T> CountWindow<T, (), ()> {
    /// Returns a builder for constructing a `CountWindow`.
    pub fn builder() -> CountWindowBuilder<T> {
        CountWindowBuilder {
            threshold: 0,
            key_fn: (),
            aggregator: (),
            _phantom: PhantomData,
        }
    }
}

/// Builder for [`CountWindow`].
#[derive(Debug)]
pub struct CountWindowBuilder<T, A = (), KF = ()> {
    threshold: u64,
    key_fn: KF,
    aggregator: A,
    _phantom: PhantomData<T>,
}

impl<T, A, KF> CountWindowBuilder<T, A, KF> {
    /// Sets the number of elements per window.
    pub fn count(mut self, n: u64) -> Self {
        self.threshold = n;
        self
    }

    /// Sets the key extraction function.
    pub fn key_fn<KF2>(self, kf: KF2) -> CountWindowBuilder<T, A, KF2> {
        CountWindowBuilder {
            threshold: self.threshold,
            key_fn: kf,
            aggregator: self.aggregator,
            _phantom: PhantomData,
        }
    }

    /// Sets the aggregator.
    pub fn aggregator<A2>(self, agg: A2) -> CountWindowBuilder<T, A2, KF> {
        CountWindowBuilder {
            threshold: self.threshold,
            key_fn: self.key_fn,
            aggregator: agg,
            _phantom: PhantomData,
        }
    }
}

impl<T, A, KF> CountWindowBuilder<T, A, KF>
where
    T: Send + Sync,
    A: Aggregator<Input = T>,
    A::Output: Send,
    KF: Fn(&T) -> String + Send + Sync,
{
    /// Builds the `CountWindow` operator.
    ///
    /// # Panics
    ///
    /// Panics if `count` is zero.
    pub fn build(self) -> CountWindow<T, A, KF> {
        assert!(self.threshold > 0, "count threshold must be > 0");
        CountWindow {
            threshold: self.threshold,
            key_fn: self.key_fn,
            aggregator: self.aggregator,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, A, KF> StreamFunction for CountWindow<T, A, KF>
where
    T: Clone + Send + Sync + std::fmt::Debug,
    A: Aggregator<Input = T> + Send + Sync,
    A::Accumulator: Serialize + serde::de::DeserializeOwned,
    A::Output: Clone + Send + std::fmt::Debug + Serialize + serde::de::DeserializeOwned,
    KF: Fn(&T) -> String + Send + Sync,
{
    type Input = T;
    type Output = CountWindowOutput<A::Output>;

    async fn process(
        &mut self,
        input: T,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<CountWindowOutput<A::Output>>> {
        let key = (self.key_fn)(&input);
        let state_key = format!("cw:{key}");
        let key_bytes = state_key.as_bytes();

        let mut state: CountState<A::Accumulator> = ctx.get(key_bytes).await?.unwrap_or_default();
        self.aggregator.accumulate(&mut state.accumulator, &input);
        state.count += 1;

        if state.count >= self.threshold {
            let output = CountWindowOutput {
                key,
                count: state.count,
                value: self.aggregator.finish(&state.accumulator),
            };
            ctx.delete(key_bytes);
            Ok(vec![output])
        } else {
            ctx.put(key_bytes, &state);
            Ok(vec![])
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::operators::aggregator::Count;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path = std::env::temp_dir().join(format!("rhei_cw_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[tokio::test]
    async fn emits_at_threshold() {
        let mut ctx = test_ctx("threshold");
        let mut win = CountWindow::<String, _, _>::builder()
            .count(3)
            .key_fn(|s: &String| s.clone())
            .aggregator(Count::<String>::new())
            .build();

        // First two: no output
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Third: emit
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, "a");
        assert_eq!(r[0].count, 3);
        assert_eq!(r[0].value, 3);

        // Reset — fourth and fifth: no output
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // Sixth: emit again
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].count, 3);
    }

    #[tokio::test]
    async fn multiple_keys_independent() {
        let mut ctx = test_ctx("multikey");
        let mut win = CountWindow::<String, _, _>::builder()
            .count(2)
            .key_fn(|s: &String| s.clone())
            .aggregator(Count::<String>::new())
            .build();

        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());
        let r = win.process("b".into(), &mut ctx).await.unwrap();
        assert!(r.is_empty());

        // "a" fires at 2
        let r = win.process("a".into(), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, "a");

        // "b" fires at 2
        let r = win.process("b".into(), &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, "b");
    }

    #[tokio::test]
    async fn checkpoint_mid_window() {
        let path = std::env::temp_dir().join(format!("rhei_cw_ckpt_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut win = CountWindow::<String, _, _>::builder()
            .count(3)
            .key_fn(|s: &String| s.clone())
            .aggregator(Count::<String>::new())
            .build();

        // Process 2 elements, then checkpoint
        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            win.process("a".into(), &mut ctx).await.unwrap();
            win.process("a".into(), &mut ctx).await.unwrap();
            ctx.checkpoint().await.unwrap();
        }

        // Reopen and process one more — should fire
        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            let r = win.process("a".into(), &mut ctx).await.unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r[0].count, 3);
        }

        let _ = std::fs::remove_file(&path);
    }
}

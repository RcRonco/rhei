//! Rolling reduce operator.
//!
//! Per-key stateful reduce that emits the updated value on every input.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

/// A per-key rolling reduce operator.
///
/// For each input, loads the current accumulated value for the key (if any),
/// applies `reduce_fn(existing, input)`, stores the result, and emits it.
/// The first element for a new key is stored and emitted as-is.
pub struct ReduceOp<F, T, KF> {
    reduce_fn: F,
    key_fn: KF,
    _phantom: PhantomData<T>,
}

impl<F: Clone, T, KF: Clone> Clone for ReduceOp<F, T, KF> {
    fn clone(&self) -> Self {
        Self {
            reduce_fn: self.reduce_fn.clone(),
            key_fn: self.key_fn.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<F, T, KF> fmt::Debug for ReduceOp<F, T, KF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReduceOp").finish_non_exhaustive()
    }
}

impl<F, T, KF> ReduceOp<F, T, KF> {
    /// Creates a new `ReduceOp`.
    ///
    /// - `key_fn`: extracts the grouping key from each input.
    /// - `reduce_fn`: combines two values into one (`(existing, new) -> combined`).
    pub fn new(key_fn: KF, reduce_fn: F) -> Self {
        Self {
            reduce_fn,
            key_fn,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, T, KF> StreamFunction for ReduceOp<F, T, KF>
where
    T: Clone + Send + Sync + std::fmt::Debug + Serialize + DeserializeOwned,
    F: Fn(T, T) -> T + Send + Sync,
    KF: Fn(&T) -> String + Send + Sync,
{
    type Input = T;
    type Output = T;

    async fn process(&mut self, input: T, ctx: &mut StateContext) -> anyhow::Result<Vec<T>> {
        let key = (self.key_fn)(&input);
        let state_key = format!("reduce:{key}");
        let key_bytes = state_key.as_bytes();

        let existing: Option<T> = ctx.get(key_bytes).await?;
        let result = match existing {
            Some(prev) => (self.reduce_fn)(prev, input),
            None => input,
        };
        ctx.put(key_bytes, &result)?;
        Ok(vec![result])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path =
            std::env::temp_dir().join(format!("rhei_reduce_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[tokio::test]
    async fn running_sum_per_key() {
        let mut ctx = test_ctx("sum");
        let mut op = ReduceOp::new(
            |v: &(String, i64)| v.0.clone(),
            |a: (String, i64), b: (String, i64)| (a.0, a.1 + b.1),
        );

        let r = op.process(("a".into(), 10), &mut ctx).await.unwrap();
        assert_eq!(r, vec![("a".into(), 10)]);

        let r = op.process(("a".into(), 5), &mut ctx).await.unwrap();
        assert_eq!(r, vec![("a".into(), 15)]);

        let r = op.process(("b".into(), 100), &mut ctx).await.unwrap();
        assert_eq!(r, vec![("b".into(), 100)]);

        let r = op.process(("a".into(), 3), &mut ctx).await.unwrap();
        assert_eq!(r, vec![("a".into(), 18)]);
    }

    #[tokio::test]
    async fn checkpoint_recovery() {
        let path =
            std::env::temp_dir().join(format!("rhei_reduce_test_ckpt_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut op = ReduceOp::new(
            |v: &(String, i64)| v.0.clone(),
            |a: (String, i64), b: (String, i64)| (a.0, a.1 + b.1),
        );

        // Process two elements, then checkpoint
        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            let r = op.process(("k".into(), 10), &mut ctx).await.unwrap();
            assert_eq!(r, vec![("k".into(), 10)]);
            let r = op.process(("k".into(), 5), &mut ctx).await.unwrap();
            assert_eq!(r, vec![("k".into(), 15)]);
            ctx.checkpoint().await.unwrap();
        }

        // Reopen from same path — state should be recovered
        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            let mut ctx = StateContext::new(Box::new(backend));
            let r = op.process(("k".into(), 3), &mut ctx).await.unwrap();
            assert_eq!(r, vec![("k".into(), 18)]);
        }

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn first_element_emitted_directly() {
        let mut ctx = test_ctx("first");
        let mut op = ReduceOp::new(|v: &i64| v.to_string(), |a: i64, b: i64| a + b);

        let r = op.process(42, &mut ctx).await.unwrap();
        assert_eq!(r, vec![42]);
    }
}

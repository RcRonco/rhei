//! Retraction stream support for incremental computation.
//!
//! In many streaming systems (Flink, Materialize), operators that update
//! previously emitted results need to retract the old value before sending
//! the new one. [`Retract<T>`] models this as a tagged enum.
//!
//! # Usage
//!
//! Operators that produce retractions wrap their output in `Retract`:
//!
//! ```rust
//! use rhei_core::operators::retract::Retract;
//!
//! let insert = Retract::Insert("hello".to_string());
//! let delete = Retract::Delete("hello".to_string());
//!
//! assert!(insert.is_insert());
//! assert!(delete.is_delete());
//! assert_eq!(insert.into_inner(), "hello".to_string());
//! ```
//!
//! [`RetractOp`] wraps any `StreamFunction` and attaches `Retract::Insert` to
//! every output. Downstream operators that need to issue retractions (e.g.
//! updating aggregations) can emit `Retract::Delete` for the old value followed
//! by `Retract::Insert` for the new value.

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::state::context::StateContext;
use crate::traits::StreamFunction;

/// A retraction-tagged value in a changelog stream.
///
/// This is the Rust equivalent of Flink's `RowKind` or Materialize's diff column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Retract<T> {
    /// A new record being inserted into the stream.
    Insert(T),
    /// A previously emitted record being retracted (deleted).
    Delete(T),
}

impl<T> Retract<T> {
    /// Returns `true` if this is an `Insert`.
    pub fn is_insert(&self) -> bool {
        matches!(self, Retract::Insert(_))
    }

    /// Returns `true` if this is a `Delete`.
    pub fn is_delete(&self) -> bool {
        matches!(self, Retract::Delete(_))
    }

    /// Returns a reference to the inner value regardless of the tag.
    pub fn value(&self) -> &T {
        match self {
            Retract::Insert(v) | Retract::Delete(v) => v,
        }
    }

    /// Consumes this `Retract` and returns the inner value.
    pub fn into_inner(self) -> T {
        match self {
            Retract::Insert(v) | Retract::Delete(v) => v,
        }
    }

    /// Maps the inner value, preserving the tag.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Retract<U> {
        match self {
            Retract::Insert(v) => Retract::Insert(f(v)),
            Retract::Delete(v) => Retract::Delete(f(v)),
        }
    }
}

impl<T: fmt::Display> fmt::Display for Retract<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Retract::Insert(v) => write!(f, "+{v}"),
            Retract::Delete(v) => write!(f, "-{v}"),
        }
    }
}

/// Wraps a `StreamFunction` to tag all outputs as `Retract::Insert`.
///
/// This is the simplest adapter for introducing retraction semantics into
/// a pipeline. The inner operator produces plain values; `RetractOp` wraps
/// each in `Insert`.
pub struct RetractOp<F> {
    inner: F,
}

impl<F: Clone> Clone for RetractOp<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<F> fmt::Debug for RetractOp<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetractOp").finish_non_exhaustive()
    }
}

impl<F> RetractOp<F> {
    /// Wraps an existing operator to emit `Retract::Insert` for every output.
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<F> StreamFunction for RetractOp<F>
where
    F: StreamFunction,
    F::Input: Clone + Send + std::fmt::Debug,
    F::Output: Clone + Send + std::fmt::Debug,
{
    type Input = F::Input;
    type Output = Retract<F::Output>;

    async fn process(
        &mut self,
        input: F::Input,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<Retract<F::Output>>> {
        let outputs = self.inner.process(input, ctx).await?;
        Ok(outputs.into_iter().map(Retract::Insert).collect())
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<Retract<F::Output>>> {
        let outputs = self.inner.on_watermark(watermark, ctx).await?;
        Ok(outputs.into_iter().map(Retract::Insert).collect())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    fn test_ctx(name: &str) -> StateContext {
        let path =
            std::env::temp_dir().join(format!("rhei_retract_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        StateContext::new(Box::new(backend))
    }

    #[test]
    fn retract_is_insert() {
        let r = Retract::Insert(42);
        assert!(r.is_insert());
        assert!(!r.is_delete());
    }

    #[test]
    fn retract_is_delete() {
        let r = Retract::Delete(42);
        assert!(r.is_delete());
        assert!(!r.is_insert());
    }

    #[test]
    fn retract_value_ref() {
        assert_eq!(*Retract::Insert(42).value(), 42);
        assert_eq!(*Retract::Delete(99).value(), 99);
    }

    #[test]
    fn retract_into_inner() {
        assert_eq!(Retract::Insert("hello".to_string()).into_inner(), "hello");
        assert_eq!(Retract::Delete("world".to_string()).into_inner(), "world");
    }

    #[test]
    fn retract_map() {
        let r = Retract::Insert(10).map(|x| x * 2);
        assert_eq!(r, Retract::Insert(20));

        let r = Retract::Delete(5).map(|x| x + 1);
        assert_eq!(r, Retract::Delete(6));
    }

    #[test]
    fn retract_display() {
        assert_eq!(Retract::Insert(42).to_string(), "+42");
        assert_eq!(Retract::Delete(42).to_string(), "-42");
    }

    #[test]
    fn retract_serialize_roundtrip() {
        let original = Retract::Insert("value".to_string());
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Retract<String> = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);

        let original = Retract::Delete(123u64);
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Retract<u64> = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    /// Minimal pass-through operator for testing RetractOp.
    struct DoubleOp;

    #[async_trait]
    impl StreamFunction for DoubleOp {
        type Input = i32;
        type Output = i32;

        async fn process(
            &mut self,
            input: i32,
            _ctx: &mut StateContext,
        ) -> anyhow::Result<Vec<i32>> {
            Ok(vec![input * 2])
        }
    }

    #[tokio::test]
    async fn retract_op_wraps_outputs_as_insert() {
        let mut ctx = test_ctx("retract_op");
        let mut op = RetractOp::new(DoubleOp);

        let r = op.process(5, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], Retract::Insert(10));
    }

    /// Operator that emits multiple outputs.
    struct FanOutOp;

    #[async_trait]
    impl StreamFunction for FanOutOp {
        type Input = i32;
        type Output = i32;

        async fn process(
            &mut self,
            input: i32,
            _ctx: &mut StateContext,
        ) -> anyhow::Result<Vec<i32>> {
            Ok(vec![input, input + 1, input + 2])
        }
    }

    #[tokio::test]
    async fn retract_op_wraps_multiple_outputs() {
        let mut ctx = test_ctx("retract_multi");
        let mut op = RetractOp::new(FanOutOp);

        let r = op.process(10, &mut ctx).await.unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0], Retract::Insert(10));
        assert_eq!(r[1], Retract::Insert(11));
        assert_eq!(r[2], Retract::Insert(12));
    }

    #[tokio::test]
    async fn retract_op_empty_output() {
        let mut ctx = test_ctx("retract_empty");

        struct EmptyOp;
        #[async_trait]
        impl StreamFunction for EmptyOp {
            type Input = i32;
            type Output = i32;
            async fn process(
                &mut self,
                _input: i32,
                _ctx: &mut StateContext,
            ) -> anyhow::Result<Vec<i32>> {
                Ok(vec![])
            }
        }

        let mut op = RetractOp::new(EmptyOp);
        let r = op.process(42, &mut ctx).await.unwrap();
        assert!(r.is_empty());
    }
}

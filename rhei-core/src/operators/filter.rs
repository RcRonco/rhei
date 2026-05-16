//! Batch filter operators (closure-based and `DataFusion` expression-based).

use std::fmt;
use std::marker::PhantomData;

use arrow_array::BooleanArray;
use async_trait::async_trait;

use crate::arrow::{BufferOutput, OperatorContext, RheiBuffer, RheiSchema, StreamFunction};

/// Zero-copy filter using a closure over Views to build a selection mask.
///
/// The closure evaluates each row via its View; rows returning `false` are
/// masked out without any data copying.
pub struct FilterFnOp<F, T> {
    predicate: F,
    _phantom: PhantomData<fn(T)>,
}

impl<F, T> fmt::Debug for FilterFnOp<F, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterFnOp").finish_non_exhaustive()
    }
}

impl<F, T> FilterFnOp<F, T>
where
    T: RheiSchema,
    F: for<'a> Fn(&T::View<'a>) -> bool,
{
    /// Creates a new closure-based batch filter.
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, T> StreamFunction for FilterFnOp<F, T>
where
    T: RheiSchema,
    F: for<'a> Fn(&T::View<'a>) -> bool + Send + Sync,
{
    type Input = T;
    type Output = T;

    async fn process(
        &mut self,
        input: RheiBuffer<T>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<T>> {
        let physical_len = input.physical_len();
        if physical_len == 0 {
            return Ok(BufferOutput::None);
        }

        let mut mask_values = vec![false; physical_len];

        for (phys_idx, view) in input.iter().enumerate_physical() {
            mask_values[phys_idx] = (self.predicate)(&view);
        }

        let mask = BooleanArray::from(mask_values);
        let filtered = input.and_mask(mask);

        if filtered.is_empty() {
            return Ok(BufferOutput::None);
        }
        Ok(BufferOutput::Single(filtered))
    }
}

/// Zero-copy filter using a `DataFusion` `PhysicalExpr`.
///
/// The expression is evaluated against the underlying `RecordBatch`,
/// producing a `BooleanArray` mask that is composed with any existing mask.
/// No data is copied — filtering is purely via selection vectors.
pub struct FilterOp<T> {
    expr: std::sync::Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    _phantom: PhantomData<fn(T)>,
}

impl<T> fmt::Debug for FilterOp<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterOp")
            .field("expr", &self.expr)
            .finish_non_exhaustive()
    }
}

impl<T: RheiSchema> FilterOp<T> {
    /// Creates a new `DataFusion` expression-based batch filter.
    pub fn new(expr: std::sync::Arc<dyn datafusion_physical_expr::PhysicalExpr>) -> Self {
        Self {
            expr,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T: RheiSchema> StreamFunction for FilterOp<T> {
    type Input = T;
    type Output = T;

    #[allow(clippy::expect_used)]
    async fn process(
        &mut self,
        input: RheiBuffer<T>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<T>> {
        if input.physical_len() == 0 {
            return Ok(BufferOutput::None);
        }

        let batch = input.as_record_batch();
        let result = self.expr.evaluate(batch)?;

        let mask = result
            .into_array(batch.num_rows())
            .expect("expression must produce an array");

        let bool_mask = mask
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("filter expression must produce BooleanArray")
            .clone();

        let filtered = input.and_mask(bool_mask);

        if filtered.is_empty() {
            return Ok(BufferOutput::None);
        }
        Ok(BufferOutput::Single(filtered))
    }
}

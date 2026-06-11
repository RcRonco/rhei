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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::cast_possible_wrap)]
mod tests {
    use super::*;
    use arrow_array::builder::ArrayBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{BooleanArray, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::arrow::RheiSchema as RheiSchemaTrait;
    use crate::arrow::{OperatorContext, RheiBuffer, RheiBuilder};
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    /// Minimal two-column row: an `id` plus a boolean `keep` flag. The boolean
    /// column lets us drive [`FilterOp`] with a bare `Column` `PhysicalExpr`
    /// (which evaluates straight to a `BooleanArray`) without pulling in the
    /// `datafusion-expr` operator surface.
    struct Row {
        id: i64,
        keep: bool,
    }

    struct RowBuilder {
        id: arrow_array::builder::Int64Builder,
        keep: arrow_array::builder::BooleanBuilder,
    }

    struct RowView {
        id: i64,
        #[allow(dead_code)]
        keep: bool,
    }

    struct RowCols<'a> {
        #[allow(dead_code)]
        id: &'a Int64Array,
        #[allow(dead_code)]
        keep: &'a BooleanArray,
    }

    impl RheiBuilder for RowBuilder {
        type Item = Row;

        fn append(&mut self, item: Row) {
            self.id.append_value(item.id);
            self.keep.append_value(item.keep);
        }

        fn append_null(&mut self) {
            self.id.append_null();
            self.keep.append_null();
        }

        fn len(&self) -> usize {
            self.id.len()
        }

        fn finish(mut self) -> RecordBatch {
            RecordBatch::try_new(
                Row::arrow_schema(),
                vec![Arc::new(self.id.finish()), Arc::new(self.keep.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for Row {
        type Builder = RowBuilder;
        type View<'a> = RowView;
        type Columns<'a> = RowCols<'a>;

        fn arrow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("keep", DataType::Boolean, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            RowBuilder {
                id: arrow_array::builder::Int64Builder::with_capacity(capacity),
                keep: arrow_array::builder::BooleanBuilder::with_capacity(capacity),
            }
        }

        fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
            RowView {
                id: batch.column(0).as_primitive::<Int64Type>().value(index),
                keep: batch.column(1).as_boolean().value(index),
            }
        }

        fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
            RowCols {
                id: batch.column(0).as_primitive::<Int64Type>(),
                keep: batch.column(1).as_boolean(),
            }
        }
    }

    fn test_ctx() -> OperatorContext {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().join("state.json"), None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    /// Builds a buffer with `id = 0, 1, .., n-1`, all sharing the `keep` flag.
    fn make_rows(n: usize, keep: bool) -> RheiBuffer<Row> {
        let mut builder = Row::builder(n);
        for id in 0..n {
            builder.append(Row {
                id: id as i64,
                keep,
            });
        }
        RheiBuffer::from_builder(builder)
    }

    fn ids(buf: &RheiBuffer<Row>) -> Vec<i64> {
        buf.iter().map(|v| v.id).collect()
    }

    // ---- FilterFnOp (closure-based) -------------------------------------

    #[tokio::test]
    async fn filter_fn_keeps_matching_rows() {
        let mut ctx = test_ctx();
        let mut op = FilterFnOp::<_, Row>::new(|v: &RowView| v.id % 2 == 0);

        let result = op.process(make_rows(5, true), &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(ids(&buf), vec![0, 2, 4]);
    }

    #[tokio::test]
    async fn filter_fn_all_excluded_returns_none() {
        let mut ctx = test_ctx();
        let mut op = FilterFnOp::<_, Row>::new(|_: &RowView| false);

        let result = op.process(make_rows(4, true), &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn filter_fn_all_kept_preserves_order() {
        let mut ctx = test_ctx();
        let mut op = FilterFnOp::<_, Row>::new(|_: &RowView| true);

        let result = op.process(make_rows(3, true), &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(ids(&buf), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn filter_fn_empty_input_returns_none() {
        let mut ctx = test_ctx();
        let mut op = FilterFnOp::<_, Row>::new(|_: &RowView| true);

        let input: RheiBuffer<Row> = RheiBuffer::from_builder(Row::builder(0));
        let result = op.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    /// The predicate is evaluated over *physical* rows, but the result must be
    /// composed (logical-AND) with any pre-existing selection vector. A row
    /// that the predicate would keep but that an upstream mask already dropped
    /// must stay dropped — this guards the `enumerate_physical` + `and_mask`
    /// interaction that is the whole point of the zero-copy filter.
    #[tokio::test]
    async fn filter_fn_composes_with_existing_mask() {
        let mut ctx = test_ctx();

        // Rows 0..5, then an upstream mask keeping only the even ids (0, 2, 4).
        let upstream =
            make_rows(5, true).with_mask(BooleanArray::from(vec![true, false, true, false, true]));
        assert_eq!(upstream.len(), 3);
        assert_eq!(upstream.physical_len(), 5);

        // Predicate keeps id >= 2. Physically that is {2, 3, 4}, but id == 3 was
        // already masked out upstream and must NOT reappear.
        let mut op = FilterFnOp::<_, Row>::new(|v: &RowView| v.id >= 2);
        let result = op.process(upstream, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(ids(&buf), vec![2, 4]);
    }

    // ---- FilterOp (DataFusion PhysicalExpr) -----------------------------

    fn keep_column_expr() -> Arc<dyn datafusion_physical_expr::PhysicalExpr> {
        datafusion_physical_expr::expressions::col("keep", &Row::arrow_schema()).unwrap()
    }

    #[tokio::test]
    async fn filter_expr_selects_on_bool_column() {
        let mut ctx = test_ctx();

        // Mixed keep flags: ids 0 and 2 kept, id 1 dropped.
        let mut builder = Row::builder(3);
        builder.append(Row { id: 0, keep: true });
        builder.append(Row { id: 1, keep: false });
        builder.append(Row { id: 2, keep: true });
        let input = RheiBuffer::from_builder(builder);

        let mut op = FilterOp::<Row>::new(keep_column_expr());
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(ids(&buf), vec![0, 2]);
    }

    #[tokio::test]
    async fn filter_expr_all_false_returns_none() {
        let mut ctx = test_ctx();
        let mut op = FilterOp::<Row>::new(keep_column_expr());

        let result = op.process(make_rows(4, false), &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn filter_expr_empty_input_returns_none() {
        let mut ctx = test_ctx();
        let mut op = FilterOp::<Row>::new(keep_column_expr());

        let input: RheiBuffer<Row> = RheiBuffer::from_builder(Row::builder(0));
        let result = op.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }
}

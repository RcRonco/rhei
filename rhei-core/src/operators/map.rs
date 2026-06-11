//! Batch map and flat-map operators.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};

/// Transforms each input row (via View) into exactly one output row.
///
/// The closure receives a zero-copy view of each input row and returns an
/// owned output value that is appended to the output builder.
pub struct MapOp<F, I, O> {
    f: F,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<F, I, O> fmt::Debug for MapOp<F, I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapOp").finish_non_exhaustive()
    }
}

impl<F, I, O> MapOp<F, I, O>
where
    I: RheiSchema,
    O: RheiSchema,
    F: for<'a> Fn(I::View<'a>) -> O,
{
    /// Creates a new batch map operator with the given transformation.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, I, O> StreamFunction for MapOp<F, I, O>
where
    I: RheiSchema,
    O: RheiSchema,
    F: for<'a> Fn(I::View<'a>) -> O + Send + Sync,
{
    type Input = I;
    type Output = O;

    async fn process(
        &mut self,
        input: RheiBuffer<I>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        let len = input.len();
        if len == 0 {
            return Ok(BufferOutput::None);
        }

        let mut builder = O::builder(len);
        for view in &input {
            builder.append((self.f)(view));
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

/// Transforms each input row (via View) into zero or more output rows.
pub struct FlatMapOp<F, I, O> {
    f: F,
    _phantom: PhantomData<fn(I) -> Vec<O>>,
}

impl<F, I, O> fmt::Debug for FlatMapOp<F, I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlatMapOp").finish_non_exhaustive()
    }
}

impl<F, I, O> FlatMapOp<F, I, O>
where
    I: RheiSchema,
    O: RheiSchema,
    F: for<'a> Fn(I::View<'a>) -> Vec<O>,
{
    /// Creates a new batch flat-map operator with the given transformation.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, I, O> StreamFunction for FlatMapOp<F, I, O>
where
    I: RheiSchema,
    O: RheiSchema,
    F: for<'a> Fn(I::View<'a>) -> Vec<O> + Send + Sync,
{
    type Input = I;
    type Output = O;

    async fn process(
        &mut self,
        input: RheiBuffer<I>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        let len = input.len();
        if len == 0 {
            return Ok(BufferOutput::None);
        }

        let mut builder = O::builder(len * 2);
        for view in &input {
            for item in (self.f)(view) {
                builder.append(item);
            }
        }

        if builder.is_empty() {
            return Ok(BufferOutput::None);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::builder::ArrayBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::arrow::RheiSchema as RheiSchemaTrait;
    use crate::arrow::{BufferOutput, OperatorContext, RheiBuffer, RheiBuilder};
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    // ---- Input schema: a single i64 `n` column --------------------------

    struct In {
        n: i64,
    }

    struct InBuilder {
        n: arrow_array::builder::Int64Builder,
    }

    struct InView {
        n: i64,
    }

    struct InCols<'a> {
        #[allow(dead_code)]
        n: &'a Int64Array,
    }

    impl RheiBuilder for InBuilder {
        type Item = In;

        fn append(&mut self, item: In) {
            self.n.append_value(item.n);
        }

        fn append_null(&mut self) {
            self.n.append_null();
        }

        fn len(&self) -> usize {
            self.n.len()
        }

        fn finish(mut self) -> RecordBatch {
            RecordBatch::try_new(In::arrow_schema(), vec![Arc::new(self.n.finish())]).unwrap()
        }
    }

    impl RheiSchemaTrait for In {
        type Builder = InBuilder;
        type View<'a> = InView;
        type Columns<'a> = InCols<'a>;

        fn arrow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            InBuilder {
                n: arrow_array::builder::Int64Builder::with_capacity(capacity),
            }
        }

        fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
            InView {
                n: batch.column(0).as_primitive::<Int64Type>().value(index),
            }
        }

        fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
            InCols {
                n: batch.column(0).as_primitive::<Int64Type>(),
            }
        }
    }

    // ---- Output schema: a `label` string + doubled `n` value ------------

    struct Out {
        label: String,
        value: i64,
    }

    struct OutBuilder {
        label: arrow_array::builder::StringBuilder,
        value: arrow_array::builder::Int64Builder,
    }

    struct OutView<'a> {
        label: &'a str,
        value: i64,
    }

    struct OutCols<'a> {
        #[allow(dead_code)]
        label: &'a StringArray,
        #[allow(dead_code)]
        value: &'a Int64Array,
    }

    impl RheiBuilder for OutBuilder {
        type Item = Out;

        fn append(&mut self, item: Out) {
            self.label.append_value(&item.label);
            self.value.append_value(item.value);
        }

        fn append_null(&mut self) {
            self.label.append_null();
            self.value.append_null();
        }

        fn len(&self) -> usize {
            self.value.len()
        }

        fn finish(mut self) -> RecordBatch {
            RecordBatch::try_new(
                Out::arrow_schema(),
                vec![Arc::new(self.label.finish()), Arc::new(self.value.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for Out {
        type Builder = OutBuilder;
        type View<'a> = OutView<'a>;
        type Columns<'a> = OutCols<'a>;

        fn arrow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("label", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            OutBuilder {
                label: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                value: arrow_array::builder::Int64Builder::with_capacity(capacity),
            }
        }

        fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
            OutView {
                label: batch.column(0).as_string::<i32>().value(index),
                value: batch.column(1).as_primitive::<Int64Type>().value(index),
            }
        }

        fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
            OutCols {
                label: batch.column(0).as_string::<i32>(),
                value: batch.column(1).as_primitive::<Int64Type>(),
            }
        }
    }

    fn test_ctx() -> OperatorContext {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().join("state.json"), None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    fn make_input(values: &[i64]) -> RheiBuffer<In> {
        let mut builder = In::builder(values.len());
        for &n in values {
            builder.append(In { n });
        }
        RheiBuffer::from_builder(builder)
    }

    // ---- MapOp ----------------------------------------------------------

    #[tokio::test]
    async fn map_transforms_each_row_one_to_one() {
        let mut ctx = test_ctx();
        let mut op = MapOp::<_, In, Out>::new(|v: InView| Out {
            label: format!("n{}", v.n),
            value: v.n * 2,
        });

        let result = op.process(make_input(&[1, 2, 3]), &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 3);
        let labels: Vec<&str> = buf.iter().map(|v| v.label).collect();
        let values: Vec<i64> = buf.iter().map(|v| v.value).collect();
        assert_eq!(labels, vec!["n1", "n2", "n3"]);
        assert_eq!(values, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn map_empty_input_returns_none() {
        let mut ctx = test_ctx();
        let mut op = MapOp::<_, In, Out>::new(|v: InView| Out {
            label: String::new(),
            value: v.n,
        });

        let result = op.process(make_input(&[]), &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    /// `MapOp` should only see rows that survive an upstream selection vector,
    /// and must emit exactly one output row per surviving input row.
    #[tokio::test]
    async fn map_respects_upstream_mask() {
        use arrow_array::BooleanArray;

        let mut ctx = test_ctx();
        let input = make_input(&[10, 20, 30, 40])
            .with_mask(BooleanArray::from(vec![true, false, true, false]));
        assert_eq!(input.len(), 2);

        let mut op = MapOp::<_, In, Out>::new(|v: InView| Out {
            label: String::new(),
            value: v.n,
        });
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        let values: Vec<i64> = buf.iter().map(|v| v.value).collect();
        assert_eq!(values, vec![10, 30]);
    }

    // ---- FlatMapOp ------------------------------------------------------

    #[tokio::test]
    async fn flat_map_expands_rows() {
        let mut ctx = test_ctx();
        // Emit `n` copies of each row, labelled by index.
        let mut op = FlatMapOp::<_, In, Out>::new(|v: InView| {
            (0..v.n)
                .map(|i| Out {
                    label: format!("{}-{}", v.n, i),
                    value: v.n,
                })
                .collect()
        });

        let result = op.process(make_input(&[1, 3]), &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        // 1 row -> 1 output, 3 row -> 3 outputs => 4 total.
        assert_eq!(buf.len(), 4);
        let labels: Vec<&str> = buf.iter().map(|v| v.label).collect();
        assert_eq!(labels, vec!["1-0", "3-0", "3-1", "3-2"]);
    }

    #[tokio::test]
    async fn flat_map_all_empty_returns_none() {
        let mut ctx = test_ctx();
        // Closure always returns no rows -> the builder stays empty -> None.
        let mut op = FlatMapOp::<_, In, Out>::new(|_v: InView| -> Vec<Out> { Vec::new() });

        let result = op.process(make_input(&[1, 2, 3]), &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn flat_map_empty_input_returns_none() {
        let mut ctx = test_ctx();
        let mut op = FlatMapOp::<_, In, Out>::new(|v: InView| {
            vec![Out {
                label: String::new(),
                value: v.n,
            }]
        });

        let result = op.process(make_input(&[]), &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }
}

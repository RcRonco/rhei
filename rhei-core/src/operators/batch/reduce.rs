//! Batch reduce and rolling aggregate operators.
//!
//! - `BatchReduceOp`: per-key stateful reduce that emits the updated value on every input.
//! - `BatchRollingAggregateOp`: per-key rolling aggregate using an accumulator.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::keyed_state::KeyedState;
use crate::arrow::{
    BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema,
};

/// Batch per-key reduce operator.
///
/// For each input row, loads the current value for the key, applies
/// `reduce_fn(existing, view) -> O`, stores and emits the result.
/// First element for a new key uses `init_fn(view)`.
pub struct BatchReduceOp<I, O, KF, IF, RF> {
    key_fn: KF,
    init_fn: IF,
    reduce_fn: RF,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<I, O, KF: Clone, IF: Clone, RF: Clone> Clone for BatchReduceOp<I, O, KF, IF, RF> {
    fn clone(&self) -> Self {
        Self {
            key_fn: self.key_fn.clone(),
            init_fn: self.init_fn.clone(),
            reduce_fn: self.reduce_fn.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, KF, IF, RF> fmt::Debug for BatchReduceOp<I, O, KF, IF, RF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchReduceOp").finish_non_exhaustive()
    }
}

impl<I, O, KF, IF, RF> BatchReduceOp<I, O, KF, IF, RF> {
    /// Creates a new per-key reduce operator.
    pub fn new(key_fn: KF, init_fn: IF, reduce_fn: RF) -> Self {
        Self {
            key_fn,
            init_fn,
            reduce_fn,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<I, O, KF, IF, RF> BatchStreamFunction for BatchReduceOp<I, O, KF, IF, RF>
where
    I: RheiSchema,
    O: RheiSchema + Serialize + DeserializeOwned,
    KF: for<'a> Fn(I::View<'a>) -> String + Send + Sync,
    IF: for<'a> Fn(I::View<'a>) -> O + Send + Sync,
    RF: for<'a> Fn(O, I::View<'a>) -> O + Send + Sync,
{
    type Input = I;
    type Output = O;

    async fn process(
        &mut self,
        input: RheiBuffer<I>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }

        let row_data: Vec<(String, usize)> = input
            .iter()
            .enumerate_physical()
            .map(|(phys_idx, _)| {
                let view = I::view(input.as_record_batch(), phys_idx);
                let key = (self.key_fn)(view);
                (key, phys_idx)
            })
            .collect();

        let batch = input.as_record_batch();
        let mut outputs: Vec<O> = Vec::new();

        for (key, phys_idx) in &row_data {
            let existing: Option<O> = {
                let mut state = KeyedState::<String, O>::new(&mut ctx.state, "red");
                state.get(key).await.unwrap_or(None)
            };

            let view = I::view(batch, *phys_idx);
            let new_val = match existing {
                Some(prev) => (self.reduce_fn)(prev, view),
                None => (self.init_fn)(view),
            };

            {
                let mut state = KeyedState::<String, O>::new(&mut ctx.state, "red");
                state.put(key, &new_val)?;
            }

            outputs.push(new_val);
        }

        if outputs.is_empty() {
            return Ok(BufferOutput::None);
        }

        let mut builder = O::builder(outputs.len());
        for item in outputs {
            builder.append(item);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

/// Batch per-key rolling aggregate operator.
///
/// For each input row, loads the accumulator for the key, accumulates,
/// stores, and emits `finish_fn(key, &acc)`.
pub struct BatchRollingAggregateOp<I, O, Acc, KF, AF, FF> {
    key_fn: KF,
    accumulate_fn: AF,
    finish_fn: FF,
    _phantom: PhantomData<fn(I, Acc) -> O>,
}

impl<I, O, Acc, KF: Clone, AF: Clone, FF: Clone> Clone
    for BatchRollingAggregateOp<I, O, Acc, KF, AF, FF>
{
    fn clone(&self) -> Self {
        Self {
            key_fn: self.key_fn.clone(),
            accumulate_fn: self.accumulate_fn.clone(),
            finish_fn: self.finish_fn.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, Acc, KF, AF, FF> fmt::Debug for BatchRollingAggregateOp<I, O, Acc, KF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchRollingAggregateOp")
            .finish_non_exhaustive()
    }
}

impl<I, O, Acc, KF, AF, FF> BatchRollingAggregateOp<I, O, Acc, KF, AF, FF> {
    /// Creates a new per-key rolling aggregate operator.
    pub fn new(key_fn: KF, accumulate_fn: AF, finish_fn: FF) -> Self {
        Self {
            key_fn,
            accumulate_fn,
            finish_fn,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<I, O, Acc, KF, AF, FF> BatchStreamFunction for BatchRollingAggregateOp<I, O, Acc, KF, AF, FF>
where
    I: RheiSchema,
    O: RheiSchema,
    Acc: Serialize + DeserializeOwned + Default + Send + Sync,
    KF: for<'a> Fn(I::View<'a>) -> String + Send + Sync,
    AF: for<'a> Fn(&mut Acc, I::View<'a>) + Send + Sync,
    FF: Fn(&str, &Acc) -> O + Send + Sync,
{
    type Input = I;
    type Output = O;

    async fn process(
        &mut self,
        input: RheiBuffer<I>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }

        let row_data: Vec<(String, usize)> = input
            .iter()
            .enumerate_physical()
            .map(|(phys_idx, _)| {
                let view = I::view(input.as_record_batch(), phys_idx);
                let key = (self.key_fn)(view);
                (key, phys_idx)
            })
            .collect();

        let batch = input.as_record_batch();
        let mut outputs: Vec<O> = Vec::new();

        for (key, phys_idx) in &row_data {
            let mut acc: Acc = {
                let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "ragg");
                state.get(key).await.unwrap_or(None).unwrap_or_default()
            };

            let view = I::view(batch, *phys_idx);
            (self.accumulate_fn)(&mut acc, view);

            {
                let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "ragg");
                state.put(key, &acc)?;
            }

            outputs.push((self.finish_fn)(key, &acc));
        }

        if outputs.is_empty() {
            return Ok(BufferOutput::None);
        }

        let mut builder = O::builder(outputs.len());
        for item in outputs {
            builder.append(item);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::arrow::RheiSchema as RheiSchemaTrait;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    use arrow_array::builder::ArrayBuilder;

    // Reuse simple schema for tests
    struct Val {
        key: String,
        n: i64,
    }
    struct ValBuilder {
        key: arrow_array::builder::StringBuilder,
        n: arrow_array::builder::Int64Builder,
    }
    struct ValView<'a> {
        key: &'a str,
        n: i64,
    }
    struct ValCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for ValBuilder {
        type Item = Val;
        fn append(&mut self, item: Val) {
            self.key.append_value(&item.key);
            self.n.append_value(item.n);
        }
        fn append_null(&mut self) {
            self.key.append_null();
            self.n.append_null();
        }
        fn len(&self) -> usize {
            self.key.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                Val::arrow_schema(),
                vec![Arc::new(self.key.finish()), Arc::new(self.n.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for Val {
        type Builder = ValBuilder;
        type View<'a> = ValView<'a>;
        type Columns<'a> = ValCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("n", arrow_schema::DataType::Int64, false),
            ]))
        }
        fn builder(capacity: usize) -> Self::Builder {
            ValBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                n: arrow_array::builder::Int64Builder::with_capacity(capacity),
            }
        }
        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            ValView {
                key: batch.column(0).as_string::<i32>().value(index),
                n: batch.column(1).as_primitive::<Int64Type>().value(index),
            }
        }
        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            ValCols {
                key: batch.column(0).as_string::<i32>(),
            }
        }
    }

    // Output: sum per key
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SumOut {
        key: String,
        sum: i64,
    }
    struct SumOutBuilder {
        key: arrow_array::builder::StringBuilder,
        sum: arrow_array::builder::Int64Builder,
    }
    #[allow(dead_code)]
    struct SumOutView<'a> {
        key: &'a str,
        sum: i64,
    }
    struct SumOutCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for SumOutBuilder {
        type Item = SumOut;
        fn append(&mut self, item: SumOut) {
            self.key.append_value(&item.key);
            self.sum.append_value(item.sum);
        }
        fn append_null(&mut self) {
            self.key.append_null();
            self.sum.append_null();
        }
        fn len(&self) -> usize {
            self.key.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                SumOut::arrow_schema(),
                vec![Arc::new(self.key.finish()), Arc::new(self.sum.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for SumOut {
        type Builder = SumOutBuilder;
        type View<'a> = SumOutView<'a>;
        type Columns<'a> = SumOutCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("sum", arrow_schema::DataType::Int64, false),
            ]))
        }
        fn builder(capacity: usize) -> Self::Builder {
            SumOutBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                sum: arrow_array::builder::Int64Builder::with_capacity(capacity),
            }
        }
        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            SumOutView {
                key: batch.column(0).as_string::<i32>().value(index),
                sum: batch.column(1).as_primitive::<Int64Type>().value(index),
            }
        }
        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            SumOutCols {
                key: batch.column(0).as_string::<i32>(),
            }
        }
    }

    fn test_ctx(name: &str) -> OperatorContext {
        let path =
            std::env::temp_dir().join(format!("rhei_batch_red_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    fn make_vals(events: &[(&str, i64)]) -> RheiBuffer<Val> {
        let mut builder = Val::builder(events.len());
        for &(key, n) in events {
            builder.append(Val {
                key: key.to_string(),
                n,
            });
        }
        RheiBuffer::from_builder(builder)
    }

    #[tokio::test]
    async fn rolling_aggregate_accumulates() {
        let mut ctx = test_ctx("ragg");
        let mut op = BatchRollingAggregateOp::<Val, SumOut, i64, _, _, _>::new(
            |v: ValView<'_>| v.key.to_string(),
            |acc: &mut i64, v: ValView<'_>| *acc += v.n,
            |key: &str, acc: &i64| SumOut {
                key: key.to_string(),
                sum: *acc,
            },
        );

        let input = make_vals(&[("a", 1), ("a", 2), ("b", 10)]);
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        // Emits after every row: a=1, a=3, b=10
        assert_eq!(buf.len(), 3);
        let v0 = SumOut::view(buf.as_record_batch(), 0);
        let v1 = SumOut::view(buf.as_record_batch(), 1);
        let v2 = SumOut::view(buf.as_record_batch(), 2);
        assert_eq!(v0.sum, 1);
        assert_eq!(v1.sum, 3);
        assert_eq!(v2.sum, 10);
    }

    #[tokio::test]
    async fn reduce_emits_every_row() {
        let mut ctx = test_ctx("reduce");
        let mut op = BatchReduceOp::<Val, SumOut, _, _, _>::new(
            |v: ValView<'_>| v.key.to_string(),
            |v: ValView<'_>| SumOut {
                key: v.key.to_string(),
                sum: v.n,
            },
            |prev: SumOut, v: ValView<'_>| SumOut {
                key: v.key.to_string(),
                sum: prev.sum + v.n,
            },
        );

        let input = make_vals(&[("a", 5), ("a", 3)]);
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 2);
        let v0 = SumOut::view(buf.as_record_batch(), 0);
        let v1 = SumOut::view(buf.as_record_batch(), 1);
        assert_eq!(v0.sum, 5);
        assert_eq!(v1.sum, 8);
    }
}

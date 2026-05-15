//! Batch temporal join operator.
//!
//! Matches events from two sides (Left/Right) by key. When both sides for a
//! key have arrived, the join function is called and the result is emitted.
//! Unmatched events are buffered in state until their counterpart arrives.
//! An optional timeout evicts stale buffered events on watermark advance.

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::keyed_state::KeyedState;
use crate::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};

/// An input event tagged with its side of the join.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JoinSide<L, R> {
    /// Left side of the join.
    Left(L),
    /// Right side of the join.
    Right(R),
}

/// Timestamped wrapper for buffered join events.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Stamped<V> {
    value: V,
    watermark_at_buffer: u64,
}

/// Batch temporal join operator.
///
/// Input type must be `JoinSide<L, R>` where both L and R implement
/// `RheiSchema`. The input buffer schema is that of the tagged enum (using
/// a discriminant column + union of both schemas).
///
/// In practice, this operator works with a single input schema that includes
/// a `side` column and the superset of L/R fields. For simplicity, we use
/// a closure-based approach where `SideF` extracts which side an input row
/// belongs to, and separate closures extract L/R data.
pub struct TemporalJoin<I, O, KF, SF, LF, RF, JF> {
    key_fn: KF,
    side_fn: SF,
    left_fn: LF,
    right_fn: RF,
    join_fn: JF,
    timeout: Option<u64>,
    last_watermark: u64,
    active_keys: HashSet<String>,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<I, O, KF: Clone, SF: Clone, LF: Clone, RF: Clone, JF: Clone> Clone
    for TemporalJoin<I, O, KF, SF, LF, RF, JF>
{
    fn clone(&self) -> Self {
        Self {
            key_fn: self.key_fn.clone(),
            side_fn: self.side_fn.clone(),
            left_fn: self.left_fn.clone(),
            right_fn: self.right_fn.clone(),
            join_fn: self.join_fn.clone(),
            timeout: self.timeout,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, KF, SF, LF, RF, JF> fmt::Debug for TemporalJoin<I, O, KF, SF, LF, RF, JF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TemporalJoin")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// Which side an input row belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// Left side of the join.
    Left,
    /// Right side of the join.
    Right,
}

impl<I, O, KF, SF, LF, RF, JF> TemporalJoin<I, O, KF, SF, LF, RF, JF> {
    /// Creates a new temporal join operator.
    pub fn new(key_fn: KF, side_fn: SF, left_fn: LF, right_fn: RF, join_fn: JF) -> Self {
        Self {
            key_fn,
            side_fn,
            left_fn,
            right_fn,
            join_fn,
            timeout: None,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }

    /// Sets a timeout for evicting unmatched buffered events on watermark advance.
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Left-side value stored in state.
type LeftStamped<L> = Stamped<L>;
/// Right-side value stored in state.
type RightStamped<R> = Stamped<R>;

#[async_trait]
impl<I, O, L, R, KF, SF, LF, RF, JF> StreamFunction for TemporalJoin<I, O, KF, SF, LF, RF, JF>
where
    I: RheiSchema,
    O: RheiSchema,
    L: Serialize + DeserializeOwned + Send + Sync,
    R: Serialize + DeserializeOwned + Send + Sync,
    KF: for<'a> Fn(I::View<'a>) -> String + Send + Sync,
    SF: for<'a> Fn(I::View<'a>) -> Side + Send + Sync,
    LF: for<'a> Fn(I::View<'a>) -> L + Send + Sync,
    RF: for<'a> Fn(I::View<'a>) -> R + Send + Sync,
    JF: Fn(&str, &L, &R) -> O + Send + Sync,
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

        let row_data: Vec<(String, Side, usize)> = input
            .iter()
            .enumerate_physical()
            .map(|(phys_idx, _)| {
                let view_k = I::view(input.as_record_batch(), phys_idx);
                let key = (self.key_fn)(view_k);
                let view_s = I::view(input.as_record_batch(), phys_idx);
                let side = (self.side_fn)(view_s);
                (key, side, phys_idx)
            })
            .collect();

        let batch = input.as_record_batch();
        let mut outputs: Vec<O> = Vec::new();

        for (key, side, phys_idx) in &row_data {
            match side {
                Side::Left => {
                    let view = I::view(batch, *phys_idx);
                    let left_val = (self.left_fn)(view);

                    let right: Option<RightStamped<R>> = {
                        let mut state =
                            KeyedState::<String, RightStamped<R>>::new(&mut ctx.state, "tj_r");
                        state.get(key).await.unwrap_or(None)
                    };

                    if let Some(r) = right {
                        outputs.push((self.join_fn)(key, &left_val, &r.value));
                        let mut state =
                            KeyedState::<String, RightStamped<R>>::new(&mut ctx.state, "tj_r");
                        state.delete(key)?;
                        self.active_keys.remove(key);
                    } else {
                        let mut state =
                            KeyedState::<String, LeftStamped<L>>::new(&mut ctx.state, "tj_l");
                        state.put(
                            key,
                            &Stamped {
                                value: left_val,
                                watermark_at_buffer: self.last_watermark,
                            },
                        )?;
                        self.active_keys.insert(key.clone());
                    }
                }
                Side::Right => {
                    let view = I::view(batch, *phys_idx);
                    let right_val = (self.right_fn)(view);

                    let left: Option<LeftStamped<L>> = {
                        let mut state =
                            KeyedState::<String, LeftStamped<L>>::new(&mut ctx.state, "tj_l");
                        state.get(key).await.unwrap_or(None)
                    };

                    if let Some(l) = left {
                        outputs.push((self.join_fn)(key, &l.value, &right_val));
                        let mut state =
                            KeyedState::<String, LeftStamped<L>>::new(&mut ctx.state, "tj_l");
                        state.delete(key)?;
                        self.active_keys.remove(key);
                    } else {
                        let mut state =
                            KeyedState::<String, RightStamped<R>>::new(&mut ctx.state, "tj_r");
                        state.put(
                            key,
                            &Stamped {
                                value: right_val,
                                watermark_at_buffer: self.last_watermark,
                            },
                        )?;
                        self.active_keys.insert(key.clone());
                    }
                }
            }
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

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        self.last_watermark = watermark;
        let Some(timeout) = self.timeout else {
            return Ok(BufferOutput::None);
        };

        let mut evicted_keys: Vec<String> = Vec::new();

        for key in &self.active_keys {
            let mut did_evict = false;

            {
                let mut state = KeyedState::<String, LeftStamped<L>>::new(&mut ctx.state, "tj_l");
                if let Some(stamped) = state.get(key).await.unwrap_or(None)
                    && watermark >= stamped.watermark_at_buffer + timeout
                {
                    state.delete(key)?;
                    did_evict = true;
                }
            }

            {
                let mut state = KeyedState::<String, RightStamped<R>>::new(&mut ctx.state, "tj_r");
                if let Some(stamped) = state.get(key).await.unwrap_or(None)
                    && watermark >= stamped.watermark_at_buffer + timeout
                {
                    state.delete(key)?;
                    did_evict = true;
                }
            }

            if did_evict {
                evicted_keys.push(key.clone());
            }
        }

        for key in &evicted_keys {
            self.active_keys.remove(key);
        }

        Ok(BufferOutput::None)
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

    // Input: side(0=left,1=right), key, value
    struct JoinInput {
        side: i64,
        key: String,
        value: String,
    }
    struct JoinInputBuilder {
        side: arrow_array::builder::Int64Builder,
        key: arrow_array::builder::StringBuilder,
        value: arrow_array::builder::StringBuilder,
    }
    struct JoinInputView<'a> {
        side: i64,
        key: &'a str,
        value: &'a str,
    }
    struct JoinInputCols<'a> {
        #[allow(dead_code)]
        side: &'a arrow_array::Int64Array,
    }

    impl crate::arrow::RheiBuilder for JoinInputBuilder {
        type Item = JoinInput;
        fn append(&mut self, item: JoinInput) {
            self.side.append_value(item.side);
            self.key.append_value(&item.key);
            self.value.append_value(&item.value);
        }
        fn append_null(&mut self) {
            self.side.append_null();
            self.key.append_null();
            self.value.append_null();
        }
        fn len(&self) -> usize {
            self.side.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                JoinInput::arrow_schema(),
                vec![
                    Arc::new(self.side.finish()),
                    Arc::new(self.key.finish()),
                    Arc::new(self.value.finish()),
                ],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for JoinInput {
        type Builder = JoinInputBuilder;
        type View<'a> = JoinInputView<'a>;
        type Columns<'a> = JoinInputCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("side", arrow_schema::DataType::Int64, false),
                arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("value", arrow_schema::DataType::Utf8, false),
            ]))
        }
        fn builder(capacity: usize) -> Self::Builder {
            JoinInputBuilder {
                side: arrow_array::builder::Int64Builder::with_capacity(capacity),
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                value: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            }
        }
        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            JoinInputView {
                side: batch.column(0).as_primitive::<Int64Type>().value(index),
                key: batch.column(1).as_string::<i32>().value(index),
                value: batch.column(2).as_string::<i32>().value(index),
            }
        }
        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            JoinInputCols {
                side: batch.column(0).as_primitive::<Int64Type>(),
            }
        }
    }

    // Output: key + joined value
    struct JoinOutput {
        key: String,
        joined: String,
    }
    struct JoinOutputBuilder {
        key: arrow_array::builder::StringBuilder,
        joined: arrow_array::builder::StringBuilder,
    }
    #[allow(dead_code)]
    struct JoinOutputView<'a> {
        key: &'a str,
        joined: &'a str,
    }
    struct JoinOutputCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for JoinOutputBuilder {
        type Item = JoinOutput;
        fn append(&mut self, item: JoinOutput) {
            self.key.append_value(&item.key);
            self.joined.append_value(&item.joined);
        }
        fn append_null(&mut self) {
            self.key.append_null();
            self.joined.append_null();
        }
        fn len(&self) -> usize {
            self.key.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                JoinOutput::arrow_schema(),
                vec![Arc::new(self.key.finish()), Arc::new(self.joined.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for JoinOutput {
        type Builder = JoinOutputBuilder;
        type View<'a> = JoinOutputView<'a>;
        type Columns<'a> = JoinOutputCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("joined", arrow_schema::DataType::Utf8, false),
            ]))
        }
        fn builder(capacity: usize) -> Self::Builder {
            JoinOutputBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                joined: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 32),
            }
        }
        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            JoinOutputView {
                key: batch.column(0).as_string::<i32>().value(index),
                joined: batch.column(1).as_string::<i32>().value(index),
            }
        }
        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            JoinOutputCols {
                key: batch.column(0).as_string::<i32>(),
            }
        }
    }

    fn test_ctx(name: &str) -> OperatorContext {
        let path =
            std::env::temp_dir().join(format!("rhei_batch_tj_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    #[allow(clippy::type_complexity)]
    fn make_join() -> TemporalJoin<
        JoinInput,
        JoinOutput,
        impl for<'a> Fn(<JoinInput as RheiSchemaTrait>::View<'a>) -> String + Send + Sync,
        impl for<'a> Fn(<JoinInput as RheiSchemaTrait>::View<'a>) -> Side + Send + Sync,
        impl for<'a> Fn(<JoinInput as RheiSchemaTrait>::View<'a>) -> String + Send + Sync,
        impl for<'a> Fn(<JoinInput as RheiSchemaTrait>::View<'a>) -> String + Send + Sync,
        impl Fn(&str, &String, &String) -> JoinOutput + Send + Sync,
    > {
        TemporalJoin::new(
            |v: JoinInputView<'_>| v.key.to_string(),
            |v: JoinInputView<'_>| {
                if v.side == 0 { Side::Left } else { Side::Right }
            },
            |v: JoinInputView<'_>| v.value.to_string(),
            |v: JoinInputView<'_>| v.value.to_string(),
            |key: &str, l: &String, r: &String| JoinOutput {
                key: key.to_string(),
                joined: format!("{l}+{r}"),
            },
        )
    }

    fn make_input(rows: &[(i64, &str, &str)]) -> RheiBuffer<JoinInput> {
        let mut builder = JoinInput::builder(rows.len());
        for &(side, key, value) in rows {
            builder.append(JoinInput {
                side,
                key: key.to_string(),
                value: value.to_string(),
            });
        }
        RheiBuffer::from_builder(builder)
    }

    #[tokio::test]
    async fn left_then_right_matches() {
        let mut ctx = test_ctx("lr_match");
        let mut join = make_join();

        let input = make_input(&[(0, "k1", "left_val")]);
        let result = join.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        let input = make_input(&[(1, "k1", "right_val")]);
        let result = join.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = JoinOutput::view(buf.as_record_batch(), 0);
        assert_eq!(v.key, "k1");
        assert_eq!(v.joined, "left_val+right_val");
    }

    #[tokio::test]
    async fn right_then_left_matches() {
        let mut ctx = test_ctx("rl_match");
        let mut join = make_join();

        let input = make_input(&[(1, "k1", "right_val")]);
        let result = join.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        let input = make_input(&[(0, "k1", "left_val")]);
        let result = join.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = JoinOutput::view(buf.as_record_batch(), 0);
        assert_eq!(v.joined, "left_val+right_val");
    }

    #[tokio::test]
    async fn timeout_evicts_buffered() {
        let mut ctx = test_ctx("timeout");
        let mut join = make_join().with_timeout(50);

        let input = make_input(&[(0, "k1", "left_val")]);
        join.process(input, &mut ctx).await.unwrap();

        // Advance watermark past timeout
        join.on_watermark(50, &mut ctx).await.unwrap();

        // Right arrives — no match (left was evicted)
        let input = make_input(&[(1, "k1", "right_val")]);
        let result = join.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn no_eviction_without_timeout() {
        let mut ctx = test_ctx("no_timeout");
        let mut join = make_join();

        let input = make_input(&[(0, "k1", "left_val")]);
        join.process(input, &mut ctx).await.unwrap();

        join.on_watermark(1_000_000, &mut ctx).await.unwrap();

        let input = make_input(&[(1, "k1", "right_val")]);
        let result = join.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
    }
}

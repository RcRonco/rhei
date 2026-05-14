//! Batch count-based window operator.
//!
//! Emits after exactly N elements per key, then resets the accumulator.
//! No watermark dependency — purely count-driven.

use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::keyed_state::KeyedState;
use crate::arrow::{
    BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema,
};

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

/// Batch count-based window operator.
///
/// # Type Parameters
///
/// - `I` — input schema
/// - `O` — output schema
/// - `Acc` — accumulator
/// - `KF` — key extraction
/// - `AF` — accumulate function
/// - `FF` — finish function: `Fn(&str, u64, &Acc) -> O` (key, count, acc)
pub struct BatchCountWindow<I, O, Acc, KF, AF, FF> {
    threshold: u64,
    key_fn: KF,
    accumulate_fn: AF,
    finish_fn: FF,
    _phantom: PhantomData<fn(I, Acc) -> O>,
}

impl<I, O, Acc, KF: Clone, AF: Clone, FF: Clone> Clone for BatchCountWindow<I, O, Acc, KF, AF, FF> {
    fn clone(&self) -> Self {
        Self {
            threshold: self.threshold,
            key_fn: self.key_fn.clone(),
            accumulate_fn: self.accumulate_fn.clone(),
            finish_fn: self.finish_fn.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, Acc, KF, AF, FF> fmt::Debug for BatchCountWindow<I, O, Acc, KF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchCountWindow")
            .field("threshold", &self.threshold)
            .finish_non_exhaustive()
    }
}

impl<I, O, Acc, KF, AF, FF> BatchCountWindow<I, O, Acc, KF, AF, FF> {
    /// Creates a new count window that fires after `threshold` elements per key.
    pub fn new(threshold: u64, key_fn: KF, accumulate_fn: AF, finish_fn: FF) -> Self {
        assert!(threshold > 0, "threshold must be > 0");
        Self {
            threshold,
            key_fn,
            accumulate_fn,
            finish_fn,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<I, O, Acc, KF, AF, FF> BatchStreamFunction for BatchCountWindow<I, O, Acc, KF, AF, FF>
where
    I: RheiSchema,
    O: RheiSchema,
    Acc: Serialize + DeserializeOwned + Default + Send + Sync,
    KF: for<'a> Fn(I::View<'a>) -> String + Send + Sync,
    AF: for<'a> Fn(&mut Acc, I::View<'a>) + Send + Sync,
    FF: Fn(&str, u64, &Acc) -> O + Send + Sync,
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
            let mut cs: CountState<Acc> = {
                let mut state = KeyedState::<String, CountState<Acc>>::new(&mut ctx.state, "cw");
                state.get(key).await.unwrap_or(None).unwrap_or_default()
            };

            let view = I::view(batch, *phys_idx);
            (self.accumulate_fn)(&mut cs.accumulator, view);
            cs.count += 1;

            if cs.count >= self.threshold {
                outputs.push((self.finish_fn)(key, cs.count, &cs.accumulator));
                cs = CountState::default();
            }

            let mut state = KeyedState::<String, CountState<Acc>>::new(&mut ctx.state, "cw");
            state.put(key, &cs)?;
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

    struct Event {
        key: String,
        value: i64,
    }
    struct EventBuilder {
        key: arrow_array::builder::StringBuilder,
        value: arrow_array::builder::Int64Builder,
    }
    struct EventView<'a> {
        key: &'a str,
        #[allow(dead_code)]
        value: i64,
    }
    struct EventCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for EventBuilder {
        type Item = Event;
        fn append(&mut self, item: Event) {
            self.key.append_value(&item.key);
            self.value.append_value(item.value);
        }
        fn append_null(&mut self) {
            self.key.append_null();
            self.value.append_null();
        }
        fn len(&self) -> usize {
            self.key.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                Event::arrow_schema(),
                vec![Arc::new(self.key.finish()), Arc::new(self.value.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for Event {
        type Builder = EventBuilder;
        type View<'a> = EventView<'a>;
        type Columns<'a> = EventCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
            ]))
        }
        fn builder(capacity: usize) -> Self::Builder {
            EventBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                value: arrow_array::builder::Int64Builder::with_capacity(capacity),
            }
        }
        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::Int64Type;
            EventView {
                key: batch.column(0).as_string::<i32>().value(index),
                value: batch.column(1).as_primitive::<Int64Type>().value(index),
            }
        }
        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            EventCols {
                key: batch.column(0).as_string::<i32>(),
            }
        }
    }

    struct CountOut {
        key: String,
        count: u64,
    }
    struct CountOutBuilder {
        key: arrow_array::builder::StringBuilder,
        count: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    }
    #[allow(dead_code)]
    struct CountOutView<'a> {
        key: &'a str,
        count: u64,
    }
    struct CountOutCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for CountOutBuilder {
        type Item = CountOut;
        fn append(&mut self, item: CountOut) {
            self.key.append_value(&item.key);
            self.count.append_value(item.count);
        }
        fn append_null(&mut self) {
            self.key.append_null();
            self.count.append_null();
        }
        fn len(&self) -> usize {
            self.key.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                CountOut::arrow_schema(),
                vec![Arc::new(self.key.finish()), Arc::new(self.count.finish())],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for CountOut {
        type Builder = CountOutBuilder;
        type View<'a> = CountOutView<'a>;
        type Columns<'a> = CountOutCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("count", arrow_schema::DataType::UInt64, false),
            ]))
        }
        fn builder(capacity: usize) -> Self::Builder {
            CountOutBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                count: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            }
        }
        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::UInt64Type;
            CountOutView {
                key: batch.column(0).as_string::<i32>().value(index),
                count: batch.column(1).as_primitive::<UInt64Type>().value(index),
            }
        }
        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            CountOutCols {
                key: batch.column(0).as_string::<i32>(),
            }
        }
    }

    fn test_ctx(name: &str) -> OperatorContext {
        let path =
            std::env::temp_dir().join(format!("rhei_batch_cw_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    fn make_events(events: &[(&str, i64)]) -> RheiBuffer<Event> {
        let mut builder = Event::builder(events.len());
        for &(key, value) in events {
            builder.append(Event {
                key: key.to_string(),
                value,
            });
        }
        RheiBuffer::from_builder(builder)
    }

    #[tokio::test]
    async fn fires_at_threshold() {
        let mut ctx = test_ctx("fire");
        let mut win = BatchCountWindow::new(
            3,
            |v: EventView<'_>| v.key.to_string(),
            |acc: &mut u64, _v: EventView<'_>| *acc += 1,
            |key: &str, count: u64, _acc: &u64| CountOut {
                key: key.to_string(),
                count,
            },
        );

        let input = make_events(&[("a", 1), ("a", 2)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        let input = make_events(&[("a", 3)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = CountOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.key, "a");
        assert_eq!(v.count, 3);
    }

    #[tokio::test]
    async fn resets_after_fire() {
        let mut ctx = test_ctx("reset");
        let mut win = BatchCountWindow::new(
            2,
            |v: EventView<'_>| v.key.to_string(),
            |acc: &mut u64, _v: EventView<'_>| *acc += 1,
            |key: &str, count: u64, _acc: &u64| CountOut {
                key: key.to_string(),
                count,
            },
        );

        // 4 events → fires twice
        let input = make_events(&[("a", 1), ("a", 2), ("a", 3), ("a", 4)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 2);
    }

    #[tokio::test]
    async fn independent_keys() {
        let mut ctx = test_ctx("keys");
        let mut win = BatchCountWindow::new(
            2,
            |v: EventView<'_>| v.key.to_string(),
            |acc: &mut u64, _v: EventView<'_>| *acc += 1,
            |key: &str, count: u64, _acc: &u64| CountOut {
                key: key.to_string(),
                count,
            },
        );

        let input = make_events(&[("a", 1), ("b", 1), ("a", 2)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        // Only "a" fired (2 events)
        assert_eq!(buf.len(), 1);
        let v = CountOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.key, "a");
    }
}

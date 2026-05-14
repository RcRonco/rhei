//! Batch session window operator.
//!
//! Groups events into sessions separated by an inactivity gap per key.
//! Sessions close when a new event arrives past the gap or when the watermark
//! advances past `last_event_time + gap + allowed_lateness`.

use std::collections::HashSet;
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
struct SessionState<Acc> {
    session_start: u64,
    last_event_time: u64,
    accumulator: Acc,
}

/// Batch session window operator.
///
/// # Type Parameters
///
/// - `I` — input schema
/// - `O` — output schema
/// - `Acc` — accumulator
/// - `KF` — key extraction
/// - `TF` — timestamp extraction
/// - `AF` — accumulate function
/// - `FF` — finish function: `Fn(&str, u64, u64, &Acc) -> O` (key, `session_start`, `last_event`, acc)
pub struct BatchSessionWindow<I, O, Acc, KF, TF, AF, FF> {
    gap: u64,
    key_fn: KF,
    time_fn: TF,
    accumulate_fn: AF,
    finish_fn: FF,
    allowed_lateness: u64,
    last_watermark: u64,
    active_keys: HashSet<String>,
    _phantom: PhantomData<fn(I, Acc) -> O>,
}

impl<I, O, Acc, KF: Clone, TF: Clone, AF: Clone, FF: Clone> Clone
    for BatchSessionWindow<I, O, Acc, KF, TF, AF, FF>
{
    fn clone(&self) -> Self {
        Self {
            gap: self.gap,
            key_fn: self.key_fn.clone(),
            time_fn: self.time_fn.clone(),
            accumulate_fn: self.accumulate_fn.clone(),
            finish_fn: self.finish_fn.clone(),
            allowed_lateness: self.allowed_lateness,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, Acc, KF, TF, AF, FF> fmt::Debug for BatchSessionWindow<I, O, Acc, KF, TF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchSessionWindow")
            .field("gap", &self.gap)
            .field("allowed_lateness", &self.allowed_lateness)
            .finish_non_exhaustive()
    }
}

impl<I, O, Acc, KF, TF, AF, FF> BatchSessionWindow<I, O, Acc, KF, TF, AF, FF> {
    /// Creates a new session window with the given inactivity gap.
    pub fn new(gap: u64, key_fn: KF, time_fn: TF, accumulate_fn: AF, finish_fn: FF) -> Self {
        assert!(gap > 0, "gap must be > 0");
        Self {
            gap,
            key_fn,
            time_fn,
            accumulate_fn,
            finish_fn,
            allowed_lateness: 0,
            last_watermark: 0,
            active_keys: HashSet::new(),
            _phantom: PhantomData,
        }
    }

    /// Sets the allowed lateness for late events.
    pub fn with_allowed_lateness(mut self, lateness: u64) -> Self {
        self.allowed_lateness = lateness;
        self
    }
}

#[async_trait]
impl<I, O, Acc, KF, TF, AF, FF> BatchStreamFunction
    for BatchSessionWindow<I, O, Acc, KF, TF, AF, FF>
where
    I: RheiSchema,
    O: RheiSchema,
    Acc: Serialize + DeserializeOwned + Default + Send + Sync,
    KF: for<'a> Fn(I::View<'a>) -> String + Send + Sync,
    TF: for<'a> Fn(I::View<'a>) -> u64 + Send + Sync,
    AF: for<'a> Fn(&mut Acc, I::View<'a>) + Send + Sync,
    FF: Fn(&str, u64, u64, &Acc) -> O + Send + Sync,
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

        let row_data: Vec<(String, u64, usize)> = input
            .iter()
            .enumerate_physical()
            .map(|(phys_idx, _)| {
                let view_k = I::view(input.as_record_batch(), phys_idx);
                let key = (self.key_fn)(view_k);
                let view_t = I::view(input.as_record_batch(), phys_idx);
                let ts = (self.time_fn)(view_t);
                (key, ts, phys_idx)
            })
            .collect();

        let batch = input.as_record_batch();
        let mut outputs: Vec<O> = Vec::new();

        for (key, timestamp, phys_idx) in &row_data {
            self.active_keys.insert(key.clone());

            let session: Option<SessionState<Acc>> = {
                let mut state =
                    KeyedState::<String, SessionState<Acc>>::new(&mut ctx.state, "sess");
                state.get(key).await.unwrap_or(None)
            };

            let new_session = match session {
                Some(s) if timestamp.saturating_sub(s.last_event_time) > self.gap => {
                    outputs.push((self.finish_fn)(
                        key,
                        s.session_start,
                        s.last_event_time,
                        &s.accumulator,
                    ));
                    let mut acc = Acc::default();
                    let view = I::view(batch, *phys_idx);
                    (self.accumulate_fn)(&mut acc, view);
                    SessionState {
                        session_start: *timestamp,
                        last_event_time: *timestamp,
                        accumulator: acc,
                    }
                }
                Some(mut s) => {
                    s.last_event_time = *timestamp;
                    let view = I::view(batch, *phys_idx);
                    (self.accumulate_fn)(&mut s.accumulator, view);
                    s
                }
                None => {
                    let mut acc = Acc::default();
                    let view = I::view(batch, *phys_idx);
                    (self.accumulate_fn)(&mut acc, view);
                    SessionState {
                        session_start: *timestamp,
                        last_event_time: *timestamp,
                        accumulator: acc,
                    }
                }
            };

            let mut state = KeyedState::<String, SessionState<Acc>>::new(&mut ctx.state, "sess");
            state.put(key, &new_session)?;
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
        let mut outputs: Vec<O> = Vec::new();
        let mut closed_keys: Vec<String> = Vec::new();

        for key in &self.active_keys {
            let session: Option<SessionState<Acc>> = {
                let mut state =
                    KeyedState::<String, SessionState<Acc>>::new(&mut ctx.state, "sess");
                state.get(key).await.unwrap_or(None)
            };

            if let Some(s) = session
                && s.last_event_time + self.gap + self.allowed_lateness <= watermark
            {
                outputs.push((self.finish_fn)(
                    key,
                    s.session_start,
                    s.last_event_time,
                    &s.accumulator,
                ));
                let mut state =
                    KeyedState::<String, SessionState<Acc>>::new(&mut ctx.state, "sess");
                state.delete(key)?;
                closed_keys.push(key.clone());
            }
        }

        for key in &closed_keys {
            self.active_keys.remove(key);
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
        ts: u64,
    }
    struct EventBuilder {
        key: arrow_array::builder::StringBuilder,
        ts: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    }
    struct EventView<'a> {
        key: &'a str,
        ts: u64,
    }
    struct EventCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for EventBuilder {
        type Item = Event;
        fn append(&mut self, item: Event) {
            self.key.append_value(&item.key);
            self.ts.append_value(item.ts);
        }
        fn append_null(&mut self) {
            self.key.append_null();
            self.ts.append_null();
        }
        fn len(&self) -> usize {
            self.key.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                Event::arrow_schema(),
                vec![Arc::new(self.key.finish()), Arc::new(self.ts.finish())],
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
                arrow_schema::Field::new("ts", arrow_schema::DataType::UInt64, false),
            ]))
        }
        fn builder(capacity: usize) -> Self::Builder {
            EventBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                ts: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            }
        }
        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::UInt64Type;
            EventView {
                key: batch.column(0).as_string::<i32>().value(index),
                ts: batch.column(1).as_primitive::<UInt64Type>().value(index),
            }
        }
        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            EventCols {
                key: batch.column(0).as_string::<i32>(),
            }
        }
    }

    struct SessOut {
        key: String,
        start: u64,
        end: u64,
        count: u64,
    }
    struct SessOutBuilder {
        key: arrow_array::builder::StringBuilder,
        start: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
        end: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
        count: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    }
    #[allow(dead_code)]
    struct SessOutView<'a> {
        key: &'a str,
        start: u64,
        end: u64,
        count: u64,
    }
    struct SessOutCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for SessOutBuilder {
        type Item = SessOut;
        fn append(&mut self, item: SessOut) {
            self.key.append_value(&item.key);
            self.start.append_value(item.start);
            self.end.append_value(item.end);
            self.count.append_value(item.count);
        }
        fn append_null(&mut self) {
            self.key.append_null();
            self.start.append_null();
            self.end.append_null();
            self.count.append_null();
        }
        fn len(&self) -> usize {
            self.key.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                SessOut::arrow_schema(),
                vec![
                    Arc::new(self.key.finish()),
                    Arc::new(self.start.finish()),
                    Arc::new(self.end.finish()),
                    Arc::new(self.count.finish()),
                ],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for SessOut {
        type Builder = SessOutBuilder;
        type View<'a> = SessOutView<'a>;
        type Columns<'a> = SessOutCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("start", arrow_schema::DataType::UInt64, false),
                arrow_schema::Field::new("end", arrow_schema::DataType::UInt64, false),
                arrow_schema::Field::new("count", arrow_schema::DataType::UInt64, false),
            ]))
        }
        fn builder(capacity: usize) -> Self::Builder {
            SessOutBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                start: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
                end: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
                count: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            }
        }
        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::UInt64Type;
            SessOutView {
                key: batch.column(0).as_string::<i32>().value(index),
                start: batch.column(1).as_primitive::<UInt64Type>().value(index),
                end: batch.column(2).as_primitive::<UInt64Type>().value(index),
                count: batch.column(3).as_primitive::<UInt64Type>().value(index),
            }
        }
        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            SessOutCols {
                key: batch.column(0).as_string::<i32>(),
            }
        }
    }

    fn test_ctx(name: &str) -> OperatorContext {
        let path = std::env::temp_dir().join(format!(
            "rhei_batch_sess_test_{name}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    fn make_events(events: &[(&str, u64)]) -> RheiBuffer<Event> {
        let mut builder = Event::builder(events.len());
        for &(key, ts) in events {
            builder.append(Event {
                key: key.to_string(),
                ts,
            });
        }
        RheiBuffer::from_builder(builder)
    }

    #[tokio::test]
    async fn gap_exceeded_closes_session() {
        let mut ctx = test_ctx("gap_close");
        let mut win = BatchSessionWindow::new(
            10,
            |v: EventView<'_>| v.key.to_string(),
            |v: EventView<'_>| v.ts,
            |acc: &mut u64, _v: EventView<'_>| *acc += 1,
            |key: &str, start: u64, end: u64, acc: &u64| SessOut {
                key: key.to_string(),
                start,
                end,
                count: *acc,
            },
        );

        let input = make_events(&[("a", 1), ("a", 5)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        // ts=20 exceeds gap(10) from last(5): 20 - 5 = 15 > 10
        let input = make_events(&[("a", 20)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = SessOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.key, "a");
        assert_eq!(v.start, 1);
        assert_eq!(v.end, 5);
        assert_eq!(v.count, 2);
    }

    #[tokio::test]
    async fn watermark_closes_session() {
        let mut ctx = test_ctx("wm_close");
        let mut win = BatchSessionWindow::new(
            10,
            |v: EventView<'_>| v.key.to_string(),
            |v: EventView<'_>| v.ts,
            |acc: &mut u64, _v: EventView<'_>| *acc += 1,
            |key: &str, start: u64, end: u64, acc: &u64| SessOut {
                key: key.to_string(),
                start,
                end,
                count: *acc,
            },
        );

        let input = make_events(&[("a", 1), ("a", 5)]);
        win.process(input, &mut ctx).await.unwrap();

        // last_event(5) + gap(10) + lateness(0) = 15 <= 16
        let result = win.on_watermark(16, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = SessOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.count, 2);
    }

    #[tokio::test]
    async fn within_gap_extends_session() {
        let mut ctx = test_ctx("extend");
        let mut win = BatchSessionWindow::new(
            10,
            |v: EventView<'_>| v.key.to_string(),
            |v: EventView<'_>| v.ts,
            |acc: &mut u64, _v: EventView<'_>| *acc += 1,
            |key: &str, start: u64, end: u64, acc: &u64| SessOut {
                key: key.to_string(),
                start,
                end,
                count: *acc,
            },
        );

        // All within gap of 10
        let input = make_events(&[("a", 1), ("a", 5), ("a", 8)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        // Watermark closes: 8 + 10 + 0 = 18 <= 20
        let result = win.on_watermark(20, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        let v = SessOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.count, 3);
        assert_eq!(v.start, 1);
        assert_eq!(v.end, 8);
    }
}

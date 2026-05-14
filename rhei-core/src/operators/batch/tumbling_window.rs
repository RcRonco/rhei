//! Batch tumbling window operator with pluggable aggregation.
//!
//! Groups rows from `RheiBuffer` batches into fixed-size, non-overlapping time
//! windows keyed by a grouping function. Windows are closed when the watermark
//! advances past their end time.

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::keyed_state::KeyedState;
use crate::arrow::{
    BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema,
};

/// A batch tumbling (fixed-size, non-overlapping) window operator.
///
/// # Type Parameters
///
/// - `I` — input schema type
/// - `O` — output schema type
/// - `Acc` — accumulator type (stored in state between batches)
/// - `KF` — key extraction: `Fn(I::View<'_>) -> String`
/// - `TF` — timestamp extraction: `Fn(I::View<'_>) -> u64`
/// - `AF` — accumulation: `Fn(&mut Acc, I::View<'_>)`
/// - `FF` — finish: `Fn(&str, u64, u64, &Acc) -> O` (key, `window_start`, `window_end`, acc)
pub struct BatchTumblingWindow<I, O, Acc, KF, TF, AF, FF> {
    window_size: u64,
    key_fn: KF,
    time_fn: TF,
    accumulate_fn: AF,
    finish_fn: FF,
    allowed_lateness: u64,
    last_watermark: u64,
    active_windows: HashMap<String, u64>,
    _phantom: PhantomData<fn(I, Acc) -> O>,
}

impl<I, O, Acc, KF: Clone, TF: Clone, AF: Clone, FF: Clone> Clone
    for BatchTumblingWindow<I, O, Acc, KF, TF, AF, FF>
{
    fn clone(&self) -> Self {
        Self {
            window_size: self.window_size,
            key_fn: self.key_fn.clone(),
            time_fn: self.time_fn.clone(),
            accumulate_fn: self.accumulate_fn.clone(),
            finish_fn: self.finish_fn.clone(),
            allowed_lateness: self.allowed_lateness,
            last_watermark: 0,
            active_windows: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, Acc, KF, TF, AF, FF> fmt::Debug for BatchTumblingWindow<I, O, Acc, KF, TF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchTumblingWindow")
            .field("window_size", &self.window_size)
            .field("allowed_lateness", &self.allowed_lateness)
            .field("active_windows", &self.active_windows.len())
            .finish_non_exhaustive()
    }
}

impl<I, O, Acc, KF, TF, AF, FF> BatchTumblingWindow<I, O, Acc, KF, TF, AF, FF> {
    /// Creates a new batch tumbling window operator.
    pub fn new(
        window_size: u64,
        key_fn: KF,
        time_fn: TF,
        accumulate_fn: AF,
        finish_fn: FF,
    ) -> Self {
        assert!(window_size > 0, "window_size must be > 0");
        Self {
            window_size,
            key_fn,
            time_fn,
            accumulate_fn,
            finish_fn,
            allowed_lateness: 0,
            last_watermark: 0,
            active_windows: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    /// Sets the allowed lateness grace period.
    pub fn with_allowed_lateness(mut self, lateness: u64) -> Self {
        self.allowed_lateness = lateness;
        self
    }
}

#[async_trait]
impl<I, O, Acc, KF, TF, AF, FF> BatchStreamFunction
    for BatchTumblingWindow<I, O, Acc, KF, TF, AF, FF>
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

        // Extract keys, timestamps, and physical indices eagerly.
        // Views borrow the batch and aren't Send, so they can't be held across .await.
        let row_data: Vec<(String, u64, usize)> = input
            .iter()
            .enumerate_physical()
            .map(|(phys_idx, _view)| {
                // Re-access the view twice: once for key, once for timestamp.
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
            let window_start = timestamp - (timestamp % self.window_size);
            let window_end = window_start + self.window_size;

            // Late event detection.
            if window_end + self.allowed_lateness <= self.last_watermark {
                metrics::counter!("late_events_dropped_total").increment(1);
                continue;
            }

            // Check if key already has an active window.
            let prev_window = self.active_windows.get(key).copied();

            if let Some(old_start) = prev_window
                && old_start != window_start
            {
                // Close the old window.
                let acc_key = format!("{key}:{old_start}");
                let old_acc: Option<Acc> = {
                    let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "acc");
                    state.get(&acc_key).await.unwrap_or(None)
                };
                if let Some(acc) = &old_acc {
                    outputs.push((self.finish_fn)(
                        key,
                        old_start,
                        old_start + self.window_size,
                        acc,
                    ));
                }
                // Delete old accumulator.
                let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "acc");
                state.delete(&acc_key)?;
            }

            // Load or create accumulator for current window.
            let acc_key = format!("{key}:{window_start}");
            let mut acc: Acc = {
                let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "acc");
                state
                    .get(&acc_key)
                    .await
                    .unwrap_or(None)
                    .unwrap_or_default()
            };

            // Accumulate — re-access the row by physical index (cheap, no copy).
            let view = I::view(batch, *phys_idx);
            (self.accumulate_fn)(&mut acc, view);

            // Store updated accumulator.
            {
                let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "acc");
                state.put(&acc_key, &acc)?;
            }

            // Track active window.
            self.active_windows.insert(key.clone(), window_start);
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

        for (key, &win_start) in &self.active_windows {
            if win_start + self.window_size + self.allowed_lateness <= watermark {
                let acc_key = format!("{key}:{win_start}");
                let acc: Option<Acc> = {
                    let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "acc");
                    state.get(&acc_key).await.unwrap_or(None)
                };
                if let Some(acc) = &acc {
                    outputs.push((self.finish_fn)(
                        key,
                        win_start,
                        win_start + self.window_size,
                        acc,
                    ));
                }
                // Clean up state.
                {
                    let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "acc");
                    state.delete(&acc_key)?;
                }
                closed_keys.push(key.clone());
            }
        }

        for key in &closed_keys {
            self.active_windows.remove(key);
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

    // ── Test schema types ────────────────────────────────────────────

    struct Event {
        key: String,
        ts: u64,
        value: f64,
    }

    struct EventBuilder {
        key: arrow_array::builder::StringBuilder,
        ts: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
        value: arrow_array::builder::PrimitiveBuilder<arrow_array::types::Float64Type>,
    }

    struct EventView<'a> {
        key: &'a str,
        ts: u64,
        #[allow(dead_code)]
        value: f64,
    }

    struct EventCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
        #[allow(dead_code)]
        ts: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
        #[allow(dead_code)]
        value: &'a arrow_array::PrimitiveArray<arrow_array::types::Float64Type>,
    }

    impl crate::arrow::RheiBuilder for EventBuilder {
        type Item = Event;

        fn append(&mut self, item: Event) {
            self.key.append_value(&item.key);
            self.ts.append_value(item.ts);
            self.value.append_value(item.value);
        }

        fn append_null(&mut self) {
            self.key.append_null();
            self.ts.append_null();
            self.value.append_null();
        }

        fn len(&self) -> usize {
            self.key.len()
        }

        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                Event::arrow_schema(),
                vec![
                    Arc::new(self.key.finish()),
                    Arc::new(self.ts.finish()),
                    Arc::new(self.value.finish()),
                ],
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
                arrow_schema::Field::new("value", arrow_schema::DataType::Float64, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            EventBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                ts: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
                value: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            }
        }

        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::{Float64Type, UInt64Type};
            EventView {
                key: batch.column(0).as_string::<i32>().value(index),
                ts: batch.column(1).as_primitive::<UInt64Type>().value(index),
                value: batch.column(2).as_primitive::<Float64Type>().value(index),
            }
        }

        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::{Float64Type, UInt64Type};
            EventCols {
                key: batch.column(0).as_string::<i32>(),
                ts: batch.column(1).as_primitive::<UInt64Type>(),
                value: batch.column(2).as_primitive::<Float64Type>(),
            }
        }
    }

    // Output schema for window results.
    struct WinOut {
        key: String,
        window_start: u64,
        window_end: u64,
        count: u64,
    }

    struct WinOutBuilder {
        key: arrow_array::builder::StringBuilder,
        window_start: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
        window_end: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
        count: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    }

    #[allow(dead_code)]
    struct WinOutView<'a> {
        key: &'a str,
        window_start: u64,
        window_end: u64,
        count: u64,
    }

    struct WinOutCols<'a> {
        #[allow(dead_code)]
        key: &'a arrow_array::StringArray,
        #[allow(dead_code)]
        window_start: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
        #[allow(dead_code)]
        window_end: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
        #[allow(dead_code)]
        count: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
    }

    impl crate::arrow::RheiBuilder for WinOutBuilder {
        type Item = WinOut;

        fn append(&mut self, item: WinOut) {
            self.key.append_value(&item.key);
            self.window_start.append_value(item.window_start);
            self.window_end.append_value(item.window_end);
            self.count.append_value(item.count);
        }

        fn append_null(&mut self) {
            self.key.append_null();
            self.window_start.append_null();
            self.window_end.append_null();
            self.count.append_null();
        }

        fn len(&self) -> usize {
            self.key.len()
        }

        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                WinOut::arrow_schema(),
                vec![
                    Arc::new(self.key.finish()),
                    Arc::new(self.window_start.finish()),
                    Arc::new(self.window_end.finish()),
                    Arc::new(self.count.finish()),
                ],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for WinOut {
        type Builder = WinOutBuilder;
        type View<'a> = WinOutView<'a>;
        type Columns<'a> = WinOutCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("window_start", arrow_schema::DataType::UInt64, false),
                arrow_schema::Field::new("window_end", arrow_schema::DataType::UInt64, false),
                arrow_schema::Field::new("count", arrow_schema::DataType::UInt64, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            WinOutBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                window_start: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
                window_end: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
                count: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            }
        }

        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::UInt64Type;
            WinOutView {
                key: batch.column(0).as_string::<i32>().value(index),
                window_start: batch.column(1).as_primitive::<UInt64Type>().value(index),
                window_end: batch.column(2).as_primitive::<UInt64Type>().value(index),
                count: batch.column(3).as_primitive::<UInt64Type>().value(index),
            }
        }

        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::UInt64Type;
            WinOutCols {
                key: batch.column(0).as_string::<i32>(),
                window_start: batch.column(1).as_primitive::<UInt64Type>(),
                window_end: batch.column(2).as_primitive::<UInt64Type>(),
                count: batch.column(3).as_primitive::<UInt64Type>(),
            }
        }
    }

    // ── Test helpers ─────────────────────────────────────────────────

    fn test_ctx(name: &str) -> OperatorContext {
        let path =
            std::env::temp_dir().join(format!("rhei_batch_tw_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    #[allow(clippy::type_complexity)]
    fn make_window() -> BatchTumblingWindow<
        Event,
        WinOut,
        u64,
        impl for<'a> Fn(<Event as RheiSchemaTrait>::View<'a>) -> String + Send + Sync,
        impl for<'a> Fn(<Event as RheiSchemaTrait>::View<'a>) -> u64 + Send + Sync,
        impl for<'a> Fn(&mut u64, <Event as RheiSchemaTrait>::View<'a>) + Send + Sync,
        impl Fn(&str, u64, u64, &u64) -> WinOut + Send + Sync,
    > {
        BatchTumblingWindow::new(
            10,
            |view: EventView<'_>| view.key.to_string(),
            |view: EventView<'_>| view.ts,
            |acc: &mut u64, _view: EventView<'_>| *acc += 1,
            |key: &str, window_start: u64, window_end: u64, acc: &u64| WinOut {
                key: key.to_string(),
                window_start,
                window_end,
                count: *acc,
            },
        )
    }

    fn make_events(events: &[(&str, u64, f64)]) -> RheiBuffer<Event> {
        let mut builder = Event::builder(events.len());
        for &(key, ts, value) in events {
            builder.append(Event {
                key: key.to_string(),
                ts,
                value,
            });
        }
        RheiBuffer::from_builder(builder)
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn accumulates_without_emitting() {
        let mut ctx = test_ctx("accum");
        let mut win = make_window();

        let input = make_events(&[("a", 1, 1.0), ("a", 5, 2.0)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn data_driven_window_closure() {
        let mut ctx = test_ctx("data_close");
        let mut win = make_window();

        // Two events in window [0, 10).
        let input = make_events(&[("a", 1, 1.0), ("a", 5, 2.0)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        // Event in window [10, 20) triggers closure of [0, 10).
        let input = make_events(&[("a", 15, 3.0)]);
        let result = win.process(input, &mut ctx).await.unwrap();

        let BufferOutput::Single(buf) = result else {
            panic!("expected Single output");
        };
        assert_eq!(buf.len(), 1);
        let v = WinOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.key, "a");
        assert_eq!(v.window_start, 0);
        assert_eq!(v.window_end, 10);
        assert_eq!(v.count, 2);
    }

    #[tokio::test]
    async fn watermark_closes_window() {
        let mut ctx = test_ctx("wm_close");
        let mut win = make_window();

        let input = make_events(&[("a", 1, 1.0), ("a", 5, 2.0), ("b", 3, 1.0)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        // Watermark at 10 closes both windows.
        let result = win.on_watermark(10, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single output");
        };
        assert_eq!(buf.len(), 2);
    }

    #[tokio::test]
    async fn late_events_dropped() {
        let mut ctx = test_ctx("late");
        let mut win = make_window();

        let input = make_events(&[("a", 3, 1.0)]);
        win.process(input, &mut ctx).await.unwrap();

        // Advance watermark past window [0, 10).
        win.on_watermark(10, &mut ctx).await.unwrap();

        // Late event for window [0, 10) should be dropped.
        let input = make_events(&[("a", 2, 1.0)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn allowed_lateness_grace() {
        let mut ctx = test_ctx("lateness");
        let mut win = make_window().with_allowed_lateness(5);

        let input = make_events(&[("a", 3, 1.0)]);
        win.process(input, &mut ctx).await.unwrap();

        // Watermark at 10: window_end(10) + allowed_lateness(5) = 15 > 10, no close.
        let result = win.on_watermark(10, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        // Watermark at 15: 0 + 10 + 5 <= 15, close.
        let result = win.on_watermark(15, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single output");
        };
        assert_eq!(buf.len(), 1);
    }

    #[tokio::test]
    async fn multiple_keys_independent() {
        let mut ctx = test_ctx("multi_key");
        let mut win = make_window();

        let input = make_events(&[("a", 1, 1.0), ("b", 2, 2.0), ("a", 3, 3.0)]);
        win.process(input, &mut ctx).await.unwrap();

        // Move key "a" to next window, "b" stays.
        let input = make_events(&[("a", 15, 4.0)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single output for key a");
        };
        assert_eq!(buf.len(), 1);
        let v = WinOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.key, "a");
        assert_eq!(v.count, 2);

        // Watermark closes "b".
        let result = win.on_watermark(10, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single output for key b");
        };
        assert_eq!(buf.len(), 1);
        let v = WinOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.key, "b");
        assert_eq!(v.count, 1);
    }

    #[tokio::test]
    async fn empty_input_returns_none() {
        let mut ctx = test_ctx("empty");
        let mut win = make_window();

        let input = make_events(&[]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }
}

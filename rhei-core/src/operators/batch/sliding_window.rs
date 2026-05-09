//! Batch sliding window operator with overlapping time windows.
//!
//! Each element belongs to every open window whose range contains its timestamp.
//! Windows are closed when the watermark advances past their end + allowed lateness.

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::arrow::{
    BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema,
};
use crate::operators::keyed_state::KeyedState;

/// Batch sliding window operator.
///
/// # Type Parameters
///
/// - `I` — input schema type
/// - `O` — output schema type
/// - `Acc` — accumulator stored in state
/// - `KF` — key extraction: `Fn(I::View<'_>) -> String`
/// - `TF` — timestamp extraction: `Fn(I::View<'_>) -> u64`
/// - `AF` — accumulation: `Fn(&mut Acc, I::View<'_>)`
/// - `FF` — finish: `Fn(&str, u64, u64, &Acc) -> O`
pub struct BatchSlidingWindow<I, O, Acc, KF, TF, AF, FF> {
    window_size: u64,
    slide: u64,
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
    for BatchSlidingWindow<I, O, Acc, KF, TF, AF, FF>
{
    fn clone(&self) -> Self {
        Self {
            window_size: self.window_size,
            slide: self.slide,
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

impl<I, O, Acc, KF, TF, AF, FF> fmt::Debug for BatchSlidingWindow<I, O, Acc, KF, TF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchSlidingWindow")
            .field("window_size", &self.window_size)
            .field("slide", &self.slide)
            .field("allowed_lateness", &self.allowed_lateness)
            .finish_non_exhaustive()
    }
}

impl<I, O, Acc, KF, TF, AF, FF> BatchSlidingWindow<I, O, Acc, KF, TF, AF, FF> {
    /// Creates a new sliding window with the given size and slide interval.
    pub fn new(
        window_size: u64,
        slide: u64,
        key_fn: KF,
        time_fn: TF,
        accumulate_fn: AF,
        finish_fn: FF,
    ) -> Self {
        assert!(window_size > 0, "window_size must be > 0");
        assert!(slide > 0, "slide must be > 0");
        assert!(slide <= window_size, "slide must be <= window_size");
        Self {
            window_size,
            slide,
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

fn window_starts_for(timestamp: u64, window_size: u64, slide: u64) -> Vec<u64> {
    let latest_slide = timestamp - (timestamp % slide);
    let mut starts = Vec::new();
    let mut start = latest_slide;
    loop {
        if start + window_size > timestamp {
            starts.push(start);
        }
        if start < slide {
            break;
        }
        start -= slide;
        if start + window_size <= timestamp {
            break;
        }
    }
    starts.sort_unstable();
    starts
}

/// State: set of active window starts for a key.
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct ActiveWindows {
    starts: Vec<u64>,
}

#[async_trait]
impl<I, O, Acc, KF, TF, AF, FF> BatchStreamFunction
    for BatchSlidingWindow<I, O, Acc, KF, TF, AF, FF>
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
        let outputs: Vec<O> = Vec::new();

        for (key, timestamp, phys_idx) in &row_data {
            let candidate_starts = window_starts_for(*timestamp, self.window_size, self.slide);

            let all_late = !candidate_starts.is_empty()
                && candidate_starts.iter().all(|&ws| {
                    ws + self.window_size + self.allowed_lateness <= self.last_watermark
                });
            if all_late {
                metrics::counter!("late_events_dropped_total").increment(1);
                continue;
            }

            self.active_keys.insert(key.clone());

            // Load active windows for this key.
            let mut active: ActiveWindows = {
                let mut state = KeyedState::<String, ActiveWindows>::new(&mut ctx.state, "sw_act");
                state.get(key).await.unwrap_or(None).unwrap_or_default()
            };

            // Accumulate into each window that contains this timestamp.
            for &win_start in &candidate_starts {
                if win_start + self.window_size + self.allowed_lateness <= self.last_watermark {
                    continue;
                }

                let acc_key = format!("{key}:{win_start}");
                let mut acc: Acc = {
                    let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "sw_acc");
                    state
                        .get(&acc_key)
                        .await
                        .unwrap_or(None)
                        .unwrap_or_default()
                };

                let view = I::view(batch, *phys_idx);
                (self.accumulate_fn)(&mut acc, view);

                {
                    let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "sw_acc");
                    state.put(&acc_key, &acc)?;
                }

                if !active.starts.contains(&win_start) {
                    active.starts.push(win_start);
                }
            }

            active.starts.sort_unstable();
            let mut state = KeyedState::<String, ActiveWindows>::new(&mut ctx.state, "sw_act");
            state.put(key, &active)?;
        }

        // Close windows that are past watermark (data-driven check not needed for sliding;
        // watermark triggers closure).

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
        let mut keys_to_remove: Vec<String> = Vec::new();

        for key in &self.active_keys {
            let mut active: ActiveWindows = {
                let mut state = KeyedState::<String, ActiveWindows>::new(&mut ctx.state, "sw_act");
                state.get(key).await.unwrap_or(None).unwrap_or_default()
            };

            let mut still_active = Vec::new();
            for &win_start in &active.starts {
                if win_start + self.window_size + self.allowed_lateness <= watermark {
                    let acc_key = format!("{key}:{win_start}");
                    let acc: Option<Acc> = {
                        let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "sw_acc");
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
                    let mut state = KeyedState::<String, Acc>::new(&mut ctx.state, "sw_acc");
                    state.delete(&acc_key)?;
                } else {
                    still_active.push(win_start);
                }
            }

            active.starts = still_active;
            if active.starts.is_empty() {
                let mut state = KeyedState::<String, ActiveWindows>::new(&mut ctx.state, "sw_act");
                state.delete(key)?;
                keys_to_remove.push(key.clone());
            } else {
                let mut state = KeyedState::<String, ActiveWindows>::new(&mut ctx.state, "sw_act");
                state.put(key, &active)?;
            }
        }

        for key in &keys_to_remove {
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
        #[allow(dead_code)]
        ts: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
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
            use arrow_array::types::UInt64Type;
            EventCols {
                key: batch.column(0).as_string::<i32>(),
                ts: batch.column(1).as_primitive::<UInt64Type>(),
            }
        }
    }

    struct WinOut {
        key: String,
        window_start: u64,
        window_end: u64,
        count: u64,
    }
    struct WinOutBuilder {
        key: arrow_array::builder::StringBuilder,
        ws: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
        we: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
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
    }

    impl crate::arrow::RheiBuilder for WinOutBuilder {
        type Item = WinOut;
        fn append(&mut self, item: WinOut) {
            self.key.append_value(&item.key);
            self.ws.append_value(item.window_start);
            self.we.append_value(item.window_end);
            self.count.append_value(item.count);
        }
        fn append_null(&mut self) {
            self.key.append_null();
            self.ws.append_null();
            self.we.append_null();
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
                    Arc::new(self.ws.finish()),
                    Arc::new(self.we.finish()),
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
                arrow_schema::Field::new("ws", arrow_schema::DataType::UInt64, false),
                arrow_schema::Field::new("we", arrow_schema::DataType::UInt64, false),
                arrow_schema::Field::new("count", arrow_schema::DataType::UInt64, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            WinOutBuilder {
                key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                ws: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
                we: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
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
            WinOutCols {
                key: batch.column(0).as_string::<i32>(),
            }
        }
    }

    fn test_ctx(name: &str) -> OperatorContext {
        let path =
            std::env::temp_dir().join(format!("rhei_batch_sw_test_{name}_{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    #[allow(clippy::type_complexity)]
    fn make_window() -> BatchSlidingWindow<
        Event,
        WinOut,
        u64,
        impl for<'a> Fn(<Event as RheiSchemaTrait>::View<'a>) -> String + Send + Sync,
        impl for<'a> Fn(<Event as RheiSchemaTrait>::View<'a>) -> u64 + Send + Sync,
        impl for<'a> Fn(&mut u64, <Event as RheiSchemaTrait>::View<'a>) + Send + Sync,
        impl Fn(&str, u64, u64, &u64) -> WinOut + Send + Sync,
    > {
        BatchSlidingWindow::new(
            10,
            5,
            |view: EventView<'_>| view.key.to_string(),
            |view: EventView<'_>| view.ts,
            |acc: &mut u64, _view: EventView<'_>| *acc += 1,
            |key: &str, ws: u64, we: u64, acc: &u64| WinOut {
                key: key.to_string(),
                window_start: ws,
                window_end: we,
                count: *acc,
            },
        )
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
    async fn watermark_closes_windows() {
        let mut ctx = test_ctx("wm_close");
        let mut win = make_window();

        // ts=3 belongs to window [0,10)
        let input = make_events(&[("a", 3)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        // Watermark at 10 closes [0,10): 0+10+0 <= 10
        let result = win.on_watermark(10, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = WinOut::view(buf.as_record_batch(), 0);
        assert_eq!(v.window_start, 0);
        assert_eq!(v.window_end, 10);
        assert_eq!(v.count, 1);
    }

    #[tokio::test]
    async fn element_in_multiple_windows() {
        let mut ctx = test_ctx("multi_win");
        let mut win = make_window();

        // ts=7 belongs to windows [0,10) and [5,15)
        let input = make_events(&[("a", 7)]);
        win.process(input, &mut ctx).await.unwrap();

        // Watermark at 15 closes both [0,10) and [5,15)
        let result = win.on_watermark(15, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 2);
    }

    #[tokio::test]
    async fn late_event_dropped() {
        let mut ctx = test_ctx("late");
        let mut win = make_window();

        let input = make_events(&[("a", 3)]);
        win.process(input, &mut ctx).await.unwrap();

        // Advance watermark past all windows for ts=2
        win.on_watermark(15, &mut ctx).await.unwrap();

        let input = make_events(&[("a", 2)]);
        let result = win.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }
}

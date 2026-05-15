#![allow(clippy::unwrap_used, clippy::expect_used, clippy::type_complexity)]
//! Integration test: watermark propagation triggers window closure.
//!
//! Uses a custom batch source that emits explicit watermarks with each batch.
//! A `TumblingWindow` operator accumulates events into windows and closes
//! them when the watermark advances past the window boundary.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::arrow::{RheiBuffer, RheiBuilder, RheiSchema, Sink, Source};
use rhei_core::operators::batch::TumblingWindow;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Schema: TimedEvent ──────────────────────────────────────────────

struct TimedEvent {
    key: String,
    ts: u64,
    value: f64,
}

struct TimedEventBuilder {
    key: arrow_array::builder::StringBuilder,
    ts: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    value: arrow_array::builder::PrimitiveBuilder<arrow_array::types::Float64Type>,
}

#[derive(Debug, Clone)]
struct TimedEventView<'a> {
    key: &'a str,
    ts: u64,
    #[allow(dead_code)]
    value: f64,
}

struct TimedEventColumns<'a> {
    #[allow(dead_code)]
    key: &'a arrow_array::StringArray,
    #[allow(dead_code)]
    ts: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
    #[allow(dead_code)]
    value: &'a arrow_array::PrimitiveArray<arrow_array::types::Float64Type>,
}

impl RheiBuilder for TimedEventBuilder {
    type Item = TimedEvent;

    fn append(&mut self, item: TimedEvent) {
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
        arrow_array::builder::ArrayBuilder::len(&self.key)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            TimedEvent::arrow_schema(),
            vec![
                Arc::new(self.key.finish()),
                Arc::new(self.ts.finish()),
                Arc::new(self.value.finish()),
            ],
        )
        .unwrap()
    }
}

impl RheiSchema for TimedEvent {
    type Builder = TimedEventBuilder;
    type View<'a> = TimedEventView<'a>;
    type Columns<'a> = TimedEventColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("ts", arrow_schema::DataType::UInt64, false),
            arrow_schema::Field::new("value", arrow_schema::DataType::Float64, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        TimedEventBuilder {
            key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
            ts: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            value: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, UInt64Type};
        TimedEventView {
            key: batch.column(0).as_string::<i32>().value(index),
            ts: batch.column(1).as_primitive::<UInt64Type>().value(index),
            value: batch.column(2).as_primitive::<Float64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, UInt64Type};
        TimedEventColumns {
            key: batch.column(0).as_string::<i32>(),
            ts: batch.column(1).as_primitive::<UInt64Type>(),
            value: batch.column(2).as_primitive::<Float64Type>(),
        }
    }
}

// ── Schema: WindowResult ────────────────────────────────────────────

struct WindowResult {
    key: String,
    window_start: u64,
    window_end: u64,
    count: u64,
}

struct WindowResultBuilder {
    key: arrow_array::builder::StringBuilder,
    window_start: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    window_end: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    count: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
}

#[derive(Debug, Clone)]
struct WindowResultView<'a> {
    key: &'a str,
    window_start: u64,
    window_end: u64,
    count: u64,
}

struct WindowResultColumns<'a> {
    #[allow(dead_code)]
    key: &'a arrow_array::StringArray,
    #[allow(dead_code)]
    window_start: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
    #[allow(dead_code)]
    window_end: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
    #[allow(dead_code)]
    count: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
}

impl RheiBuilder for WindowResultBuilder {
    type Item = WindowResult;

    fn append(&mut self, item: WindowResult) {
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
        arrow_array::builder::ArrayBuilder::len(&self.key)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            WindowResult::arrow_schema(),
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

impl RheiSchema for WindowResult {
    type Builder = WindowResultBuilder;
    type View<'a> = WindowResultView<'a>;
    type Columns<'a> = WindowResultColumns<'a>;

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
        WindowResultBuilder {
            key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
            window_start: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            window_end: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            count: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        WindowResultView {
            key: batch.column(0).as_string::<i32>().value(index),
            window_start: batch.column(1).as_primitive::<UInt64Type>().value(index),
            window_end: batch.column(2).as_primitive::<UInt64Type>().value(index),
            count: batch.column(3).as_primitive::<UInt64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        WindowResultColumns {
            key: batch.column(0).as_string::<i32>(),
            window_start: batch.column(1).as_primitive::<UInt64Type>(),
            window_end: batch.column(2).as_primitive::<UInt64Type>(),
            count: batch.column(3).as_primitive::<UInt64Type>(),
        }
    }
}

// ── Custom source with explicit watermarks ──────────────────────────

/// A batch source that emits pre-built batches with explicit watermarks.
/// Each batch is associated with a watermark value. After emitting a batch,
/// `current_watermark()` returns the associated watermark.
struct WatermarkedSource {
    /// Remaining batches and their watermarks: `(events, watermark)`.
    batches: Vec<(Vec<TimedEvent>, u64)>,
    /// Index of the next batch to emit.
    idx: usize,
    /// Current watermark (set after emitting a batch).
    watermark: Option<u64>,
}

impl WatermarkedSource {
    fn new(batches: Vec<(Vec<TimedEvent>, u64)>) -> Self {
        Self {
            batches,
            idx: 0,
            watermark: None,
        }
    }
}

#[async_trait]
impl Source for WatermarkedSource {
    type Output = TimedEvent;

    async fn next_batch(&mut self) -> Option<RheiBuffer<TimedEvent>> {
        if self.idx >= self.batches.len() {
            return None;
        }
        let (events, wm) = &self.batches[self.idx];
        self.idx += 1;
        self.watermark = Some(*wm);

        let mut builder = TimedEvent::builder(events.len());
        for e in events {
            builder.append(TimedEvent {
                key: e.key.clone(),
                ts: e.ts,
                value: e.value,
            });
        }
        Some(RheiBuffer::from_builder(builder))
    }

    fn should_emit_watermark(&self) -> bool {
        self.watermark.is_some()
    }

    fn current_watermark(&self) -> Option<u64> {
        self.watermark
    }
}

// ── Collecting sink ─────────────────────────────────────────────────

struct WindowCollectSink {
    collected: Arc<Mutex<Vec<(String, u64, u64, u64)>>>,
}

#[async_trait]
impl Sink for WindowCollectSink {
    type Input = WindowResult;

    async fn write_batch(&mut self, input: RheiBuffer<WindowResult>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((
                view.key.to_string(),
                view.window_start,
                view.window_end,
                view.count,
            ));
        }
        Ok(())
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn watermark_triggers_window_closure() {
    let dir = std::env::temp_dir().join(format!("rhei_wm_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Batch 1: events in window [0, 10) for keys "a" and "b". Watermark = 5.
    // Batch 2: more events in [0, 10). Watermark = 9 (still < 10, no closure).
    // Batch 3: event in [10, 20). Watermark = 15 (>= 10, closes window [0,10)).
    let source = WatermarkedSource::new(vec![
        (
            vec![
                TimedEvent {
                    key: "a".into(),
                    ts: 1,
                    value: 1.0,
                },
                TimedEvent {
                    key: "a".into(),
                    ts: 5,
                    value: 2.0,
                },
                TimedEvent {
                    key: "b".into(),
                    ts: 3,
                    value: 3.0,
                },
            ],
            5,
        ),
        (
            vec![TimedEvent {
                key: "a".into(),
                ts: 8,
                value: 4.0,
            }],
            9,
        ),
        (
            vec![
                TimedEvent {
                    key: "a".into(),
                    ts: 12,
                    value: 5.0,
                },
                TimedEvent {
                    key: "a".into(),
                    ts: 15,
                    value: 6.0,
                },
            ],
            15,
        ),
    ]);

    let collected: Arc<Mutex<Vec<(String, u64, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let window = TumblingWindow::new(
        10,
        |view: <TimedEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |view: <TimedEvent as RheiSchema>::View<'_>| view.ts,
        |acc: &mut u64, _view: <TimedEvent as RheiSchema>::View<'_>| *acc += 1,
        |key: &str, window_start: u64, window_end: u64, acc: &u64| WindowResult {
            key: key.to_string(),
            window_start,
            window_end,
            count: *acc,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .operator("tumbling_10", window)
        .sink(WindowCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // Expected windows after pipeline completes:
    // "a" [0,10) count=3 (ts: 1, 5, 8) — closed by data-driven (event at ts=12
    //     shifts key "a" to window [10,20)) or by watermark=15.
    // "a" [10,20) count=2 (ts: 12, 15) — closed at pipeline end.
    // "b" [0,10) count=1 (ts: 3) — closed by watermark=15.
    assert!(
        results.len() >= 2,
        "expected at least 2 window outputs, got {results:?}"
    );

    // Verify the [0,10) window for key "a" is present with count=3.
    let a_first = results.iter().find(|r| r.0 == "a" && r.1 == 0);
    assert!(
        a_first.is_some(),
        "expected window [0,10) for 'a', got {results:?}"
    );
    assert_eq!(a_first.unwrap().3, 3, "a:[0,10) should have count=3");

    // Verify window for key "b" is present.
    let b_window = results.iter().find(|r| r.0 == "b" && r.1 == 0);
    assert!(
        b_window.is_some(),
        "expected window [0,10) for 'b', got {results:?}"
    );
    assert_eq!(b_window.unwrap().3, 1, "b:[0,10) should have count=1");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn watermark_advance_closes_multiple_keys() {
    let dir = std::env::temp_dir().join(format!("rhei_wm_multi_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // All events in window [0,10) for three keys. Watermark jumps to 20 in
    // the second batch — should close all three windows.
    let source = WatermarkedSource::new(vec![
        (
            vec![
                TimedEvent {
                    key: "x".into(),
                    ts: 1,
                    value: 1.0,
                },
                TimedEvent {
                    key: "y".into(),
                    ts: 2,
                    value: 2.0,
                },
                TimedEvent {
                    key: "z".into(),
                    ts: 3,
                    value: 3.0,
                },
                TimedEvent {
                    key: "x".into(),
                    ts: 7,
                    value: 4.0,
                },
            ],
            5,
        ),
        (
            vec![TimedEvent {
                key: "x".into(),
                ts: 25,
                value: 5.0,
            }],
            20,
        ),
    ]);

    let collected: Arc<Mutex<Vec<(String, u64, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let window = TumblingWindow::new(
        10,
        |view: <TimedEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |view: <TimedEvent as RheiSchema>::View<'_>| view.ts,
        |acc: &mut u64, _view: <TimedEvent as RheiSchema>::View<'_>| *acc += 1,
        |key: &str, window_start: u64, window_end: u64, acc: &u64| WindowResult {
            key: key.to_string(),
            window_start,
            window_end,
            count: *acc,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .operator("tumbling_10_multi", window)
        .sink(WindowCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // Key "x" has a data-driven closure: event at ts=25 is window [20,30),
    // which closes [0,10) for key "x" during process().
    // Keys "y" and "z" are closed by watermark=20 (via on_watermark or
    // pipeline-end watermark).
    let x_window = results.iter().find(|r| r.0 == "x" && r.1 == 0);
    let y_window = results.iter().find(|r| r.0 == "y" && r.1 == 0);
    let z_window = results.iter().find(|r| r.0 == "z" && r.1 == 0);

    assert!(x_window.is_some(), "expected [0,10) window for 'x'");
    assert_eq!(x_window.unwrap().3, 2, "x:[0,10) should have count=2");

    assert!(y_window.is_some(), "expected [0,10) window for 'y'");
    assert_eq!(y_window.unwrap().3, 1, "y:[0,10) should have count=1");

    assert!(z_window.is_some(), "expected [0,10) window for 'z'");
    assert_eq!(z_window.unwrap().3, 1, "z:[0,10) should have count=1");

    let _ = std::fs::remove_dir_all(&dir);
}

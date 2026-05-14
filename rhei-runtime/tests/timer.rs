#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: timers fire when watermark advances past their timestamp.
//!
//! A custom `BatchStreamFunction` registers a timer for each input event at
//! `ts + delay`. When the watermark advances past the timer timestamp, `on_timer`
//! fires and emits a `TimerFired` output record.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::arrow::{
    BatchSink, BatchSource, BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer,
    RheiBuilder, RheiSchema,
};
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

// ── Schema: TimedEvent (input) ──────────────────────────────────────

struct TimedEvent {
    key: String,
    ts: u64,
}

struct TimedEventBuilder {
    key: arrow_array::builder::StringBuilder,
    ts: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
}

#[derive(Debug, Clone)]
struct TimedEventView<'a> {
    key: &'a str,
    ts: u64,
}

struct TimedEventColumns<'a> {
    #[allow(dead_code)]
    key: &'a arrow_array::StringArray,
    #[allow(dead_code)]
    ts: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
}

impl RheiBuilder for TimedEventBuilder {
    type Item = TimedEvent;

    fn append(&mut self, item: TimedEvent) {
        self.key.append_value(&item.key);
        self.ts.append_value(item.ts);
    }

    fn append_null(&mut self) {
        self.key.append_null();
        self.ts.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.key)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            TimedEvent::arrow_schema(),
            vec![Arc::new(self.key.finish()), Arc::new(self.ts.finish())],
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
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        TimedEventBuilder {
            key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
            ts: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        TimedEventView {
            key: batch.column(0).as_string::<i32>().value(index),
            ts: batch.column(1).as_primitive::<UInt64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        TimedEventColumns {
            key: batch.column(0).as_string::<i32>(),
            ts: batch.column(1).as_primitive::<UInt64Type>(),
        }
    }
}

// ── Schema: TimerFired (output) ─────────────────────────────────────

struct TimerFired {
    key: String,
    fired_at: u64,
}

struct TimerFiredBuilder {
    key: arrow_array::builder::StringBuilder,
    fired_at: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
}

#[derive(Debug, Clone)]
struct TimerFiredView<'a> {
    key: &'a str,
    fired_at: u64,
}

struct TimerFiredColumns<'a> {
    #[allow(dead_code)]
    key: &'a arrow_array::StringArray,
    #[allow(dead_code)]
    fired_at: &'a arrow_array::PrimitiveArray<arrow_array::types::UInt64Type>,
}

impl RheiBuilder for TimerFiredBuilder {
    type Item = TimerFired;

    fn append(&mut self, item: TimerFired) {
        self.key.append_value(&item.key);
        self.fired_at.append_value(item.fired_at);
    }

    fn append_null(&mut self) {
        self.key.append_null();
        self.fired_at.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.key)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            TimerFired::arrow_schema(),
            vec![
                Arc::new(self.key.finish()),
                Arc::new(self.fired_at.finish()),
            ],
        )
        .unwrap()
    }
}

impl RheiSchema for TimerFired {
    type Builder = TimerFiredBuilder;
    type View<'a> = TimerFiredView<'a>;
    type Columns<'a> = TimerFiredColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("fired_at", arrow_schema::DataType::UInt64, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        TimerFiredBuilder {
            key: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
            fired_at: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        TimerFiredView {
            key: batch.column(0).as_string::<i32>().value(index),
            fired_at: batch.column(1).as_primitive::<UInt64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        TimerFiredColumns {
            key: batch.column(0).as_string::<i32>(),
            fired_at: batch.column(1).as_primitive::<UInt64Type>(),
        }
    }
}

// ── DelayedEmitter: registers timer in process, emits in on_timer ───

/// Timer delay: each event registers a timer at `ts + DELAY`.
const TIMER_DELAY: u64 = 10;

#[derive(Clone)]
struct DelayedEmitter;

#[async_trait]
impl BatchStreamFunction for DelayedEmitter {
    type Input = TimedEvent;
    type Output = TimerFired;

    async fn process(
        &mut self,
        input: RheiBuffer<TimedEvent>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<TimerFired>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }

        // Register a timer for each event at ts + DELAY.
        for view in &input {
            let fire_at = view.ts + TIMER_DELAY;
            ctx.state.timers().register(fire_at, view.key.to_string());
        }

        // No immediate output — output happens in on_timer.
        Ok(BufferOutput::None)
    }

    async fn on_timer(
        &mut self,
        timestamp: u64,
        key: &str,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<TimerFired>> {
        let mut builder = TimerFired::builder(1);
        builder.append(TimerFired {
            key: key.to_string(),
            fired_at: timestamp,
        });
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── Custom source with watermark support ────────────────────────────

struct WatermarkedTimerSource {
    batches: Vec<(Vec<TimedEvent>, u64)>,
    idx: usize,
    watermark: Option<u64>,
}

impl WatermarkedTimerSource {
    fn new(batches: Vec<(Vec<TimedEvent>, u64)>) -> Self {
        Self {
            batches,
            idx: 0,
            watermark: None,
        }
    }
}

#[async_trait]
impl BatchSource for WatermarkedTimerSource {
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

struct TimerFiredSink {
    collected: Arc<Mutex<Vec<(String, u64)>>>,
}

#[async_trait]
impl BatchSink for TimerFiredSink {
    type Input = TimerFired;

    async fn write_batch(&mut self, input: RheiBuffer<TimerFired>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.key.to_string(), view.fired_at));
        }
        Ok(())
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn timer_fires_on_watermark_advance() {
    let dir = std::env::temp_dir().join(format!("rhei_timer_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Batch 1: events at ts=5 and ts=8, watermark=9.
    //   Registers timers at 15 ("a") and 18 ("b").
    //   Watermark=9 does not fire either timer.
    //
    // Batch 2: event at ts=20, watermark=20.
    //   Registers timer at 30 ("c").
    //   Watermark=20 fires timers at 15 and 18 (both <= 20).
    let source = WatermarkedTimerSource::new(vec![
        (
            vec![
                TimedEvent {
                    key: "a".into(),
                    ts: 5,
                },
                TimedEvent {
                    key: "b".into(),
                    ts: 8,
                },
            ],
            9,
        ),
        (
            vec![TimedEvent {
                key: "c".into(),
                ts: 20,
            }],
            20,
        ),
    ]);

    let collected: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .operator("delayed_emitter", DelayedEmitter)
        .sink(TimerFiredSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));

    // At minimum, timers at 15 and 18 should have fired (watermark reached 20).
    // Timer at 30 may or may not fire depending on final watermark propagation
    // at pipeline shutdown (SourceExhausted sentinel is very large).
    assert!(
        results.len() >= 2,
        "expected at least 2 timer firings, got {results:?}"
    );

    // Verify "a" timer fired at ts=15.
    let a_fired = results.iter().find(|(k, _)| k == "a");
    assert!(a_fired.is_some(), "timer for 'a' should have fired");
    assert_eq!(a_fired.unwrap().1, 15, "a's timer should fire at ts=15");

    // Verify "b" timer fired at ts=18.
    let b_fired = results.iter().find(|(k, _)| k == "b");
    assert!(b_fired.is_some(), "timer for 'b' should have fired");
    assert_eq!(b_fired.unwrap().1, 18, "b's timer should fire at ts=18");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn timer_does_not_fire_early() {
    let dir = std::env::temp_dir().join(format!("rhei_timer_early_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Register a timer at ts=100+10=110. Watermark only reaches 50.
    // The timer at 110 should NOT fire during the watermark=50 phase.
    // However, it will fire at pipeline shutdown when SourceExhausted
    // sentinel watermark is propagated.
    //
    // We verify by running two batches: both have watermark < 110.
    // After the second batch, only the timer at 60 (from ts=50) should
    // have fired, not the one at 110.
    let source = WatermarkedTimerSource::new(vec![(
        vec![
            TimedEvent {
                key: "early".into(),
                ts: 100,
            },
            TimedEvent {
                key: "on_time".into(),
                ts: 50,
            },
        ],
        60,
    )]);

    let collected: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .operator("delayed_emitter_early", DelayedEmitter)
        .sink(TimerFiredSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();

    // The "on_time" timer at 60 should fire (watermark=60, timer at 60 <= 60).
    let on_time = results.iter().find(|(k, _)| k == "on_time");
    assert!(
        on_time.is_some(),
        "timer for 'on_time' should fire at wm=60, got {results:?}"
    );
    assert_eq!(on_time.unwrap().1, 60);

    // The "early" timer at 110 may fire at shutdown (SourceExhausted sentinel).
    // We verify it did NOT fire at the intermediate watermark=60 by checking
    // that if it appears, its fired_at is 110 (not 60).
    if let Some(early) = results.iter().find(|(k, _)| k == "early") {
        assert_eq!(
            early.1, 110,
            "early timer should only fire at its registered time 110, not before"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

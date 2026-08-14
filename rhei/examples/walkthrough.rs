//! The finished pipeline from `docs/walkthrough.md`.
//!
//! Sensor readings are filtered, keyed by sensor, run through a stateful
//! overheat detector that alerts on the *transition* past a threshold, and
//! aggregated into per-sensor tumbling windows. Both the alerts and the window
//! statistics are collected in memory and asserted at the end, so the example
//! doubles as an end-to-end check.
//!
//! Run with: `cargo run -p rhei --example walkthrough`

use std::sync::{Arc, Mutex};

use rhei::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, Sink, StreamFunction,
};
use rhei::{DataflowGraph, KeyedState, PipelineController, TumblingWindow, VecSource};

// ── Schemas ─────────────────────────────────────────────────────────

#[derive(Clone, rhei::RheiSchema)]
struct Reading {
    sensor_id: String,
    celsius: f64,
    timestamp: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Alert {
    sensor_id: String,
    message: String,
    timestamp: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct WindowStats {
    sensor_id: String,
    window_start: u64,
    window_end: u64,
    count: u64,
    max_celsius: f64,
}

// ── Stateful operator ───────────────────────────────────────────────

/// Alerts the first time a sensor crosses `threshold`, not on every hot
/// reading. The previous peak per sensor lives in keyed state, so the operator
/// only fires on the transition.
#[derive(Clone)]
struct OverheatDetector {
    threshold: f64,
}

#[async_trait::async_trait]
impl StreamFunction for OverheatDetector {
    type Input = Reading;
    type Output = Alert;

    async fn process(
        &mut self,
        input: RheiBuffer<Reading>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Alert>> {
        let mut builder = Alert::builder(input.len());
        let mut peaks = KeyedState::<String, f64>::new(&mut ctx.state, "peak");

        for view in &input {
            let sensor_id = view.sensor_id.to_string();
            let previous_peak = peaks.get(&sensor_id).await?.unwrap_or(f64::MIN);

            if view.celsius > previous_peak {
                peaks.put(&sensor_id, &view.celsius)?;
            }

            if view.celsius > self.threshold && previous_peak <= self.threshold {
                builder.append(Alert {
                    sensor_id,
                    message: format!("crossed {:.1}C", self.threshold),
                    timestamp: view.timestamp,
                });
            }
        }

        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── Collecting sink ─────────────────────────────────────────────────

/// The pattern used throughout `rhei-runtime/tests/`: pair `VecSource` with a
/// sink that collects into a shared `Vec` and assert on it afterwards.
#[derive(Clone)]
struct CollectSink<T> {
    rows: Arc<Mutex<Vec<T>>>,
}

impl<T> CollectSink<T> {
    fn new() -> (Self, Arc<Mutex<Vec<T>>>) {
        let rows = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                rows: Arc::clone(&rows),
            },
            rows,
        )
    }
}

#[async_trait::async_trait]
impl Sink for CollectSink<(String, String)> {
    type Input = Alert;

    async fn write_batch(&mut self, batch: RheiBuffer<Alert>) -> anyhow::Result<()> {
        let mut rows = self
            .rows
            .lock()
            .map_err(|_| anyhow::anyhow!("alert sink lock poisoned"))?;
        for view in &batch {
            rows.push((view.sensor_id.to_string(), view.message.to_string()));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for CollectSink<(String, u64, f64)> {
    type Input = WindowStats;

    async fn write_batch(&mut self, batch: RheiBuffer<WindowStats>) -> anyhow::Result<()> {
        let mut rows = self
            .rows
            .lock()
            .map_err(|_| anyhow::anyhow!("window sink lock poisoned"))?;
        for view in &batch {
            rows.push((view.sensor_id.to_string(), view.count, view.max_celsius));
        }
        Ok(())
    }
}

// ── Pipeline ────────────────────────────────────────────────────────

fn readings() -> Vec<Reading> {
    // Two sensors. boiler-1 crosses 90C once; boiler-2 never does.
    vec![
        Reading {
            sensor_id: "boiler-1".into(),
            celsius: 68.0,
            timestamp: 1_000,
        },
        Reading {
            sensor_id: "boiler-2".into(),
            celsius: 55.0,
            timestamp: 1_100,
        },
        Reading {
            sensor_id: "boiler-1".into(),
            celsius: 91.5,
            timestamp: 2_000,
        },
        Reading {
            sensor_id: "boiler-1".into(),
            celsius: 93.0,
            timestamp: 3_000,
        },
        Reading {
            sensor_id: "boiler-2".into(),
            celsius: 61.0,
            timestamp: 3_100,
        },
    ]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rhei_walkthrough_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let (alert_sink, alerts) = CollectSink::<(String, String)>::new();
    let (window_sink, windows) = CollectSink::<(String, u64, f64)>::new();

    let window = TumblingWindow::new(
        60_000,
        |v: ReadingView<'_>| v.sensor_id.to_string(),
        |v: ReadingView<'_>| v.timestamp,
        |acc: &mut (u64, f64), v: ReadingView<'_>| {
            acc.0 += 1;
            acc.1 = acc.1.max(v.celsius);
        },
        |key: &str, window_start: u64, window_end: u64, acc: &(u64, f64)| WindowStats {
            sensor_id: key.to_string(),
            window_start,
            window_end,
            count: acc.0,
            max_celsius: acc.1,
        },
    )
    .with_allowed_lateness(5_000);

    let graph = DataflowGraph::new();

    // `Stream` is `Copy`, so reusing the keyed handle fans out to two branches.
    let keyed = graph
        .source(VecSource::new(readings()))
        .filter_fn(|r| r.celsius > 0.0) // drop obviously broken probes
        .key_by(|r| r.sensor_id.to_string());

    keyed
        .operator("overheat_detector", OverheatDetector { threshold: 90.0 })
        .name("alerts")
        .sink(alert_sink);

    keyed
        .operator("temp_window", window)
        .name("windows")
        .sink(window_sink);

    PipelineController::builder()
        .checkpoint_dir(&dir)
        .workers(1)
        .build()?
        .run(graph)
        .await?;

    // ── Results ─────────────────────────────────────────────────────

    let alerts = alerts
        .lock()
        .map_err(|_| anyhow::anyhow!("alert lock poisoned"))?
        .clone();
    let mut windows = windows
        .lock()
        .map_err(|_| anyhow::anyhow!("window lock poisoned"))?
        .clone();
    windows.sort_by(|a, b| a.0.cmp(&b.0));

    println!("alerts:");
    for (sensor, message) in &alerts {
        println!("  {sensor}: {message}");
    }

    println!("windows:");
    for (sensor, count, max_celsius) in &windows {
        println!("  {sensor}: {count} readings, max {max_celsius:.1}C");
    }

    // boiler-1 crosses the threshold exactly once, despite two hot readings.
    assert_eq!(alerts.len(), 1, "expected exactly one transition alert");
    assert_eq!(alerts[0].0, "boiler-1");

    // All timestamps fall in the first 60s window, so one window per sensor.
    assert_eq!(windows.len(), 2, "expected one window per sensor");
    assert_eq!(windows[0], ("boiler-1".to_string(), 3, 93.0));
    assert_eq!(windows[1], ("boiler-2".to_string(), 2, 61.0));

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

//! Tumbling window aggregation example over Timely Dataflow.
//!
//! Sensor readings are aggregated per-sensor over fixed 10-second tumbling
//! windows. When a reading's timestamp crosses into a new window, the previous
//! window's aggregate (count, sum, avg) is emitted.
//!
//! Uses `run_dataflow` to execute the pipeline as a Timely computation with
//! epoch-based progress tracking and automatic checkpointing.
//!
//! Run with: `cargo run -p rill-runtime --example window_agg`

use async_trait::async_trait;
use rill_core::connectors::print_sink::PrintSink;
use rill_core::connectors::vec_source::VecSource;
use rill_core::state::context::StateContext;
use rill_core::traits::StreamFunction;
use rill_runtime::executor::{Executor, OperatorSlot};
use serde::{Deserialize, Serialize};

/// A single sensor reading with a logical timestamp.
#[derive(Clone, Serialize, Deserialize)]
struct SensorReading {
    sensor_id: String,
    value: f64,
    timestamp: u64,
}

impl std::fmt::Display for SensorReading {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}={:.1}@t={}",
            self.sensor_id, self.value, self.timestamp
        )
    }
}

/// Running accumulator for a single window.
#[derive(Serialize, Deserialize)]
struct WindowAccumulator {
    count: u64,
    sum: f64,
}

/// Tumbling window aggregation operator.
///
/// Groups sensor readings into fixed-size time windows and emits per-sensor
/// aggregates (count, sum, average) when a window closes.
struct TumblingWindow {
    window_size: u64,
}

#[async_trait]
impl StreamFunction for TumblingWindow {
    type Input = SensorReading;
    type Output = String;

    async fn process(
        &mut self,
        input: SensorReading,
        ctx: &mut StateContext,
    ) -> Vec<String> {
        let window_start = input.timestamp - (input.timestamp % self.window_size);
        let mut outputs = Vec::new();

        // Check the active window for this sensor
        let win_key = format!("win:{}", input.sensor_id);
        let active_window = ctx
            .get(win_key.as_bytes())
            .await
            .unwrap_or(None)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])));

        // If the active window differs, close the old one and emit its aggregate
        if let Some(old_start) = active_window
            && old_start != window_start
        {
            let acc_key = format!("acc:{}:{old_start}", input.sensor_id);
            if let Some(bytes) = ctx.get(acc_key.as_bytes()).await.unwrap_or(None) {
                let acc: WindowAccumulator = serde_json::from_slice(&bytes).unwrap();
                let old_end = old_start + self.window_size;
                #[allow(clippy::cast_precision_loss)]
                let avg = acc.sum / acc.count as f64;
                outputs.push(format!(
                    "window: {} [{old_start}..{old_end}) count={} sum={:.2} avg={avg:.2}",
                    input.sensor_id, acc.count, acc.sum
                ));
            }
            // Clean up old accumulator
            let old_acc_key = format!("acc:{}:{old_start}", input.sensor_id);
            ctx.delete(old_acc_key.as_bytes());
        }

        // Load or create accumulator for the current window
        let acc_key = format!("acc:{}:{window_start}", input.sensor_id);
        let mut acc = ctx
            .get(acc_key.as_bytes())
            .await
            .unwrap_or(None)
            .map_or(WindowAccumulator { count: 0, sum: 0.0 }, |bytes| {
                serde_json::from_slice::<WindowAccumulator>(&bytes).unwrap()
            });

        acc.count += 1;
        acc.sum += input.value;

        let data = serde_json::to_vec(&acc).unwrap();
        ctx.put(acc_key.as_bytes(), &data);

        // Update active window tracker
        ctx.put(win_key.as_bytes(), &window_start.to_le_bytes());

        outputs
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rill_window_agg_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let executor = Executor::new(dir.clone());
    let ctx = executor.create_context("tumbling_window").await?;

    // Two sensors across three 10-second windows (0-9, 10-19, 20-29)
    let source = VecSource::new(vec![
        SensorReading { sensor_id: "temp-1".into(), value: 22.5, timestamp: 1 },
        SensorReading { sensor_id: "temp-2".into(), value: 18.3, timestamp: 2 },
        SensorReading { sensor_id: "temp-1".into(), value: 23.1, timestamp: 5 },
        SensorReading { sensor_id: "temp-2".into(), value: 19.0, timestamp: 7 },
        // These cross into window 10 → close window 0 for each sensor
        SensorReading { sensor_id: "temp-1".into(), value: 24.0, timestamp: 12 },
        SensorReading { sensor_id: "temp-2".into(), value: 17.5, timestamp: 13 },
        SensorReading { sensor_id: "temp-1".into(), value: 23.8, timestamp: 18 },
        SensorReading { sensor_id: "temp-2".into(), value: 20.1, timestamp: 19 },
        // These cross into window 20 → close window 10 for each sensor
        SensorReading { sensor_id: "temp-1".into(), value: 25.0, timestamp: 22 },
        SensorReading { sensor_id: "temp-2".into(), value: 21.0, timestamp: 25 },
    ]);

    let operators = vec![OperatorSlot::new(
        "tumbling_window",
        TumblingWindow { window_size: 10 },
        ctx,
    )];
    let sink: PrintSink<String> = PrintSink::new().with_prefix("output");

    executor
        .run_dataflow(source, operators, sink)
        .await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

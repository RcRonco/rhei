//! Tumbling window aggregation example using the dataflow API.
//!
//! Sensor readings are aggregated per-sensor over fixed 10-second tumbling
//! windows using [`TumblingWindow`] with the built-in [`Avg`] aggregator.
//! When a reading's timestamp crosses into a new window, the previous
//! window's average is emitted.
//!
//! Run with: `cargo run -p rill-runtime --example window_agg`

use rill_core::connectors::print_sink::PrintSink;
use rill_core::connectors::vec_source::VecSource;
use rill_core::operators::{Avg, TumblingWindow, WindowOutput};
use rill_runtime::executor::Executor;
use serde::{Deserialize, Serialize};

/// A single sensor reading with a logical timestamp.
#[derive(Clone, Serialize, Deserialize)]
struct SensorReading {
    sensor_id: String,
    value: f64,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rill_window_agg_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let executor = Executor::builder().checkpoint_dir(&dir).build();

    // Two sensors across three 10-second windows (0-9, 10-19, 20-29)
    let source = VecSource::new(vec![
        SensorReading {
            sensor_id: "temp-1".into(),
            value: 22.5,
            timestamp: 1,
        },
        SensorReading {
            sensor_id: "temp-2".into(),
            value: 18.3,
            timestamp: 2,
        },
        SensorReading {
            sensor_id: "temp-1".into(),
            value: 23.1,
            timestamp: 5,
        },
        SensorReading {
            sensor_id: "temp-2".into(),
            value: 19.0,
            timestamp: 7,
        },
        // These cross into window 10 → close window 0 for each sensor
        SensorReading {
            sensor_id: "temp-1".into(),
            value: 24.0,
            timestamp: 12,
        },
        SensorReading {
            sensor_id: "temp-2".into(),
            value: 17.5,
            timestamp: 13,
        },
        SensorReading {
            sensor_id: "temp-1".into(),
            value: 23.8,
            timestamp: 18,
        },
        SensorReading {
            sensor_id: "temp-2".into(),
            value: 20.1,
            timestamp: 19,
        },
        // These cross into window 20 → close window 10 for each sensor
        SensorReading {
            sensor_id: "temp-1".into(),
            value: 25.0,
            timestamp: 22,
        },
        SensorReading {
            sensor_id: "temp-2".into(),
            value: 21.0,
            timestamp: 25,
        },
    ]);

    let op = TumblingWindow::builder()
        .window_size(10)
        .key_fn(|r: &SensorReading| r.sensor_id.clone())
        .time_fn(|r: &SensorReading| r.timestamp)
        .aggregator(Avg::new(|r: &SensorReading| r.value))
        .build();

    let readings = executor.source(source);
    readings
        .key_by(|r: &SensorReading| r.sensor_id.clone())
        .operator("tumbling_window", op)
        .sink(PrintSink::<WindowOutput<f64>>::new().with_prefix("output"));

    executor.run().await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

//! Batch tumbling window aggregation example using the Arrow columnar pipeline.
//!
//! Sensor readings are aggregated per-sensor over fixed 10-second tumbling
//! windows using `TumblingWindow` with a sum accumulator.
//! Windows close when data crosses into a new time bucket or on source exhaustion.
//!
//! Run with: `cargo run -p rhei --example batch_window_agg`

use rhei::{
    DataflowGraph, PipelineController, PrintSink, RheiSchema as RheiSchemaDerive, TumblingWindow,
    VecSource,
};

#[derive(Debug, Clone, RheiSchemaDerive)]
struct SensorReading {
    sensor_id: String,
    value: f64,
    timestamp: u64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct WindowResult {
    sensor_id: String,
    window_start: u64,
    window_end: u64,
    sum: f64,
    count: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rhei_batch_window_agg_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let readings = vec![
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
        // Cross into window [10,20) → close window [0,10) for each sensor
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
        // Cross into window [20,30) → close window [10,20) for each sensor
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
    ];

    let source = VecSource::new(readings).with_batch_size(10);

    let window = TumblingWindow::new(
        10,
        |view: SensorReadingView<'_>| view.sensor_id.to_string(),
        |view: SensorReadingView<'_>| view.timestamp,
        |acc: &mut (f64, u64), view: SensorReadingView<'_>| {
            acc.0 += view.value;
            acc.1 += 1;
        },
        |key: &str, window_start: u64, window_end: u64, acc: &(f64, u64)| WindowResult {
            sensor_id: key.to_string(),
            window_start,
            window_end,
            sum: acc.0,
            count: acc.1,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .operator("sensor_window", window)
        .sink(PrintSink::<WindowResult>::new().with_prefix("output"));

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

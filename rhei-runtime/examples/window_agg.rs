#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Tumbling window aggregation example using the batch Arrow dataflow API.
//!
//! Sensor readings are aggregated per-sensor over fixed 10-second tumbling
//! windows using [`TumblingWindow`] with a `(f64, u64)` accumulator for average.
//!
//! Run with: `cargo run -p rhei-runtime --example window_agg`

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::ArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use rhei_core::arrow::{RheiBuilder, RheiSchema};
use rhei_core::connectors::batch::{PrintSink, VecSource};
use rhei_core::operators::TumblingWindow;
use rhei_runtime::Executor;
use rhei_runtime::dataflow::DataflowGraph;

// ── SensorReading schema ────────────────────────────────────────────

struct SensorReading {
    sensor_id: String,
    value: f64,
    timestamp: u64,
}

struct SensorReadingBuilder {
    sensor_id: arrow_array::builder::StringBuilder,
    value: arrow_array::builder::PrimitiveBuilder<Float64Type>,
    timestamp: arrow_array::builder::PrimitiveBuilder<UInt64Type>,
}

#[derive(Debug)]
struct SensorReadingView<'a> {
    sensor_id: &'a str,
    value: f64,
    timestamp: u64,
}

struct SensorReadingCols<'a> {
    #[allow(dead_code)]
    sensor_id: &'a arrow_array::StringArray,
    #[allow(dead_code)]
    value: &'a arrow_array::PrimitiveArray<Float64Type>,
    #[allow(dead_code)]
    timestamp: &'a arrow_array::PrimitiveArray<UInt64Type>,
}

impl RheiBuilder for SensorReadingBuilder {
    type Item = SensorReading;
    fn append(&mut self, item: SensorReading) {
        self.sensor_id.append_value(&item.sensor_id);
        self.value.append_value(item.value);
        self.timestamp.append_value(item.timestamp);
    }
    fn append_null(&mut self) {
        self.sensor_id.append_null();
        self.value.append_null();
        self.timestamp.append_null();
    }
    fn len(&self) -> usize {
        self.sensor_id.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            SensorReading::arrow_schema(),
            vec![
                Arc::new(self.sensor_id.finish()),
                Arc::new(self.value.finish()),
                Arc::new(self.timestamp.finish()),
            ],
        )
        .expect("SensorReading schema mismatch")
    }
}

impl RheiSchema for SensorReading {
    type Builder = SensorReadingBuilder;
    type View<'a> = SensorReadingView<'a>;
    type Columns<'a> = SensorReadingCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("sensor_id", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::UInt64, false),
        ]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        SensorReadingBuilder {
            sensor_id: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            value: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            timestamp: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        SensorReadingView {
            sensor_id: batch.column(0).as_string::<i32>().value(index),
            value: batch.column(1).as_primitive::<Float64Type>().value(index),
            timestamp: batch.column(2).as_primitive::<UInt64Type>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        SensorReadingCols {
            sensor_id: batch.column(0).as_string::<i32>(),
            value: batch.column(1).as_primitive::<Float64Type>(),
            timestamp: batch.column(2).as_primitive::<UInt64Type>(),
        }
    }
}

// ── WindowAvg output schema ─────────────────────────────────────────

struct WindowAvg {
    sensor_id: String,
    window_start: u64,
    window_end: u64,
    avg_value: f64,
}

struct WindowAvgBuilder {
    sensor_id: arrow_array::builder::StringBuilder,
    window_start: arrow_array::builder::PrimitiveBuilder<UInt64Type>,
    window_end: arrow_array::builder::PrimitiveBuilder<UInt64Type>,
    avg_value: arrow_array::builder::PrimitiveBuilder<Float64Type>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct WindowAvgView<'a> {
    sensor_id: &'a str,
    window_start: u64,
    window_end: u64,
    avg_value: f64,
}

struct WindowAvgCols<'a> {
    #[allow(dead_code)]
    sensor_id: &'a arrow_array::StringArray,
    #[allow(dead_code)]
    avg_value: &'a arrow_array::PrimitiveArray<Float64Type>,
}

impl RheiBuilder for WindowAvgBuilder {
    type Item = WindowAvg;
    fn append(&mut self, item: WindowAvg) {
        self.sensor_id.append_value(&item.sensor_id);
        self.window_start.append_value(item.window_start);
        self.window_end.append_value(item.window_end);
        self.avg_value.append_value(item.avg_value);
    }
    fn append_null(&mut self) {
        self.sensor_id.append_null();
        self.window_start.append_null();
        self.window_end.append_null();
        self.avg_value.append_null();
    }
    fn len(&self) -> usize {
        self.sensor_id.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            WindowAvg::arrow_schema(),
            vec![
                Arc::new(self.sensor_id.finish()),
                Arc::new(self.window_start.finish()),
                Arc::new(self.window_end.finish()),
                Arc::new(self.avg_value.finish()),
            ],
        )
        .expect("WindowAvg schema mismatch")
    }
}

impl RheiSchema for WindowAvg {
    type Builder = WindowAvgBuilder;
    type View<'a> = WindowAvgView<'a>;
    type Columns<'a> = WindowAvgCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("sensor_id", DataType::Utf8, false),
            Field::new("window_start", DataType::UInt64, false),
            Field::new("window_end", DataType::UInt64, false),
            Field::new("avg_value", DataType::Float64, false),
        ]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        WindowAvgBuilder {
            sensor_id: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            window_start: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            window_end: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            avg_value: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        WindowAvgView {
            sensor_id: batch.column(0).as_string::<i32>().value(index),
            window_start: batch.column(1).as_primitive::<UInt64Type>().value(index),
            window_end: batch.column(2).as_primitive::<UInt64Type>().value(index),
            avg_value: batch.column(3).as_primitive::<Float64Type>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        WindowAvgCols {
            sensor_id: batch.column(0).as_string::<i32>(),
            avg_value: batch.column(3).as_primitive::<Float64Type>(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rhei_window_agg_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let graph = DataflowGraph::new();

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
        // Cross into window 10 -> close window 0 for each sensor
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
        // Cross into window 20 -> close window 10 for each sensor
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

    // Accumulator: (sum, count)
    let op = TumblingWindow::new(
        10,
        |v: SensorReadingView<'_>| v.sensor_id.to_string(),
        |v: SensorReadingView<'_>| v.timestamp,
        |acc: &mut (f64, u64), v: SensorReadingView<'_>| {
            acc.0 += v.value;
            acc.1 += 1;
        },
        |key: &str, window_start: u64, window_end: u64, acc: &(f64, u64)| WindowAvg {
            sensor_id: key.to_string(),
            window_start,
            window_end,
            avg_value: if acc.1 > 0 { acc.0 / acc.1 as f64 } else { 0.0 },
        },
    );

    let readings = graph.source(source);
    readings
        .key_by(|v: &SensorReadingView<'_>| v.sensor_id.to_string())
        .operator("tumbling_window", op)
        .sink(PrintSink::<WindowAvg>::new().with_prefix("output"));

    let executor = Executor::builder().checkpoint_dir(&dir).build()?;
    executor.run(graph).await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

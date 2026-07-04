#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Temporal join example using the batch Arrow dataflow API.
//!
//! Interleaved order and shipment events are joined by `order_id` using
//! [`TemporalJoin`]. When both sides of a join arrive, a matched result is
//! emitted. Unmatched events are buffered in operator state.
//!
//! Run with: `cargo run -p rhei-runtime --example temporal_join`

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::ArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::{DataType, Field, Schema};
use rhei_core::arrow::{RheiBuilder, RheiSchema};
use rhei_core::connectors::batch::{PrintSink, VecSource};
use rhei_core::operators::TemporalJoin;
use rhei_core::operators::temporal_join::Side;
use rhei_runtime::Executor;
use rhei_runtime::dataflow::DataflowGraph;

// ── OrderEvent: combined tagged schema ──────────────────────────────
// side: 0 = order (left), 1 = shipment (right)
// Fields shared across both sides via a flat union layout.

struct OrderEvent {
    side: i64,
    order_id: String,
    detail: String,
}

struct OrderEventBuilder {
    side: arrow_array::builder::Int64Builder,
    order_id: arrow_array::builder::StringBuilder,
    detail: arrow_array::builder::StringBuilder,
}

#[derive(Debug)]
struct OrderEventView<'a> {
    side: i64,
    order_id: &'a str,
    detail: &'a str,
}

struct OrderEventCols<'a> {
    #[allow(dead_code)]
    side: &'a arrow_array::Int64Array,
}

impl RheiBuilder for OrderEventBuilder {
    type Item = OrderEvent;
    fn append(&mut self, item: OrderEvent) {
        self.side.append_value(item.side);
        self.order_id.append_value(&item.order_id);
        self.detail.append_value(&item.detail);
    }
    fn append_null(&mut self) {
        self.side.append_null();
        self.order_id.append_null();
        self.detail.append_null();
    }
    fn len(&self) -> usize {
        self.side.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            OrderEvent::arrow_schema(),
            vec![
                Arc::new(self.side.finish()),
                Arc::new(self.order_id.finish()),
                Arc::new(self.detail.finish()),
            ],
        )
        .expect("OrderEvent schema mismatch")
    }
}

impl RheiSchema for OrderEvent {
    type Builder = OrderEventBuilder;
    type View<'a> = OrderEventView<'a>;
    type Columns<'a> = OrderEventCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("side", DataType::Int64, false),
            Field::new("order_id", DataType::Utf8, false),
            Field::new("detail", DataType::Utf8, false),
        ]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        OrderEventBuilder {
            side: arrow_array::builder::Int64Builder::with_capacity(capacity),
            order_id: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            detail: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 64),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        OrderEventView {
            side: batch.column(0).as_primitive::<Int64Type>().value(index),
            order_id: batch.column(1).as_string::<i32>().value(index),
            detail: batch.column(2).as_string::<i32>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        OrderEventCols {
            side: batch.column(0).as_primitive::<Int64Type>(),
        }
    }
}

// ── JoinResult output schema ────────────────────────────────────────

struct JoinResult {
    result: String,
}

struct JoinResultBuilder {
    result: arrow_array::builder::StringBuilder,
}

#[derive(Debug)]
#[allow(dead_code)]
struct JoinResultView<'a> {
    result: &'a str,
}

struct JoinResultCols<'a> {
    #[allow(dead_code)]
    result: &'a arrow_array::StringArray,
}

impl RheiBuilder for JoinResultBuilder {
    type Item = JoinResult;
    fn append(&mut self, item: JoinResult) {
        self.result.append_value(&item.result);
    }
    fn append_null(&mut self) {
        self.result.append_null();
    }
    fn len(&self) -> usize {
        self.result.len()
    }
    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            JoinResult::arrow_schema(),
            vec![Arc::new(self.result.finish())],
        )
        .expect("JoinResult schema mismatch")
    }
}

impl RheiSchema for JoinResult {
    type Builder = JoinResultBuilder;
    type View<'a> = JoinResultView<'a>;
    type Columns<'a> = JoinResultCols<'a>;

    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Utf8,
            false,
        )]))
    }
    fn builder(capacity: usize) -> Self::Builder {
        JoinResultBuilder {
            result: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 64),
        }
    }
    fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
        JoinResultView {
            result: batch.column(0).as_string::<i32>().value(index),
        }
    }
    fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
        JoinResultCols {
            result: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn order(order_id: &str, item: &str, amount: f64) -> OrderEvent {
    OrderEvent {
        side: 0,
        order_id: order_id.into(),
        detail: format!("{item} (${amount:.2})"),
    }
}

fn shipment(order_id: &str, carrier: &str, tracking: &str) -> OrderEvent {
    OrderEvent {
        side: 1,
        order_id: order_id.into(),
        detail: format!("{carrier} [{tracking}]"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rhei_temporal_join_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let graph = DataflowGraph::new();

    let source = VecSource::new(vec![
        order("ORD-001", "Laptop", 999.99),
        order("ORD-002", "Keyboard", 79.50),
        shipment("ORD-002", "FedEx", "FX-100"),
        shipment("ORD-003", "UPS", "UP-200"),
        order("ORD-003", "Mouse", 29.99),
        shipment("ORD-001", "DHL", "DH-300"),
        order("ORD-004", "Monitor", 549.00), // no matching shipment
    ]);

    let op = TemporalJoin::new(
        |v: OrderEventView<'_>| v.order_id.to_string(),
        |v: OrderEventView<'_>| {
            if v.side == 0 { Side::Left } else { Side::Right }
        },
        |v: OrderEventView<'_>| v.detail.to_string(),
        |v: OrderEventView<'_>| v.detail.to_string(),
        |key: &str, left: &String, right: &String| JoinResult {
            result: format!("joined: {key} | {left} shipped via {right}"),
        },
    );

    let events = graph.source(source);
    events
        .key_by_ref(|v: &OrderEventView<'_>, _| v.order_id)
        .operator("temporal_join", op)
        .sink(PrintSink::<JoinResult>::new().with_prefix("output"));

    let executor = Executor::builder().checkpoint_dir(&dir).build()?;
    executor.run(graph).await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

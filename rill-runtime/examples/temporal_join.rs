//! Temporal join example using the operators library.
//!
//! Interleaved order and shipment events are joined by `order_id` using
//! [`TemporalJoin`]. When both sides of a join arrive, a matched result is
//! emitted. Unmatched events are buffered in operator state until their
//! counterpart appears.
//!
//! Uses `run_dataflow` to execute the pipeline as a Timely computation with
//! epoch-based progress tracking and automatic checkpointing.
//!
//! Run with: `cargo run -p rill-runtime --example temporal_join`

use rill_core::connectors::print_sink::PrintSink;
use rill_core::connectors::vec_source::VecSource;
use rill_core::operators::{JoinSide, TemporalJoin};
use rill_runtime::executor::{Executor, OperatorSlot};
use serde::{Deserialize, Serialize};

/// An order event.
#[derive(Clone, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
struct Order {
    order_id: String,
    item: String,
    amount: f64,
}

/// A shipment event.
#[derive(Clone, Serialize, Deserialize)]
struct Shipment {
    order_id: String,
    carrier: String,
    tracking: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("rill_temporal_join_example");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let executor = Executor::new(dir.clone());
    let ctx = executor.create_context("temporal_join").await?;

    let source = VecSource::new(vec![
        // ORD-001 arrives first, no shipment yet → buffered
        JoinSide::Left(Order {
            order_id: "ORD-001".into(),
            item: "Laptop".into(),
            amount: 999.99,
        }),
        // ORD-002 arrives, no shipment yet → buffered
        JoinSide::Left(Order {
            order_id: "ORD-002".into(),
            item: "Keyboard".into(),
            amount: 79.50,
        }),
        // Shipment for ORD-002 → matches → emit
        JoinSide::Right(Shipment {
            order_id: "ORD-002".into(),
            carrier: "FedEx".into(),
            tracking: "FX-100".into(),
        }),
        // Shipment for ORD-003 arrives first, no order yet → buffered
        JoinSide::Right(Shipment {
            order_id: "ORD-003".into(),
            carrier: "UPS".into(),
            tracking: "UP-200".into(),
        }),
        // Order for ORD-003 → matches buffered shipment → emit
        JoinSide::Left(Order {
            order_id: "ORD-003".into(),
            item: "Mouse".into(),
            amount: 29.99,
        }),
        // Shipment for ORD-001 → matches buffered order → emit
        JoinSide::Right(Shipment {
            order_id: "ORD-001".into(),
            carrier: "DHL".into(),
            tracking: "DH-300".into(),
        }),
        // ORD-004 has no matching shipment → stays buffered
        JoinSide::Left(Order {
            order_id: "ORD-004".into(),
            item: "Monitor".into(),
            amount: 549.00,
        }),
    ]);

    let op = TemporalJoin::builder()
        .key_fn(|side: &JoinSide<Order, Shipment>| match side {
            JoinSide::Left(o) => o.order_id.clone(),
            JoinSide::Right(s) => s.order_id.clone(),
        })
        .join_fn(|order: Order, shipment: Shipment| {
            format!(
                "joined: {} | {} (${:.2}) shipped via {} [{}]",
                order.order_id, order.item, order.amount, shipment.carrier, shipment.tracking
            )
        })
        .build();

    let operators = vec![OperatorSlot::new(
        "temporal_join",
        op,
        ctx,
        Some(tokio::runtime::Handle::current()),
    )];
    let sink: PrintSink<String> = PrintSink::new().with_prefix("output");

    executor.run_dataflow(source, operators, sink).await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

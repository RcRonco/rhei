//! Temporal join example demonstrating stateful event matching over Timely Dataflow.
//!
//! Interleaved order and shipment events are joined by `order_id`. When both
//! sides of a join arrive, a matched result is emitted. Unmatched events are
//! buffered in operator state until their counterpart appears.
//!
//! Uses `run_dataflow` to execute the pipeline as a Timely computation with
//! epoch-based progress tracking and automatic checkpointing.
//!
//! Run with: `cargo run -p rill-runtime --example temporal_join`

use async_trait::async_trait;
use rill_core::connectors::print_sink::PrintSink;
use rill_core::connectors::vec_source::VecSource;
use rill_core::state::context::StateContext;
use rill_core::traits::StreamFunction;
use rill_runtime::executor::{Executor, OperatorSlot};
use serde::{Deserialize, Serialize};

/// An input event — either an order or a shipment.
#[derive(Clone, Serialize, Deserialize)]
enum JoinInput {
    Order {
        order_id: String,
        item: String,
        amount: f64,
    },
    Shipment {
        order_id: String,
        carrier: String,
        tracking: String,
    },
}

impl std::fmt::Display for JoinInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Order {
                order_id,
                item,
                amount,
            } => write!(f, "Order({order_id}, {item}, ${amount:.2})"),
            Self::Shipment {
                order_id,
                carrier,
                tracking,
            } => write!(f, "Shipment({order_id}, {carrier}, {tracking})"),
        }
    }
}

/// Buffered order data stored in state.
#[derive(Serialize, Deserialize)]
struct OrderData {
    item: String,
    amount: f64,
}

/// Buffered shipment data stored in state.
#[derive(Serialize, Deserialize)]
struct ShipmentData {
    carrier: String,
    tracking: String,
}

/// A temporal join operator that matches orders with shipments by `order_id`.
struct TemporalJoin;

#[async_trait]
impl StreamFunction for TemporalJoin {
    type Input = JoinInput;
    type Output = String;

    async fn process(&mut self, input: JoinInput, ctx: &mut StateContext) -> Vec<String> {
        match input {
            JoinInput::Order {
                order_id,
                item,
                amount,
            } => {
                let shipment_key = format!("shipment:{order_id}");
                if let Some(bytes) = ctx.get(shipment_key.as_bytes()).await.unwrap_or(None) {
                    // Matching shipment already buffered — emit joined result
                    let shipment: ShipmentData = serde_json::from_slice(&bytes).unwrap();
                    ctx.delete(shipment_key.as_bytes());
                    vec![format!(
                        "joined: {order_id} | {item} (${amount:.2}) shipped via {} [{}]",
                        shipment.carrier, shipment.tracking
                    )]
                } else {
                    // No shipment yet — buffer the order
                    let data = serde_json::to_vec(&OrderData { item, amount }).unwrap();
                    let order_key = format!("order:{order_id}");
                    ctx.put(order_key.as_bytes(), &data);
                    vec![]
                }
            }
            JoinInput::Shipment {
                order_id,
                carrier,
                tracking,
            } => {
                let order_key = format!("order:{order_id}");
                if let Some(bytes) = ctx.get(order_key.as_bytes()).await.unwrap_or(None) {
                    // Matching order already buffered — emit joined result
                    let order: OrderData = serde_json::from_slice(&bytes).unwrap();
                    ctx.delete(order_key.as_bytes());
                    vec![format!(
                        "joined: {order_id} | {} (${:.2}) shipped via {carrier} [{tracking}]",
                        order.item, order.amount
                    )]
                } else {
                    // No order yet — buffer the shipment
                    let data = serde_json::to_vec(&ShipmentData { carrier, tracking }).unwrap();
                    let shipment_key = format!("shipment:{order_id}");
                    ctx.put(shipment_key.as_bytes(), &data);
                    vec![]
                }
            }
        }
    }
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
        JoinInput::Order {
            order_id: "ORD-001".into(),
            item: "Laptop".into(),
            amount: 999.99,
        },
        // ORD-002 arrives, no shipment yet → buffered
        JoinInput::Order {
            order_id: "ORD-002".into(),
            item: "Keyboard".into(),
            amount: 79.50,
        },
        // Shipment for ORD-002 → matches → emit
        JoinInput::Shipment {
            order_id: "ORD-002".into(),
            carrier: "FedEx".into(),
            tracking: "FX-100".into(),
        },
        // Shipment for ORD-003 arrives first, no order yet → buffered
        JoinInput::Shipment {
            order_id: "ORD-003".into(),
            carrier: "UPS".into(),
            tracking: "UP-200".into(),
        },
        // Order for ORD-003 → matches buffered shipment → emit
        JoinInput::Order {
            order_id: "ORD-003".into(),
            item: "Mouse".into(),
            amount: 29.99,
        },
        // Shipment for ORD-001 → matches buffered order → emit
        JoinInput::Shipment {
            order_id: "ORD-001".into(),
            carrier: "DHL".into(),
            tracking: "DH-300".into(),
        },
        // ORD-004 has no matching shipment → stays buffered
        JoinInput::Order {
            order_id: "ORD-004".into(),
            item: "Monitor".into(),
            amount: 549.00,
        },
    ]);

    let operators = vec![OperatorSlot::new("temporal_join", TemporalJoin, ctx)];
    let sink: PrintSink<String> = PrintSink::new().with_prefix("output");

    executor
        .run_dataflow(source, operators, sink)
        .await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

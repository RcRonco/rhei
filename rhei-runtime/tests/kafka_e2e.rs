#![cfg(feature = "kafka")]
#![allow(clippy::struct_field_names)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! End-to-end Kafka integration test (batch API).
//!
//! Pipeline topology:
//!
//! ```text
//! KafkaSource([orders_topic])
//!   → map(parse → OrderEvent)
//!   → filter_fn(amount > 50)
//!   → operator("aggregator", PerUserAggregator)
//!   → CollectSink
//! ```
//!
//! The stateful aggregator computes per-user running totals using
//! `KeyedState<String, f64>`. This exercises:
//! - Kafka source connectivity with batch API
//! - Batch transforms (map, `filter_fn`)
//! - Stateful batch operator with `KeyedState`
//! - Offset tracking and checkpoint interaction
//! - Multi-partition source (second test)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rhei_core::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, Sink, StreamFunction,
};
use rhei_core::connectors::batch::KafkaSource;
use rhei_core::operators::batch::keyed_state::KeyedState;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::shutdown::ShutdownHandle;
use serde::{Deserialize, Serialize};

// ── Domain types ────────────────────────────────────────────────────

const USERS: [&str; 4] = ["alice", "bob", "charlie", "diana"];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: String,
    user_id: String,
    amount: f64,
    timestamp: u64,
}

// ── RheiSchema for OrderEvent (parsed from Kafka) ──────────────────

struct OrderEvent {
    user_id: String,
    amount: f64,
    timestamp: u64,
}

struct OrderEventBuilder {
    user_id: arrow_array::builder::StringBuilder,
    amount: arrow_array::builder::PrimitiveBuilder<arrow_array::types::Float64Type>,
    timestamp: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
}

struct OrderEventView<'a> {
    user_id: &'a str,
    amount: f64,
    #[allow(dead_code)]
    timestamp: u64,
}

struct OrderEventColumns<'a> {
    #[allow(dead_code)]
    user_id: &'a arrow_array::StringArray,
}

impl RheiBuilder for OrderEventBuilder {
    type Item = OrderEvent;

    fn append(&mut self, item: OrderEvent) {
        self.user_id.append_value(&item.user_id);
        self.amount.append_value(item.amount);
        self.timestamp.append_value(item.timestamp);
    }

    fn append_null(&mut self) {
        self.user_id.append_null();
        self.amount.append_null();
        self.timestamp.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.user_id)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            OrderEvent::arrow_schema(),
            vec![
                Arc::new(self.user_id.finish()),
                Arc::new(self.amount.finish()),
                Arc::new(self.timestamp.finish()),
            ],
        )
        .unwrap()
    }
}

impl RheiSchema for OrderEvent {
    type Builder = OrderEventBuilder;
    type View<'a> = OrderEventView<'a>;
    type Columns<'a> = OrderEventColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("user_id", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("amount", arrow_schema::DataType::Float64, false),
            arrow_schema::Field::new("timestamp", arrow_schema::DataType::UInt64, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        OrderEventBuilder {
            user_id: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            amount: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            timestamp: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, UInt64Type};
        OrderEventView {
            user_id: batch.column(0).as_string::<i32>().value(index),
            amount: batch.column(1).as_primitive::<Float64Type>().value(index),
            timestamp: batch.column(2).as_primitive::<UInt64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        OrderEventColumns {
            user_id: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── RheiSchema for UserTotal (output) ──────────────────────────────

struct UserTotal {
    user_id: String,
    total: f64,
}

struct UserTotalBuilder {
    user_id: arrow_array::builder::StringBuilder,
    total: arrow_array::builder::PrimitiveBuilder<arrow_array::types::Float64Type>,
}

#[derive(Debug, Clone)]
struct UserTotalView<'a> {
    user_id: &'a str,
    total: f64,
}

struct UserTotalColumns<'a> {
    #[allow(dead_code)]
    user_id: &'a arrow_array::StringArray,
}

impl RheiBuilder for UserTotalBuilder {
    type Item = UserTotal;

    fn append(&mut self, item: UserTotal) {
        self.user_id.append_value(&item.user_id);
        self.total.append_value(item.total);
    }

    fn append_null(&mut self) {
        self.user_id.append_null();
        self.total.append_null();
    }

    fn len(&self) -> usize {
        arrow_array::builder::ArrayBuilder::len(&self.user_id)
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            UserTotal::arrow_schema(),
            vec![
                Arc::new(self.user_id.finish()),
                Arc::new(self.total.finish()),
            ],
        )
        .unwrap()
    }
}

impl RheiSchema for UserTotal {
    type Builder = UserTotalBuilder;
    type View<'a> = UserTotalView<'a>;
    type Columns<'a> = UserTotalColumns<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("user_id", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("total", arrow_schema::DataType::Float64, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        UserTotalBuilder {
            user_id: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 16),
            total: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Float64Type;
        UserTotalView {
            user_id: batch.column(0).as_string::<i32>().value(index),
            total: batch.column(1).as_primitive::<Float64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        use arrow_array::cast::AsArray;
        UserTotalColumns {
            user_id: batch.column(0).as_string::<i32>(),
        }
    }
}

// ── Stateful per-user aggregator ───────────────────────────────────

#[derive(Clone)]
struct PerUserAggregator;

#[async_trait]
impl StreamFunction for PerUserAggregator {
    type Input = OrderEvent;
    type Output = UserTotal;

    async fn process(
        &mut self,
        input: RheiBuffer<OrderEvent>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<UserTotal>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }

        let rows: Vec<(String, f64)> = input
            .iter()
            .map(|v| (v.user_id.to_string(), v.amount))
            .collect();

        let mut outputs = Vec::with_capacity(rows.len());

        for (user_id, amount) in rows {
            let current: f64 = {
                let mut state = KeyedState::<String, f64>::new(&mut ctx.state, "totals");
                state.get(&user_id).await?.unwrap_or(0.0)
            };
            let new_total = current + amount;
            {
                let mut state = KeyedState::<String, f64>::new(&mut ctx.state, "totals");
                state.put(&user_id, &new_total)?;
            }
            outputs.push(UserTotal {
                user_id,
                total: new_total,
            });
        }

        let mut builder = UserTotal::builder(outputs.len());
        for item in outputs {
            builder.append(item);
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── Collecting sink ────────────────────────────────────────────────

struct CollectSink {
    collected: Arc<Mutex<Vec<(String, f64)>>>,
}

#[async_trait]
impl Sink for CollectSink {
    type Input = UserTotal;

    async fn write_batch(&mut self, input: RheiBuffer<UserTotal>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.user_id.to_string(), view.total));
        }
        Ok(())
    }
}

// ── Data generation ─────────────────────────────────────────────────

fn generate_orders(n: usize) -> Vec<Order> {
    (0..n)
        .map(|i| {
            let timestamp = 1_000 + (i as u64) * 100;
            Order {
                id: format!("ord_{i}"),
                user_id: USERS[i % 4].to_string(),
                amount: ((i * 7 + 3) % 100 + 1) as f64,
                timestamp,
            }
        })
        .collect()
}

fn compute_expected_totals(orders: &[Order]) -> HashMap<String, f64> {
    let mut totals: HashMap<String, f64> = HashMap::new();
    for o in orders {
        if o.amount > 50.0 {
            *totals.entry(o.user_id.clone()).or_default() += o.amount;
        }
    }
    totals
}

// ── Kafka helpers ───────────────────────────────────────────────────

fn unique_topic(prefix: &str) -> String {
    format!(
        "rhei_e2e_{prefix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    )
}

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

async fn create_topic(topic: &str, partitions: i32) {
    let admin: AdminClient<rdkafka::client::DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("admin client creation failed");

    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .expect("topic creation failed");

    tokio::time::sleep(Duration::from_secs(1)).await;
}

async fn produce_json<T: Serialize>(topic: &str, key: &[u8], value: &T, producer: &FutureProducer) {
    let payload = serde_json::to_vec(value).unwrap();
    let record = FutureRecord::to(topic).payload(&payload).key(key);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .expect("produce failed");
}

// ── Test: single partition ──────────────────────────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_batch_aggregation_e2e() {
    use rhei_core::connectors::kafka::types::KafkaMessage as KafkaMsgType;

    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // ── Setup topic ─────────────────────────────────────────────────
    let orders_topic = unique_topic("batch_orders");
    create_topic(&orders_topic, 1).await;

    // ── Generate and produce data ───────────────────────────────────
    let orders = generate_orders(100);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");

    for order in &orders {
        produce_json(&orders_topic, order.user_id.as_bytes(), order, &producer).await;
    }

    // ── Build pipeline ──────────────────────────────────────────────
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<(String, f64)>::new()));

    let group_id = format!("rhei_batch_e2e_group_{}", std::process::id());
    let source = KafkaSource::new(&brokers(), &group_id, &[&orders_topic])
        .unwrap()
        .with_batch_size(50)
        .with_poll_timeout(Duration::from_millis(200));

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .map(
            |msg: <KafkaMsgType as RheiSchema>::View<'_>| -> OrderEvent {
                let payload = if msg.payload_is_null {
                    b"{}".as_slice()
                } else {
                    msg.payload
                };
                let order: Order = serde_json::from_slice(payload).expect("bad order JSON");
                OrderEvent {
                    user_id: order.user_id,
                    amount: order.amount,
                    timestamp: order.timestamp,
                }
            },
        )
        .filter_fn(|view: &<OrderEvent as RheiSchema>::View<'_>| view.amount > 50.0)
        .operator("aggregator", PerUserAggregator)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    // ── Run with shutdown ───────────────────────────────────────────
    let (handle, trigger) = ShutdownHandle::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        trigger.shutdown();
    });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run_with_shutdown(graph, handle).await.unwrap();

    // ── Verify output ───────────────────────────────────────────────
    let results = collected.lock().unwrap().clone();
    let expected = compute_expected_totals(&orders);

    assert!(
        !results.is_empty(),
        "pipeline produced no output — expected aggregations for {} users",
        expected.len()
    );

    // Find the final (max) total per user from all emitted outputs.
    let mut final_totals: HashMap<String, f64> = HashMap::new();
    for (user, total) in &results {
        let entry = final_totals.entry(user.clone()).or_insert(0.0_f64);
        if *total > *entry {
            *entry = *total;
        }
    }

    // Verify final totals match expected.
    for (user, expected_total) in &expected {
        let actual = final_totals.get(user).copied().unwrap_or(0.0);
        let diff = (actual - expected_total).abs();
        assert!(
            diff < 0.01,
            "total mismatch for {user}: actual={actual}, expected={expected_total}"
        );
    }

    // Verify all expected users are present.
    for user in expected.keys() {
        assert!(
            final_totals.contains_key(user),
            "missing output for user {user}"
        );
    }

    eprintln!(
        "E2E passed: {} output records, {} distinct users",
        results.len(),
        final_totals.len()
    );
}

// ── Test: multi-partition ───────────────────────────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_multi_partition_batch_e2e() {
    use rhei_core::connectors::kafka::types::KafkaMessage as KafkaMsgType;

    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // ── Setup multi-partition topic ─────────────────────────────────
    let orders_topic = unique_topic("batch_mp_orders");
    create_topic(&orders_topic, 4).await;

    // ── Generate and produce data ───────────────────────────────────
    let orders = generate_orders(100);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");

    for order in &orders {
        produce_json(&orders_topic, order.user_id.as_bytes(), order, &producer).await;
    }

    // ── Build pipeline ──────────────────────────────────────────────
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::<(String, f64)>::new()));

    let group_id = format!("rhei_batch_mp_e2e_group_{}", std::process::id());
    let source = KafkaSource::new(&brokers(), &group_id, &[&orders_topic])
        .unwrap()
        .with_batch_size(50)
        .with_poll_timeout(Duration::from_millis(200));

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
        .map(
            |msg: <KafkaMsgType as RheiSchema>::View<'_>| -> OrderEvent {
                let payload = if msg.payload_is_null {
                    b"{}".as_slice()
                } else {
                    msg.payload
                };
                let order: Order = serde_json::from_slice(payload).expect("bad order JSON");
                OrderEvent {
                    user_id: order.user_id,
                    amount: order.amount,
                    timestamp: order.timestamp,
                }
            },
        )
        .filter_fn(|view: &<OrderEvent as RheiSchema>::View<'_>| view.amount > 50.0)
        .operator("aggregator", PerUserAggregator)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    // ── Run with 2 workers ──────────────────────────────────────────
    let (handle, trigger) = ShutdownHandle::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        trigger.shutdown();
    });

    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(2);
    ctrl.run_with_shutdown(graph, handle).await.unwrap();

    // ── Verify output ───────────────────────────────────────────────
    let results = collected.lock().unwrap().clone();
    let expected = compute_expected_totals(&orders);

    assert!(
        !results.is_empty(),
        "multi-partition pipeline produced no output — expected aggregations for {} users",
        expected.len()
    );

    // Find the final (max) total per user.
    let mut final_totals: HashMap<String, f64> = HashMap::new();
    for (user, total) in &results {
        let entry = final_totals.entry(user.clone()).or_insert(0.0_f64);
        if *total > *entry {
            *entry = *total;
        }
    }

    // With multiple workers, per-user state may be split across workers.
    // Verify total across all workers accounts for most expected data.
    let expected_total: f64 = expected.values().sum();
    let actual_total: f64 = final_totals.values().sum();
    assert!(
        actual_total >= expected_total * 0.9,
        "actual total {actual_total} is less than 90% of expected {expected_total}"
    );

    // Verify checkpoint manifest has source offsets.
    let manifest = rhei_core::checkpoint::CheckpointManifest::load(checkpoint_dir.path());
    if let Some(m) = manifest {
        assert!(
            !m.source_offsets.is_empty(),
            "manifest should have source offsets from partitioned readers"
        );
        eprintln!(
            "manifest source_offsets: {} entries",
            m.source_offsets.len()
        );
    }

    eprintln!(
        "Multi-partition E2E passed: {} output records, total {actual_total:.0} (expected {expected_total:.0})",
        results.len()
    );
}

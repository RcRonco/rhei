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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rhei_core::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, Sink, StreamFunction,
};
use rhei_core::connectors::batch::{KafkaDlqSink, KafkaSink, KafkaSource};
use rhei_core::connectors::kafka::types::{KafkaMessage, KafkaRecord};
use rhei_core::dlq::{DeadLetterRecord, ErrorPolicy};
use rhei_core::operators::keyed_state::KeyedState;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::shutdown::{ShutdownHandle, ShutdownTrigger};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

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
async fn kafka_aggregation_e2e() {
    use rhei_core::connectors::kafka::types::KafkaMessage as KafkaMsgType;

    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // ── Setup topic ─────────────────────────────────────────────────
    let orders_topic = unique_topic("orders");
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

    let group_id = format!("rhei_e2e_group_{}", std::process::id());
    let consumer: rdkafka::consumer::StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "1000")
        .create()
        .expect("consumer creation failed");
    consumer
        .subscribe(&[&orders_topic])
        .expect("subscribe failed");

    let source = KafkaSource::from_consumer(consumer)
        .with_batch_size(50)
        .with_poll_timeout(Duration::from_millis(500));

    let graph = DataflowGraph::new();
    graph
        .source(source)
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
    let collected_for_shutdown = collected.clone();
    tokio::spawn(async move {
        // Wait until output appears or timeout after 30s.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if !collected_for_shutdown.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_secs(2)).await;
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
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
async fn kafka_multi_partition_e2e() {
    use rhei_core::connectors::kafka::types::KafkaMessage as KafkaMsgType;

    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // ── Setup multi-partition topic ─────────────────────────────────
    let orders_topic = unique_topic("mp_orders");
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

    let group_id = format!("rhei_mp_e2e_group_{}", std::process::id());
    let consumer: rdkafka::consumer::StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "1000")
        .create()
        .expect("consumer creation failed");
    consumer
        .subscribe(&[&orders_topic])
        .expect("subscribe failed");

    let source = KafkaSource::from_consumer(consumer)
        .with_batch_size(50)
        .with_poll_timeout(Duration::from_millis(500));

    let graph = DataflowGraph::new();
    graph
        .source(source)
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
    let collected_for_shutdown = collected.clone();
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if !collected_for_shutdown.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_secs(2)).await;
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
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

// ════════════════════════════════════════════════════════════════════
//  Extended coverage: sink, DLQ, null keys, multi-topic, idle, restart
// ════════════════════════════════════════════════════════════════════

// ── Shared helpers for the extended tests ───────────────────────────

/// Build a manual-commit, earliest `StreamConsumer` subscribed to `topics`.
fn make_consumer(group_id: &str, topics: &[&str]) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "1000")
        .create()
        .expect("consumer creation failed");
    consumer.subscribe(topics).expect("subscribe failed");
    consumer
}

/// Produce a JSON value with no key (null-key / keyless record).
async fn produce_json_no_key<T: Serialize>(topic: &str, value: &T, producer: &FutureProducer) {
    let payload = serde_json::to_vec(value).unwrap();
    // Key type must be named even though no key is set (null-key record).
    let record: FutureRecord<'_, [u8], Vec<u8>> = FutureRecord::to(topic).payload(&payload);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .expect("produce failed");
}

/// Produce an opaque payload under a key (used to inject malformed records).
async fn produce_raw(topic: &str, key: &[u8], payload: &[u8], producer: &FutureProducer) {
    let record = FutureRecord::to(topic).payload(payload).key(key);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .expect("produce failed");
}

/// Drain up to `want` records (key, payload) from `consumer`, stopping early
/// once `timeout` elapses with no further data.
async fn drain_records(
    consumer: &StreamConsumer,
    want: usize,
    timeout: Duration,
) -> Vec<(Option<Vec<u8>>, Vec<u8>)> {
    let mut out = Vec::new();
    let deadline = Instant::now() + timeout;
    while out.len() < want {
        let remaining = deadline.duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => out.push((
                msg.key().map(<[u8]>::to_vec),
                msg.payload().map(<[u8]>::to_vec).unwrap_or_default(),
            )),
            _ => break,
        }
    }
    out
}

/// Trigger shutdown once `progress` stops growing for 3s, or after 40s.
///
/// Robust termination for unbounded Kafka pipelines: it waits for the input
/// to actually drain rather than firing on the first emitted output.
fn spawn_quiescent_shutdown<F>(progress: F, trigger: ShutdownTrigger)
where
    F: Fn() -> usize + Send + 'static,
{
    tokio::spawn(async move {
        let start = Instant::now();
        let mut last = 0usize;
        let mut last_change = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let cur = progress();
            if cur != last {
                last = cur;
                last_change = Instant::now();
            }
            if cur > 0 && last_change.elapsed() >= Duration::from_secs(3) {
                break;
            }
            if start.elapsed() >= Duration::from_secs(40) {
                break;
            }
        }
        trigger.shutdown();
    });
}

/// Final (max) per-user total across every emitted output row.
fn final_totals(results: &[(String, f64)]) -> HashMap<String, f64> {
    let mut totals: HashMap<String, f64> = HashMap::new();
    for (user, total) in results {
        let entry = totals.entry(user.clone()).or_insert(0.0_f64);
        if *total > *entry {
            *entry = *total;
        }
    }
    totals
}

/// Run the canonical `parse → filter(amount>50) → aggregate → collect`
/// pipeline against `topics` with the given consumer group and checkpoint
/// directory, returning every emitted `(user, running_total)` pair.
async fn run_aggregation(
    topics: &[&str],
    group_id: &str,
    checkpoint_dir: &std::path::Path,
) -> Vec<(String, f64)> {
    let consumer = make_consumer(group_id, topics);
    let source = KafkaSource::from_consumer(consumer)
        .with_batch_size(20)
        .with_poll_timeout(Duration::from_millis(300))
        // Emit watermarks frequently so checkpoints (and offset commits) fire
        // during the short test window.
        .with_watermark_interval(10);

    let collected = Arc::new(Mutex::new(Vec::<(String, f64)>::new()));
    let graph = DataflowGraph::new();
    graph
        .source(source)
        .map(
            |msg: <KafkaMessage as RheiSchema>::View<'_>| -> OrderEvent {
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

    let (handle, trigger) = ShutdownHandle::new();
    {
        let progress = collected.clone();
        spawn_quiescent_shutdown(move || progress.lock().unwrap().len(), trigger);
    }

    let ctrl = PipelineController::new(checkpoint_dir.to_path_buf()).with_workers(1);
    ctrl.run_with_shutdown(graph, handle).await.unwrap();

    collected.lock().unwrap().clone()
}

// ── Operator: parse Kafka payloads, failing on malformed JSON ───────

/// Parses each `KafkaMessage` payload into an `OrderEvent`, returning an error
/// for any malformed record so the runtime can route it to the DLQ.
#[derive(Clone)]
struct ParseOrders;

#[async_trait]
impl StreamFunction for ParseOrders {
    type Input = KafkaMessage;
    type Output = OrderEvent;

    async fn process(
        &mut self,
        input: RheiBuffer<KafkaMessage>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<OrderEvent>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }

        let mut builder = OrderEvent::builder(input.len());
        for view in &input {
            let payload = if view.payload_is_null {
                b"".as_slice()
            } else {
                view.payload
            };
            let order: Order = serde_json::from_slice(payload)
                .map_err(|e| anyhow::anyhow!("bad order JSON at offset {}: {e}", view.offset))?;
            builder.append(OrderEvent {
                user_id: order.user_id,
                amount: order.amount,
                timestamp: order.timestamp,
            });
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── Test: KafkaSink round-trip ──────────────────────────────────────

/// Source topic → parse → filter → re-serialize → `KafkaSink` → output topic.
/// An independent consumer then verifies every produced record.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_sink_roundtrip_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let input_topic = unique_topic("sink_in");
    let output_topic = unique_topic("sink_out");
    create_topic(&input_topic, 1).await;
    create_topic(&output_topic, 1).await;

    let orders = generate_orders(80);
    let expected_passing = orders.iter().filter(|o| o.amount > 50.0).count();
    assert!(
        expected_passing > 0,
        "test data must contain passing orders"
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");
    for order in &orders {
        produce_json(&input_topic, order.user_id.as_bytes(), order, &producer).await;
    }

    // Background consumer on the OUTPUT topic — used only to drive shutdown
    // once the sink has stopped producing.
    let out_seen = Arc::new(Mutex::new(Vec::<()>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let bg_consumer = make_consumer(
        &format!("rhei_sink_bg_{}", std::process::id()),
        &[&output_topic],
    );
    let bg = {
        let out_seen = out_seen.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_millis(400), bg_consumer.recv()).await
                {
                    out_seen.lock().unwrap().push(());
                }
            }
        })
    };

    let consumer = make_consumer(
        &format!("rhei_sink_src_{}", std::process::id()),
        &[&input_topic],
    );
    let source = KafkaSource::from_consumer(consumer)
        .with_batch_size(50)
        .with_poll_timeout(Duration::from_millis(500));

    let sink = KafkaSink::new(&brokers(), &output_topic).expect("sink creation failed");

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .map(
            |msg: <KafkaMessage as RheiSchema>::View<'_>| -> OrderEvent {
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
        .map(
            |view: <OrderEvent as RheiSchema>::View<'_>| -> KafkaRecord {
                let payload = serde_json::to_vec(&serde_json::json!({
                    "user_id": view.user_id,
                    "amount": view.amount,
                    "timestamp": view.timestamp,
                }))
                .expect("serialize output order");
                KafkaRecord::with_key(view.user_id.as_bytes().to_vec(), payload)
            },
        )
        .sink(sink);

    let (handle, trigger) = ShutdownHandle::new();
    {
        let progress = out_seen.clone();
        spawn_quiescent_shutdown(move || progress.lock().unwrap().len(), trigger);
    }

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run_with_shutdown(graph, handle).await.unwrap();

    stop.store(true, Ordering::Relaxed);
    let _ = bg.await;

    // Independent verification: a fresh group re-reads the output topic.
    let verify = make_consumer(
        &format!("rhei_sink_verify_{}", std::process::id()),
        &[&output_topic],
    );
    let emitted = drain_records(&verify, expected_passing, Duration::from_secs(10)).await;

    assert_eq!(
        emitted.len(),
        expected_passing,
        "KafkaSink must produce exactly one record per passing order"
    );
    for (key, payload) in &emitted {
        let key = key.as_ref().expect("sink must preserve the record key");
        let user = String::from_utf8(key.clone()).unwrap();
        assert!(
            USERS.contains(&user.as_str()),
            "unexpected sink key: {user}"
        );
        let parsed: serde_json::Value = serde_json::from_slice(payload).expect("bad sink JSON");
        let amount = parsed["amount"].as_f64().expect("missing amount field");
        assert!(
            amount > 50.0,
            "sink emitted a filtered-out record: {amount}"
        );
    }

    eprintln!("KafkaSink round-trip verified: {} records", emitted.len());
}

// ── Test: DLQ routing to a Kafka topic ──────────────────────────────

/// Malformed payloads fail the parse operator and are routed to a Kafka DLQ
/// topic via `KafkaDlqSink`, while valid orders still reach the sink.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_dlq_routes_bad_records_to_kafka_topic() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let input_topic = unique_topic("dlq_in");
    let dlq_topic = unique_topic("dlq_out");
    create_topic(&input_topic, 1).await;
    create_topic(&dlq_topic, 1).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");

    // 12 valid orders interleaved with 4 malformed payloads.
    let valid = generate_orders(12);
    let mut bad = 0usize;
    for (i, order) in valid.iter().enumerate() {
        produce_json(&input_topic, order.user_id.as_bytes(), order, &producer).await;
        if i % 3 == 2 {
            produce_raw(&input_topic, b"corrupt", b"this is not json", &producer).await;
            bad += 1;
        }
    }
    assert_eq!(bad, 4, "test fixture should inject 4 malformed records");

    // One message per batch so each bad record fails in isolation.
    let consumer = make_consumer(
        &format!("rhei_dlq_src_{}", std::process::id()),
        &[&input_topic],
    );
    let source = KafkaSource::from_consumer(consumer)
        .with_batch_size(1)
        .with_poll_timeout(Duration::from_millis(300));

    let collected = Arc::new(Mutex::new(Vec::<(String, f64)>::new()));
    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("parse", ParseOrders)
        .operator("aggregator", PerUserAggregator)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let (handle, trigger) = ShutdownHandle::new();
    {
        let progress = collected.clone();
        spawn_quiescent_shutdown(move || progress.lock().unwrap().len(), trigger);
    }

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let ctrl = PipelineController::builder()
        .checkpoint_dir(checkpoint_dir.path().to_path_buf())
        .workers(1)
        .error_policy(ErrorPolicy::SendToDlq)
        .dlq_sink(KafkaDlqSink::new(&brokers(), &dlq_topic).expect("dlq sink creation failed"))
        .build()
        .unwrap();
    ctrl.run_with_shutdown(graph, handle).await.unwrap();

    // Every valid order still reached the sink (one aggregate output per row).
    let results = collected.lock().unwrap().clone();
    assert_eq!(
        results.len(),
        valid.len(),
        "all valid orders must reach the sink"
    );

    // Every malformed record produced exactly one DLQ message.
    let dlq_consumer = make_consumer(
        &format!("rhei_dlq_verify_{}", std::process::id()),
        &[&dlq_topic],
    );
    let dead = drain_records(&dlq_consumer, bad, Duration::from_secs(15)).await;
    assert_eq!(
        dead.len(),
        bad,
        "every malformed record must yield one DLQ record"
    );
    for (key, payload) in &dead {
        let key = String::from_utf8(key.clone().expect("DLQ key is the operator name")).unwrap();
        assert_eq!(key, "parse", "DLQ key should be the failing operator name");
        let record: DeadLetterRecord = serde_json::from_slice(payload).expect("bad DLQ JSON");
        assert_eq!(record.operator_name, "parse");
        assert!(
            record.error.contains("bad order JSON"),
            "unexpected DLQ error text: {}",
            record.error
        );
    }

    eprintln!(
        "Kafka DLQ routing verified: {} dead-letter records",
        dead.len()
    );
}

// ── Test: null-key (keyless) records ────────────────────────────────

/// Keyless records must surface `key_is_null` and still flow through the
/// pipeline unharmed.
#[tokio::test]
async fn kafka_null_key_records_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let topic = unique_topic("nokey");
    create_topic(&topic, 1).await;

    let orders = generate_orders(40);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");
    for order in &orders {
        produce_json_no_key(&topic, order, &producer).await;
    }

    let consumer = make_consumer(&format!("rhei_nokey_{}", std::process::id()), &[&topic]);
    let source = KafkaSource::from_consumer(consumer)
        .with_batch_size(50)
        .with_poll_timeout(Duration::from_millis(500));

    let total_seen = Arc::new(AtomicUsize::new(0));
    let null_keys = Arc::new(AtomicUsize::new(0));
    let collected = Arc::new(Mutex::new(Vec::<(String, f64)>::new()));

    let seen = total_seen.clone();
    let nulls = null_keys.clone();
    let graph = DataflowGraph::new();
    graph
        .source(source)
        .map(
            move |msg: <KafkaMessage as RheiSchema>::View<'_>| -> OrderEvent {
                seen.fetch_add(1, Ordering::Relaxed);
                if msg.key_is_null {
                    nulls.fetch_add(1, Ordering::Relaxed);
                }
                let order: Order = serde_json::from_slice(msg.payload).expect("bad order JSON");
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

    let (handle, trigger) = ShutdownHandle::new();
    {
        let progress = collected.clone();
        spawn_quiescent_shutdown(move || progress.lock().unwrap().len(), trigger);
    }

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run_with_shutdown(graph, handle).await.unwrap();

    let seen = total_seen.load(Ordering::Relaxed);
    let nulls = null_keys.load(Ordering::Relaxed);
    assert!(seen > 0, "pipeline consumed no records");
    assert_eq!(
        nulls, seen,
        "every keyless record must surface key_is_null ({nulls}/{seen})"
    );
    assert!(
        !collected.lock().unwrap().is_empty(),
        "passing orders should still produce output"
    );

    eprintln!("Keyless-record handling verified: {seen} records, all null-keyed");
}

// ── Test: multi-topic subscription ──────────────────────────────────

/// A single source subscribed to two topics consumes records from both.
#[tokio::test]
async fn kafka_multi_topic_subscription_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let topic_a = unique_topic("multi_a");
    let topic_b = unique_topic("multi_b");
    create_topic(&topic_a, 1).await;
    create_topic(&topic_b, 1).await;

    let orders = generate_orders(80);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");
    for (i, order) in orders.iter().enumerate() {
        let topic = if i % 2 == 0 { &topic_a } else { &topic_b };
        produce_json(topic, order.user_id.as_bytes(), order, &producer).await;
    }

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let group_id = format!("rhei_multi_topic_{}", std::process::id());
    let results = run_aggregation(&[&topic_a, &topic_b], &group_id, checkpoint_dir.path()).await;

    assert!(
        !results.is_empty(),
        "multi-topic pipeline produced no output"
    );

    // Single worker → deterministic per-user totals over BOTH topics.
    let expected = compute_expected_totals(&orders);
    let finals = final_totals(&results);
    for (user, expected_total) in &expected {
        let actual = finals.get(user).copied().unwrap_or(0.0);
        assert!(
            (actual - expected_total).abs() < 0.01,
            "multi-topic total mismatch for {user}: actual={actual}, expected={expected_total}"
        );
    }

    eprintln!(
        "Multi-topic subscription verified: {} users across 2 topics",
        expected.len()
    );
}

// ── Test: empty topic shuts down cleanly ────────────────────────────

/// An idle (empty) topic must not block shutdown: the unbounded source keeps
/// polling, and an external shutdown drains the pipeline to a clean stop.
#[tokio::test]
async fn kafka_empty_topic_shuts_down_cleanly() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let topic = unique_topic("empty");
    create_topic(&topic, 1).await;

    let consumer = make_consumer(&format!("rhei_empty_{}", std::process::id()), &[&topic]);
    let source = KafkaSource::from_consumer(consumer)
        .with_batch_size(10)
        .with_poll_timeout(Duration::from_millis(200));

    let collected = Arc::new(Mutex::new(Vec::<(String, f64)>::new()));
    let graph = DataflowGraph::new();
    graph
        .source(source)
        .map(
            |msg: <KafkaMessage as RheiSchema>::View<'_>| -> OrderEvent {
                let order: Order = serde_json::from_slice(msg.payload).expect("bad order JSON");
                OrderEvent {
                    user_id: order.user_id,
                    amount: order.amount,
                    timestamp: order.timestamp,
                }
            },
        )
        .operator("aggregator", PerUserAggregator)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    // No data will ever arrive — shut down after a fixed grace period.
    let (handle, trigger) = ShutdownHandle::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(6)).await;
        trigger.shutdown();
    });

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let ctrl = PipelineController::new(checkpoint_dir.path().to_path_buf()).with_workers(1);
    ctrl.run_with_shutdown(graph, handle)
        .await
        .expect("idle pipeline should shut down cleanly");

    assert!(
        collected.lock().unwrap().is_empty(),
        "no output expected from an empty topic"
    );

    eprintln!("Empty-topic clean shutdown verified");
}

// ── Test: restart recovers state (at-least-once) ────────────────────

/// Across a restart sharing the consumer group and checkpoint directory,
/// operator state persists and the source resumes consuming, so every record
/// is reflected in the cumulative totals at least once.
///
/// Note on semantics: with the externally-subscribed Kafka consumer and
/// asynchronous offset commits, a restart may *reprocess* some already-handled
/// records (at-least-once), so cumulative totals land in `[expected, 2*expected]`
/// rather than exactly `expected`. The invariants asserted here are the ones the
/// framework actually guarantees on a Kafka restart:
/// - run 1 wrote a checkpoint manifest with tracked source offsets;
/// - run 2 continued from run 1's persisted state (totals grew, not reset);
/// - every qualifying record is counted at least once (no data loss), with
///   reprocessing bounded to at most one extra pass.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_restart_recovers_state_at_least_once_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let topic = unique_topic("restart");
    create_topic(&topic, 1).await;
    let group_id = format!("rhei_restart_group_{}", std::process::id());
    let checkpoint_dir = tempfile::tempdir().unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");

    let all_orders = generate_orders(100);
    let (first, second) = all_orders.split_at(60);

    // ── Run 1: first 60 orders ──────────────────────────────────────
    for order in first {
        produce_json(&topic, order.user_id.as_bytes(), order, &producer).await;
    }
    let run1 = run_aggregation(&[&topic], &group_id, checkpoint_dir.path()).await;
    assert!(!run1.is_empty(), "run 1 produced no output");
    let run1_totals = final_totals(&run1);

    // A checkpoint must have been written, recording source offsets.
    let manifest = rhei_core::checkpoint::CheckpointManifest::load(checkpoint_dir.path())
        .expect("run 1 should have written a checkpoint manifest");
    assert!(
        !manifest.source_offsets.is_empty(),
        "run 1 manifest must record source offsets for restart"
    );

    // ── Run 2: 40 more orders, same group + checkpoint dir ──────────
    for order in second {
        produce_json(&topic, order.user_id.as_bytes(), order, &producer).await;
    }
    let run2 = run_aggregation(&[&topic], &group_id, checkpoint_dir.path()).await;
    assert!(!run2.is_empty(), "run 2 produced no output");

    // At-least-once across the restart: cumulative totals after run 2 cover the
    // full single-pass total (no loss) and continued from run 1's state, with
    // reprocessing bounded to at most one extra pass.
    let expected = compute_expected_totals(&all_orders);
    let finals = final_totals(&run2);
    for (user, expected_total) in &expected {
        let actual = finals.get(user).copied().unwrap_or(0.0);
        let run1_total = run1_totals.get(user).copied().unwrap_or(0.0);
        assert!(
            actual >= expected_total - 0.01,
            "data loss across restart for {user}: actual={actual}, expected at least {expected_total}"
        );
        assert!(
            actual <= 2.0 * expected_total + 0.01,
            "excessive reprocessing for {user}: actual={actual}, expected at most {}",
            2.0 * expected_total
        );
        assert!(
            actual > run1_total - 0.01,
            "run 2 did not continue from persisted state for {user}: actual={actual}, run1={run1_total}"
        );
    }

    eprintln!(
        "Kafka restart at-least-once recovery verified across {} users",
        expected.len()
    );
}

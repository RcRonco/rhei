#![cfg(feature = "kafka")]
#![allow(clippy::struct_field_names)]

//! End-to-end Kafka integration test.
//!
//! Pipeline topology (single exchange, two stateful operators):
//!
//! ```text
//! KafkaSource([orders_topic, payments_topic])
//!   → map(parse by topic → JoinSide<Order, Payment>)
//!   → filter(Left: amount > 50, Right: always pass)
//!   → key_by(user_id)
//!   → TemporalJoin(order_id)  → JoinedOrder
//!   → TumblingWindow(Sum(amount), window_size=10_000)  → WindowOutput<f64>
//!   → FileSink
//! ```
//!
//! The `key_by(user_id)` exchange routes all events for a given user to the
//! same worker. This is sufficient for both the join (all order+payment pairs
//! for the same user share a worker) and the window (per-user aggregation).

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rhei_core::connectors::file_sink::FileSink;
use rhei_core::connectors::kafka::source::KafkaSource;
use rhei_core::connectors::kafka::types::KafkaMessage;
use rhei_core::operators::Sum;
use rhei_core::operators::temporal_join::{JoinSide, TemporalJoin};
use rhei_core::operators::tumbling_window::{TumblingWindow, WindowOutput};
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::executor::Executor;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payment {
    id: String,
    user_id: String,
    method: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinedOrder {
    id: String,
    user_id: String,
    amount: f64,
    payment_method: String,
    timestamp: u64,
}

// ── Data generation ─────────────────────────────────────────────────

/// Generates deterministic orders across 4 users.
///
/// - Orders 0-79: timestamps in `[1_000, 8_900]` (first time range)
/// - Orders 80-99: timestamps in `[100_000, 101_900]` (second time range)
///
/// The large gap between ranges ensures the `TumblingWindow` closes the first
/// window for every user when events from the second range arrive.
///
/// Amounts: `((i*7 + 3) % 100 + 1)` — deterministic spread, roughly 60% > 50.
fn generate_orders(n: usize) -> Vec<Order> {
    (0..n)
        .map(|i| {
            let timestamp = if i < 80 {
                1_000 + (i as u64) * 100
            } else {
                100_000 + ((i - 80) as u64) * 100
            };
            Order {
                id: format!("ord_{i}"),
                user_id: USERS[i % 4].to_string(),
                amount: ((i * 7 + 3) % 100 + 1) as f64,
                timestamp,
            }
        })
        .collect()
}

/// Generates one payment per order.
fn generate_payments(orders: &[Order]) -> Vec<Payment> {
    orders
        .iter()
        .map(|o| Payment {
            id: o.id.clone(),
            user_id: o.user_id.clone(),
            method: "card".to_string(),
            timestamp: o.timestamp + 50,
        })
        .collect()
}

/// Computes the expected window outputs by replaying the pipeline logic.
///
/// Returns the set of `(user_id, window_start) → sum(amount)` entries that
/// the `TumblingWindow` should emit. The last window per user is excluded
/// (`TumblingWindow` only emits closed windows).
fn compute_expected_windows(orders: &[Order], window_size: u64) -> HashMap<(String, u64), f64> {
    // Step 1: filter orders (amount > 50)
    let filtered: Vec<&Order> = orders.iter().filter(|o| o.amount > 50.0).collect();

    // Step 2: all filtered orders have matching payments → all join
    // Step 3: group joined events into tumbling windows by user_id
    let mut window_sums: HashMap<(String, u64), f64> = HashMap::new();
    for o in &filtered {
        let ws = o.timestamp - (o.timestamp % window_size);
        *window_sums.entry((o.user_id.clone(), ws)).or_default() += o.amount;
    }

    // Step 4: find the last window per user (this one won't be emitted)
    let mut last_window_per_user: HashMap<String, u64> = HashMap::new();
    for (user, ws) in window_sums.keys() {
        let entry = last_window_per_user.entry(user.clone()).or_default();
        *entry = (*entry).max(*ws);
    }

    // Step 5: remove last window per user
    window_sums.retain(|(user, ws), _| *ws < last_window_per_user[user]);

    window_sums
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

// ── Test ────────────────────────────────────────────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_join_window_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // ── Setup topics ────────────────────────────────────────────────
    let orders_topic = unique_topic("orders");
    let payments_topic = unique_topic("payments");

    // Single partition per topic for deterministic ordering.
    create_topic(&orders_topic, 1).await;
    create_topic(&payments_topic, 1).await;

    // ── Generate and produce data ───────────────────────────────────
    let orders = generate_orders(100);
    let payments = generate_payments(&orders);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");

    // Produce all orders first, then all payments. Since the TemporalJoin
    // buffers unmatched events, ordering between topics doesn't matter
    // for correctness — only within-topic ordering matters for the window.
    for order in &orders {
        produce_json(&orders_topic, order.user_id.as_bytes(), order, &producer).await;
    }
    for payment in &payments {
        produce_json(
            &payments_topic,
            payment.user_id.as_bytes(),
            payment,
            &producer,
        )
        .await;
    }

    // ── Build pipeline ──────────────────────────────────────────────
    let output_dir = tempfile::tempdir().unwrap();
    let output_path = output_dir.path().join("output.jsonl");
    let checkpoint_dir = tempfile::tempdir().unwrap();

    let group_id = format!("rhei_e2e_group_{}", std::process::id());
    let source = KafkaSource::new(&brokers(), &group_id, &[&orders_topic, &payments_topic])
        .unwrap()
        .with_batch_size(50)
        .with_poll_timeout(Duration::from_millis(200));

    let file_sink = FileSink::<WindowOutput<f64>>::new(&output_path).unwrap();

    // Capture topic name for the map closure.
    let ot = orders_topic.clone();

    let join_op = TemporalJoin::<Order, Payment, _, _>::new(
        // Join key: order_id (internal state matching).
        |side: &JoinSide<Order, Payment>| match side {
            JoinSide::Left(o) => o.id.clone(),
            JoinSide::Right(p) => p.id.clone(),
        },
        // Join function: combine order + payment.
        |order: Order, payment: Payment| JoinedOrder {
            id: order.id,
            user_id: order.user_id,
            amount: order.amount,
            payment_method: payment.method,
            timestamp: order.timestamp,
        },
    );

    let window_size = 10_000u64;
    let window_op = TumblingWindow::builder()
        .window_size(window_size)
        .key_fn(|j: &JoinedOrder| j.user_id.clone())
        .time_fn(|j: &JoinedOrder| j.timestamp)
        .aggregator(Sum::new(|j: &JoinedOrder| j.amount))
        .build();

    let graph = DataflowGraph::new();
    graph
        .source(source)
        // Parse each Kafka message by topic into JoinSide<Order, Payment>.
        .map(move |msg: KafkaMessage| -> JoinSide<Order, Payment> {
            let payload = msg.payload.expect("missing payload");
            if msg.topic == ot {
                JoinSide::Left(serde_json::from_slice(&payload).expect("bad order JSON"))
            } else {
                JoinSide::Right(serde_json::from_slice(&payload).expect("bad payment JSON"))
            }
        })
        // Filter left leg: only orders with amount > 50.
        .filter(|side: &JoinSide<Order, Payment>| match side {
            JoinSide::Left(o) => o.amount > 50.0,
            JoinSide::Right(_) => true,
        })
        // Exchange by user_id — serves both join and window.
        .key_by(|side: &JoinSide<Order, Payment>| match side {
            JoinSide::Left(o) => o.user_id.clone(),
            JoinSide::Right(p) => p.user_id.clone(),
        })
        .operator("temporal_join", join_op)
        .operator("tumbling_window", window_op)
        .sink(file_sink);

    // ── Run with 2 workers ──────────────────────────────────────────
    let (handle, trigger) = ShutdownHandle::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        trigger.shutdown();
    });

    let executor = Executor::builder()
        .checkpoint_dir(checkpoint_dir.path())
        .workers(2)
        .build();

    executor.run_with_shutdown(graph, handle).await.unwrap();

    // ── Verify output ───────────────────────────────────────────────
    let content = std::fs::read_to_string(&output_path).unwrap();
    let windows: Vec<WindowOutput<f64>> = content
        .lines()
        .map(|line| serde_json::from_str(line).expect("invalid output JSON"))
        .collect();

    let expected = compute_expected_windows(&orders, window_size);

    assert!(
        !windows.is_empty(),
        "pipeline produced no output — expected {} window records",
        expected.len()
    );

    // Verify structural properties.
    let expected_users: std::collections::HashSet<&str> =
        expected.keys().map(|(u, _)| u.as_str()).collect();

    for w in &windows {
        assert!(
            expected_users.contains(w.key.as_str()),
            "unexpected user in output: {}",
            w.key
        );
        assert!(w.value > 0.0, "window value must be positive");
        assert_eq!(
            w.window_end,
            w.window_start + window_size,
            "window_end should be window_start + window_size"
        );
    }

    // Aggregate actual output by (user, window_start).
    let mut actual_sums: HashMap<(String, u64), f64> = HashMap::new();
    for w in &windows {
        *actual_sums
            .entry((w.key.clone(), w.window_start))
            .or_default() += w.value;
    }

    // Verify: every emitted window matches expected sum.
    for ((user, ws), actual_sum) in &actual_sums {
        if let Some(&expected_sum) = expected.get(&(user.clone(), *ws)) {
            let diff = (actual_sum - expected_sum).abs();
            assert!(
                diff < 0.01,
                "sum mismatch for ({user}, window {ws}): actual={actual_sum}, expected={expected_sum}"
            );
        }
        // Windows not in `expected` could be the last-window for a user on a
        // particular worker (due to per-worker state). We allow these.
    }

    // Verify: total amount across all windows accounts for most of the expected data.
    let expected_total: f64 = expected.values().sum();
    let actual_total: f64 = actual_sums.values().sum();
    assert!(
        actual_total >= expected_total * 0.9,
        "actual total {actual_total} is less than 90% of expected {expected_total}"
    );

    eprintln!(
        "E2E passed: {} window records, total amount {actual_total:.0} (expected {expected_total:.0})",
        windows.len()
    );
}

// ── Multi-partition Kafka E2E ────────────────────────────────────
//
// Same pipeline topology as above, but topics have 4 partitions each.
// KafkaSource detects 8 total partitions and creates per-worker
// partition readers. With 4 workers, each gets ~2 partitions.

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_multi_partition_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // ── Setup multi-partition topics ─────────────────────────────
    let orders_topic = unique_topic("mp_orders");
    let payments_topic = unique_topic("mp_payments");

    // 4 partitions per topic — Kafka distributes by key hash.
    create_topic(&orders_topic, 4).await;
    create_topic(&payments_topic, 4).await;

    // ── Generate and produce data ────────────────────────────────
    let orders = generate_orders(100);
    let payments = generate_payments(&orders);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer creation failed");

    for order in &orders {
        produce_json(&orders_topic, order.user_id.as_bytes(), order, &producer).await;
    }
    for payment in &payments {
        produce_json(
            &payments_topic,
            payment.user_id.as_bytes(),
            payment,
            &producer,
        )
        .await;
    }

    // ── Build pipeline ───────────────────────────────────────────
    let output_dir = tempfile::tempdir().unwrap();
    let output_path = output_dir.path().join("output.jsonl");
    let checkpoint_dir = tempfile::tempdir().unwrap();

    let group_id = format!("rhei_e2e_mp_group_{}", std::process::id());
    let source = KafkaSource::new(&brokers(), &group_id, &[&orders_topic, &payments_topic])
        .unwrap()
        .with_batch_size(50)
        .with_poll_timeout(Duration::from_millis(200));

    let file_sink = FileSink::<WindowOutput<f64>>::new(&output_path).unwrap();

    let ot = orders_topic.clone();

    let join_op = TemporalJoin::<Order, Payment, _, _>::new(
        |side: &JoinSide<Order, Payment>| match side {
            JoinSide::Left(o) => o.id.clone(),
            JoinSide::Right(p) => p.id.clone(),
        },
        |order: Order, payment: Payment| JoinedOrder {
            id: order.id,
            user_id: order.user_id,
            amount: order.amount,
            payment_method: payment.method,
            timestamp: order.timestamp,
        },
    );

    let window_size = 10_000u64;
    let window_op = TumblingWindow::builder()
        .window_size(window_size)
        .key_fn(|j: &JoinedOrder| j.user_id.clone())
        .time_fn(|j: &JoinedOrder| j.timestamp)
        .aggregator(Sum::new(|j: &JoinedOrder| j.amount))
        .build();

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .map(move |msg: KafkaMessage| -> JoinSide<Order, Payment> {
            let payload = msg.payload.expect("missing payload");
            if msg.topic == ot {
                JoinSide::Left(serde_json::from_slice(&payload).expect("bad order JSON"))
            } else {
                JoinSide::Right(serde_json::from_slice(&payload).expect("bad payment JSON"))
            }
        })
        .filter(|side: &JoinSide<Order, Payment>| match side {
            JoinSide::Left(o) => o.amount > 50.0,
            JoinSide::Right(_) => true,
        })
        .key_by(|side: &JoinSide<Order, Payment>| match side {
            JoinSide::Left(o) => o.user_id.clone(),
            JoinSide::Right(p) => p.user_id.clone(),
        })
        .operator("temporal_join", join_op)
        .operator("tumbling_window", window_op)
        .sink(file_sink);

    // ── Run with 4 workers ───────────────────────────────────────
    // 8 partitions (4 per topic) distributed across 4 workers:
    // each worker reads from ~2 partitions in parallel.
    let (handle, trigger) = ShutdownHandle::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        trigger.shutdown();
    });

    let executor = Executor::builder()
        .checkpoint_dir(checkpoint_dir.path())
        .workers(4)
        .build();

    executor.run_with_shutdown(graph, handle).await.unwrap();

    // ── Verify output ────────────────────────────────────────────
    let content = std::fs::read_to_string(&output_path).unwrap();
    let windows: Vec<WindowOutput<f64>> = content
        .lines()
        .map(|line| serde_json::from_str(line).expect("invalid output JSON"))
        .collect();

    let expected = compute_expected_windows(&orders, window_size);

    assert!(
        !windows.is_empty(),
        "multi-partition pipeline produced no output — expected {} window records",
        expected.len()
    );

    // Verify structural properties.
    let expected_users: std::collections::HashSet<&str> =
        expected.keys().map(|(u, _)| u.as_str()).collect();

    for w in &windows {
        assert!(
            expected_users.contains(w.key.as_str()),
            "unexpected user in output: {}",
            w.key
        );
        assert!(w.value > 0.0, "window value must be positive");
        assert_eq!(
            w.window_end,
            w.window_start + window_size,
            "window_end should be window_start + window_size"
        );
    }

    // Aggregate actual output.
    let mut actual_sums: HashMap<(String, u64), f64> = HashMap::new();
    for w in &windows {
        *actual_sums
            .entry((w.key.clone(), w.window_start))
            .or_default() += w.value;
    }

    // Verify window sums match expected.
    for ((user, ws), actual_sum) in &actual_sums {
        if let Some(&expected_sum) = expected.get(&(user.clone(), *ws)) {
            let diff = (actual_sum - expected_sum).abs();
            assert!(
                diff < 0.01,
                "sum mismatch for ({user}, window {ws}): actual={actual_sum}, expected={expected_sum}"
            );
        }
    }

    // Verify total amount.
    let expected_total: f64 = expected.values().sum();
    let actual_total: f64 = actual_sums.values().sum();
    assert!(
        actual_total >= expected_total * 0.9,
        "actual total {actual_total} is less than 90% of expected {expected_total}"
    );

    // Verify checkpoint manifest has partition-level offsets.
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
        "Multi-partition E2E passed: {} window records, total {actual_total:.0} (expected {expected_total:.0})",
        windows.len()
    );
}

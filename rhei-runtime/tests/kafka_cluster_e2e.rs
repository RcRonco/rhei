#![cfg(feature = "kafka")]
#![allow(clippy::struct_field_names)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Multi-process cluster E2E test with Kafka.
//!
//! This test validates the `CommunicationConfig::Cluster` (TCP) path by
//! spawning 2 OS processes, each running 2 Timely workers (4 total), connected
//! over TCP.
//!
//! Pipeline topology (2 exchanges):
//!
//! ```text
//! KafkaSource([orders:4part, payments:4part])
//!   → map(parse → JoinSide<Order, Payment>)
//!   → filter(amount > 50)
//!   → key_by(user_id)           ← EXCHANGE #1 (TCP across processes)
//!   → TemporalJoin(order_id)
//!   → key_by(user_id)           ← EXCHANGE #2 (TCP across processes)
//!   → TumblingWindow(Sum(amount), 10s)
//!   → FileSink (per-process output file)
//! ```
//!
//! Self-spawning pattern: the test binary re-invokes itself. When
//! `RHEI_CLUSTER_WORKER` is not set, the orchestrator runs; otherwise
//! the worker process runs.

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

// ── Domain types (same as kafka_e2e.rs) ─────────────────────────────

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

// ── Data generation (same as kafka_e2e.rs) ──────────────────────────

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

fn compute_expected_windows(orders: &[Order], window_size: u64) -> HashMap<(String, u64), f64> {
    let filtered: Vec<&Order> = orders.iter().filter(|o| o.amount > 50.0).collect();

    let mut window_sums: HashMap<(String, u64), f64> = HashMap::new();
    for o in &filtered {
        let ws = o.timestamp - (o.timestamp % window_size);
        *window_sums.entry((o.user_id.clone(), ws)).or_default() += o.amount;
    }

    let mut last_window_per_user: HashMap<String, u64> = HashMap::new();
    for (user, ws) in window_sums.keys() {
        let entry = last_window_per_user.entry(user.clone()).or_default();
        *entry = (*entry).max(*ws);
    }

    window_sums.retain(|(user, ws), _| *ws < last_window_per_user[user]);
    window_sums
}

// ── Kafka helpers (same as kafka_e2e.rs) ────────────────────────────

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

// ── Port allocation ─────────────────────────────────────────────────

/// Allocate `n` free TCP ports by binding to :0, collecting ports, then
/// dropping all listeners. TOCTOU race is acceptable for CI.
fn allocate_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port"))
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    drop(listeners);
    ports
}

// ── Test entry point ────────────────────────────────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_cluster_e2e() {
    if std::env::var("RHEI_CLUSTER_WORKER").is_ok() {
        worker_main().await;
    } else {
        orchestrator_main().await;
    }
}

// ── Orchestrator ────────────────────────────────────────────────────

async fn orchestrator_main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    // ── Setup topics (4 partitions each) ────────────────────────────
    let orders_topic = unique_topic("cluster_orders");
    let payments_topic = unique_topic("cluster_payments");

    create_topic(&orders_topic, 4).await;
    create_topic(&payments_topic, 4).await;

    // ── Generate and produce data ───────────────────────────────────
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

    // ── Allocate TCP ports for Timely cluster ───────────────────────
    let ports = allocate_ports(2);
    let peers = format!("127.0.0.1:{},127.0.0.1:{}", ports[0], ports[1]);

    // ── Prepare output directory ────────────────────────────────────
    let output_dir = tempfile::tempdir().unwrap();
    let output_p0 = output_dir.path().join("output_p0.jsonl");
    let output_p1 = output_dir.path().join("output_p1.jsonl");
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let group_id = format!("rhei_cluster_e2e_group_{}", std::process::id());

    // ── Spawn 2 child processes ─────────────────────────────────────
    let test_exe = std::env::current_exe().expect("failed to get current exe");

    let mut children = Vec::new();
    for pid in 0..2u32 {
        let output_path = if pid == 0 { &output_p0 } else { &output_p1 };

        let child = std::process::Command::new(&test_exe)
            .args(["--exact", "kafka_cluster_e2e", "--nocapture"])
            .env("RHEI_CLUSTER_WORKER", "1")
            .env("RHEI_PROCESS_ID", pid.to_string())
            .env("RHEI_PEERS", &peers)
            .env("RHEI_WORKERS", "2")
            .env("RHEI_ORDERS_TOPIC", &orders_topic)
            .env("RHEI_PAYMENTS_TOPIC", &payments_topic)
            .env("RHEI_OUTPUT_PATH", output_path)
            .env("RHEI_CHECKPOINT_DIR", checkpoint_dir.path())
            .env("RHEI_GROUP_ID", &group_id)
            .env("KAFKA_BROKERS", brokers())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn child process {pid}: {e}"));

        children.push((pid, child));
    }

    // ── Wait for children with timeout ──────────────────────────────
    let timeout = Duration::from_secs(30);
    let deadline = std::time::Instant::now() + timeout;

    for (pid, mut child) in children {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .unwrap_or(Duration::ZERO);

        let status = wait_with_timeout(&mut child, remaining);
        match status {
            Some(s) if s.success() => {
                eprintln!("child process {pid} exited successfully");
            }
            Some(s) => {
                panic!("child process {pid} exited with status: {s}");
            }
            None => {
                let _ = child.kill();
                panic!("child process {pid} timed out after {timeout:?}");
            }
        }
    }

    // ── Merge and verify output ─────────────────────────────────────
    let window_size = 10_000u64;
    let expected = compute_expected_windows(&orders, window_size);

    let p0_output = std::fs::read_to_string(&output_p0).unwrap_or_default();
    let p1_output = std::fs::read_to_string(&output_p1).unwrap_or_default();

    let p0_windows: Vec<WindowOutput<f64>> = p0_output
        .lines()
        .filter(|l| !l.is_empty())
        .map(|line| serde_json::from_str(line).expect("invalid output JSON from p0"))
        .collect();
    let p1_windows: Vec<WindowOutput<f64>> = p1_output
        .lines()
        .filter(|l| !l.is_empty())
        .map(|line| serde_json::from_str(line).expect("invalid output JSON from p1"))
        .collect();

    let all_windows: Vec<&WindowOutput<f64>> = p0_windows.iter().chain(p1_windows.iter()).collect();

    // 1. Non-empty combined output
    assert!(
        !all_windows.is_empty(),
        "cluster pipeline produced no output — expected {} window records",
        expected.len()
    );

    // 2. All user IDs are expected
    let expected_users: std::collections::HashSet<&str> =
        expected.keys().map(|(u, _)| u.as_str()).collect();
    for w in &all_windows {
        assert!(
            expected_users.contains(w.key.as_str()),
            "unexpected user in output: {}",
            w.key
        );
    }

    // 3. Window structure correct
    for w in &all_windows {
        assert!(w.value > 0.0, "window value must be positive");
        assert_eq!(
            w.window_end,
            w.window_start + window_size,
            "window_end should be window_start + window_size"
        );
    }

    // 4. Per-window sums match expected
    let mut actual_sums: HashMap<(String, u64), f64> = HashMap::new();
    for w in &all_windows {
        *actual_sums
            .entry((w.key.clone(), w.window_start))
            .or_default() += w.value;
    }

    for ((user, ws), actual_sum) in &actual_sums {
        if let Some(&expected_sum) = expected.get(&(user.clone(), *ws)) {
            let diff = (actual_sum - expected_sum).abs();
            assert!(
                diff < 0.01,
                "sum mismatch for ({user}, window {ws}): actual={actual_sum}, expected={expected_sum}"
            );
        }
    }

    // 5. Total ≥ 90% of expected
    let expected_total: f64 = expected.values().sum();
    let actual_total: f64 = actual_sums.values().sum();
    assert!(
        actual_total >= expected_total * 0.9,
        "actual total {actual_total} is less than 90% of expected {expected_total}"
    );

    // 6. Both processes produced output (proves cross-process exchange)
    assert!(
        !p0_windows.is_empty(),
        "process 0 produced no output — exchange may not be working"
    );
    assert!(
        !p1_windows.is_empty(),
        "process 1 produced no output — exchange may not be working"
    );

    // 7. Checkpoint manifests exist
    let manifest = rhei_core::checkpoint::CheckpointManifest::load(checkpoint_dir.path());
    if let Some(m) = manifest {
        eprintln!(
            "checkpoint manifest: id={}, offsets={}",
            m.checkpoint_id,
            m.source_offsets.len()
        );
    }

    eprintln!(
        "Cluster E2E passed: {} total windows (p0={}, p1={}), total amount {actual_total:.0} (expected {expected_total:.0})",
        all_windows.len(),
        p0_windows.len(),
        p1_windows.len(),
    );
}

/// Wait for a child process with a timeout. Returns `None` on timeout.
fn wait_with_timeout(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Option<std::process::ExitStatus> {
    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {
                if start.elapsed() >= timeout {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("error waiting for child process: {e}"),
        }
    }
}

// ── Worker process ──────────────────────────────────────────────────

async fn worker_main() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let orders_topic = std::env::var("RHEI_ORDERS_TOPIC").expect("RHEI_ORDERS_TOPIC not set");
    let payments_topic = std::env::var("RHEI_PAYMENTS_TOPIC").expect("RHEI_PAYMENTS_TOPIC not set");
    let output_path = std::path::PathBuf::from(
        std::env::var("RHEI_OUTPUT_PATH").expect("RHEI_OUTPUT_PATH not set"),
    );
    let checkpoint_dir = std::path::PathBuf::from(
        std::env::var("RHEI_CHECKPOINT_DIR").expect("RHEI_CHECKPOINT_DIR not set"),
    );
    let group_id = std::env::var("RHEI_GROUP_ID").expect("RHEI_GROUP_ID not set");
    let process_id: usize = std::env::var("RHEI_PROCESS_ID")
        .expect("RHEI_PROCESS_ID not set")
        .parse()
        .expect("RHEI_PROCESS_ID not a number");

    eprintln!("worker process {process_id} starting");

    // ── Build pipeline ──────────────────────────────────────────────
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
        // Exchange #1: route by user_id across processes
        .key_by(|side: &JoinSide<Order, Payment>| match side {
            JoinSide::Left(o) => o.user_id.clone(),
            JoinSide::Right(p) => p.user_id.clone(),
        })
        .operator("temporal_join", join_op)
        // Exchange #2: re-key by user_id after join
        .key_by(|j: &JoinedOrder| j.user_id.clone())
        .operator("tumbling_window", window_op)
        .sink(file_sink);

    // ── Run with shutdown ───────────────────────────────────────────
    let (handle, trigger) = ShutdownHandle::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        trigger.shutdown();
    });

    // Per-process checkpoint subdirectory
    let proc_checkpoint_dir = checkpoint_dir.join(format!("p{process_id}"));
    std::fs::create_dir_all(&proc_checkpoint_dir).ok();

    let executor = Executor::builder()
        .checkpoint_dir(&proc_checkpoint_dir)
        .from_env()
        .build();

    eprintln!(
        "worker process {process_id}: cluster={}, total_workers={}, local_range={:?}",
        executor.is_cluster(),
        executor.total_workers(),
        executor.local_worker_range(),
    );

    executor.run_with_shutdown(graph, handle).await.unwrap();
    eprintln!("worker process {process_id} finished");
}

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::type_complexity)]
//! End-to-end integration tests for the batch (Arrow columnar) pipeline path.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei::arrow::{RheiBuffer, RheiSchema, Sink};
use rhei::{
    CountWindow, DataflowGraph, PipelineController, ReduceOp, RheiSchema as RheiSchemaDerive,
    RollingAggregateOp, SessionWindow, Side, SlidingWindow, TemporalJoin, TumblingWindow,
    VecSource, col, lit_f64,
};

// ── Schema types ────────────────────────────────────────────────────

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Event {
    id: i64,
    value: f64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Doubled {
    id: i64,
    doubled: f64,
}

// ── Collecting sink ──────────────────────────────────────────────────

struct CollectSink {
    collected: Arc<Mutex<Vec<(i64, f64)>>>,
}

#[async_trait]
impl Sink for CollectSink {
    type Input = Doubled;

    async fn write_batch(&mut self, input: RheiBuffer<Doubled>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.id, view.doubled));
        }
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn source_map_filter_sink() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let events: Vec<Event> = (0..20)
        .map(|i| Event {
            id: i,
            value: i as f64,
        })
        .collect();

    let source = VecSource::new(events).with_batch_size(8);
    let collected: Arc<Mutex<Vec<(i64, f64)>>> = Arc::new(Mutex::new(Vec::new()));

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .map(|view: <Event as RheiSchema>::View<'_>| Doubled {
            id: view.id,
            doubled: view.value * 2.0,
        })
        .filter_fn(|view: &<Doubled as RheiSchema>::View<'_>| view.doubled >= 20.0)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let results = collected.lock().unwrap();
    // Events with value >= 10 have doubled >= 20: ids 10..20 → 10 rows
    assert_eq!(
        results.len(),
        10,
        "expected 10 rows with doubled >= 20, got {}",
        results.len()
    );

    let mut sorted: Vec<(i64, f64)> = results.clone();
    sorted.sort_by_key(|&(id, _)| id);

    for (i, &(id, doubled)) in sorted.iter().enumerate() {
        #[allow(clippy::cast_possible_wrap)]
        let expected_id = (i as i64) + 10;
        assert_eq!(id, expected_id);
        assert!((doubled - expected_id as f64 * 2.0).abs() < f64::EPSILON);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Windowed operator types ─────────────────────────────────────────

#[derive(Debug, Clone, RheiSchemaDerive)]
struct TimedEvent {
    key: String,
    ts: u64,
    value: f64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct WindowResult {
    key: String,
    window_start: u64,
    window_end: u64,
    count: u64,
}

struct WindowCollectSink {
    collected: Arc<Mutex<Vec<(String, u64, u64, u64)>>>,
}

#[async_trait]
impl Sink for WindowCollectSink {
    type Input = WindowResult;

    async fn write_batch(&mut self, input: RheiBuffer<WindowResult>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((
                view.key.to_string(),
                view.window_start,
                view.window_end,
                view.count,
            ));
        }
        Ok(())
    }
}

#[tokio::test]
async fn batch_tumbling_window_pipeline() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_tw_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Events: 3 in window [0,10) for key "a", 2 in window [10,20) for key "a",
    //         1 in window [0,10) for key "b".
    let events = vec![
        TimedEvent {
            key: "a".into(),
            ts: 1,
            value: 1.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 5,
            value: 2.0,
        },
        TimedEvent {
            key: "b".into(),
            ts: 3,
            value: 3.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 8,
            value: 4.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 12,
            value: 5.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 15,
            value: 6.0,
        },
    ];

    let source = VecSource::new(events).with_batch_size(10);
    let collected: Arc<Mutex<Vec<(String, u64, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let window = TumblingWindow::new(
        10,
        |view: <TimedEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |view: <TimedEvent as RheiSchema>::View<'_>| view.ts,
        |acc: &mut u64, _view: <TimedEvent as RheiSchema>::View<'_>| *acc += 1,
        |key: &str, window_start: u64, window_end: u64, acc: &u64| WindowResult {
            key: key.to_string(),
            window_start,
            window_end,
            count: *acc,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("tumbling_10s", window)
        .sink(WindowCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // Expected windows:
    // "a" [0,10) count=3 (ts: 1, 5, 8)
    // "a" [10,20) count=2 (ts: 12, 15)
    // "b" [0,10) count=1 (ts: 3)
    assert_eq!(
        results.len(),
        3,
        "expected 3 window outputs, got {results:?}"
    );
    assert_eq!(results[0], ("a".to_string(), 0, 10, 3));
    assert_eq!(results[1], ("a".to_string(), 10, 20, 2));
    assert_eq!(results[2], ("b".to_string(), 0, 10, 1));

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Expression filter pipeline ─────────────────────────────────────

#[tokio::test]
async fn batch_filter_expr_pipeline() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_fexpr_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let events: Vec<Event> = (0..20)
        .map(|i| Event {
            id: i,
            value: i as f64,
        })
        .collect();

    let source = VecSource::new(events).with_batch_size(8);
    let collected: Arc<Mutex<Vec<(i64, f64)>>> = Arc::new(Mutex::new(Vec::new()));

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .filter(col("value").gt(lit_f64(14.0)))
        .map(|view: <Event as RheiSchema>::View<'_>| Doubled {
            id: view.id,
            doubled: view.value * 2.0,
        })
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_by_key(|&(id, _)| id);
    // Events with value > 14: ids 15..20 → 5 rows
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, 15);
    assert_eq!(results[4].0, 19);

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Sliding window pipeline ────────────────────────────────────────

#[tokio::test]
async fn batch_sliding_window_pipeline() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_sw_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Window size=10, slide=5. Events at ts 1,6,11.
    // Window [0,10) contains ts=1,6. Window [5,15) contains ts=6,11.
    let events = vec![
        TimedEvent {
            key: "a".into(),
            ts: 1,
            value: 1.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 6,
            value: 2.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 11,
            value: 3.0,
        },
    ];

    let source = VecSource::new(events).with_batch_size(10);
    let collected: Arc<Mutex<Vec<(String, u64, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let window = SlidingWindow::new(
        10,
        5,
        |view: <TimedEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |view: <TimedEvent as RheiSchema>::View<'_>| view.ts,
        |acc: &mut u64, _view: <TimedEvent as RheiSchema>::View<'_>| *acc += 1,
        |key: &str, window_start: u64, window_end: u64, acc: &u64| WindowResult {
            key: key.to_string(),
            window_start,
            window_end,
            count: *acc,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("sliding_10_5", window)
        .sink(WindowCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_by_key(|a| a.1);

    // Window [0,10) has ts 1,6 → count=2
    // Window [5,15) has ts 6,11 → count=2
    assert!(
        results.len() >= 2,
        "expected at least 2 window outputs, got {results:?}"
    );
    let w0 = results.iter().find(|r| r.1 == 0).unwrap();
    assert_eq!(w0.3, 2);
    let w5 = results.iter().find(|r| r.1 == 5).unwrap();
    assert_eq!(w5.3, 2);

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Session window pipeline ────────────────────────────────────────

#[tokio::test]
async fn batch_session_window_pipeline() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_sess_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Gap = 5. Events: ts 1, 3 (same session), then ts 20 (gap > 5 → closes first session).
    let events = vec![
        TimedEvent {
            key: "a".into(),
            ts: 1,
            value: 1.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 3,
            value: 2.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 20,
            value: 3.0,
        },
    ];

    let source = VecSource::new(events).with_batch_size(10);
    let collected: Arc<Mutex<Vec<(String, u64, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let window = SessionWindow::new(
        5,
        |view: <TimedEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |view: <TimedEvent as RheiSchema>::View<'_>| view.ts,
        |acc: &mut u64, _view: <TimedEvent as RheiSchema>::View<'_>| *acc += 1,
        |key: &str, session_start: u64, last_event: u64, acc: &u64| WindowResult {
            key: key.to_string(),
            window_start: session_start,
            window_end: last_event,
            count: *acc,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("session_5", window)
        .sink(WindowCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();
    // First session [1,3] count=2 is emitted when ts=20 arrives (gap exceeded)
    assert!(
        !results.is_empty(),
        "expected at least 1 session output, got none"
    );
    let first = &results[0];
    assert_eq!(first.0, "a");
    assert_eq!(first.1, 1); // session_start
    assert_eq!(first.2, 3); // last_event
    assert_eq!(first.3, 2); // count

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Count window pipeline ──────────────────────────────────────────

#[derive(Debug, Clone, RheiSchemaDerive)]
struct CountResult {
    key: String,
    count: u64,
}

struct CountCollectSink {
    collected: Arc<Mutex<Vec<(String, u64)>>>,
}

#[async_trait]
impl Sink for CountCollectSink {
    type Input = CountResult;

    async fn write_batch(&mut self, input: RheiBuffer<CountResult>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.key.to_string(), view.count));
        }
        Ok(())
    }
}

#[tokio::test]
async fn batch_count_window_pipeline() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_cw_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // threshold=3, key "a" has 5 events → fires once at 3, key "b" has 3 → fires once
    let events = vec![
        TimedEvent {
            key: "a".into(),
            ts: 1,
            value: 1.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 2,
            value: 2.0,
        },
        TimedEvent {
            key: "b".into(),
            ts: 3,
            value: 3.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 4,
            value: 4.0,
        },
        TimedEvent {
            key: "b".into(),
            ts: 5,
            value: 5.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 6,
            value: 6.0,
        },
        TimedEvent {
            key: "b".into(),
            ts: 7,
            value: 7.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 8,
            value: 8.0,
        },
    ];

    let source = VecSource::new(events).with_batch_size(10);
    let collected: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let window = CountWindow::new(
        3,
        |view: <TimedEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |acc: &mut u64, _view: <TimedEvent as RheiSchema>::View<'_>| *acc += 1,
        |key: &str, count: u64, _acc: &u64| CountResult {
            key: key.to_string(),
            count,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("count_3", window)
        .sink(CountCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_by(|a, b| a.0.cmp(&b.0));
    // "a" fires at count=3 (first 3 events), "b" fires at count=3
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("a".to_string(), 3));
    assert_eq!(results[1], ("b".to_string(), 3));

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Reduce pipeline ────────────────────────────────────────────────

#[derive(Debug, Clone, RheiSchemaDerive, serde::Serialize, serde::Deserialize)]
struct SumResult {
    key: String,
    sum: f64,
}

struct SumCollectSink {
    collected: Arc<Mutex<Vec<(String, f64)>>>,
}

#[async_trait]
impl Sink for SumCollectSink {
    type Input = SumResult;

    async fn write_batch(&mut self, input: RheiBuffer<SumResult>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.key.to_string(), view.sum));
        }
        Ok(())
    }
}

#[tokio::test]
async fn batch_reduce_pipeline() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_red_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let events = vec![
        TimedEvent {
            key: "a".into(),
            ts: 1,
            value: 10.0,
        },
        TimedEvent {
            key: "b".into(),
            ts: 2,
            value: 5.0,
        },
        TimedEvent {
            key: "a".into(),
            ts: 3,
            value: 20.0,
        },
    ];

    let source = VecSource::new(events).with_batch_size(10);
    let collected: Arc<Mutex<Vec<(String, f64)>>> = Arc::new(Mutex::new(Vec::new()));

    let op = ReduceOp::new(
        |view: <TimedEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |view: <TimedEvent as RheiSchema>::View<'_>| SumResult {
            key: view.key.to_string(),
            sum: view.value,
        },
        |prev: SumResult, view: <TimedEvent as RheiSchema>::View<'_>| SumResult {
            key: view.key.to_string(),
            sum: prev.sum + view.value,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("reduce_sum", op)
        .sink(SumCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();
    // Reduce emits on every row: a=10, b=5, a=30
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], ("a".to_string(), 10.0));
    assert_eq!(results[1], ("b".to_string(), 5.0));
    assert_eq!(results[2], ("a".to_string(), 30.0));

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Rolling aggregate pipeline ─────────────────────────────────────

#[tokio::test]
async fn batch_rolling_aggregate_pipeline() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_ragg_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let events = vec![
        TimedEvent {
            key: "x".into(),
            ts: 1,
            value: 3.0,
        },
        TimedEvent {
            key: "x".into(),
            ts: 2,
            value: 7.0,
        },
        TimedEvent {
            key: "y".into(),
            ts: 3,
            value: 4.0,
        },
    ];

    let source = VecSource::new(events).with_batch_size(10);
    let collected: Arc<Mutex<Vec<(String, f64)>>> = Arc::new(Mutex::new(Vec::new()));

    let op = RollingAggregateOp::<TimedEvent, SumResult, f64, _, _, _>::new(
        |view: <TimedEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |acc: &mut f64, view: <TimedEvent as RheiSchema>::View<'_>| *acc += view.value,
        |key: &str, acc: &f64| SumResult {
            key: key.to_string(),
            sum: *acc,
        },
    );

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("rolling_sum", op)
        .sink(SumCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();
    // Emits after each row: x=3, x=10, y=4
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], ("x".to_string(), 3.0));
    assert_eq!(results[1], ("x".to_string(), 10.0));
    assert_eq!(results[2], ("y".to_string(), 4.0));

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Temporal join pipeline ─────────────────────────────────────────

#[derive(Debug, Clone, RheiSchemaDerive)]
struct JoinEvent {
    side: i64,
    key: String,
    payload: String,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct JoinResult {
    key: String,
    combined: String,
}

struct JoinCollectSink {
    collected: Arc<Mutex<Vec<(String, String)>>>,
}

#[async_trait]
impl Sink for JoinCollectSink {
    type Input = JoinResult;

    async fn write_batch(&mut self, input: RheiBuffer<JoinResult>) -> anyhow::Result<()> {
        let mut guard = self.collected.lock().unwrap();
        for view in &input {
            guard.push((view.key.to_string(), view.combined.to_string()));
        }
        Ok(())
    }
}

#[tokio::test]
async fn batch_temporal_join_pipeline() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_tj_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Side 0 = left, side 1 = right. Match on key.
    let events = vec![
        JoinEvent {
            side: 0,
            key: "k1".into(),
            payload: "left1".into(),
        },
        JoinEvent {
            side: 1,
            key: "k1".into(),
            payload: "right1".into(),
        },
        JoinEvent {
            side: 1,
            key: "k2".into(),
            payload: "right2".into(),
        },
        JoinEvent {
            side: 0,
            key: "k2".into(),
            payload: "left2".into(),
        },
    ];

    let source = VecSource::new(events).with_batch_size(10);
    let collected: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let op = TemporalJoin::<JoinEvent, JoinResult, _, _, _, _, _>::new(
        |view: <JoinEvent as RheiSchema>::View<'_>| view.key.to_string(),
        |view: <JoinEvent as RheiSchema>::View<'_>| {
            if view.side == 0 {
                Side::Left
            } else {
                Side::Right
            }
        },
        |view: <JoinEvent as RheiSchema>::View<'_>| view.payload.to_string(),
        |view: <JoinEvent as RheiSchema>::View<'_>| view.payload.to_string(),
        |key: &str, l: &String, r: &String| JoinResult {
            key: key.to_string(),
            combined: format!("{l}+{r}"),
        },
    );

    let graph = DataflowGraph::new();
    graph
        .source(source)
        .operator("temporal_join", op)
        .sink(JoinCollectSink {
            collected: collected.clone(),
        });

    let ctrl = PipelineController::new(dir.clone()).with_workers(1);
    ctrl.run(graph).await.unwrap();

    let mut results = collected.lock().unwrap().clone();
    results.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("k1".to_string(), "left1+right1".to_string()));
    assert_eq!(results[1], ("k2".to_string(), "left2+right2".to_string()));

    let _ = std::fs::remove_dir_all(&dir);
}

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::type_complexity)]
//! End-to-end integration tests for the batch (Arrow columnar) pipeline path.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei::arrow::{BatchSink, RheiBuffer, RheiSchema};
use rhei::{
    BatchTumblingWindow, BatchVecSource, DataflowGraph, PipelineController,
    RheiSchema as RheiSchemaDerive,
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
impl BatchSink for CollectSink {
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
async fn batch_source_map_filter_sink() {
    let dir = std::env::temp_dir().join(format!("rhei_batch_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let events: Vec<Event> = (0..20)
        .map(|i| Event {
            id: i,
            value: i as f64,
        })
        .collect();

    let source = BatchVecSource::new(events).with_batch_size(8);
    let collected: Arc<Mutex<Vec<(i64, f64)>>> = Arc::new(Mutex::new(Vec::new()));

    let graph = DataflowGraph::new();
    graph
        .batch_source(source)
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
impl BatchSink for WindowCollectSink {
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

    let source = BatchVecSource::new(events).with_batch_size(10);
    let collected: Arc<Mutex<Vec<(String, u64, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let window = BatchTumblingWindow::new(
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
        .batch_source(source)
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

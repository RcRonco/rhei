#![allow(clippy::unwrap_used, clippy::expect_used)]
//! End-to-end integration test for the batch (Arrow columnar) pipeline path.
//!
//! Verifies: `BatchVecSource` → `map` → `filter_fn` → collecting `BatchSink`
//! through the full `PipelineController` execution.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei::arrow::{BatchSink, RheiBuffer, RheiSchema};
use rhei::{DataflowGraph, PipelineController, RheiSchema as RheiSchemaDerive};
use rhei_core::connectors::batch::BatchVecSource;

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

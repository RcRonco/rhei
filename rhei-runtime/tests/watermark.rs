#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration tests for watermark propagation through the pipeline.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::connectors::vec_source::VecSource;
use rhei_core::operators::aggregator::Count;
use rhei_core::operators::tumbling_window::{TumblingWindow, WindowOutput};
use rhei_core::traits::{Sink, Source};
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::executor::Executor;
use serde::{Deserialize, Serialize};

// ── Helpers ────────────────────────────────────────────────────────

/// A `VecSource` that also reports a watermark based on a timestamp extractor.
struct WatermarkVecSource<T: Send> {
    inner: VecSource<T>,
    max_ts: Option<u64>,
    time_fn: Box<dyn Fn(&T) -> u64 + Send + Sync>,
}

impl<T: Send + Sync + Clone + 'static> WatermarkVecSource<T> {
    fn new(items: Vec<T>, time_fn: impl Fn(&T) -> u64 + Send + Sync + 'static) -> Self {
        let len = items.len();
        Self {
            inner: VecSource::new(items).with_batch_size(len),
            max_ts: None,
            time_fn: Box::new(time_fn),
        }
    }
}

#[async_trait]
impl<T: Send + Sync + Clone + 'static> Source for WatermarkVecSource<T> {
    type Output = T;

    async fn next_batch(&mut self) -> Option<Vec<T>> {
        let batch = self.inner.next_batch().await?;
        for item in &batch {
            let ts = (self.time_fn)(item);
            self.max_ts = Some(self.max_ts.map_or(ts, |prev| prev.max(ts)));
        }
        Some(batch)
    }

    fn current_watermark(&self) -> Option<u64> {
        self.max_ts
    }
}

struct CollectSink<T> {
    collected: Arc<Mutex<Vec<T>>>,
}

#[async_trait]
impl<T: Send + Sync + 'static> Sink for CollectSink<T> {
    type Input = T;

    async fn write(&mut self, input: T) -> anyhow::Result<()> {
        self.collected.lock().unwrap().push(input);
        Ok(())
    }
}

/// A timestamped event for testing.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Event {
    key: String,
    ts: u64,
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.key, self.ts)
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_watermark_propagation_closes_window() {
    let dir = std::env::temp_dir().join(format!("rhei_wm_propagation_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Events: two in window [0,10), then one in window [10,20).
    // The window [0,10) should be closed when watermark advances past 10.
    let events = vec![
        Event {
            key: "a".into(),
            ts: 1,
        },
        Event {
            key: "a".into(),
            ts: 5,
        },
        Event {
            key: "a".into(),
            ts: 15,
        }, // triggers data-driven closure of [0,10)
    ];

    let source = WatermarkVecSource::new(events, |e: &Event| e.ts);
    let collected: Arc<Mutex<Vec<WindowOutput<u64>>>> = Arc::new(Mutex::new(Vec::new()));

    let graph = DataflowGraph::new();
    let stream = graph.source(source);
    let window = TumblingWindow::new(
        10,
        |e: &Event| e.key.clone(),
        |e: &Event| e.ts,
        Count::new(),
    );
    stream
        .key_by(|e: &Event| e.key.clone())
        .operator("tumbling", window)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let executor = Executor::builder().checkpoint_dir(&dir).workers(1).build();
    executor.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();
    // The window [0,10) should have been closed with count=2
    assert!(!results.is_empty(), "expected at least one window output");
    let w = results
        .iter()
        .find(|w| w.window_start == 0)
        .expect("window [0,10) not found");
    assert_eq!(w.key, "a");
    assert_eq!(w.window_end, 10);
    assert_eq!(w.value, 2);

    let _ = std::fs::remove_dir_all(&dir);
}

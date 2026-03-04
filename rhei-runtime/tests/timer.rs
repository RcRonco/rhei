//! Integration test for timer firing on watermark advance.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::connectors::vec_source::VecSource;
use rhei_core::state::context::StateContext;
use rhei_core::traits::{Sink, Source, StreamFunction};
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::executor::Executor;
use serde::{Deserialize, Serialize};

// ── Helpers ────────────────────────────────────────────────────────

/// A `VecSource` wrapper that tracks the maximum timestamp and reports it as the
/// watermark, same pattern as `tests/watermark.rs`.
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

/// A timestamped event for timer testing.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TimerEvent {
    key: String,
    ts: u64,
}

impl std::fmt::Display for TimerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.key, self.ts)
    }
}

/// Operator that registers a timer at t=100 during `process()` and emits
/// from `on_timer()` when the watermark advances past the registered time.
#[derive(Clone)]
struct TimerOp;

#[async_trait]
impl StreamFunction for TimerOp {
    type Input = TimerEvent;
    type Output = String;

    async fn process(
        &mut self,
        input: TimerEvent,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<String>> {
        ctx.timers().register(100, input.key.clone());
        Ok(vec![]) // no immediate output
    }

    async fn on_timer(
        &mut self,
        ts: u64,
        key: &str,
        _ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<String>> {
        Ok(vec![format!("timer:{key}@{ts}")])
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn timer_fires_on_watermark_advance() {
    let dir = std::env::temp_dir().join(format!("rhei_timer_integration_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Events: first two have timestamps below 100, last has timestamp 150
    // which advances the watermark past 100, triggering the timer.
    let events = vec![
        TimerEvent {
            key: "x".into(),
            ts: 10,
        },
        TimerEvent {
            key: "x".into(),
            ts: 50,
        },
        TimerEvent {
            key: "x".into(),
            ts: 150, // watermark advances past 100 → timer fires
        },
    ];

    let source = WatermarkVecSource::new(events, |e: &TimerEvent| e.ts);
    let collected: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let graph = DataflowGraph::new();
    let stream = graph.source(source);
    stream
        .key_by(|e: &TimerEvent| e.key.clone())
        .operator("timer_op", TimerOp)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let executor = Executor::builder().checkpoint_dir(&dir).workers(1).build();
    executor.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();
    assert!(!results.is_empty(), "expected timer output, got nothing");
    assert!(
        results.contains(&"timer:x@100".to_string()),
        "expected 'timer:x@100' in {results:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

//! End-to-end pipeline throughput benchmarks.
//!
//! These drive a full `DataflowGraph` through the `PipelineController`
//! (Timely-backed executor, async source/sink bridging, state) rather than
//! invoking operators in isolation. They capture per-row overhead of the real
//! execution path and how it scales with worker count.
//!
//! Timely startup dominates very small runs, so these use a reduced sample
//! count and larger inputs to amortize fixed costs.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::doc_markdown
)]

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use rhei_core::arrow::{RheiBuffer, RheiSchema, Sink};
use rhei_core::connectors::batch::VecSource;
use rhei_core::operators::TumblingWindow;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;

use rhei_macros::RheiSchema as RheiSchemaDerive;

// ── Schemas ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Event {
    key: u64,
    ts: u64,
    value: f64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Out {
    key: u64,
    doubled: f64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct WinOut {
    window_start: u64,
    window_end: u64,
    count: u64,
}

// ── Counting sink (discards rows, records the count) ────────────────

struct CountingSink<T> {
    counter: Arc<AtomicUsize>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> CountingSink<T> {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self {
            counter,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T: RheiSchema> Sink for CountingSink<T> {
    type Input = T;

    async fn write_batch(&mut self, input: RheiBuffer<T>) -> anyhow::Result<()> {
        self.counter.fetch_add(input.len(), Ordering::Relaxed);
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

const KEYS: u64 = 128;

fn make_events(n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| Event {
            key: (i as u64) % KEYS,
            ts: i as u64,
            value: i as f64,
        })
        .collect()
}

/// `source → map → filter → sink` — the stateless fast path.
fn run_map_filter(rt: &Runtime, data: Vec<Event>, workers: usize) {
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let source = VecSource::new(data).with_batch_size(4096);
        let graph = DataflowGraph::new();
        graph
            .source(source)
            .map(|v: EventView<'_>| Out {
                key: v.key,
                doubled: v.value * 2.0,
            })
            .filter_fn(|v: &OutView<'_>| v.doubled > 100.0)
            .sink(CountingSink::<Out>::new(counter));
        let ctrl = PipelineController::new(dir.path().to_path_buf()).with_workers(workers);
        ctrl.run(graph).await.unwrap();
    });
}

/// `source → key_by → tumbling window → sink` — the stateful keyed path.
fn run_keyed_window(rt: &Runtime, data: Vec<Event>, workers: usize) {
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let source = VecSource::new(data).with_batch_size(4096);
        let window = TumblingWindow::new(
            100,
            |v: EventView<'_>| v.key.to_string(),
            |v: EventView<'_>| v.ts,
            |acc: &mut u64, _v: EventView<'_>| *acc += 1,
            |_k: &str, ws: u64, we: u64, acc: &u64| WinOut {
                window_start: ws,
                window_end: we,
                count: *acc,
            },
        );
        let graph = DataflowGraph::new();
        graph
            .source(source)
            .key_by(|v: &EventView<'_>| v.key.to_string())
            .operator("window", window)
            .sink(CountingSink::<WinOut>::new(counter));
        let ctrl = PipelineController::new(dir.path().to_path_buf()).with_workers(workers);
        ctrl.run(graph).await.unwrap();
    });
}

// ── Benchmarks ──────────────────────────────────────────────────────

fn bench_map_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("e2e_map_filter");
    for size in [10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        let data = make_events(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| run_map_filter(&rt, data.clone(), 1));
        });
    }
    group.finish();
}

fn bench_keyed_window(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("e2e_keyed_window");
    for size in [10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        let data = make_events(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| run_keyed_window(&rt, data.clone(), 1));
        });
    }
    group.finish();
}

fn bench_worker_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("e2e_worker_scaling");
    let size = 100_000;
    let data = make_events(size);
    for workers in [1usize, 2, 4] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(workers), &workers, |b, &w| {
            b.iter(|| run_map_filter(&rt, data.clone(), w));
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    // Each iteration spins up a Timely executor, so keep samples low.
    config = Criterion::default().sample_size(10);
    targets = bench_map_filter, bench_keyed_window, bench_worker_scaling
}
criterion_main!(benches);

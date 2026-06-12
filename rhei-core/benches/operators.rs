//! Throughput benchmarks for the stateful Arrow operator library.
//!
//! Covers the operators that carry per-key state through `StateContext`:
//! tumbling/sliding windows, per-key reduce, rolling aggregate, temporal
//! join, plus the stateless `flat_map` and the `key_by` partitioning path.
//!
//! Each benchmark reports throughput in elements/sec so regressions show up
//! as a drop in rows processed per second rather than an opaque time delta.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::doc_markdown
)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use rhei_core::arrow::{OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction};
use rhei_core::operators::{
    FlatMapOp, ReduceOp, RollingAggregateOp, Side, SlidingWindow, TemporalJoin, TumblingWindow,
};
use rhei_core::state::context::StateContext;
use rhei_core::state::local_backend::LocalBackend;

use rhei_macros::RheiSchema as RheiSchemaDerive;
use serde::{Deserialize, Serialize};

// ── Schema types (generated via the derive macro) ───────────────────

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Event {
    key: u64,
    ts: u64,
    value: f64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct WinOut {
    window_start: u64,
    window_end: u64,
    count: u64,
}

#[derive(Debug, Clone, RheiSchemaDerive, Serialize, Deserialize)]
struct Sum {
    key: u64,
    total: f64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Exploded {
    value: f64,
}

// ── Temporal-join schemas ───────────────────────────────────────────

#[derive(Debug, Clone, RheiSchemaDerive)]
struct JoinInput {
    key: u64,
    side: u8,
    value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeftRec {
    value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RightRec {
    value: f64,
}

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Joined {
    sum: f64,
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Number of distinct keys to spread rows across (controls state fan-out).
const KEYS: u64 = 128;

fn make_events(n: usize) -> RheiBuffer<Event> {
    let mut builder = Event::builder(n);
    for i in 0..n {
        builder.append(Event {
            key: (i as u64) % KEYS,
            ts: i as u64,
            value: i as f64,
        });
    }
    RheiBuffer::from_builder(builder)
}

fn make_join_input(n: usize) -> RheiBuffer<JoinInput> {
    let mut builder = JoinInput::builder(n);
    for i in 0..n {
        builder.append(JoinInput {
            key: (i as u64) % KEYS,
            side: (i % 2) as u8,
            value: i as f64,
        });
    }
    RheiBuffer::from_builder(builder)
}

fn make_ctx() -> (OperatorContext, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(dir.path().join("state.json"), None).unwrap();
    (
        OperatorContext::new(StateContext::new(Box::new(backend))),
        dir,
    )
}

const SIZES: [usize; 3] = [1_000, 10_000, 100_000];

// ── Benchmarks ──────────────────────────────────────────────────────

fn bench_tumbling_window(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("tumbling_window");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let buffer = make_events(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = TumblingWindow::new(
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
                    let (mut ctx, _dir) = make_ctx();
                    let _ = op.process(buffer.clone(), &mut ctx).await.unwrap();
                    // Advance the watermark past the last event to flush all windows.
                    let _ = op.on_watermark(n as u64 + 1, &mut ctx).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

fn bench_sliding_window(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("sliding_window");
    // Sliding windows are heavier (each row lands in size/slide windows), so
    // cap the largest size to keep wall-clock reasonable.
    for size in [1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let buffer = make_events(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = SlidingWindow::new(
                        100,
                        50,
                        |v: EventView<'_>| v.key.to_string(),
                        |v: EventView<'_>| v.ts,
                        |acc: &mut u64, _v: EventView<'_>| *acc += 1,
                        |_k: &str, ws: u64, we: u64, acc: &u64| WinOut {
                            window_start: ws,
                            window_end: we,
                            count: *acc,
                        },
                    );
                    let (mut ctx, _dir) = make_ctx();
                    let _ = op.process(buffer.clone(), &mut ctx).await.unwrap();
                    let _ = op.on_watermark(n as u64 + 1, &mut ctx).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

fn bench_reduce(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("reduce");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let buffer = make_events(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = ReduceOp::new(
                        |v: EventView<'_>| v.key.to_string(),
                        |v: EventView<'_>| Sum {
                            key: v.key,
                            total: v.value,
                        },
                        |mut acc: Sum, v: EventView<'_>| {
                            acc.total += v.value;
                            acc
                        },
                    );
                    let (mut ctx, _dir) = make_ctx();
                    let _ = op.process(buffer.clone(), &mut ctx).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

fn bench_rolling_aggregate(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("rolling_aggregate");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let buffer = make_events(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = RollingAggregateOp::new(
                        |v: EventView<'_>| v.key.to_string(),
                        |acc: &mut u64, _v: EventView<'_>| *acc += 1,
                        |_k: &str, acc: &u64| WinOut {
                            window_start: 0,
                            window_end: 0,
                            count: *acc,
                        },
                    );
                    let (mut ctx, _dir) = make_ctx();
                    let _ = op.process(buffer.clone(), &mut ctx).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

fn bench_temporal_join(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("temporal_join");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let buffer = make_join_input(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = TemporalJoin::new(
                        |v: JoinInputView<'_>| v.key.to_string(),
                        |v: JoinInputView<'_>| {
                            if v.side == 0 { Side::Left } else { Side::Right }
                        },
                        |v: JoinInputView<'_>| LeftRec { value: v.value },
                        |v: JoinInputView<'_>| RightRec { value: v.value },
                        |_k: &str, l: &LeftRec, r: &RightRec| Joined {
                            sum: l.value + r.value,
                        },
                    );
                    let (mut ctx, _dir) = make_ctx();
                    let _ = op.process(buffer.clone(), &mut ctx).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

fn bench_flat_map(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("flat_map");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let buffer = make_events(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = FlatMapOp::new(|v: EventView<'_>| {
                        vec![
                            Exploded { value: v.value },
                            Exploded {
                                value: v.value * 2.0,
                            },
                        ]
                    });
                    let (mut ctx, _dir) = make_ctx();
                    let _ = op.process(buffer.clone(), &mut ctx).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

fn bench_partition_by_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_by_key");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let buffer = make_events(n);
            b.iter(|| {
                // Mirrors the key_by/exchange split: hash each row's key into
                // one of N partition buffers.
                let parts = buffer.partition_by_key(|v: &EventView<'_>| v.key, 8);
                criterion::black_box(parts.len());
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_tumbling_window,
    bench_sliding_window,
    bench_reduce,
    bench_rolling_aggregate,
    bench_temporal_join,
    bench_flat_map,
    bench_partition_by_key
);
criterion_main!(benches);

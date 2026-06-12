//! Throughput benchmarks for the state hierarchy hot path.
//!
//! Operators reach state through `StateContext` (L1 memtable in front of a
//! backend) and the typed `KeyedState` wrapper. These benchmarks measure:
//!   - raw `put_raw`/`get_raw` against the L1 memtable + `LocalBackend`,
//!   - typed `KeyedState` get/put under both the JSON (default) and Bincode
//!     encoders, which is the main serialization lever operators control.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::doc_markdown
)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use rhei_core::operators::keyed_state::{BincodeEncoder, KeyedState};
use rhei_core::state::context::StateContext;
use rhei_core::state::local_backend::LocalBackend;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Stats {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

fn make_stats(i: u64) -> Stats {
    Stats {
        count: i,
        sum: i as f64,
        min: 0.0,
        max: i as f64,
    }
}

fn make_ctx() -> (StateContext, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(dir.path().join("state.json"), None).unwrap();
    (StateContext::new(Box::new(backend)), dir)
}

const SIZES: [usize; 3] = [1_000, 10_000, 100_000];

/// Raw byte put into the L1 memtable (the path every operator write takes).
fn bench_put_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_put_raw");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let value = b"the quick brown fox jumps over the lazy dog";
            b.iter(|| {
                let (mut ctx, _dir) = make_ctx();
                for i in 0..n {
                    let key = (i as u64).to_be_bytes();
                    ctx.put_raw(&key, value);
                }
                criterion::black_box(&mut ctx);
            });
        });
    }
    group.finish();
}

/// Raw byte get after populating, exercising the memtable read path.
fn bench_get_raw(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("state_get_raw");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let value = b"the quick brown fox jumps over the lazy dog";
            b.iter(|| {
                rt.block_on(async {
                    let (mut ctx, _dir) = make_ctx();
                    for i in 0..n {
                        ctx.put_raw(&(i as u64).to_be_bytes(), value);
                    }
                    for i in 0..n {
                        let got = ctx.get_raw(&(i as u64).to_be_bytes()).await.unwrap();
                        criterion::black_box(&got);
                    }
                });
            });
        });
    }
    group.finish();
}

/// Typed `KeyedState` put+get comparing the JSON vs Bincode encoders.
fn bench_keyed_state_encoders(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("keyed_state");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("json", size), &size, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let (mut ctx, _dir) = make_ctx();
                    let mut state = KeyedState::<u64, Stats>::new(&mut ctx, "stats");
                    for i in 0..n as u64 {
                        state.put(&i, &make_stats(i)).unwrap();
                    }
                    for i in 0..n as u64 {
                        let got = state.get(&i).await.unwrap();
                        criterion::black_box(&got);
                    }
                });
            });
        });

        group.bench_with_input(BenchmarkId::new("bincode", size), &size, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let (mut ctx, _dir) = make_ctx();
                    let mut state = KeyedState::<u64, Stats, BincodeEncoder>::with_encoder(
                        &mut ctx,
                        "stats",
                        BincodeEncoder,
                    );
                    for i in 0..n as u64 {
                        state.put(&i, &make_stats(i)).unwrap();
                    }
                    for i in 0..n as u64 {
                        let got = state.get(&i).await.unwrap();
                        criterion::black_box(&got);
                    }
                });
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_put_raw,
    bench_get_raw,
    bench_keyed_state_encoders
);
criterion_main!(benches);

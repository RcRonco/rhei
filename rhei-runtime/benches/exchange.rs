//! Benchmarks for the Exchange wire format.
//!
//! When `key_by` routes rows across workers, each `RheiBuffer` is type-erased
//! to an `ErasedBuffer` and serialized (Arrow IPC for the batch + bincode
//! framing) before crossing the Timely Exchange pact, then deserialized on the
//! receiving worker. These benchmarks measure encode and decode separately so
//! a regression in either direction is visible.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::doc_markdown
)]

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use rhei_core::arrow::{RheiBuffer, RheiBuilder, RheiSchema};
use rhei_runtime::arena::BatchArena;
use rhei_runtime::erased_buffer::{ErasedBuffer, KeyFn};

use rhei_macros::RheiSchema as RheiSchemaDerive;

#[derive(Debug, Clone, RheiSchemaDerive)]
struct Event {
    key: u64,
    ts: u64,
    value: f64,
    label: String,
}

fn make_events(n: usize) -> RheiBuffer<Event> {
    let mut builder = Event::builder(n);
    for i in 0..n {
        builder.append(Event {
            key: (i as u64) % 128,
            ts: i as u64,
            value: i as f64,
            label: "event-label".to_string(),
        });
    }
    RheiBuffer::from_builder(builder)
}

const SIZES: [usize; 3] = [1_000, 10_000, 100_000];

fn bench_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("exchange_serialize");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let erased = ErasedBuffer::from_typed(make_events(n));
            b.iter(|| {
                let bytes = bincode::serialize(&erased).unwrap();
                criterion::black_box(bytes.len());
            });
        });
    }
    group.finish();
}

fn bench_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("exchange_deserialize");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let erased = ErasedBuffer::from_typed(make_events(n));
            let bytes = bincode::serialize(&erased).unwrap();
            b.iter(|| {
                let decoded: ErasedBuffer = bincode::deserialize(&bytes).unwrap();
                criterion::black_box(decoded.num_rows());
            });
        });
    }
    group.finish();
}

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("exchange_roundtrip");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let buffer = make_events(n);
            b.iter(|| {
                let erased = ErasedBuffer::from_typed(buffer.clone());
                let bytes = bincode::serialize(&erased).unwrap();
                let decoded: ErasedBuffer = bincode::deserialize(&bytes).unwrap();
                criterion::black_box(decoded.num_rows());
            });
        });
    }
    group.finish();
}

/// The `key_by` split stage: hash every row's key and gather per-worker
/// sub-batches. The row-routing scratch is bump-allocated from a
/// `BatchArena` that is recycled between iterations — exactly how the
/// operator reuses it between batches at runtime.
fn bench_partition(c: &mut Criterion) {
    let key_fn: KeyFn = Arc::new(|batch, idx| {
        use arrow_array::cast::AsArray;
        use arrow_array::types::UInt64Type;
        batch
            .column(0)
            .as_primitive::<UInt64Type>()
            .value(idx)
            .to_string()
    });

    let mut group = c.benchmark_group("exchange_partition");
    for size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let erased = ErasedBuffer::from_typed(make_events(n));
            let mut arena = BatchArena::new();
            b.iter(|| {
                let parts = erased.partition_for_exchange(&key_fn, 8, &mut arena);
                criterion::black_box(parts.len());
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_serialize,
    bench_deserialize,
    bench_roundtrip,
    bench_partition
);
criterion_main!(benches);

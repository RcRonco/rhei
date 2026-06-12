//! Global-allocator comparison benchmark (documentation aid).
//!
//! rhei never imposes a global allocator — the binary or service that embeds
//! the runtime picks its own with `#[global_allocator]`. This benchmark exists
//! purely so we can document how the common choices behave on rhei's actual hot
//! paths. The allocator is selected at compile time via a feature flag, so run
//! it once per allocator and compare the reports:
//!
//! ```bash
//! cargo bench -p rhei-runtime --bench allocator                       # system (glibc)
//! cargo bench -p rhei-runtime --bench allocator --features bench-mimalloc
//! cargo bench -p rhei-runtime --bench allocator --features bench-jemalloc
//! ```
//!
//! The two workloads mirror the allocation patterns that dominate a running
//! pipeline:
//!   - `exchange_roundtrip` — single-threaded churn of short-lived buffers:
//!     per-batch build + Arrow IPC serialize/deserialize, the `key_by`/Exchange
//!     wire path.
//!   - `cross_thread_free` — buffers allocated on producer threads and freed on
//!     consumer threads. This is the pattern the Exchange pact creates and the
//!     one glibc's per-thread arenas handle worst, so it is where modern
//!     allocators tend to separate from the default.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::doc_markdown
)]

// Exactly one global allocator may be active. The features are mutually
// exclusive; with neither set the platform default (glibc ptmalloc2 on Linux)
// is used.
#[cfg(all(feature = "bench-mimalloc", feature = "bench-jemalloc"))]
compile_error!("enable at most one of `bench-mimalloc` / `bench-jemalloc`");

#[cfg(feature = "bench-mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "bench-jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::thread;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use rhei_core::arrow::{RheiBuffer, RheiBuilder, RheiSchema};
use rhei_macros::RheiSchema as RheiSchemaDerive;
use rhei_runtime::erased_buffer::ErasedBuffer;

/// Which allocator this binary was built with — folded into every benchmark id
/// so the report records the variant alongside the numbers.
const ALLOCATOR: &str = if cfg!(feature = "bench-mimalloc") {
    "mimalloc"
} else if cfg!(feature = "bench-jemalloc") {
    "jemalloc"
} else {
    "system"
};

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

/// Single-threaded: clone a buffer, erase it, and round-trip it through the
/// Exchange wire format. Each iteration allocates and frees a batch worth of
/// Arrow + IPC buffers on one thread.
fn bench_exchange_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("alloc/{ALLOCATOR}/exchange_roundtrip"));
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

/// Multi-threaded cross-thread free: `w` producer threads each build a fixed
/// number of erased batches and hand them to `w` consumer threads, which drop
/// them. The thread that frees a buffer is never the thread that allocated it —
/// the case glibc's per-arena retention handles worst and where mimalloc /
/// jemalloc's sharded free lists pay off.
fn bench_cross_thread_free(c: &mut Criterion) {
    const BATCH: usize = 2_000;
    const BATCHES_PER_PRODUCER: usize = 64;

    let mut group = c.benchmark_group(format!("alloc/{ALLOCATOR}/cross_thread_free"));
    for workers in [2usize, 4, 8] {
        let elems = (workers * BATCHES_PER_PRODUCER * BATCH) as u64;
        group.throughput(Throughput::Elements(elems));
        group.bench_with_input(BenchmarkId::from_parameter(workers), &workers, |b, &w| {
            b.iter(|| {
                let (tx, rx) = flume::bounded::<ErasedBuffer>(w * 4);
                thread::scope(|scope| {
                    // Consumers free what producers allocate.
                    for _ in 0..w {
                        let rx = rx.clone();
                        scope.spawn(move || {
                            while let Ok(buf) = rx.recv() {
                                criterion::black_box(buf.num_rows());
                                // `buf` is dropped here, off the allocating thread.
                            }
                        });
                    }
                    drop(rx);
                    for _ in 0..w {
                        let tx = tx.clone();
                        scope.spawn(move || {
                            for _ in 0..BATCHES_PER_PRODUCER {
                                tx.send(ErasedBuffer::from_typed(make_events(BATCH)))
                                    .unwrap();
                            }
                        });
                    }
                    drop(tx);
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_exchange_roundtrip, bench_cross_thread_free);
criterion_main!(benches);

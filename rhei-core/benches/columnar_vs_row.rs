//! Benchmark measuring Arrow columnar operator throughput.
//!
//! Measures throughput for map, filter, and chained operations at various batch sizes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_possible_wrap)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use rhei_core::arrow::{
    BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema,
};
use rhei_core::operators::batch::{BatchFilterFnOp, BatchMapOp};
use rhei_core::state::context::StateContext;
use rhei_core::state::local_backend::LocalBackend;

use arrow_array::builder::ArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};

// ── Schema types ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Event {
    id: i64,
    value: f64,
}

#[derive(Debug, Clone)]
struct Output {
    id: i64,
    doubled: f64,
}

// ── Manual RheiSchema impls ─────────────────────────────────────────

struct EventBuilder {
    id: arrow_array::builder::Int64Builder,
    value: arrow_array::builder::PrimitiveBuilder<Float64Type>,
}

struct EventView {
    id: i64,
    value: f64,
}

struct EventCols<'a> {
    #[allow(dead_code)]
    id: &'a arrow_array::Int64Array,
}

impl RheiBuilder for EventBuilder {
    type Item = Event;

    fn append(&mut self, item: Event) {
        self.id.append_value(item.id);
        self.value.append_value(item.value);
    }

    fn append_null(&mut self) {
        self.id.append_null();
        self.value.append_null();
    }

    fn len(&self) -> usize {
        self.id.len()
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            Event::arrow_schema(),
            vec![Arc::new(self.id.finish()), Arc::new(self.value.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for Event {
    type Builder = EventBuilder;
    type View<'a> = EventView;
    type Columns<'a> = EventCols<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("value", arrow_schema::DataType::Float64, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        EventBuilder {
            id: arrow_array::builder::Int64Builder::with_capacity(capacity),
            value: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        EventView {
            id: batch.column(0).as_primitive::<Int64Type>().value(index),
            value: batch.column(1).as_primitive::<Float64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        EventCols {
            id: batch.column(0).as_primitive::<Int64Type>(),
        }
    }
}

struct OutputBuilder {
    id: arrow_array::builder::Int64Builder,
    doubled: arrow_array::builder::PrimitiveBuilder<Float64Type>,
}

#[allow(dead_code)]
struct OutputView {
    id: i64,
    doubled: f64,
}

struct OutputCols<'a> {
    #[allow(dead_code)]
    id: &'a arrow_array::Int64Array,
}

impl RheiBuilder for OutputBuilder {
    type Item = Output;

    fn append(&mut self, item: Output) {
        self.id.append_value(item.id);
        self.doubled.append_value(item.doubled);
    }

    fn append_null(&mut self) {
        self.id.append_null();
        self.doubled.append_null();
    }

    fn len(&self) -> usize {
        self.id.len()
    }

    fn finish(mut self) -> arrow_array::RecordBatch {
        use std::sync::Arc;
        arrow_array::RecordBatch::try_new(
            Output::arrow_schema(),
            vec![Arc::new(self.id.finish()), Arc::new(self.doubled.finish())],
        )
        .unwrap()
    }
}

impl RheiSchema for Output {
    type Builder = OutputBuilder;
    type View<'a> = OutputView;
    type Columns<'a> = OutputCols<'a>;

    fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
        use std::sync::Arc;
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("doubled", arrow_schema::DataType::Float64, false),
        ]))
    }

    fn builder(capacity: usize) -> Self::Builder {
        OutputBuilder {
            id: arrow_array::builder::Int64Builder::with_capacity(capacity),
            doubled: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
        OutputView {
            id: batch.column(0).as_primitive::<Int64Type>().value(index),
            doubled: batch.column(1).as_primitive::<Float64Type>().value(index),
        }
    }

    fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
        OutputCols {
            id: batch.column(0).as_primitive::<Int64Type>(),
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn make_buffer(n: usize) -> RheiBuffer<Event> {
    let mut builder = Event::builder(n);
    for i in 0..n {
        builder.append(Event {
            id: i as i64,
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

// ── Benchmarks ──────────────────────────────────────────────────────

fn bench_map(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("map");

    for size in [100, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("columnar", size), &size, |b, &n| {
            let buffer = make_buffer(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = BatchMapOp::new(|view: EventView| Output {
                        id: view.id,
                        doubled: view.value * 2.0,
                    });
                    let (mut ctx, _dir) = make_ctx();
                    let _ = op.process(buffer.clone(), &mut ctx).await;
                });
            });
        });
    }
    group.finish();
}

fn bench_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("filter");

    for size in [100, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("columnar", size), &size, |b, &n| {
            let buffer = make_buffer(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = BatchFilterFnOp::new(|view: &EventView| view.value > 50.0);
                    let (mut ctx, _dir) = make_ctx();
                    let result = op.process(buffer.clone(), &mut ctx).await.unwrap();
                    criterion::black_box(&result);
                });
            });
        });
    }
    group.finish();
}

fn bench_filter_expr(c: &mut Criterion) {
    use rhei_core::operators::batch::filter_expr::eval_predicate;
    use rhei_core::operators::batch::{col, lit_f64};

    let mut group = c.benchmark_group("filter_expr");

    for size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        // Arrow kernel predicate eval (pure compute, no operator overhead)
        group.bench_with_input(BenchmarkId::new("kernel_only", size), &size, |b, &n| {
            let buffer = make_buffer(n);
            let expr = col("value").gt(lit_f64(50.0));
            let batch = buffer.as_record_batch();
            b.iter(|| {
                let mask = eval_predicate(&expr, batch).unwrap();
                criterion::black_box(mask.len());
            });
        });

        // Full operator (kernel + and_mask)
        group.bench_with_input(BenchmarkId::new("operator", size), &size, |b, &n| {
            use rhei_core::operators::batch::BatchFilterExprOp;
            let rt = tokio::runtime::Runtime::new().unwrap();
            let buffer = make_buffer(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut op = BatchFilterExprOp::<Event>::new(col("value").gt(lit_f64(50.0)));
                    let (mut ctx, _dir) = make_ctx();
                    let result = op.process(buffer.clone(), &mut ctx).await.unwrap();
                    criterion::black_box(&result);
                });
            });
        });
    }
    group.finish();
}

fn bench_map_filter_chain(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("map_then_filter");

    for size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("columnar", size), &size, |b, &n| {
            let buffer = make_buffer(n);
            b.iter(|| {
                rt.block_on(async {
                    let mut map_op = BatchMapOp::new(|view: EventView| Output {
                        id: view.id,
                        doubled: view.value * 2.0,
                    });
                    let mut filter_op =
                        BatchFilterFnOp::new(|view: &OutputView| view.doubled > 100.0);
                    let (mut ctx, _dir) = make_ctx();
                    let mapped = map_op.process(buffer.clone(), &mut ctx).await.unwrap();
                    if let BufferOutput::Single(buf) = mapped {
                        let _ = filter_op.process(buf, &mut ctx).await.unwrap();
                    }
                });
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_map,
    bench_filter,
    bench_filter_expr,
    bench_map_filter_chain
);
criterion_main!(benches);

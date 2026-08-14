# Operator Reference

Every built-in operator, its exact constructor, and a compiled example. All Rust blocks on this page are compiled by CI.

Operators come in two families:

- **Stream methods** — `map`, `filter`, `key_by`, … — called directly on a `Stream<'a, T>`.
- **`StreamFunction` implementations** — windows, joins, pattern matching — constructed as values and attached with `.operator("name", op)`.

Constructor style is not uniform, and the difference is load-bearing:

| Family | Style |
|--------|-------|
| Windows (`TumblingWindow`, `SlidingWindow`, `SessionWindow`, `CountWindow`) | `::new(...)` with positional closures, plus `.with_allowed_lateness(n)` |
| `TemporalJoin` | `::new(...)` with positional closures, plus `.with_timeout(n)` |
| `ReduceOp`, `RollingAggregateOp` | `::new(...)` with positional closures |
| `SequenceDetect` | **builder** — `::builder()....build()` |

---

## Stream methods

All of these live on `Stream<'a, T>` and take closures over **zero-copy views**.

| Method | Signature | Data movement |
|--------|-----------|---------------|
| `map` | `Fn(T::View<'_>) -> O` | none |
| `flat_map` | `Fn(T::View<'_>) -> Vec<O>` | none |
| `filter_fn` | `Fn(&T::View<'_>) -> bool` | none |
| `filter` | `Expr` | none |
| `inspect` | `Fn(&T::View<'_>)` | none |
| `distinct_by` | `Fn(&T::View<'_>) -> String` | none |
| `limit` | `usize` | none |
| `batch` | `usize` | none |
| `name` | `&str` | none |
| `merge` | `Stream<'a, T>` | none |
| `key_by` | `Fn(&T::View<'_>) -> String` | **exchange** |
| `operator` | `StreamFunction<Input = T> + Clone` | none |
| `sink` | `Sink<Input = T>` | none |

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Event {
    user: String,
    path: String,
}

#[derive(Clone, rhei::RheiSchema)]
struct Word {
    user: String,
    segment: String,
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![Event {
            user: "u1".into(),
            path: "/a/b".into(),
        }]))
        .inspect(|e| eprintln!("seen user={}", e.user))
        .filter_fn(|e| !e.path.is_empty())
        .flat_map(|e| {
            e.path
                .split('/')
                .filter(|s| !s.is_empty())
                .map(|s| Word {
                    user: e.user.to_string(),
                    segment: s.to_string(),
                })
                .collect()
        })
        .distinct_by(|w| format!("{}:{}", w.user, w.segment))
        .limit(1_000)
        .name("segments")
        .sink(PrintSink::<Word>::new());
}
```

### Filter expressions

`filter` takes an `Expr`, evaluated as an Arrow compute kernel over the whole column — cheaper than a per-row closure for plain comparisons.

Builders: `col`, `lit_i64`, `lit_u64`, `lit_f64`, `lit_str`, `lit_bool`.
Combinators: `gt`, `gt_eq`, `lt`, `lt_eq`, `eq`, `not_eq`, `and`, `or`, `negate`.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource, col, lit_f64, lit_str};

#[derive(Clone, rhei::RheiSchema)]
struct Reading {
    sensor_id: String,
    celsius: f64,
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![Reading {
            sensor_id: "boiler-1".into(),
            celsius: 91.0,
        }]))
        .filter(
            col("celsius")
                .gt(lit_f64(90.0))
                .and(col("sensor_id").not_eq(lit_str("test-probe"))),
        )
        .sink(PrintSink::<Reading>::new());
}
```

---

## Windows

All windows are **event-time** based. They fire when the watermark passes `window_end + allowed_lateness` — see [time-and-watermarks.md](time-and-watermarks.md). Events later than that are dropped and counted in `late_events_dropped_total`; there is no side output for them.

The accumulator type is inferred from `accumulate_fn` and must implement `Default`.

### `TumblingWindow`

Fixed-size, non-overlapping windows. `window_size` must be > 0.

```text
TumblingWindow::new(window_size, key_fn, time_fn, accumulate_fn, finish_fn)
    .with_allowed_lateness(lateness)   // optional, default 0
```

| Closure | Signature |
|---------|-----------|
| `key_fn` | `Fn(I::View<'_>) -> String` |
| `time_fn` | `Fn(I::View<'_>) -> u64` |
| `accumulate_fn` | `Fn(&mut Acc, I::View<'_>)` |
| `finish_fn` | `Fn(&str, u64, u64, &Acc) -> O` — key, window_start, window_end, accumulator |

```rust,no_run
use rhei::{DataflowGraph, PrintSink, TumblingWindow, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Reading {
    sensor_id: String,
    celsius: f64,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Stats {
    sensor_id: String,
    window_start: u64,
    window_end: u64,
    max_celsius: f64,
}

fn build(graph: &DataflowGraph) {
    let window = TumblingWindow::new(
        60_000,
        |v: ReadingView<'_>| v.sensor_id.to_string(),
        |v: ReadingView<'_>| v.ts,
        |acc: &mut f64, v: ReadingView<'_>| *acc = acc.max(v.celsius),
        |key: &str, start: u64, end: u64, acc: &f64| Stats {
            sensor_id: key.to_string(),
            window_start: start,
            window_end: end,
            max_celsius: *acc,
        },
    )
    .with_allowed_lateness(5_000);

    graph
        .source(VecSource::new(vec![Reading {
            sensor_id: "s1".into(),
            celsius: 20.0,
            ts: 1,
        }]))
        .key_by(|r| r.sensor_id.to_string())
        .operator("tumble", window)
        .sink(PrintSink::<Stats>::new());
}
```

### `SlidingWindow`

Overlapping windows. Each event belongs to `window_size / slide` windows, so cost scales with that ratio.

```text
SlidingWindow::new(window_size, slide, key_fn, time_fn, accumulate_fn, finish_fn)
    .with_allowed_lateness(lateness)
```

Asserted at construction: `window_size > 0`, `slide > 0`, `slide <= window_size`.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, SlidingWindow, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Hit {
    user: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Rate {
    user: String,
    window_start: u64,
    window_end: u64,
    hits: u64,
}

fn build(graph: &DataflowGraph) {
    // 5-minute window, advancing every minute.
    let window = SlidingWindow::new(
        300_000,
        60_000,
        |v: HitView<'_>| v.user.to_string(),
        |v: HitView<'_>| v.ts,
        |acc: &mut u64, _v: HitView<'_>| *acc += 1,
        |key: &str, start: u64, end: u64, acc: &u64| Rate {
            user: key.to_string(),
            window_start: start,
            window_end: end,
            hits: *acc,
        },
    );

    graph
        .source(VecSource::new(vec![Hit { user: "u1".into(), ts: 1 }]))
        .key_by(|h| h.user.to_string())
        .operator("rate", window)
        .sink(PrintSink::<Rate>::new());
}
```

> **KI-10:** closed sliding windows are not evicted from `ActiveWindows`. With a
> small slide relative to window size, per-key bookkeeping accumulates. Prefer a
> slide that is a meaningful fraction of the window.

### `SessionWindow`

Gap-based: a session closes after `gap` of inactivity for that key. Window bounds are data-driven, not aligned to a grid.

```text
SessionWindow::new(gap, key_fn, time_fn, accumulate_fn, finish_fn)
    .with_allowed_lateness(lateness)
```

```rust,no_run
use rhei::{DataflowGraph, PrintSink, SessionWindow, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Click {
    user: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Session {
    user: String,
    start: u64,
    end: u64,
    clicks: u64,
}

fn build(graph: &DataflowGraph) {
    // Close a session after 30 minutes of inactivity.
    let window = SessionWindow::new(
        1_800_000,
        |v: ClickView<'_>| v.user.to_string(),
        |v: ClickView<'_>| v.ts,
        |acc: &mut u64, _v: ClickView<'_>| *acc += 1,
        |key: &str, start: u64, end: u64, acc: &u64| Session {
            user: key.to_string(),
            start,
            end,
            clicks: *acc,
        },
    );

    graph
        .source(VecSource::new(vec![Click { user: "u1".into(), ts: 1 }]))
        .key_by(|c| c.user.to_string())
        .operator("sessions", window)
        .sink(PrintSink::<Session>::new());
}
```

### `CountWindow`

Fires every `threshold` rows per key. **No `time_fn`** — it is count-triggered, so it does not depend on watermarks.

```text
CountWindow::new(threshold, key_fn, accumulate_fn, finish_fn)
```

| Closure | Signature |
|---------|-----------|
| `key_fn` | `Fn(I::View<'_>) -> String` |
| `accumulate_fn` | `Fn(&mut Acc, I::View<'_>)` |
| `finish_fn` | `Fn(&str, u64, &Acc) -> O` — key, **row count**, accumulator |

Note the middle argument: `CountWindow` reports how many rows triggered the
fire, where the time windows report `window_start` and `window_end`.

```rust,no_run
use rhei::{CountWindow, DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Sample {
    device: String,
    value: f64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Batch100 {
    device: String,
    rows: u64,
    sum: f64,
}

fn build(graph: &DataflowGraph) {
    let window = CountWindow::new(
        100,
        |v: SampleView<'_>| v.device.to_string(),
        |acc: &mut f64, v: SampleView<'_>| *acc += v.value,
        |key: &str, count: u64, acc: &f64| Batch100 {
            device: key.to_string(),
            rows: count,
            sum: *acc,
        },
    );

    graph
        .source(VecSource::new(vec![Sample {
            device: "d1".into(),
            value: 1.0,
        }]))
        .key_by(|s| s.device.to_string())
        .operator("every_100", window)
        .sink(PrintSink::<Batch100>::new());
}
```

---

## Joins

### `TemporalJoin`

Joins two logical streams that have been **merged into one typed stream** and keyed on the join key. Rather than two input ports, you tell the operator which side each row is on.

```text
TemporalJoin::new(key_fn, side_fn, left_fn, right_fn, join_fn)
    .with_timeout(timeout)   // optional watermark-driven eviction
```

| Closure | Signature | Purpose |
|---------|-----------|---------|
| `key_fn` | `Fn(I::View<'_>) -> String` | join key |
| `side_fn` | `Fn(I::View<'_>) -> Side` | `Side::Left` or `Side::Right` |
| `left_fn` | `Fn(I::View<'_>) -> L` | extract the left payload (serializable) |
| `right_fn` | `Fn(I::View<'_>) -> R` | extract the right payload (serializable) |
| `join_fn` | `Fn(&str, &L, &R) -> O` | produce the joined row; first argument is the join key |

`L` and `R` must be `Serialize + DeserializeOwned` — they are buffered in state until their counterpart arrives.

`with_timeout(n)` evicts buffered events once `watermark >= buffered_timestamp + n`, tracked by `temporal_join_evicted_total`. **Without a timeout, unmatched events are buffered indefinitely.** Set one for any unbounded stream.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, Side, TemporalJoin, VecSource};
use serde::{Deserialize, Serialize};

// Both legs are carried in one schema, tagged by `is_order`.
#[derive(Clone, rhei::RheiSchema)]
struct Leg {
    order_id: String,
    is_order: bool,
    amount: f64,
    carrier: String,
}

#[derive(Clone, rhei::RheiSchema)]
struct Fulfilled {
    order_id: String,
    amount: f64,
    carrier: String,
}

#[derive(Serialize, Deserialize)]
struct OrderPayload {
    amount: f64,
}

#[derive(Serialize, Deserialize)]
struct ShipmentPayload {
    carrier: String,
}

fn build(graph: &DataflowGraph) {
    let join = TemporalJoin::new(
        |v: LegView<'_>| v.order_id.to_string(),
        |v: LegView<'_>| if v.is_order { Side::Left } else { Side::Right },
        |v: LegView<'_>| OrderPayload { amount: v.amount },
        |v: LegView<'_>| ShipmentPayload {
            carrier: v.carrier.to_string(),
        },
        |key: &str, order: &OrderPayload, shipment: &ShipmentPayload| Fulfilled {
            order_id: key.to_string(),
            amount: order.amount,
            carrier: shipment.carrier.clone(),
        },
    )
    .with_timeout(3_600_000); // give up after an hour of event time

    let orders = graph.source(VecSource::new(vec![Leg {
        order_id: "o1".into(),
        is_order: true,
        amount: 42.0,
        carrier: String::new(),
    }]));
    let shipments = graph.source(VecSource::new(vec![Leg {
        order_id: "o1".into(),
        is_order: false,
        amount: 0.0,
        carrier: "dhl".into(),
    }]));

    orders
        .merge(shipments)
        .key_by(|l| l.order_id.to_string())
        .operator("fulfilment_join", join)
        .sink(PrintSink::<Fulfilled>::new());
}
```

The `merge` → `key_by` → `operator` sequence is mandatory: merge discards partitioning, so the `key_by` after it is what puts both legs of an order on the same worker.

---

## Aggregation

### `ReduceOp`

Per-key fold with an explicit initial value. Emits on every row — the running value, not a windowed one.

```text
ReduceOp::new(key_fn, init_fn, reduce_fn)
```

### `RollingAggregateOp`

Per-key accumulator emitting a derived output on each row.

```text
RollingAggregateOp::new(key_fn, accumulate_fn, finish_fn)
```

```rust,no_run
use rhei::{DataflowGraph, PrintSink, RollingAggregateOp, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Trade {
    symbol: String,
    price: f64,
}

#[derive(Clone, rhei::RheiSchema)]
struct RunningMean {
    symbol: String,
    mean: f64,
}

fn build(graph: &DataflowGraph) {
    let agg = RollingAggregateOp::new(
        |v: TradeView<'_>| v.symbol.to_string(),
        |acc: &mut (u64, f64), v: TradeView<'_>| {
            acc.0 += 1;
            acc.1 += v.price;
        },
        |key: &str, acc: &(u64, f64)| RunningMean {
            symbol: key.to_string(),
            #[allow(clippy::cast_precision_loss)]
            mean: if acc.0 == 0 { 0.0 } else { acc.1 / acc.0 as f64 },
        },
    );

    graph
        .source(VecSource::new(vec![Trade {
            symbol: "AAPL".into(),
            price: 100.0,
        }]))
        .key_by(|t| t.symbol.to_string())
        .operator("running_mean", agg)
        .sink(PrintSink::<RunningMean>::new());
}
```

---

## Pattern matching

### `SequenceDetect`

Ordered event-sequence matching — the equivalent of SQL `MATCH_RECOGNIZE`. This is the one operator with a **builder**.

| Builder method | Purpose |
|----------------|---------|
| `key_fn(f)` | partition key (required) |
| `time_fn(f)` | event timestamp (required) |
| `correlate_by(f)` | events in one sequence must share this value; partition becomes key + correlation |
| `step(name, predicate)` | add an ordered step; predicate is `Fn(I::View<'_>) -> bool` |
| `at_least(n)` / `at_most(n)` | repetition bounds for the step just added |
| `within(Duration)` | max span from first to last event |
| `after_match(strategy)` | `AfterMatch::SkipToNextRow` (default, overlapping) or `SkipPastMatch` (non-overlapping) |
| `retain_events()` | buffer matched events for access in `emit` |
| `max_in_flight(n)` | cap in-flight sequences per partition (default 1000) |
| `emit(f)` | called on a match: `Fn(&str, &MatchCtx) -> O` (required) |
| `build()` | construct; panics if a required field is missing |

`MatchCtx` exposes `first_time()`, `last_time()`, `duration()`, and `step_count(name)`.

```rust,no_run
use std::time::Duration;

use rhei::{AfterMatch, DataflowGraph, MatchCtx, PrintSink, SequenceDetect, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Auth {
    account: String,
    outcome: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Suspicious {
    account: String,
    failures: u64,
    span_ms: u64,
}

fn build(graph: &DataflowGraph) {
    // Three or more failures, then a success, within five minutes.
    let detector = SequenceDetect::builder()
        .key_fn(|v: AuthView<'_>| v.account.to_string())
        .time_fn(|v: AuthView<'_>| v.ts)
        .step("fail", |v: AuthView<'_>| v.outcome == "fail")
        .at_least(3)
        .step("success", |v: AuthView<'_>| v.outcome == "success")
        .within(Duration::from_secs(300))
        .after_match(AfterMatch::SkipPastMatch)
        .emit(|key: &str, ctx: &MatchCtx| Suspicious {
            account: key.to_string(),
            failures: ctx.step_count("fail") as u64,
            span_ms: ctx.duration(),
        })
        .build();

    graph
        .source(VecSource::new(vec![Auth {
            account: "a1".into(),
            outcome: "fail".into(),
            ts: 1,
        }]))
        .key_by(|a| a.account.to_string())
        .operator("brute_force", detector)
        .sink(PrintSink::<Suspicious>::new());
}
```

---

## State primitives

Used *inside* operators, not attached to streams.

| Type | Shape |
|------|-------|
| `KeyedState<K, V>` | key-value map, `get(&K).await`, `put(&K, &V)` |
| `ValueState` | single value per key |
| `ListState` | append-only list per key |
| `MapState` | nested map per key |
| `TimerService` | register event-time timers; fire via `on_timer` |

`KeyedState` uses JSON encoding by default. For compactness, use bincode:

```rust,no_run
use rhei::arrow::{BufferOutput, OperatorContext, RheiBuffer};
use rhei::KeyedState;
use rhei_core::operators::keyed_state::BincodeEncoder;

#[derive(Clone, rhei::RheiSchema)]
struct Event {
    key: String,
}

#[derive(Clone)]
struct Op;

#[async_trait::async_trait]
impl rhei::arrow::StreamFunction for Op {
    type Input = Event;
    type Output = Event;

    async fn process(
        &mut self,
        input: RheiBuffer<Event>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Event>> {
        let mut state =
            KeyedState::<String, u64, _>::with_encoder(&mut ctx.state, "counts", BincodeEncoder);
        for view in &input {
            let key = view.key.to_string();
            let n = state.get(&key).await?.unwrap_or(0) + 1;
            state.put(&key, &n)?;
        }
        Ok(BufferOutput::None)
    }
}
```

---

## Connectors

### Sources

| Source | Notes |
|--------|-------|
| `VecSource<T>` | In-memory; `.with_batch_size(n)`, default 1024. Testing and examples |
| `PartitionedVecSource<T>` | In-memory with `partition_count()`, for exercising parallel consumption |
| `KafkaSource` | `kafka` feature. Consumer group, per-partition parallel readers, watermarks, checkpointed offsets |

### Sinks

| Sink | Notes |
|------|-------|
| `PrintSink<T>` | stdout; `.with_prefix(s)` |
| `KafkaSink` | `kafka` feature |

### DLQ sinks

| Sink | Notes |
|------|-------|
| `FileDlqSink` | newline-delimited records to a file |
| `LogDlqSink` | `tracing::error!` per record |
| `KafkaDlqSink` | `kafka` feature |

Attach with `.dlq_sink(..)` on the controller together with `.error_policy(ErrorPolicy::SendToDlq)`.

---

## Writing your own operator

Implement `StreamFunction`. The full trait, with defaults:

| Method | Default | When to override |
|--------|---------|------------------|
| `process` | required | Every operator |
| `open(ctx)` | no-op | Restore state or warm caches at startup |
| `close()` | no-op | Flush external resources |
| `on_watermark(wm, ctx)` | returns `None` | Time-driven emission (this is how windows fire) |
| `on_timer(ts, key, ctx)` | returns `None` | Per-key timers registered via `TimerService` |
| `on_error(err, ctx)` | propagates | Recover instead of failing the batch |

```rust,no_run
use rhei::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};
use rhei::KeyedState;

#[derive(Clone, rhei::RheiSchema)]
struct Ping {
    device: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Offline {
    device: String,
    last_seen: u64,
}

/// Emits an `Offline` row for any device silent for `timeout` of event time.
#[derive(Clone)]
struct OfflineDetector {
    timeout: u64,
}

#[async_trait::async_trait]
impl StreamFunction for OfflineDetector {
    type Input = Ping;
    type Output = Offline;

    async fn process(
        &mut self,
        input: RheiBuffer<Ping>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Offline>> {
        let mut last = KeyedState::<String, u64>::new(&mut ctx.state, "last_seen");
        for view in &input {
            last.put(&view.device.to_string(), &view.ts)?;
        }
        // Nothing to emit on data arrival — emission is watermark-driven.
        Ok(BufferOutput::None)
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Offline>> {
        let mut builder = Offline::builder(0);
        let mut last = KeyedState::<String, u64>::new(&mut ctx.state, "last_seen");

        // A real implementation would iterate the operator's owned key groups;
        // this shows the shape of watermark-driven emission.
        if let Some(ts) = last.get(&"device-1".to_string()).await?
            && watermark.saturating_sub(ts) > self.timeout
        {
            builder.append(Offline {
                device: "device-1".to_string(),
                last_seen: ts,
            });
        }

        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}
```

Operators must be `Clone` (one instance per worker) and `Send + Sync`. State is per worker; two workers never share a `KeyedState`.

---

## Choosing an operator

| You want | Use |
|----------|-----|
| Drop rows by a column comparison | `filter` with an `Expr` |
| Drop rows by arbitrary logic | `filter_fn` |
| Reshape rows | `map` / `flat_map` |
| Non-overlapping periodic aggregates | `TumblingWindow` |
| Moving averages, overlapping periods | `SlidingWindow` |
| Activity bursts with idle gaps | `SessionWindow` |
| Fire every N rows, ignore time | `CountWindow` |
| Running total emitted per row | `RollingAggregateOp` / `ReduceOp` |
| Correlate two streams on a key | `merge` → `key_by` → `TemporalJoin` |
| "A then B then C within T" | `SequenceDetect` |
| Anything else stateful | Your own `StreamFunction` |

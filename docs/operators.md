# Operator Reference

Every built-in operator, its exact constructor, and a compiled example. All Rust blocks on this page are compiled by CI.

Operators come in two families:

- **Stream methods** — `map`, `filter`, `key_by`, … — called directly on a `Stream<'a, T>`.
- **`StreamFunction` implementations** — windows, joins, pattern matching — constructed as values and attached with `.operator("name", op)`.

Constructor style is not uniform, and the difference is load-bearing:

| Family | Style |
|--------|-------|
| Windows (`TumblingWindow`, `SlidingWindow`, `SessionWindow`, `CountWindow`) | **builder** — `Window::tumbling(n)....build()`, or `::new(...)` with positional closures plus `.with_allowed_lateness(n)` |
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
struct PageView {
    user_id: String,
    path: String,
}

#[derive(Clone, rhei::RheiSchema)]
struct Segment {
    user_id: String,
    name: String,
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            path: "/product/shoes".into(),
        }]))
        .inspect(|v| eprintln!("seen user={}", v.user_id))
        .filter_fn(|v| !v.path.is_empty())
        // "/product/shoes" -> two segment rows
        .flat_map(|v| {
            v.path
                .split('/')
                .filter(|s| !s.is_empty())
                .map(|s| Segment {
                    user_id: v.user_id.to_string(),
                    name: s.to_string(),
                })
                .collect()
        })
        // Each user counts once per distinct segment.
        .distinct_by(|s| format!("{}:{}", s.user_id, s.name))
        .limit(1_000)
        .name("segments")
        .sink(PrintSink::<Segment>::new());
}
```

### Filter expressions

`filter` takes an `Expr`, evaluated as an Arrow compute kernel over the whole column — cheaper than a per-row closure for plain comparisons.

Builders: `col`, `lit_i64`, `lit_u64`, `lit_f64`, `lit_str`, `lit_bool`.
Combinators: `gt`, `gt_eq`, `lt`, `lt_eq`, `eq`, `not_eq`, `and`, `or`, `negate`, `is_null`, `is_not_null`.

Literals are narrowed to the column's Arrow type when the predicate runs, so
`lit_i64` works against an `Int32` or `UInt16` column. A literal that does not
fit the column, or one of the wrong kind, is a pipeline error.

The [typed handles](#typed-columns) below check the column name and the literal
type at compile time and are the preferred form.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource, col, lit_f64, lit_str};

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    dwell_ms: f64,
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            dwell_ms: 4_200.0,
        }]))
        // Engaged views, excluding synthetic monitoring traffic.
        .filter(
            col("dwell_ms")
                .gt(lit_f64(3_000.0))
                .and(col("user_id").not_eq(lit_str("monitoring"))),
        )
        .sink(PrintSink::<PageView>::new());
}
```

### Typed columns

`col("dwell_ms")` and `lit_f64(..)` are unchecked: a misspelled column or a
literal of the wrong width is only discovered when the batch is evaluated.
`#[derive(RheiSchema)]` also generates a typed handle per field, reached
through the generated `col()` associated function, so the name comes from the
schema and the literal type comes from the field:

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    dwell_ms: f64,
    referrer: Option<String>,
}

fn build(graph: &DataflowGraph) {
    let c = PageView::col();
    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            dwell_ms: 4_200.0,
            referrer: None,
        }]))
        .filter(
            c.dwell_ms()
                .gt(3_000.0)
                .and(c.user_id().not_eq("monitoring"))
                .and(c.referrer().is_not_null()),
        )
        .sink(PrintSink::<PageView>::new());
}
```

`PageView::col().dwell_ms()` is a `Col<f64>`; `PageView::col().referrer()` is a
`Col<String>` — an `Option<T>` field compares against plain `T`. Beyond the six
comparisons, a `Col<T>` offers:

| Method | Meaning |
|--------|---------|
| `between(low, high)` | inclusive range, `low <= column <= high` |
| `is_in([a, b, ..])` | membership; an empty set matches no rows |
| `is_null()`, `is_not_null()` | null checks, also available on any `Expr` |
| `expr()`, `name()` | drop down to the untyped `Expr` / the column name |

A `Col<bool>` is a predicate on its own: `filter` takes anything convertible
into an `Expr`, so `.filter(PageView::col().is_bot())` needs no comparison.

Columns whose type has no scalar form (`Vec<T>`, `Vec<u8>`) still get a handle,
but only `expr()`, `name()`, and the null checks apply to it.

Literals widen to `i64`/`u64`/`f64` and are narrowed back to the column's Arrow
type when the predicate runs, so an `i32` or `f32` column compares correctly. A
literal that does not fit the column — or one of the wrong kind entirely — is a
pipeline error, not a panic.

---

## Windows

All windows are **event-time** based. They fire when the watermark passes `window_end + allowed_lateness` — see [time-and-watermarks.md](time-and-watermarks.md). Events later than that are dropped and counted in `late_events_dropped_total`; there is no side output for them.

The accumulator type is inferred from `accumulate_fn` and must implement `Default`.

### Building a window

Every window takes the same four closures, and at a call site four positional
lambdas are hard to tell apart. `Window` names them:

```text
Window::tumbling(window_size)        // or ::sliding(size, slide), ::session(gap), ::count(n)
    .key(key_fn)
    .time(time_fn)                   // not on ::count
    .accumulate(accumulate_fn)
    .finish(finish_fn)
    .allowed_lateness(lateness)      // optional, default 0; not on ::count
    .build()
```

Slots may be filled in any order, and each one is tracked in the builder's
type: `build()` does not exist until every required closure has been supplied,
so a forgotten `.time(..)` fails to compile rather than at run time. The result
is exactly the operator the positional constructor produces — the builder adds
no runtime cost, and `::new(...)` remains available.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource, Window};

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct PerMinute {
    user_id: String,
    window_start: u64,
    window_end: u64,
    views: u64,
}

fn build(graph: &DataflowGraph) {
    // Page views per user per minute, tolerating 5s of lateness.
    let window = Window::tumbling(60_000)
        .key(|v: PageViewView<'_>| v.user_id.to_string())
        .time(|v: PageViewView<'_>| v.ts)
        .accumulate(|acc: &mut u64, _v: PageViewView<'_>| *acc += 1)
        .finish(|user_id: &str, start: u64, end: u64, views: &u64| PerMinute {
            user_id: user_id.to_string(),
            window_start: start,
            window_end: end,
            views: *views,
        })
        .allowed_lateness(5_000)
        .build();

    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            ts: 60_000,
        }]))
        .key_by(|v| v.user_id.to_string())
        .operator("tumble", window)
        .sink(PrintSink::<PerMinute>::new());
}
```

The sections below give each window's parameters and closure signatures using
the positional constructor; the builder takes the same closures under the names
above.

### `TumblingWindow`

Fixed-size, non-overlapping windows. `window_size` must be > 0.

```text
Window::tumbling(window_size)....build()
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
struct PageView {
    user_id: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct PerMinute {
    user_id: String,
    window_start: u64,
    window_end: u64,
    views: u64,
}

fn build(graph: &DataflowGraph) {
    // Page views per user per minute.
    let window = TumblingWindow::new(
        60_000,
        |v: PageViewView<'_>| v.user_id.to_string(),
        |v: PageViewView<'_>| v.ts,
        |acc: &mut u64, _v: PageViewView<'_>| *acc += 1,
        |user_id: &str, start: u64, end: u64, views: &u64| PerMinute {
            user_id: user_id.to_string(),
            window_start: start,
            window_end: end,
            views: *views,
        },
    )
    .with_allowed_lateness(5_000);

    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            ts: 60_000,
        }]))
        .key_by(|v| v.user_id.to_string())
        .operator("tumble", window)
        .sink(PrintSink::<PerMinute>::new());
}
```

### `SlidingWindow`

Overlapping windows. Each event belongs to `window_size / slide` windows, so cost scales with that ratio.

```text
Window::sliding(window_size, slide)....build()
SlidingWindow::new(window_size, slide, key_fn, time_fn, accumulate_fn, finish_fn)
    .with_allowed_lateness(lateness)
```

Asserted at construction: `window_size > 0`, `slide > 0`, `slide <= window_size`.

```rust,no_run
use rhei::{DataflowGraph, PrintSink, SlidingWindow, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Rate {
    user_id: String,
    window_start: u64,
    window_end: u64,
    views: u64,
}

fn build(graph: &DataflowGraph) {
    // Trailing 5-minute view rate, refreshed every minute.
    let window = SlidingWindow::new(
        300_000,
        60_000,
        |v: PageViewView<'_>| v.user_id.to_string(),
        |v: PageViewView<'_>| v.ts,
        |acc: &mut u64, _v: PageViewView<'_>| *acc += 1,
        |user_id: &str, start: u64, end: u64, views: &u64| Rate {
            user_id: user_id.to_string(),
            window_start: start,
            window_end: end,
            views: *views,
        },
    );

    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            ts: 60_000,
        }]))
        .key_by(|v| v.user_id.to_string())
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
Window::session(gap)....build()
SessionWindow::new(gap, key_fn, time_fn, accumulate_fn, finish_fn)
    .with_allowed_lateness(lateness)
```

```rust,no_run
use rhei::{DataflowGraph, PrintSink, SessionWindow, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Session {
    user_id: String,
    started_at: u64,
    ended_at: u64,
    views: u64,
}

fn build(graph: &DataflowGraph) {
    // Close a session after 30 minutes of inactivity.
    let window = SessionWindow::new(
        1_800_000,
        |v: PageViewView<'_>| v.user_id.to_string(),
        |v: PageViewView<'_>| v.ts,
        |acc: &mut u64, _v: PageViewView<'_>| *acc += 1,
        |user_id: &str, started_at: u64, ended_at: u64, views: &u64| Session {
            user_id: user_id.to_string(),
            started_at,
            ended_at,
            views: *views,
        },
    );

    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            ts: 60_000,
        }]))
        .key_by(|v| v.user_id.to_string())
        .operator("sessions", window)
        .sink(PrintSink::<Session>::new());
}
```

### `CountWindow`

Fires every `threshold` rows per key. **No `time_fn`** — it is count-triggered, so it does not depend on watermarks.

```text
Window::count(threshold)....build()
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
struct PageView {
    user_id: String,
    dwell_ms: f64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Every100 {
    user_id: String,
    views: u64,
    total_dwell_ms: f64,
}

fn build(graph: &DataflowGraph) {
    // Emit a summary every 100 views by the same user, regardless of time.
    let window = CountWindow::new(
        100,
        |v: PageViewView<'_>| v.user_id.to_string(),
        |acc: &mut f64, v: PageViewView<'_>| *acc += v.dwell_ms,
        |user_id: &str, views: u64, total: &f64| Every100 {
            user_id: user_id.to_string(),
            views,
            total_dwell_ms: *total,
        },
    );

    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            dwell_ms: 1_200.0,
        }]))
        .key_by(|v| v.user_id.to_string())
        .operator("every_100", window)
        .sink(PrintSink::<Every100>::new());
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

// Both legs share one schema, tagged by `kind`. An impression records that an
// ad was shown; a click records that it was clicked.
#[derive(Clone, rhei::RheiSchema)]
struct AdEvent {
    impression_id: String,
    kind: String, // "impression" | "click"
    campaign: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Attributed {
    impression_id: String,
    campaign: String,
    time_to_click_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct Shown {
    campaign: String,
    ts: u64,
}

#[derive(Serialize, Deserialize)]
struct Clicked {
    ts: u64,
}

fn build(graph: &DataflowGraph) {
    let join = TemporalJoin::new(
        |v: AdEventView<'_>| v.impression_id.to_string(),
        |v: AdEventView<'_>| {
            if v.kind == "impression" {
                Side::Left
            } else {
                Side::Right
            }
        },
        |v: AdEventView<'_>| Shown {
            campaign: v.campaign.to_string(),
            ts: v.ts,
        },
        |v: AdEventView<'_>| Clicked { ts: v.ts },
        |impression_id: &str, shown: &Shown, clicked: &Clicked| Attributed {
            impression_id: impression_id.to_string(),
            campaign: shown.campaign.clone(),
            time_to_click_ms: clicked.ts.saturating_sub(shown.ts),
        },
    )
    // An unclicked impression is buffered no longer than the attribution
    // window. Without this, every impression is retained forever.
    .with_timeout(1_800_000);

    let impressions = graph.source(VecSource::new(vec![AdEvent {
        impression_id: "imp-1".into(),
        kind: "impression".into(),
        campaign: "spring-sale".into(),
        ts: 60_000,
    }]));
    let clicks = graph.source(VecSource::new(vec![AdEvent {
        impression_id: "imp-1".into(),
        kind: "click".into(),
        campaign: String::new(),
        ts: 64_000,
    }]));

    impressions
        .merge(clicks)
        .key_by(|e| e.impression_id.to_string())
        .operator("attribution_join", join)
        .sink(PrintSink::<Attributed>::new());
}
```

The `merge` → `key_by` → `operator` sequence is mandatory: merge discards partitioning, so the `key_by` after it is what puts an impression and its click on the same worker.

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
struct PageView {
    user_id: String,
    dwell_ms: f64,
}

#[derive(Clone, rhei::RheiSchema)]
struct MeanDwell {
    user_id: String,
    mean_ms: f64,
}

fn build(graph: &DataflowGraph) {
    // Running mean dwell time per user, emitted on every view.
    let agg = RollingAggregateOp::new(
        |v: PageViewView<'_>| v.user_id.to_string(),
        |acc: &mut (u64, f64), v: PageViewView<'_>| {
            acc.0 += 1;
            acc.1 += v.dwell_ms;
        },
        |user_id: &str, acc: &(u64, f64)| MeanDwell {
            user_id: user_id.to_string(),
            #[allow(clippy::cast_precision_loss)]
            mean_ms: if acc.0 == 0 { 0.0 } else { acc.1 / acc.0 as f64 },
        },
    );

    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            dwell_ms: 1_200.0,
        }]))
        .key_by(|v| v.user_id.to_string())
        .operator("mean_dwell", agg)
        .sink(PrintSink::<MeanDwell>::new());
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
struct PageView {
    user_id: String,
    path: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct FunnelCompletion {
    user_id: String,
    product_views: u64,
    span_ms: u64,
}

fn build(graph: &DataflowGraph) {
    // The checkout funnel: browse one or more products, add to cart, check out
    // — all within thirty minutes. The walkthrough builds the same logic by
    // hand with KeyedState; this expresses it declaratively.
    let funnel = SequenceDetect::builder()
        .key_fn(|v: PageViewView<'_>| v.user_id.to_string())
        .time_fn(|v: PageViewView<'_>| v.ts)
        .step("browse", |v: PageViewView<'_>| v.path == "/product")
        .at_least(1)
        .step("cart", |v: PageViewView<'_>| v.path == "/cart")
        .step("checkout", |v: PageViewView<'_>| v.path == "/checkout")
        .within(Duration::from_secs(1_800))
        // A user who converts starts a fresh funnel rather than continuing
        // to match overlapping ones.
        .after_match(AfterMatch::SkipPastMatch)
        .emit(|user_id: &str, ctx: &MatchCtx| FunnelCompletion {
            user_id: user_id.to_string(),
            product_views: ctx.step_count("browse") as u64,
            span_ms: ctx.duration(),
        })
        .build();

    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            path: "/product".into(),
            ts: 60_000,
        }]))
        .key_by(|v| v.user_id.to_string())
        .operator("checkout_funnel", funnel)
        .sink(PrintSink::<FunnelCompletion>::new());
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
struct PageView {
    user_id: String,
}

#[derive(Clone)]
struct CountViews;

#[async_trait::async_trait]
impl rhei::arrow::StreamFunction for CountViews {
    type Input = PageView;
    type Output = PageView;

    async fn process(
        &mut self,
        input: RheiBuffer<PageView>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<PageView>> {
        let mut state =
            KeyedState::<String, u64, _>::with_encoder(&mut ctx.state, "views", BincodeEncoder);
        for view in &input {
            let user_id = view.user_id.to_string();
            let n = state.get(&user_id).await?.unwrap_or(0) + 1;
            state.put(&user_id, &n)?;
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
struct PageView {
    user_id: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct Abandoned {
    user_id: String,
    last_seen: u64,
}

/// Emits an `Abandoned` row for any user idle for `timeout` of event time —
/// the kind of "they left without converting" signal that has no triggering
/// record, so it can only be produced when the watermark advances.
#[derive(Clone)]
struct AbandonmentDetector {
    timeout: u64,
}

#[async_trait::async_trait]
impl StreamFunction for AbandonmentDetector {
    type Input = PageView;
    type Output = Abandoned;

    async fn process(
        &mut self,
        input: RheiBuffer<PageView>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Abandoned>> {
        let mut last = KeyedState::<String, u64>::new(&mut ctx.state, "last_seen");
        for view in &input {
            last.put(&view.user_id.to_string(), &view.ts)?;
        }
        // Nothing to emit on data arrival — emission is watermark-driven.
        Ok(BufferOutput::None)
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Abandoned>> {
        let mut builder = Abandoned::builder(0);
        let mut last = KeyedState::<String, u64>::new(&mut ctx.state, "last_seen");

        // A real implementation would iterate the operator's owned key groups;
        // this shows the shape of watermark-driven emission.
        if let Some(ts) = last.get(&"alice".to_string()).await?
            && watermark.saturating_sub(ts) > self.timeout
        {
            builder.append(Abandoned {
                user_id: "alice".to_string(),
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
| Views per user per minute | `TumblingWindow` |
| A trailing rate that refreshes often | `SlidingWindow` |
| Visits separated by idle gaps | `SessionWindow` |
| Fire every N rows, ignore time | `CountWindow` |
| A running total emitted per row | `RollingAggregateOp` / `ReduceOp` |
| Correlate two streams on a key (impression ↔ click) | `merge` → `key_by` → `TemporalJoin` |
| "A then B then C within T" (a funnel) | `SequenceDetect` |
| Emit when nothing arrives (abandonment, timeouts) | Your own `StreamFunction` with `on_watermark` |
| Anything else stateful | Your own `StreamFunction` |

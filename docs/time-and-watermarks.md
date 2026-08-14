# Time, Watermarks, and Frontiers

How Rhei decides that a window is complete. This is the part of a stream processor that is hardest to reason about from the outside, so this page traces the mechanism from the source to the operator, naming the code at each step.

**Short version:** Rhei does not maintain a separate watermark channel. The Timely Dataflow **timestamp is the watermark**. A source stamps its output with the watermark it reports; Timely's progress tracking propagates that as the **frontier**; each operator reads the frontier minimum and calls `on_watermark`. Window operators fire from there.

---

## Event time only

Windows are computed over **event time** — the timestamp your `time_fn` extracts from each record. There is no processing-time windowing mode.

The unit is whatever you use consistently: `time_fn` returns `u64`, and window sizes, slides, gaps, lateness, and join timeouts are all in that same unit. If `time_fn` returns milliseconds, a 60-second tumbling window is `60_000`. Rhei never converts.

---

## The timeline

Timely timestamps, watermarks, and epochs all share one `u64` timeline. Two values at the top of the range are reserved:

| Sentinel | Value | Meaning |
|----------|-------|---------|
| `SourceExhausted` | `u64::MAX - 1` | A source has run dry |
| `Shutdown` | `u64::MAX` | Teardown signal on the checkpoint channel |

*Source: `rhei-runtime/src/executor.rs`, `enum Sentinel`.*

Real event timestamps must stay below these. Millisecond epochs have roughly 584 million years of headroom, so this is only a hazard if you put something other than a timestamp in `time_fn`.

---

## How a watermark travels

### 1. The source reports one

A `Source` implementation optionally overrides:

```text
fn should_emit_watermark(&self) -> bool     // default: false
fn current_watermark(&self) -> Option<u64>  // default: None
```

`KafkaSource` tracks the maximum event timestamp seen per partition and reports the minimum across assigned partitions. `VecSource` reports nothing.

### 2. The bridge carries it alongside the batch

Sources run as async Tokio tasks. The bridge task reads `next_batch()`, calls `source.current_watermark()`, and pushes `(ErasedBuffer, Option<u64>)` through a bounded `flume` channel to the Timely worker.

*Source: `rhei-runtime/src/bridge.rs`.*

### 3. The Timely source operator turns it into the epoch

This is the step that matters. In the source operator:

```text
if let Some(wm) = wm { epoch = epoch.max(wm); } else { epoch += 1; }
capability.downgrade(&epoch);
```

**When a source emits watermarks, the epoch *is* the watermark.** When it does not, the epoch is a monotonic batch counter and no meaningful event time flows — windows will only close at end of stream.

`epoch.max(wm)` keeps the timeline monotonic: a source that reports a regressing watermark cannot move the epoch backwards.

*Source: `rhei-runtime/src/executor.rs`, `build_batch_source`.*

### 4. Timely propagates it as the frontier

Downgrading the capability is what tells Timely "I will not produce anything below this epoch again". Timely's progress tracking combines that across all sources and all workers into a **frontier** at every downstream operator: the set of epochs that may still arrive.

This is where the guarantee comes from. It is not a heuristic — it is the same progress tracking that makes Timely's dataflow model work, and it accounts for data still in flight across the exchange.

### 5. The operator reads it and fires

Stateful operators are built with `unary_frontier`. On each activation:

```text
let wm = frontier_min_or_max(frontier.frontier());
if wm > last_watermark {
    timely_op.advance_time(wm, &mut last_watermark, &rt);   // calls on_watermark
}
```

`frontier_min_or_max` returns the minimum epoch in the frontier, or `u64::MAX` when the frontier is **empty** — an empty frontier means no more data will ever arrive, so everything is complete.

`advance_time` invokes your operator's `on_watermark(wm, ctx)`. That is where windows close.

*Source: `rhei-runtime/src/executor.rs`, `build_batch_operator` and `frontier_min_or_max`.*

### The whole path

```text
Source::current_watermark()          Option<u64>
        │
   bridge task                       (ErasedBuffer, Option<u64>) over flume
        │
 Timely source operator              epoch = max(epoch, wm); cap.downgrade(epoch)
        │
 Timely progress tracking            frontier, across workers and processes
        │
 unary_frontier operator             wm = min(frontier); if advanced →
        │
 StreamFunction::on_watermark        windows close, timers fire
```

---

## When windows fire

A window closes when:

```text
watermark >= window_end + allowed_lateness
```

Concretely, for a 60-second tumbling window with 5 seconds of allowed lateness, the window `[60_000, 120_000)` fires once the watermark reaches `125_000`.

Until the watermark advances, results are buffered in state. **A pipeline whose sources emit no watermarks will produce no windowed output until the sources are exhausted.** This is the single most common "my pipeline hangs with no output" cause. If you are reading from a source without watermark support, no amount of waiting will close a window.

### End of stream

When a source is exhausted, its watermark is set to `SourceExhausted` (`u64::MAX - 1`) and the epoch is raised to match. That value exceeds every real `window_end`, so all pending windows close and emit. This is why `VecSource`-driven examples produce window output at the end despite emitting no watermarks during the run.

The source operator then enters a draining state, staying alive until every source has reported exhaustion, so that late-closing windows still have a valid capability to emit through.

---

## Late events

An event is late when its window has already fired:

```text
event_time + allowed_lateness < last_watermark
```

Late events are **dropped**, and `late_events_dropped_total` is incremented. They are not routed anywhere — side-output routing for late events is on the roadmap and not implemented. If dropping is unacceptable, raise `allowed_lateness`, at the cost of holding window state longer.

Set it per operator:

```rust,no_run
use rhei::TumblingWindow;

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    ts: u64,
}

#[derive(Clone, rhei::RheiSchema)]
struct ViewsPerMinute {
    user_id: String,
    views: u64,
}

fn window() -> impl rhei::arrow::StreamFunction<Input = PageView, Output = ViewsPerMinute> {
    TumblingWindow::new(
        60_000,
        |v: PageViewView<'_>| v.user_id.to_string(),
        |v: PageViewView<'_>| v.ts,
        |acc: &mut u64, _v: PageViewView<'_>| *acc += 1,
        |user_id: &str, _start: u64, _end: u64, views: &u64| ViewsPerMinute {
            user_id: user_id.to_string(),
            views: *views,
        },
    )
    .with_allowed_lateness(30_000) // tolerate 30s of out-of-order arrival
}
```

The trade-off is the usual one: lateness buys completeness and costs latency plus state.

---

## Watermarks with multiple sources and workers

The frontier is a **global minimum**. With two sources, downstream operators see the minimum of both — one idle source holds the watermark back for the whole pipeline. The same applies across an exchange: a worker's frontier accounts for data still in flight from every upstream worker, which is what makes windowing correct after a repartition.

This has a practical consequence. A Kafka topic with an idle partition holds back every window in the pipeline, because that partition's watermark never advances. Idle-partition detection is not implemented.

For the source operator's own draining logic (deciding when to shut down, not when to fire windows), the runtime separately computes the minimum over all source watermark atomics, ignoring zeros:

```text
fn compute_min_watermark(all: &[Arc<AtomicU64>]) -> u64
```

Sources that have not yet reported (value `0`) are skipped rather than pinning the minimum at zero.

---

## Timers

`TimerService` registers per-key event-time timers. When the watermark passes a timer's timestamp, `on_timer(timestamp, key, ctx)` fires on the operator. Timers are persisted with operator state and restored on startup via `restore_timers()`, so a timer registered before a crash still fires after recovery.

Timers use the same watermark as windows — they are not wall-clock timers.

---

## Epochs and checkpoints share the timeline

Frontier advancement drives two things at once:

1. **Windows fire** — `on_watermark` at the new frontier minimum.
2. **Checkpoints trigger** — the runtime checkpoints when the frontier advances past the last checkpointed epoch, subject to `checkpoint_interval`.

That coupling is deliberate: a checkpoint at a frontier boundary is exactly the point where "all data up to epoch E has been processed" holds, so the state snapshot and the source offsets describe the same cut of the stream.

The consequence is worth knowing: **if the watermark never advances, checkpoints do not fire either.** A stalled watermark stalls durability, not just output.

See [state-and-checkpointing.md](state-and-checkpointing.md).

---

## Debugging watermarks

| Symptom | Likely cause |
|---------|--------------|
| No windowed output at all, pipeline still running | Source emits no watermarks. Confirm `current_watermark()` is implemented |
| Output only appears when the pipeline ends | Same — you are seeing the `SourceExhausted` flush |
| Windows fire far later than expected | One slow or idle source/partition is holding the global minimum down |
| `late_events_dropped_total` climbing | Watermark is ahead of your data's real disorder. Raise `allowed_lateness` |
| Checkpoints not happening | Frontier is not advancing; same root cause as stalled windows |
| Windows fire with partial data | Missing `key_by` before the operator, so keys are split across workers |

Useful signals:

```bash
rhei run --log-level debug          # per-batch progress and frontier moves
curl localhost:9090/metrics | grep -E 'late_events|checkpoint'
```

---

## Limits

Stated plainly, because they affect correctness of results:

- **No processing-time windows.** Event time only.
- **No idle-source detection.** An idle partition stalls the global watermark indefinitely.
- **No late-event side output.** Late events are dropped and counted.
- **No watermark alignment or per-source skew handling.**
- **Watermarks originate only at sources.** Operators cannot emit or adjust them.

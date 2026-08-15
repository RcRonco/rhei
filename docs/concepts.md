# Concepts

The ideas Rhei is built on, and what each one costs you.

This page is not an API reference — see [API.md](../API.md) and [operators.md](operators.md) for that. It explains *why* the API looks the way it does, so that when you hit a surprise (your closure gets a `&str`, your counter is wrong at four workers, your window never fires) the behaviour is predictable rather than mysterious.

Each section names the code that implements the idea, so you can go read it.

---

## 1. Unbounded data has no end

A batch job reads all its input, computes, and stops. A stream never stops. There is no moment at which you have "all the data", so there is no moment at which an answer is final.

Everything awkward about stream processing follows from this. You cannot sort. You cannot count distinct without bounding memory. You cannot join without deciding how long to wait. Every result is a statement about *a prefix of the stream*, and the interesting engineering is in deciding when a prefix is complete enough to act on.

Rhei's answer, in one line: **completeness is derived from progress tracking, and everything else — windows, joins, checkpoints — is triggered by it.** Sections 4 through 6 are that mechanism.

What this means for you: the operators that look like batch operations are not. `TumblingWindow` does not compute a group-by; it accumulates state per key and emits when it has reason to believe no more input for that window will arrive. Understanding *what gives it that reason* is the difference between a pipeline that works and one that silently emits nothing.

---

## 2. The unit of work is a batch of columns, not a record

Rhei moves data as Apache Arrow record batches — columns of values — rather than one record at a time.

The reason is mechanical. Per-record processing spends most of its time on overhead: a virtual call, a bounds check, a cache miss, an allocation, repeated per record. Columnar batches amortise all of that. A filter over a million rows becomes a loop over a contiguous array with a predictable branch, and often a SIMD kernel.

This is not a hidden implementation detail — it shapes the API you write against, in three visible ways.

**Your operator receives a batch.** `StreamFunction::process` takes a whole `RheiBuffer<T>`, not a record. You write a loop. In exchange, you can also reach for the underlying `RecordBatch` and run Arrow compute kernels over entire columns when that is faster.

**Your closures receive borrowed views, not owned values.** A row is not materialised as a struct; you get a `PageViewView<'a>` whose `String` fields read back as `&'a str`, pointing straight into the Arrow buffer. Nothing is copied until you ask:

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    path: String,
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![PageView {
            user_id: "alice".into(),
            path: "/product".into(),
        }]))
        // `v.path` is `&str` borrowed from the Arrow buffer — no allocation.
        .filter_fn(|v| v.path.starts_with("/product"))
        // `.to_string()` is where you choose to pay for a copy.
        .key_by(|v| v.user_id.to_string())
        .sink(PrintSink::<PageView>::new());
}
```

The `.to_string()` calls scattered through the examples are not noise. They are the allocation being made explicit, per field, at a point you control — rather than paid on every field of every row whether you use it or not.

**Filtering marks, it does not copy.** A filtered batch is the original batch plus a boolean mask. Ten chained filters produce one batch and ten mask operations, not ten copies. This is why a buffer has two lengths — the number of rows you can see, and the number physically present — and why iteration skips masked-out rows for you.

The cost of all this: writing an operator is slightly more ceremony than a per-record callback. You allocate a builder, loop, append. That is the price of the throughput.

*Implementation: `rhei-core/src/arrow/` — `RheiBuffer<T>` is the typed batch, `#[derive(RheiSchema)]` generates the schema, builder and view.*

---

## 3. Types are checked when you build the graph, not when data flows

You describe a pipeline as a graph of sources, transforms, operators and sinks. That description is a value — `DataflowGraph` — not a running program. The runtime compiles it into a Timely Dataflow program afterwards.

Separating the two buys three things: the topology can be validated before anything runs (a stream that never reaches a sink is an error, not a silent leak); the same graph can be compiled for one worker or forty; and rescaling can rebuild the execution without rebuilding your description of it.

The handles you chain off are `Copy`. That has a consequence worth knowing, because it is how fan-out is expressed — there is no `split()` method, you just use the handle twice:

```rust,no_run
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct PageView {
    user_id: String,
    path: String,
}

fn build(graph: &DataflowGraph) {
    let views = graph.source(VecSource::new(vec![PageView {
        user_id: "alice".into(),
        path: "/product".into(),
    }]));

    // Two independent consumers. Both see every row.
    views.filter_fn(|v| v.path == "/checkout").sink(PrintSink::<PageView>::new());
    views.sink(PrintSink::<PageView>::new());
}
```

Element types are checked at compile time: connecting a stream of one schema to an operator expecting another does not compile.

**What is deliberately *not* checked is keying**, and this is the sharpest edge in the API. There is one stream type. `.operator()` is available on any stream, keyed or not. Attaching a stateful operator without a preceding `key_by` compiles, runs, and produces correct results with one worker — then produces silently wrong results with two, because each worker sees an arbitrary slice of every key. Nothing errors, and no metric fires.

A `KeyedStream` type would make this a compile error. It is not implemented; the guarantee is currently a convention. Concretely: **always `key_by` before `operator`, and test stateful pipelines with more than one worker**, because one worker cannot reveal the bug.

*Implementation: `rhei-runtime/src/dataflow.rs` (graph and handles), `compiler.rs` (topological compilation).*

---

## 4. Event time is the only honest clock

Every record carries the time the thing happened. The machine also has a wall clock, which tells you when the record arrived. These are different, and the gap between them is where stream processing gets hard.

Rhei windows on **event time** exclusively. There is no processing-time mode.

The reason is reproducibility. A processing-time window says "everything that reached me between 12:00 and 12:01". Replay the same input tomorrow and you get different windows — a different answer to the same question. An event-time window says "everything that happened between 12:00 and 12:01", which is a property of the data, so re-running produces the same result. For a system whose recovery story is *replay from a checkpoint*, answers that change under replay are not acceptable.

The cost is that event time does not advance on its own. Wall clocks tick; event time only moves when data arrives carrying later timestamps. A source with nothing to say does not advance the clock, and everything downstream waits. That is the trade: correctness under replay, in exchange for progress that depends entirely on your input.

The unit is whatever you use consistently. `time_fn` returns a `u64`; window sizes, slides, session gaps, lateness and join timeouts are all in that same unit. Rhei never converts, and never inspects a wall clock on your behalf.

---

## 5. Completeness cannot be known, only claimed — and the claim is the timestamp

If event time only moves when data arrives, how does a window ever decide it is done?

The usual answer is a **watermark**: an assertion, made by whoever is reading the data, that nothing older than time T will arrive. It is a claim, not a fact. Set it too aggressively and you drop data that was merely slow; too conservatively and results are late.

Rhei's specific choice is worth understanding, because it explains behaviour you will otherwise find baffling: **there is no separate watermark channel. The watermark is the dataflow timestamp itself.**

When a source reports a watermark, that value becomes the Timely timestamp stamped on its output. Timely's progress tracking — the machinery that already exists to know when a computation is finished — then propagates it. A downstream operator's "current watermark" is just the minimum of its input frontier, the set of timestamps that might still arrive.

Two consequences fall directly out of this.

**The guarantee is structural, not heuristic.** The frontier already accounts for data in flight across an exchange, buffered on another worker, or travelling between processes. There is no separate mechanism that could disagree with where the data actually is.

**A source that reports no watermark advances no clock.** With nothing to stamp, the timestamp becomes a batch counter, and no meaningful event time flows. Windows accumulate and never fire. This is by far the most common "my pipeline runs but produces nothing" cause, and it is not a bug — the system genuinely has no basis to believe any window is complete. When such a source is finally exhausted it emits a sentinel far above any real timestamp, which flushes everything at once. That is why an in-memory example prints all its windows at the end and nothing before.

Because the frontier is a global minimum, one idle input holds back the entire pipeline. An idle Kafka partition stalls every window in the job. Rhei does not detect this and time it out.

The same signal drives durability (section 8), so a stalled watermark stalls checkpoints too, not just output.

*Implementation: `rhei-runtime/src/executor.rs` — `build_batch_source` stamps the epoch, `build_batch_operator` reads the frontier. Full trace in [time-and-watermarks.md](time-and-watermarks.md).*

---

## 6. Lateness is a budget you set, not an error you handle

Given that a watermark is a claim, some data will violate it — arriving after its window has already been declared complete.

Rhei makes this an explicit parameter. `allowed_lateness` is how long past a window's end you are willing to keep it open. Within the budget, late data is folded in. Past it, the record is dropped and a counter increments.

This is a real dial with a real trade on both sides, and it deserves a deliberate answer rather than a default:

- **Raise it** and you tolerate more disorder, at the cost of holding window state longer and emitting results later.
- **Lower it** and you emit sooner and hold less, at the cost of dropping more.

There is no setting that avoids the choice. What Rhei does not currently offer is the third option other systems provide — routing late records to a side output so you can reconcile them separately. Late data is dropped and counted, and `late_events_dropped_total` is the only trace it leaves. **Alert on it.** A rising count means your results are quietly incomplete.

---

## 7. State belongs to a key, not to a machine

A stateful operator accumulates something per key. That state has to live somewhere, and how you *address* it turns out to determine whether your system can be resized.

The obvious scheme is to route by `hash(key) % worker_count` and store state under the worker index. It works until the worker count changes — at which point nearly every key hashes to a different worker, while its state still sits under the old worker's prefix. The new owner looks under a prefix nobody writes to and finds nothing. It does not crash; it reads empty state and carries on with wrong answers.

Rhei inserts an indirection. Keys hash into a fixed number of **key groups**, and workers own contiguous *ranges* of groups:

```text
key_group = seahash(key) % max_parallelism
worker    = key_group * effective_parallelism / max_parallelism
```

`max_parallelism` is fixed for the pipeline's lifetime, so a key's group never changes. Only the range-to-worker mapping is recomputed when the worker count changes. Because state is addressed by group rather than by worker, a worker that gains a range issues exactly the reads the previous owner issued, served from shared storage.

**Rescaling therefore moves ownership, not bytes.** No migration, no state transfer — the gaining worker simply starts reading a range it did not read before. (It faults those groups in lazily on first access, so there is a latency bump right after a topology change; proactive warming is not implemented.)

The design is deliberately identical to Flink's, so the operational model carries over: choose a maximum parallelism up front, rescale freely below it.

Three practical consequences:

- **`max_parallelism` is a permanent decision.** Changing it re-partitions the entire key space, so every existing checkpoint becomes unreadable. It is recorded in the checkpoint manifest and validated on restore — you get a rejection, not silent corruption. Choose it as the highest worker count you might ever want, with headroom. The default is 128.
- **It caps useful parallelism.** There are only that many groups to hand out; workers beyond the limit own no keyed state and act as standby capacity.
- **Key groups do not fix key skew.** They distribute *groups* evenly, not traffic. One very hot key pins one worker, and no amount of rescaling helps — the key itself has to be split.

The invariant that holds the whole scheme together: **the group must be derived from the bytes the exchange routed on** — your `key_by` output — not from whatever storage key a state wrapper happens to build. Getting this wrong keeps reads and writes self-consistent, so nothing appears broken, while quietly destroying every per-key-group operation. This was a real bug in Rhei's history, fixed in commit `0ed4d07`.

*Implementation: `rhei-core/src/cluster/key_group.rs` (the assignment math and its agreement tests), `rhei-core/src/state/key_group_addressing.rs` (the physical layout).*

---

## 8. Routing is the only coupling between workers

Workers share nothing. Each has its own in-memory state, its own local cache, its own slice of the key space. They do not lock, and they do not consult each other.

The single exception is `key_by`, which is the only operation that moves a row from one worker to another. Everything else — mapping, filtering, windowing, joining, sinking — runs wherever the data already is.

This is why the placement of `key_by` matters so much. It is not a hint or an annotation; it is the one point in your pipeline where data crosses a boundary, and therefore the one point that costs serialisation (Arrow IPC, when crossing processes). Re-key when the grain of your computation genuinely changes; not defensively.

It also explains a rule that otherwise looks arbitrary: **`merge` discards partitioning.** Merging two streams concatenates them without re-routing, so whatever key alignment the inputs had is meaningless afterwards. A stateful operator after a merge needs its own `key_by` first. The temporal join pattern — merge, then key, then join — exists for exactly this reason.

*Implementation: `rhei-runtime/src/executor.rs`, `build_batch_key_by` — a local split into per-worker sub-batches, then a routed exchange.*

---

## 9. The state hierarchy is a bet on your working set

Keyed state is served from three tiers: an in-process memtable, a local NVMe cache, and durable object storage. A read tries each in turn.

The bet is that the keys you touch repeatedly fit in memory, so the overwhelming majority of reads never leave the process, while the full key space — which may be far larger than RAM — stays available and durable.

When the bet holds, state access is effectively free. When it does not, **throughput does not degrade gracefully; it falls off a cliff.** The reason is specific and worth knowing rather than discovering: a miss is resolved by blocking the worker thread until the fetch completes. That thread does nothing else in the meantime. This is a known limitation, not a design goal — the operator API borrows across the await point, so a non-blocking path requires an API redesign.

What follows for you:

- **Watch the L1 hit ratio.** It is the leading indicator for everything else. A falling ratio means the blocking path has moved onto your hot path.
- **Size the memtable for the hot key set**, not the total key space.
- **Put the local cache on real NVMe.** Its default directory is under `/tmp`, which on many systems is a RAM disk — meaning the tier meant to save memory is consuming it.

There is a second, subtler bound. Writes accumulate in memory as *dirty* entries and are not evicted, because evicting them would discard data that has not yet been made durable. Between checkpoints, memory therefore grows with the number of distinct keys written. A workload with high write cardinality and a long checkpoint interval can exhaust RAM. Shortening the interval bounds it.

*Implementation: `rhei-core/src/state/` — `memtable.rs` (L1 and its eviction), `tiered_backend.rs` (L2 over L3). Tuning in [state-and-checkpointing.md](state-and-checkpointing.md).*

---

## 10. Durability is a consistent cut, and it decides your delivery guarantee

A checkpoint is not a periodic backup. It is a **consistent cut** across the whole pipeline: a point at which "everything up to here has been processed" is true simultaneously for every operator.

Rhei takes that cut where the frontier advances — the same signal that fires windows. That is not a coincidence but the point: a frontier boundary is precisely where the claim holds, so the state snapshot and the recorded source positions describe the same prefix of the stream. Recovery restores both together and resumes from a coherent point.

This is also where a system's delivery guarantee is decided, and it comes down to ordering. Rhei flushes state, writes the checkpoint, and *then* commits source offsets. A crash in the window between processing and committing replays those records on restart. That ordering yields **at-least-once**.

Reversing it — committing offsets first — would give at-most-once and lose data instead. Getting exactly-once requires a third thing: a transactional sink participating in a two-phase commit, so downstream effects are staged and committed atomically with the checkpoint. Rhei does not have this.

**The practical instruction: make your sinks idempotent.** Key writes by something derived from the record so that a replay overwrites rather than appends. If your sink appends blindly, a crash produces duplicates, and no configuration option prevents it.

Across processes, a lightweight protocol makes the cut global: each process reports readiness for an epoch, and one process commits once all have. That coordinator is process 0 by convention — there is no election, and no failover if it dies.

*Implementation: `rhei-core/src/checkpoint.rs` (the manifest and its compatibility checks), `rhei-runtime/src/checkpoint_coord.rs` (the cross-process protocol).*

---

## 11. What Rhei deliberately does not have

The absences shape the design as much as the features. Some are deliberate; some are gaps that have not been filled. Both are worth knowing before you commit.

**Deliberate:**

| Absent | Instead |
|--------|---------|
| Processing-time windows | Event time only, so results are reproducible under replay |
| Separate watermark channel | The dataflow timestamp *is* the watermark |
| Builder patterns on operators | Positional closures on `::new(...)`, so the type checker infers accumulator types |
| A test harness | `VecSource` plus a collecting sink — the same pattern the engine's own tests use |
| Row-by-row execution | Columnar batches throughout; there is no per-record path to fall back to |

**Not yet built:**

| Absent | Consequence |
|--------|-------------|
| `KeyedStream` type | Missing `key_by` before a stateful operator is a silent correctness bug, not a compile error |
| Exactly-once delivery | Replay produces duplicates; sinks must be idempotent |
| Job manager, scheduler, submission API | You start and supervise processes yourself |
| Leader election | Process 0 coordinates checkpoints; if it dies, coordination stops |
| Late-event side outputs | Late data is dropped and counted, not recoverable |
| Idle-source detection | One idle partition stalls every window indefinitely |
| Non-blocking state cold path | An L1 miss blocks a worker thread |
| Checkpoint versioning | Changing a state value's serialised shape breaks restore |
| SQL or Table API | Rust only |

[KNOWN-ISSUES.md](../KNOWN-ISSUES.md) tracks these with severities; [ROADMAP.md](../ROADMAP.md) separates built from planned.

---

## Vocabulary

The terms above, and where each lives.

| Term | Meaning | Code |
|------|---------|------|
| **Batch** | Arrow `RecordBatch` plus a selection mask — the unit of work | `rhei-core/src/arrow/buffer.rs` |
| **View** | Zero-copy borrowed row reference; what your closures receive | generated by `#[derive(RheiSchema)]` |
| **Schema** | A struct that can flow through the graph | `rhei-core/src/arrow/schema.rs` |
| **Graph** | The topology description, compiled before it runs | `rhei-runtime/src/dataflow.rs` |
| **Stream handle** | A `Copy` reference to a point in the graph; reuse creates fan-out | `rhei-runtime/src/dataflow.rs` |
| **Operator** | A stateful `StreamFunction`, cloned once per worker | `rhei-core/src/arrow/traits.rs` |
| **Worker** | One Timely thread; shared-nothing, owns a range of key groups | `rhei-runtime/src/executor.rs` |
| **Epoch** | The dataflow timestamp — and the watermark, when sources emit one | `rhei-runtime/src/executor.rs` |
| **Frontier** | Timestamps that may still arrive; the completeness signal | Timely progress tracking |
| **Watermark** | The claim that nothing older will arrive | [time-and-watermarks.md](time-and-watermarks.md) |
| **Key** | The `String` your `key_by` returns; decides routing *and* state location | — |
| **Key group** | Fixed bucket of keys; the unit of ownership transfer | `rhei-core/src/cluster/key_group.rs` |
| **`max_parallelism`** | Number of key groups; permanent, caps useful worker count | `rhei-core/src/cluster/key_group.rs` |
| **Exchange** | The only operation that moves rows between workers | `rhei-runtime/src/executor.rs` |
| **Checkpoint** | A consistent cut: state flushed, then offsets committed | `rhei-core/src/checkpoint.rs` |
| **Manifest** | What a checkpoint records, including the topology it was taken at | `rhei-core/src/checkpoint.rs` |
| **DLQ** | Where failed records go under `ErrorPolicy::SendToDlq` | `rhei-core/src/dlq.rs` |

---

## Where to go next

| To | Read |
|----|------|
| Build something | [walkthrough.md](walkthrough.md) |
| Look up an operator | [operators.md](operators.md) |
| Understand when windows fire, in detail | [time-and-watermarks.md](time-and-watermarks.md) |
| Understand routing and rescaling, in detail | [exchange-and-partitioning.md](exchange-and-partitioning.md) |
| Configure state and checkpoints | [state-and-checkpointing.md](state-and-checkpointing.md) |
| Run it in production | [deployment.md](deployment.md) |
| See how the runtime is put together | [internals.md](internals.md) |

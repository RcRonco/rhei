# Exchange and Partitioning

How `key_by` moves rows between workers, why routing goes through key groups instead of `hash(key) % workers`, and what that buys you when the worker count changes.

---

## The rule

**`key_by` is the only operation that moves data between workers.** Everything else — `map`, `filter`, `flat_map`, `inspect`, `merge`, `operator`, `sink` — runs on whatever worker already holds the data.

| Operation | Timely pact | Movement |
|-----------|-------------|----------|
| `map`, `filter`, `filter_fn`, `flat_map`, `inspect`, `distinct_by`, `limit`, `batch` | `Pipeline` | none |
| `key_by` | **`Exchange`** | rows routed by key group |
| `operator` | `Pipeline` | none |
| `merge` | `Pipeline` (`concatenate`) | none |
| `sink` | `Pipeline` | each worker writes its own partition |

Note the second-to-last row. `.operator()` does **not** exchange. It processes whatever its worker happens to hold — which is why the `key_by` before it is what makes stateful results correct.

---

## What `key_by` actually does

Implemented as **two Timely operators**, not one.

### Stage 1 — split (Pipeline pact)

Each incoming buffer is partitioned into per-worker sub-buffers. For every row: run the key function, hash the key into a key group, map the group to a worker, and append the row to that worker's sub-buffer. Each sub-buffer is tagged with its `exchange_target`.

This happens locally, with no data movement — it is a columnar partition of one batch into N smaller batches.

### Stage 2 — route (Exchange pact)

```text
partitioned.exchange(|buf| buf.exchange_target().unwrap_or(0))
```

Timely routes each sub-buffer to the tagged worker. Within a process this is a channel handoff. Across processes it is a TCP send with **Arrow IPC** serialization.

*Source: `rhei-runtime/src/executor.rs`, `build_batch_key_by`.*

Splitting first is what keeps this efficient: one exchange decision per sub-buffer instead of per row, and rows stay in columnar form the whole way.

---

## Key groups

Routing does **not** use `hash(key) % num_workers`. It goes through a fixed number of key groups:

```text
key_group = seahash(key) % max_parallelism
worker    = key_group * effective_parallelism / max_parallelism
```

Workers own **contiguous ranges** of key groups.

### Why the indirection

With `hash(key) % num_workers`, durable state is addressed by worker index. Change the worker count from 4 to 6 and essentially every key moves to a different worker, while its persisted state still sits under the old worker's prefix. Every key looks under a prefix nobody writes to any more — and it fails *silently*, reading empty state rather than erroring.

With key groups, `max_parallelism` is fixed for the pipeline's lifetime, so **a key's group never changes**. Only the range→worker mapping is recomputed. State is addressed by group (`kg{group:05}/{operator}/{key}`), so a worker that gains a range simply issues the same reads the previous owner issued, served from shared L3.

The result: rescaling moves *ownership*, not bytes.

This is deliberately identical to Flink's `KeyGroupRangeAssignment`, so the operational model carries over — pick a max parallelism up front, rescale freely below it.

*Source: `rhei-core/src/cluster/key_group.rs`.*

### `seahash`, specifically

A fixed-seed hash, not `RandomState`. Rust's default hasher is randomly seeded per process, which would repartition every key on every restart and orphan all state. The hash must be stable across processes, restarts, and platforms.

### `max_parallelism`

| Property | Value |
|----------|-------|
| Default | 128 |
| Upper bound | 32,768 (key group IDs are `u16` on the wire) |
| Configured by | `.max_parallelism(n)`, `RHEI_MAX_PARALLELISM`, or `cluster.max_parallelism` |
| Changeable later? | **No.** It is recorded in the checkpoint manifest and validated on restore |

It is a hard ceiling on useful parallelism: there are only that many groups to hand out. Workers beyond it own nothing.

Rhei **clamps** rather than rejecting when `workers > max_parallelism` — Flink errors, but here the worker count is driven by cluster membership, and a node joining should not fail the pipeline. Surplus workers still run stateless work and act as standby capacity.

```text
effective_parallelism = min(max_parallelism, parallelism).max(1)
```

Pick `max_parallelism` as the highest worker count you might ever want, with headroom. The per-group bookkeeping is cheap; the cost of getting it too low is being unable to scale past it without invalidating every checkpoint.

### Range assignment

```text
start = ceil(worker_index * max_parallelism / effective)
end   = ((worker_index + 1) * max_parallelism - 1) / effective
```

The ceiling on `start` is not cosmetic. It is what makes this the exact inverse of the routing formula `kg * effective / max_parallelism`. The intuitive floor-split tiles the space correctly but disagrees with routing whenever `max_parallelism` is not a multiple of the worker count — and a disagreement means rows arriving at a worker that does not own their state.

The `routing_agrees_with_ranges` test pins this invariant across the whole parameter space, and `partition_key_agrees_with_state_ownership` checks the same property end to end.

Example, `max_parallelism = 128`:

| Workers | Ranges |
|---------|--------|
| 1 | w0: 0–127 |
| 4 | w0: 0–31, w1: 32–63, w2: 64–95, w3: 96–127 |
| 6 | w0: 0–21, w1: 22–42, w2: 43–63, w3: 64–85, w4: 86–106, w5: 107–127 |
| 200 | w0–w127 own one group each; w128–w199 own nothing |

---

## The key must be the key

The key group is derived from **the bytes the exchange routed on** — your `key_by` output — never from the storage key a state wrapper happens to build.

This is easy to get wrong and fails silently. `KeyedState` stores under `"{namespace}:{encoded_key}"`, so hashing *that* would put `counts:"alpha"` in a different group than routing put `alpha` in. Reads and writes would still be self-consistent — every worker derives the physical key identically — so nothing breaks immediately. What breaks is every per-key-group operation: warming a gained range, scanning an owned range, reasoning about ownership at all.

This was a real bug, fixed in `0ed4d07` ("derive the key group from the partition key, not the storage key").

Practical implication for your code: **the key you pass to `key_by` and the key you use inside the operator's state must be the same string.** If you key by `user_id` and store state under `format!("{user}:{day}")`, that still works — but the routing key is `user_id`, and that is what determines ownership.

---

## The missing guarantee

Rhei has **no `KeyedStream` type**. `.operator()` is available on every `Stream<'a, T>`.

```rust,no_run
use rhei::arrow::{BufferOutput, OperatorContext, RheiBuffer};
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Event {
    user: String,
}

#[derive(Clone)]
struct Counter;

#[async_trait::async_trait]
impl rhei::arrow::StreamFunction for Counter {
    type Input = Event;
    type Output = Event;
    async fn process(
        &mut self,
        _input: RheiBuffer<Event>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Event>> {
        Ok(BufferOutput::None)
    }
}

fn build(graph: &DataflowGraph) {
    let events = graph.source(VecSource::new(vec![Event { user: "u1".into() }]));

    // WRONG: compiles, runs, and silently produces wrong results with >1 worker.
    // Each worker sees an arbitrary slice of every user's events.
    events.operator("bad", Counter).sink(PrintSink::<Event>::new());

    // RIGHT: every event for a user lands on the worker owning that user's state.
    events
        .key_by(|e| e.user.to_string())
        .operator("good", Counter)
        .sink(PrintSink::<Event>::new());
}
```

There is no error, no warning, and no metric for this. With one worker the results even look correct — the bug only appears under `--workers 2`. **Test stateful pipelines with more than one worker.**

Making this a compile-time guarantee is a tracked roadmap item.

---

## Re-keying

A second `key_by` triggers a second exchange. This is how you aggregate at one grain and then at another:

```rust,no_run
use rhei::arrow::{BufferOutput, OperatorContext, RheiBuffer};
use rhei::{DataflowGraph, PrintSink, VecSource};

#[derive(Clone, rhei::RheiSchema)]
struct Order {
    customer: String,
    region: String,
}

#[derive(Clone)]
struct Agg;

#[async_trait::async_trait]
impl rhei::arrow::StreamFunction for Agg {
    type Input = Order;
    type Output = Order;
    async fn process(
        &mut self,
        _i: RheiBuffer<Order>,
        _c: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Order>> {
        Ok(BufferOutput::None)
    }
}

fn build(graph: &DataflowGraph) {
    graph
        .source(VecSource::new(vec![Order {
            customer: "c1".into(),
            region: "eu".into(),
        }]))
        .key_by(|o| o.customer.to_string())
        .operator("per_customer", Agg)
        .key_by(|o| o.region.to_string()) // second exchange
        .operator("per_region", Agg)
        .sink(PrintSink::<Order>::new());
}
```

Each exchange costs a repartition — locally a channel handoff, across processes a serialize/send/deserialize. Re-key when the grain genuinely changes, not defensively.

---

## Merge discards partitioning

`merge` concatenates two streams with a Pipeline pact. It does not re-partition, so the result carries no useful key alignment even if both inputs were keyed. **Always `key_by` after a `merge` and before a stateful operator** — this is exactly what the `TemporalJoin` pattern does.

---

## Sources and parallelism

By default a source runs on **worker 0** and its output fans out from there.

A source that implements `partition_count()` and `create_partition_source()` is consumed in parallel: partitions are assigned round-robin across workers, and each worker runs its own reader. `KafkaSource` and `PartitionedVecSource` do this.

This is independent of `key_by`. A partitioned Kafka source reads in parallel by *Kafka* partition; if you then `key_by` on something other than the Kafka key, an exchange still occurs.

---

## Rescaling

Because state is addressed by key group, the worker count can change between runs — or during one, via `PipelineController::run_dynamic()`.

The sequence on a rescale:

1. Checkpoint at the current topology
2. Compute the new key group assignment for the new worker count
3. Rebuild the `TaskManager` and restart the Timely dataflow
4. Workers read their newly-owned groups from shared L3 on first access

Restoring a checkpoint at a **different worker count** is supported — that is the point. Restoring at a different **`max_parallelism`** is rejected, because every key would hash into a different group.

Gossip-based membership (behind the optional `chitchat` feature) drives this automatically, with phi-accrual failure detection and a debounce window (`rescale_debounce_secs`) so a rolling restart does not trigger one rescale per node.

Observable via:

```text
rhei_cluster_key_groups_owned
rhei_cluster_key_groups_moved_total
rhei_cluster_rescales_total
rhei_cluster_rescales_suppressed_total
rhei_cluster_rescale_duration_seconds
rhei_cluster_idle_workers          # workers beyond max_parallelism
```

Known gap: key groups are **not** warmed proactively after a rescale. The gaining worker faults them in on first access, so there is a latency bump right after a topology change.

---

## Cost model

| Concern | Guidance |
|---------|----------|
| Exchange cost | One serialize/deserialize per sub-buffer across processes; a channel handoff within one |
| Key cardinality | Very low cardinality means idle workers — 3 distinct keys cannot use 8 workers |
| Key skew | One hot key pins one worker. Key groups do not help; the key itself must be split (e.g. salt it, aggregate twice) |
| Key cost | The key function runs per row and allocates a `String`. Keep it cheap |
| Batch size | Larger batches amortise exchange overhead; smaller ones lower latency |

---

## Checklist

- `key_by` before every stateful operator, and after every `merge`
- The routing key matches what the operator keys its state on
- `max_parallelism` set once, at or above your maximum future worker count
- Stateful pipelines tested with `--workers 2` or more
- Watch `rhei_cluster_key_groups_owned` for skew across workers

# State and Checkpointing

How operator state is stored, tiered, addressed, and made durable — and what recovery actually guarantees.

---

## The state APIs

Inside an operator, state is reached through `ctx.state` (a `StateContext`). You never construct one; the runtime creates a `StateContext` per worker per operator.

| Wrapper | Shape | Typical use |
|---------|-------|-------------|
| `KeyedState<K, V>` | key → value map | counters, last-seen, per-key aggregates |
| `ValueState` | one value per key | a flag, a running total |
| `ListState` | append-only list per key | buffering events for a window |
| `MapState` | nested map per key | per-key dictionaries |
| `TimerService` | event-time timers | fire logic at a future watermark |

`KeyedState` is what most operators want:

```rust,no_run
use rhei::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};
use rhei::KeyedState;

#[derive(Clone, rhei::RheiSchema)]
struct Event {
    user: String,
}

#[derive(Clone, rhei::RheiSchema)]
struct Count {
    user: String,
    seen: u64,
}

#[derive(Clone)]
struct Counter;

#[async_trait::async_trait]
impl StreamFunction for Counter {
    type Input = Event;
    type Output = Count;

    async fn process(
        &mut self,
        input: RheiBuffer<Event>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Count>> {
        let mut builder = Count::builder(input.len());
        // Namespace: distinct state under the same operator uses distinct prefixes.
        let mut counts = KeyedState::<String, u64>::new(&mut ctx.state, "seen");

        for view in &input {
            let user = view.user.to_string();
            let seen = counts.get(&user).await?.unwrap_or(0) + 1;
            counts.put(&user, &seen)?;
            builder.append(Count { user, seen });
        }

        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}
```

Two API facts that surprise people:

- `get` and `put` take `&K`. A view field is `&str`, so `String` keys need `.to_string()`.
- `put` returns `Result<()>` — serialization can fail.

Values are JSON-encoded by default. `KeyedState::with_encoder(ctx, prefix, BincodeEncoder)` switches to bincode: more compact and faster, but not human-readable when you inspect state through `/api/state/operators/{name}`.

---

## The tiers

| Tier | Backend | Contents | Bounded by |
|------|---------|----------|------------|
| **L1** | `HashMap` (dirty) + `moka` W-TinyLFU cache (clean) | hot working set, in-process | `MemTableConfig` |
| **L2** | Foyer `HybridCache` | local NVMe cache | `TieredBackendConfig` |
| **L3** | SlateDB on object storage | durable source of truth | object store |

A read tries L1, then L2, then L3, promoting on the way back. A write goes to L1 and marks the key dirty; it reaches L3 at the next checkpoint.

### L1 in detail

L1 is two structures, and the split matters:

- **Dirty entries** — written since the last flush — live in a plain `HashMap` and are **never evicted**. Evicting them would lose data that has not been checkpointed.
- **Clean entries** — already persisted — live in a `moka` cache with W-TinyLFU admission, bounded by `MemTableConfig`.

```text
MemTableConfig {
    max_bytes:   32 MiB      // approximate, clean cache only
    max_entries: 500_000     // clean cache only
}
```

Set it with `.memtable_config(..)` on the controller builder.

> **KI-7:** because dirty entries are unbounded, L1 can still grow without limit
> *between* checkpoints, proportional to the number of distinct keys written. A
> workload with high write cardinality and a long `checkpoint_interval` can
> exhaust RAM. The mitigation today is a shorter interval.

### L2 in detail

```text
TieredBackendConfig {
    foyer_dir:             /tmp/rhei-foyer-{pid}   // PID-scoped
    foyer_memory_capacity: 64 MiB
    foyer_disk_capacity:   256 MiB
    foyer_block_size:      256 KiB
}
```

The default directory is under `/tmp`, which on many systems is tmpfs — meaning L2 is competing for the RAM you are trying to save. **Point `foyer_dir` at real NVMe in production.**

Block size trades read amplification against metadata overhead. 256 KiB suits point lookups, which is what a state backend mostly does; raise it only for large values or scan-heavy access.

### L3 in detail

SlateDB over `object_store`, so S3, GCS, Azure Blob, or MinIO. Enabled by the `remote-state` feature plus a `RemoteStateConfig`:

```text
RemoteStateConfig {
    bucket:     "my-bucket"
    prefix:     "rhei/state/"
    endpoint:   Some("http://localhost:9000")   // MinIO / Azurite
    region:     "us-east-1"
    allow_http: true                            // local dev only
}
```

Credentials come from the environment (`AWS_ACCESS_KEY_ID`, …) or instance metadata / IAM role — never from this struct.

Without `remote-state`, state is local to each process. **Multi-process mode without remote state will not share state correctly.**

### The cold path blocks

An L1 miss is served by `tokio::runtime::Handle::block_on` from inside the Timely operator closure — the Timely worker thread **stops** for the duration of the L2/L3 fetch. Other workers proceed independently.

This is a known limitation (KI-11), not a design goal. The operator API borrows `&mut self` and `&mut StateContext` across the await, so a genuinely non-blocking cold path needs an API redesign (`'static` futures, state prefetch, or a split prepare/complete).

Practical consequence: **keep the working set in L1.** A pipeline whose access pattern misses L1 constantly will see throughput collapse to L3 latency, not degrade gracefully. Watch `state_l1_hits_total` against `state_l1_misses_total`.

---

## Physical key layout

```text
kg{group:05}/{operator}/{state_key}
```

Three properties are deliberate:

- **Key group first** — a whole key group is contiguous across every operator, which is what warming a gained range wants to scan.
- **Zero-padded** — lexicographic order matches numeric order, so an owned range is a contiguous scan in an ordered store like SlateDB.
- **No worker or process index** — the physical key is identical no matter who performs the access. That is what makes rescaling a matter of ownership rather than migration.

The group is derived from the **partition key** (your `key_by` output), not from the storage key `KeyedState` builds. See [exchange-and-partitioning.md](exchange-and-partitioning.md#the-key-must-be-the-key).

*Source: `rhei-core/src/state/key_group_addressing.rs`.*

---

## Checkpointing

### The trigger

Checkpoints are **frontier-driven**. When Timely's frontier advances past the last checkpointed epoch, and at least `checkpoint_interval` batches have passed (default 100), a checkpoint fires.

A frontier boundary is exactly the point where "all data up to epoch E has been processed" holds, so the state snapshot and the recorded source offsets describe the same cut of the stream.

The corollary is important: **if the watermark never advances, checkpoints never fire.** A stalled watermark stalls durability. See [time-and-watermarks.md](time-and-watermarks.md).

### The sequence

```text
frontier advances
    │
    ├─ L1 dirty keys flush → L2/L3
    ├─ SlateDB uploads SSTables to object storage
    ├─ (cluster) process reports Ready{process_id, epoch}, waits for Committed
    ├─ checkpoint manifest written
    └─ source offsets committed  ← after the manifest, hence at-least-once
```

Mid-execution checkpoints run concurrently with the dataflow. There is no stop-the-world pause.

### The manifest

JSON, describing one checkpoint:

| Field | Meaning |
|-------|---------|
| `version` | schema version |
| `checkpoint_id` | monotonically increasing |
| `timestamp_ms` | wall clock at write time |
| `operators` | sorted operator names |
| `source_offsets` | source-defined, e.g. `"topic/partition" → "offset"` |
| `n_processes`, `workers_per_process`, `total_workers` | topology that wrote it |
| `max_parallelism` | key group count — **validated on restore** |
| `cluster_members` | node IDs, for operational forensics |

`validate_compatible` rejects a restore where `max_parallelism` differs, because every key would hash into a different group and read empty state instead of failing. A different worker count is fine — that is rescaling.

*Source: `rhei-core/src/checkpoint.rs`.*

### Coordinated checkpoints across processes

Process 0 runs a `CheckpointCoordinator` on a TCP port (`RHEI_CHECKPOINT_PORT`), separate from Timely's data plane.

```text
Participant → Coordinator :  Ready { process_id, epoch }
   (once every process has reported for that epoch)
Coordinator → Participants:  Committed { epoch }
```

Messages are length-prefixed bincode. Only after `Committed` is the merged manifest written.

Process 0 is the coordinator **by convention** — there is no leader election. If process 0 dies, checkpoint coordination stops.

*Source: `rhei-runtime/src/checkpoint_coord.rs`.*

---

## Recovery

On startup with a checkpoint present, the runtime loads the manifest, validates `max_parallelism`, extracts `source_offsets`, and calls `source.restore_offsets()` before any batch is read. `KafkaSource` implements this by building a `TopicPartitionList` at `offset + 1` and calling `consumer.assign()`.

Note that assigning explicitly **overrides consumer group rebalancing** for the restored partitions.

### What recovery guarantees

**At-least-once.** Offsets are committed after the checkpoint completes, so a crash between processing and committing replays the records in that gap. Downstream effects are re-applied.

Exactly-once would require a transactional producer sink and two-phase commit across source, state, and sink. Neither is implemented.

If duplicates matter, make your sink idempotent — key writes by something derived from the record rather than appending blindly.

---

## Fork mode

Restore a production checkpoint on your laptop, against local Kafka:

```bash
cargo run -- \
  --from-checkpoint checkpoints/manifest.json \
  --offset-delta -1000
```

`ForkBackend` is copy-on-write: reads fall back to the read-only remote state, writes stay local. The remote checkpoint is never mutated. `--offset-delta` shifts recorded offsets so they line up with a locally re-indexed topic.

Worker count must match the manifest — topology validation rejects a mismatch.

Requires the `remote-state` feature. Tombstone bookkeeping is visible in `state_fork_tombstone_count` and `state_fork_tombstone_overflow_total`.

---

## Tuning

| Setting | Default | Effect |
|---------|---------|--------|
| `checkpoint_interval` | 100 batches | Lower: less replay on crash, more I/O, bounded dirty L1. Higher: the opposite |
| `memtable_config.max_entries` | 500,000 | Clean L1 cache size |
| `memtable_config.max_bytes` | 32 MiB | Clean L1 cache bytes |
| `foyer_memory_capacity` | 64 MiB | L2 in-memory portion |
| `foyer_disk_capacity` | 256 MiB | L2 on-disk portion |
| `foyer_dir` | `/tmp/rhei-foyer-{pid}` | **Move to real NVMe in production** |
| `max_parallelism` | 128 | Key groups; fixed for the pipeline's lifetime |

Rules of thumb:

- Size L1 to hold the hot key set. The cold path blocks, so misses are expensive.
- Size L2 to hold the working set across a checkpoint interval.
- Shorten `checkpoint_interval` if dirty L1 growth is a concern (KI-7) or replay time matters.
- Lengthen it if checkpoint I/O dominates.

---

## Metrics

```text
state_gets_total                    state_puts_total
state_deletes_total                 state_get_duration_seconds
state_l1_hits_total                 state_l1_misses_total
state_l2_hits_total                 state_l2_misses_total
state_l3_hits_total                 state_l3_misses_total
state_checkpoints_total             state_checkpoint_duration_seconds
state_checkpoint_dirty_keys         executor_checkpoint_duration_seconds
state_ttl_expired_total             state_ttl_swept_total
state_fork_tombstone_count          state_fork_tombstone_overflow_total
```

What to watch:

- **L1 hit ratio** — the headline number. Falling means the working set outgrew L1 and the blocking cold path is now on your hot path.
- **`state_checkpoint_dirty_keys`** — how much L1 growth each interval accumulates.
- **`state_checkpoint_duration_seconds`** — if it approaches the interval, checkpoints are backing up.

State can also be inspected live over HTTP: `/api/state/operators` and `/api/state/operators/{name}`.

---

## Limits

- **No exactly-once.** At-least-once only.
- **Dirty L1 is unbounded** between checkpoints (KI-7).
- **The cold path blocks** the Timely worker thread (KI-11).
- **No checkpoint versioning or state migration.** Changing a value's serialized shape breaks restore.
- **No incremental checkpoints.** Every dirty key flushes each time.
- **No proactive key-group warming** after a rescale — gained groups fault in on first access.
- **`max_parallelism` is immutable** for a pipeline's lifetime.
- **No leader election.** Process 0 coordinates by convention.

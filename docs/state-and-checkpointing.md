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

### Enable the correctness protocol

Set `PipelineController::builder().coordinated_recovery(true)` and use a fresh
checkpoint directory. Legacy `manifest.json` checkpoints cannot be safely
migrated; the default legacy executor does not guarantee a consistent recovery cut.

The first implementation supports fixed topology with multiple processes. Use the
same static peers, process IDs, workers per process, key-group count, graph and
source partition layout on restart. All processes must share the publication
directory on a durable filesystem with atomic rename, directory fsync, and
cross-client exclusive locks. Object-store-only metadata publication is not yet
supported, even when state itself is stored remotely.

### The trigger and sequence

Checkpoints use **execution epochs**, independent of event time. A process requests
a checkpoint after `checkpoint_interval` admitted batches (default 100), on source
exhaustion, or on the coordinator's one-second idle tick. Stalled event-time
watermarks do not prevent checkpoints.

1. Pause admission on every process. Each admitted batch carries its exact
   post-batch cursor; prefetched batches do not advance checkpoint offsets.
2. Align admitted-prefix watermarks, advance source capabilities, and drain every
   worker and branch, including timer and watermark outputs.
3. Snapshot managed state plus timers, runtime watermarks, and built-in
   window/join/sequence discovery indexes. Dirty state is only cleared after
   persistence succeeds.
4. Durably flush every sink through a FIFO request, then create immutable state
   snapshots. Every process sends a Prepared acknowledgement for the same epoch.
5. Process 0 durably publishes `recovery.json` by atomic rename. Only then does it
   send Commit and allow admission to resume.

Local snapshots are immutable files. SlateDB snapshots are pinned checkpoint IDs;
each restart clones the exact checkpoint into a fresh database path. Uncommitted
writes and old cache contents cannot enter the restored state. Coordinated SlateDB
execution currently bypasses the external Foyer cache.

### Publication and coordination

`recovery.json` is the sole authoritative decision. Its version, epoch, topology,
worker/process/key-group counts, and per-process snapshot references, source
cursors, watermarks, and partition layout describe one complete cut. Missing or
malformed snapshots and incompatible layouts fail startup; they never become
empty state. Prepared attempt directories are not recovery candidates.

Process 0 coordinates over length-prefixed JSON on `RHEI_CHECKPOINT_PORT`, separate
from Timely. Exclusive directory locks fence competing attempts. Handshakes verify
topology, prior epoch, and shared-directory visibility. Unexpected phases, stale or
duplicate acknowledgements, disconnections, and 60-second epoch deadlines fail the
attempt. There is no leader election: restart **all processes** after failure.
If a Commit response is lost, restart reads the durable publication to resolve it.

*Source: `rhei-runtime/src/recovery.rs` and `recovery/coordination.rs`; design and
limitations: [ADR/recovery-protocol.md](../ADR/recovery-protocol.md).*

---

## Recovery

Coordinated recovery restores immutable state and calls `source.restore_offsets()`
before any source reads. Sources must declare `supports_recovery()`: cursors must
cover every assigned partition and an empty cursor must mean the same initial
position on every attempt. `VecSource` and `PartitionedVecSource` readers implement
this contract when given identical input on restart. Kafka does not yet implement
the full contract and is rejected in coordinated mode. The protocol does not call
the legacy live-cursor `on_checkpoint_complete` hook.

Custom operators must preserve all recovery-relevant metadata through managed
state and the `snapshot`/`open` hooks. Do not use reserved `__rhei_*` keys. Keep code,
schemas, source content and window configuration compatible: graph comparison
cannot detect arbitrary code changes. TTL-dependent recovery is unsupported
because TTL timestamps are not persisted.

### What recovery guarantees

**At-least-once**, conditional on replayable sources and durable `Sink::flush`.
A crash before publication replays the uncommitted prefix; output already written
from that prefix can be duplicated. Propagated state/operator/sink errors fail the
whole attempt. Durable DLQ coordination is not yet supported.

Exactly-once would require a transactional producer sink and two-phase commit across source, state, and sink. Neither is implemented.

If duplicates matter, make your sink idempotent — key writes by something derived from the record rather than appending blindly.

---

## Fork mode

This is a legacy facility, not compatible with coordinated recovery. It does not
provide the immutable-snapshot guarantees above.

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

# Known Issues

Tracked gaps, limitations, and potential correctness issues in the Rhei codebase.
Severity labels: **CRITICAL** (data loss or incorrect results), **HIGH** (resource
exhaustion, silent misbehaviour), **MEDIUM** (missing feature, workaround exists).

Resolved entries are kept rather than deleted: what a system used to get wrong
is part of what you need to know when judging it, and a register that only
lists open items reads as though nothing was ever broken.

Security issues are tracked separately in [SECURITY.md](SECURITY.md) as `SI-N`.

**Still open:** KI-14, KI-18, KI-26, KI-27, and the dirty half of KI-7.

---

## CRITICAL

### ~~KI-1: Sink send errors silently dropped~~ (RESOLVED)

**Fixed in:** `ADR/temporal-join-timeout.md` (same batch of fixes)

`blocking_send` failures are now logged at `error` level with a
`sink_send_errors_total` metrics counter. Errors cannot be propagated out of the
Timely closure, but the sink task error surfaces when its `JoinHandle` is awaited.

### ~~KI-2: Checkpoint source offsets never reloaded on restart~~ (RESOLVED)

**Fixed in:** `ADR/checkpoint-restore.md`

On restart, `run_graph` now extracts `source_offsets` from the loaded manifest and
calls `source.restore_offsets()` before any batches are read. `KafkaSource`
implements `restore_offsets()` by calling `consumer.assign()` with the checkpointed
offsets, restoring at-least-once semantics.

### ~~KI-3: DLQ write errors silently dropped~~ (RESOLVED)

**Fixed in:** `ADR/temporal-join-timeout.md` (same batch of fixes),
updated in `ADR/dead-letter-queue.md` (per-worker channel refactor)

DLQ writes now use per-worker `tokio::sync::mpsc` channels bridged to async sink
tasks — the same pattern as regular sinks. Channel send failures are logged at
`error` level with a `dlq_write_errors_total` metrics counter. The shared
`Arc<Mutex<DlqFileSink>>` has been replaced, eliminating mutex poisoning as a
failure mode. Any `Sink` implementation (file, Kafka, custom) can serve as a DLQ
destination via the `DlqSinkFactory` trait.

---

## HIGH

### ~~KI-4: Multiple exchanges (key_by) not supported~~ (RESOLVED)

**Fixed in:** `ADR/timely-exchange-dag.md`

The unified Timely DAG executor uses the Exchange pact for each `key_by()` node.
Multiple exchanges in a single pipeline work correctly — each triggers a full
repartition via Timely's built-in worker routing.

### ~~KI-5: Temporal join has no timeout or eviction~~ (PARTIALLY RESOLVED)

**Fixed in:** `ADR/temporal-join-timeout.md`

An optional `timeout` parameter triggers watermark-driven eviction of stale buffered
events. Buffered events are timestamped with the watermark at buffer time and evicted
when `watermark >= buffered_timestamp + timeout`. A `temporal_join_evicted_total`
metric tracks eviction frequency. Side-output routing of evicted events to a separate
stream is not yet implemented.

### ~~KI-6: Window operators silently drop late events~~ (PARTIALLY RESOLVED)

**Fixed in:** `ADR/watermark-propagation.md`

Window operators now detect late events using watermark-based tracking. Events arriving
after `window_end + allowed_lateness <= last_watermark` are dropped with a
`late_events_dropped_total` metric increment. The `allowed_lateness` parameter is
configurable per window operator via the builder API. Side-output routing of late
events to a separate stream is not yet implemented.

### KI-7: L1 memtable dirty entries are unbounded (PARTIALLY RESOLVED)

**File:** `rhei-core/src/state/memtable.rs`

**Resolved part:** clean entries (already persisted to L2/L3) now live in a
[`moka`] cache with W-TinyLFU admission and eviction, bounded by
`MemTableConfig` (`max_entries`, default 500,000, plus optional byte-weighted
`max_bytes`). Configure it with
`PipelineController::builder().memtable_config(..)`.

**Remaining:** dirty entries — writes not yet flushed to the backend — are held
in a plain `HashMap` and are **never evicted**, because evicting them would lose
data that has not been checkpointed. Between checkpoints, L1 can still grow
without bound in proportion to the number of distinct keys written. A workload
with high write cardinality and a long checkpoint interval can exhaust RAM.
Mitigation today is a shorter `checkpoint_interval`; a real fix needs
write-triggered flushing or backpressure. See
[docs/deployment.md](docs/deployment.md#capacity-planning) for how to size it.

### ~~KI-8: Checkpoint interval is hardcoded~~ (RESOLVED)

**Fixed in:** `ADR/checkpoint-restore.md`

The checkpoint interval is now configurable via
`PipelineController::builder().checkpoint_interval(n)`. Defaults to 100 batches.
Both `PipelineController` and `PipelineControllerBuilder` carry the field, and
the multi-worker main loop reads it from the controller at runtime.

### ~~KI-9: No merge / fan-in support in executor~~ (RESOLVED)

**Fixed in:** `ADR/timely-exchange-dag.md`

The unified Timely DAG executor supports merge nodes via `scope.concatenate()`,
combining multiple input streams into one. The `stream.merge(other)` API works
at execution time.

### ~~KI-10: Sliding window unbounded active windows~~ (RESOLVED)

**File:** `rhei-core/src/operators/sliding_window.rs`

`on_watermark` now closes every window whose end plus `allowed_lateness` has
passed the watermark: it emits the result, deletes the accumulator, and prunes
the start time from `ActiveWindows`. A key whose windows have all closed is
removed from state and from the active key set entirely.

Concurrent windows per key are therefore bounded by
`window_size / slide_interval`, not by how long the pipeline has run.

---

## MEDIUM

### KI-11: State cold path blocks the Timely worker thread (PARTIALLY RESOLVED)

**File:** `rhei-runtime/src/async_operator.rs`, `rhei-runtime/src/timely_operator.rs:34`

Operator futures are driven with `tokio::runtime::Handle::block_on` from inside
the Timely operator closure. An L1 state miss therefore **blocks that worker
thread** for the duration of the L2/L3 fetch. Other workers are unaffected, but
throughput on the blocked worker stalls. Documentation must not describe the
cold path as non-blocking.

The `rt = None` data loss path now logs at `error` level with an
`async_operator_dropped_elements_total` metric. The `let _ = cap` pattern was
replaced with explicit `drop(cap)` calls with comments explaining the intent.
Doc comments were added to `pending`, `drain_completed`, and `poll_pending`
documenting that they are scaffolding for a future async cold path.

The underlying ordering issue remains: a true async cold path requires an API
redesign (e.g., `'static` futures, state prefetch, or split prepare/complete)
since `StreamFunction::process` borrows `&mut self` + `&mut StateContext`.

### ~~KI-12: Single-worker checkpoint has no source offsets~~ (RESOLVED)

**Fixed in:** `ADR/checkpoint-restore.md`

`erased_source_bridge_with_offsets()` shares an `Arc<Mutex<HashMap>>` between
the bridge task and the executor. After each `next_batch()`, the bridge copies
`current_offsets()` into the shared map. The single-worker manifest now records
actual source offsets.

### ~~KI-13: Watermarks tracked but never propagated~~ (RESOLVED)

**Fixed in:** `ADR/watermark-propagation.md`

Sources now implement `current_watermark()` which is read by the bridge and propagated
to operators via a shared `Arc<AtomicU64>`. A global watermark task computes the
minimum of all non-zero source watermarks every 100ms. Operators read this watermark
in their `unary_frontier` callback and call `on_watermark()` on the wrapped
`StreamFunction`. Window operators use this to close eligible windows on idle sources.

### KI-14: Tracing log channel drops entries under backpressure

**File:** `rhei-runtime/src/tracing_capture.rs:103`

```rust
let _ = self.tx.try_send(entry);
```

The tracing capture layer uses non-blocking `try_send`. When the channel is full,
log entries are silently dropped. This is documented and intentional (backpressure),
but under high log volume it means observability gaps.

### ~~KI-15: No checkpoint failure propagation in single-worker~~ (RESOLVED)

**Fixed in:** `ADR/checkpoint-restore.md`

Single-worker now creates a `Barrier::new(1)` and `mpsc::channel`, passes them
into `build_timely_dataflow`. After Timely completes, the executor drains
checkpoint notifications and writes intermediate manifests with source offsets,
matching multi-worker behavior.

### ~~KI-16: Kafka consumer group does not seek on restart~~ (RESOLVED)

**Fixed in:** `ADR/checkpoint-restore.md`

The `Source` trait now has a default no-op `restore_offsets()` method. `KafkaSource`
implements it by parsing `"topic/partition"` keys from the manifest, building a
`TopicPartitionList` with `Offset::Offset(offset + 1)`, and calling
`consumer.assign()` to seek to the correct positions. Note: this overrides consumer
group rebalancing when restoring from checkpoint.

### ~~KI-17: No fan-out support~~ (RESOLVED)

**Fixed in:** `ADR/timely-exchange-dag.md`

The unified Timely DAG executor supports fan-out implicitly via Timely's internal
Tee. The same stream can feed multiple downstream operators and sinks. Stream
handles are `Copy`, so `stream.sink(sink1); stream.sink(sink2);` works correctly.

### KI-18: Limited integration tests for failure scenarios

Partitioned source and multi-worker checkpoint restart are now covered
(`checkpoint_restart.rs`). Kafka multi-partition E2E is covered
(`kafka_e2e.rs`). Still missing:
- Backpressure behaviour when channels fill up
- DLQ routing under sustained error rates
- Late event handling across window boundaries
- Checkpoint failure and recovery
- Network partition behaviour (relevant for Phase 2 multi-process)
- Source exhaustion during checkpoint cycle

---

## Production readiness

Issues found and fixed during a production-readiness review, plus the gaps that
remain. See [OPERATIONS.md](OPERATIONS.md) for how to run around them.

### ~~KI-19: Cluster checkpoint coordination bound to loopback~~ (RESOLVED)

**File:** `rhei-runtime/src/task_manager.rs`, `rhei-runtime/src/checkpoint_coord.rs`

**Severity: CRITICAL.** Process 0 bound the checkpoint coordinator on
`127.0.0.1`, and every other process dialled `127.0.0.1` — its *own* loopback,
not process 0's host. Multi-process clustering therefore only worked when every
process shared a host, which is how the tests ran and why it went unnoticed. Any
genuinely distributed deployment failed to start with
`failed to connect to checkpoint coordinator`.

The coordinator now binds all interfaces and peers derive the coordinator's host
from the first entry of the peer list. The connect retry budget also grew from
5 seconds to 120 with exponential backoff, so a staggered rollout where process 0
starts last is not a crash loop.

### ~~KI-20: Merged cluster manifests dropped the key group count~~ (RESOLVED)

**File:** `rhei-core/src/checkpoint.rs`

**Severity: CRITICAL.** `merge_partials` did not carry `max_parallelism` into
the merged `manifest.json` — the manifest a restart actually reads. Since
`validate_compatible` accepts a `None` value, the key-group safety check was
silently disabled in cluster mode, exactly where it matters. Restarting a
cluster at a different `max_parallelism` re-partitioned the key space and read
empty state instead of failing.

The merge now carries the cluster shape through and rejects partials that
disagree on `max_parallelism`.

### ~~KI-21: Corrupt checkpoint manifests read as "no checkpoint"~~ (RESOLVED)

**File:** `rhei-core/src/checkpoint.rs`

**Severity: CRITICAL.** `CheckpointManifest::load` mapped every failure to
`None`, so a corrupt manifest was indistinguishable from a pipeline that had
never checkpointed: the pipeline restarted from offset zero and reprocessed the
entire stream without a word.

Recovery paths now use `load_checked`, which returns `Ok(None)` only for a
genuinely absent manifest and errors on corruption, I/O failure, or an unknown
schema version. `load` remains lossy for read-only surfaces and logs.

### ~~KI-22: Checkpoint manifest writes were not durable~~ (RESOLVED)

**File:** `rhei-core/src/checkpoint.rs`

The manifest write claimed crash-safety it did not have. `rename` makes the swap
atomic for a concurrent *reader* but orders nothing against power loss, so the
rename could reach disk before the data it points at — leaving a manifest that
is present, parseable, and empty. Writes now fsync the file before the rename
and the directory after it.

### ~~KI-23: Liveness probe could not detect a wedged pipeline~~ (RESOLVED)

**File:** `rhei-runtime/src/health.rs`

`/healthz` returned 200 whenever the process was alive. A pipeline whose Timely
loop had deadlocked still answered TCP, so an orchestrator never restarted it.
Workers now stamp a heartbeat each turn of the loop and `/healthz` returns 503
once any worker goes quiet past `RHEI_LIVENESS_TIMEOUT_SECS`.

### ~~KI-24: Malformed environment variables were silently ignored~~ (RESOLVED)

**File:** `rhei-runtime/src/controller.rs`

`from_env` discarded unparseable values, so `RHEI_WORKERS=four` ran at default
parallelism and a malformed `RHEI_METRICS_ADDR` left monitoring dark with no
indication. Malformed variables are now collected and reported together by
`build()`. `RHEI_CHECKPOINT_DIR`, `RHEI_CHECKPOINT_INTERVAL` and
`RHEI_MAX_PARALLELISM` were also only honoured through the TOML path; `from_env`
now reads them too.

### ~~KI-25: Unauthenticated state explorer with permissive CORS~~ (RESOLVED)

**Tracked as SI-6 in [SECURITY.md](SECURITY.md).** File: `rhei-runtime/src/http_server.rs`

`/api/state/**` returns checkpointed application data — your keys and values —
and was served to anyone who could reach the port, under
`CorsLayer::permissive()`, meaning any website could read it from a visitor's
browser. The state explorer is now off unless `RHEI_STATE_EXPLORER=1`, CORS
origins must be listed explicitly, and a bearer token can be required via
`RHEI_METRICS_TOKEN`. Listing operators also no longer loads every operator's
full state into memory to count entries, and inspection refuses state above
`MAX_INSPECTABLE_STATE_BYTES`.

### KI-26: Cluster data plane has no transport security

**Tracked as SI-4 in [SECURITY.md](SECURITY.md).**

The Timely inter-process data plane (port 2101) and the checkpoint coordination
channel are plaintext TCP with no authentication. Anyone who can reach those
ports can read pipeline data in flight, inject records, and forge checkpoint
readiness messages.

**Workaround:** run them on a trusted network — a private subnet, a service mesh
providing mTLS, or a NetworkPolicy restricting 2101 to pods in the same
StatefulSet. Do not expose these ports beyond the cluster.

### KI-27: Delivery is at-least-once, not exactly-once

Source offsets are committed after a checkpoint completes, so a crash replays
everything processed since the last checkpoint. `kafka_e2e.rs` asserts this
explicitly.

Sinks must tolerate duplicates — idempotent writes or downstream deduplication.
Rhei should not be deployed behind a sink that assumes each record arrives once.


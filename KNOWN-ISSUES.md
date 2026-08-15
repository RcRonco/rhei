# Known Issues

Tracked gaps, limitations, and potential correctness issues in the Rhei codebase.
Severity labels: **CRITICAL** (data loss or incorrect results), **HIGH** (resource
exhaustion, silent misbehaviour), **MEDIUM** (missing feature, workaround exists).

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
write-triggered flushing or backpressure.

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

### KI-10: Sliding window unbounded active windows

**File:** `rhei-core/src/operators/sliding_window.rs`

`ActiveWindows` stores all active window start times per key. With small slide
intervals relative to window size, thousands of overlapping windows can accumulate
per key with no eviction when windows close.

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

# Known Issues

Tracked gaps, limitations, and potential correctness issues in the Rill codebase.
Severity labels: **CRITICAL** (data loss or incorrect results), **HIGH** (resource
exhaustion, silent misbehaviour), **MEDIUM** (missing feature, workaround exists).

---

## CRITICAL

### KI-1: Sink send errors silently dropped

**File:** `rill-runtime/src/executor.rs:582`

```rust
let _ = sink_tx.blocking_send(item);
```

If `blocking_send()` fails (receiver dropped, channel full after close), the error
is discarded with `let _ =`. Elements are permanently lost with no error propagation
or logging. This affects the single-worker execution path.

### KI-2: Checkpoint source offsets never reloaded on restart

**Files:** `rill-runtime/src/executor.rs:1028-1055`, `rill-core/src/checkpoint.rs`

The checkpoint manifest persists `source_offsets` and loads them at startup, but
there is no code to feed those offsets back into the source. `KafkaSource` starts
fresh from `auto.offset.reset=earliest` on every restart instead of seeking to the
last committed offset. This causes full re-consumption of all data, violating
at-least-once semantics.

### KI-3: DLQ write errors silently dropped

**File:** `rill-runtime/src/executor.rs:518`

```rust
let _ = sink.write_record(&record);
```

If the DLQ file write fails (disk full, permissions), the error is discarded.
Records that failed processing AND failed DLQ persistence are permanently lost.

---

## HIGH

### KI-4: Multiple exchanges (key_by) not supported

**Files:** `rill-runtime/src/compiler.rs:150` (`split_at_first_exchange`),
`rill-runtime/src/executor.rs:335,566`

`split_at_first_exchange()` splits the segment list at the first `Exchange` only.
Subsequent `Segment::Exchange` entries are silently ignored (no-op match arms in
`build_pre_exchange_steps` and `build_timely_dataflow`). Pipelines with multiple
`key_by()` calls lose key affinity after the first repartition in multi-worker mode.

### KI-5: Temporal join has no timeout or eviction

**File:** `rill-core/src/operators/temporal_join.rs:152-190`

Unmatched events are buffered in operator state indefinitely. There is no timeout,
TTL, or eviction mechanism. If one side never produces a matching event, state grows
without bound. Risk of OOM in long-running pipelines with skewed join keys.

### KI-6: Window operators silently drop late events

**Files:** `rill-core/src/operators/tumbling_window.rs:215-235`,
`rill-core/src/operators/sliding_window.rs`,
`rill-core/src/operators/session_window.rs`

When a tumbling window receives an element whose timestamp maps to a different window
than the currently active one, the old window is closed and emitted. If a late element
arrives for the closed window, it opens a new window — but the old aggregation is
already emitted and cannot be updated. No metrics, logging, or configurable policy
(drop/redirect/update) exists for late arrivals.

### KI-7: L1 memtable has no size limit or eviction

**File:** `rill-core/src/state/memtable.rs:8-11`

`MemTable` is a plain `HashMap` with a dirty set. There is no capacity limit,
eviction policy, or backpressure mechanism. Between checkpoints, L1 can grow
without bound. Under high cardinality workloads this can exhaust available RAM.

### KI-8: Checkpoint interval is hardcoded

**File:** `rill-runtime/src/executor.rs:908`

```rust
let checkpoint_interval: u64 = 100;
```

The multi-worker checkpoint interval is a compile-time constant (100 batches).
Too frequent = overhead; too infrequent = large replay window on failure and
unbounded L1 growth. This should be configurable.

### KI-9: No merge / fan-in support in executor

**File:** `rill-runtime/src/compiler.rs:64-65,93-94`

Merge nodes are modelled in the graph (`NodeKind::Merge`, multi-input `GraphNode`)
but explicitly rejected at compile time with
`"merge nodes are not yet supported in the execution engine"`.
Users needing fan-in must run independent pipelines.

### KI-10: Sliding window unbounded active windows

**File:** `rill-core/src/operators/sliding_window.rs`

`ActiveWindows` stores all active window start times per key. With small slide
intervals relative to window size, thousands of overlapping windows can accumulate
per key with no eviction when windows close.

---

## MEDIUM

### KI-11: Stash ordering under async pending

**File:** `rill-runtime/src/async_operator.rs:109-119`

When a future is pending (L2/L3 state miss), `process_stash()` stops draining and
the pending item stays in the stash. If a subsequent element arrives and resolves
synchronously (L1 hit), it can be emitted before the stashed element finishes,
causing out-of-order delivery within a key partition.

Capability tokens are explicitly dropped with `let _ = cap` (lines 112, 116) rather
than being retained for frontier tracking, which may allow the frontier to advance
prematurely for elements with pending futures.

### KI-12: Single-worker checkpoint has no source offsets

**File:** `rill-runtime/src/executor.rs:748-757`

In single-worker mode the source is consumed by `erased_source_bridge()`, making
`current_offsets()` inaccessible. The manifest records empty offsets. This means
single-worker restarts for Kafka pipelines have no persisted offset information
in the manifest (though Kafka's `__consumer_offsets` may still have committed offsets).

### KI-13: Watermarks tracked but never propagated

**Files:** `rill-core/src/traits.rs:34-37`, `rill-core/src/connectors/kafka_source.rs:22-24,143-155`

`KafkaSource` tracks `records_since_watermark` and implements `should_emit_watermark()`.
The `Source` trait defines the watermark hook. However, no downstream consumer reads
the watermark signal. Watermarks are generated but never propagated through the
dataflow, making them a no-op.

### KI-14: Tracing log channel drops entries under backpressure

**File:** `rill-runtime/src/tracing_capture.rs:103`

```rust
let _ = self.tx.try_send(entry);
```

The tracing capture layer uses non-blocking `try_send`. When the channel is full,
log entries are silently dropped. This is documented and intentional (backpressure),
but under high log volume it means observability gaps.

### KI-15: No checkpoint failure propagation in single-worker

**File:** `rill-runtime/src/executor.rs:721-722`

Single-worker mode passes `None` for both `checkpoint_barrier` and
`checkpoint_notify`. Checkpoint errors in the Timely operator (e.g. L3 write
failure) are not propagated to the main task. The pipeline continues without
acknowledging the failure.

### KI-16: Kafka consumer group does not seek on restart

**File:** `rill-core/src/connectors/kafka_source.rs:41-48`

`KafkaSource::new()` sets `enable.auto.commit=false` and `auto.offset.reset=earliest`.
There is no `restore_offsets()` or seek API. Even if checkpoint manifest offsets are
eventually loaded (fixing KI-2), there is no mechanism to pass them to the Kafka
consumer. The consumer relies entirely on Kafka's `__consumer_offsets` topic, which
is only updated by `on_checkpoint_complete()`.

### KI-17: No fan-out support

**File:** `rill-runtime/src/compiler.rs`

The compiler walks backward from each sink to a source, validating linear topology.
A single source feeding multiple sinks (fan-out) is modelled in the graph (stream
handles are `Copy`) but not supported at execution time. Each sink-to-source path
must be independent.

### KI-18: No integration tests for failure scenarios

No tests exist for:
- Backpressure behaviour when channels fill up
- DLQ routing under sustained error rates
- Late event handling across window boundaries
- Checkpoint failure and recovery
- Network partition behaviour (relevant for Phase 2 multi-process)
- Source exhaustion during checkpoint cycle

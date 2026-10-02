# Coordinated recovery for a fixed topology

## Context

The legacy frontier checkpoint path does not establish a recoverable cut: source
bridges prefetch ahead of execution, workers snapshot independently, mutable
backend state can move beyond the manifest, and sinks flush only at shutdown.
Timers and window discovery indexes also need to survive restart. A metadata
file naming offsets is not sufficient evidence of a durable checkpoint.

## Decision

Introduce opt-in `PipelineController::builder().coordinated_recovery(true)`.
Keep legacy execution available, but do not claim recovery correctness for it.
Start with a fresh checkpoint directory; unsafe legacy snapshots cannot be
automatically upgraded.

Support multiple OS processes immediately, with static peers, identical graphs,
fixed process IDs, worker counts, key-group count, and source partition layout.
All processes share one durable publication directory. It must support atomic
rename, file and directory fsync, and cross-client exclusive file locks. These
semantics must be checked for the actual filesystem; an arbitrary NFS mount or
object-store FUSE mount is not sufficient. Run control/data ports on a trusted
network; neither authenticates peers.

Process 0 is the sole publisher. Per-process OS locks fence overlapping attempts;
process 0 also holds the publication lock. A TCP startup handshake checks topology
and the previously committed epoch. A fresh shared-directory identity confirms
that participants can see the same attempt. Connections belong to that attempt;
messages carry execution epochs and unexpected phases, duplicates, stale epochs,
missing processes, and broken connections fail the attempt. No leader election
or in-place partial recovery is attempted. Restart every process after failure.

### Checkpoint invariant

For committed epoch E, its source cursors describe precisely the batches admitted
through E. Every graph branch and worker has finished that prefix; all managed
state, timer sets, runtime watermarks, and built-in window/join/sequence indexes
are in immutable snapshots, and every sink has acknowledged durable flush.

1. Bridges attach the post-batch cursor to each batch. Prefetch is bounded, but
   only admission into Timely advances the checkpoint cursor.
2. At the batch interval (per process), exhaustion, or a one-second idle tick,
   process 0 pauses admission everywhere. Execution epochs are independent of
   event time. Participants report the watermarks of their admitted prefixes;
   process 0 distributes their minimum.
3. Source capabilities advance beyond E. Every operator/sink branch is probed;
   watermark/timer output uses a retained capability and must drain as well.
4. Every local operator snapshots transient metadata and flushes managed state.
   Dirty keys, deletes, and timer mutations remain dirty until persistence succeeds.
5. After all local workers are ready, each sink receives a FIFO flush request.
   Each process creates immutable backend snapshots and acknowledges preparation.
6. After all processes prepare E, process 0 fsyncs the complete manifest, atomically
   renames it to `recovery.json`, and fsyncs the directory. This is the sole durable
   decision. Only then are Commit messages sent and admission resumed.

An uncertain commit response is resolved by reading `recovery.json` on restart.
Prepared artifacts without that decision are never recovery candidates.

Local state uses immutable per-operator files. SlateDB uses pinned checkpoint IDs
and clones the exact checkpoint into a fresh database path on each attempt.
Abandoned writes cannot enter recovery, including through stale caches. The first
implementation bypasses the external Foyer cache for coordinated SlateDB state.
With `remote_state`, each process has a separate root database path. Callers using
`with_tiered_storage` must likewise supply distinct per-process databases.

## Diagram

```mermaid
sequenceDiagram
    participant P as Process 0
    participant W as All process workers
    participant S as All sinks
    participant B as Immutable state storage
    participant M as Shared recovery.json
    P->>W: Pause E; collect admitted cursors and watermarks
    P->>W: Drain E with aligned watermark
    W->>W: Drain all branches, timers, state and indexes
    W->>S: FIFO durable flush
    S-->>W: Flushed
    W->>B: Create immutable snapshots
    W-->>P: Prepared E with snapshot references and cursors
    P->>M: Atomic durable publication
    P-->>W: Commit E; admit E+1
```

## Source, operator, and sink contracts

- Sources must declare `supports_recovery()`. Their offsets must identify the
  exact prefix read, including all assigned partitions; empty offsets must denote
  a stable initial position. `restore_offsets()` runs before the first read.
  `VecSource` and readers from `PartitionedVecSource` support this when supplied
  identical input on restart. Kafka has not yet met this contract and is rejected.
  The coordinator does not commit a source's live/prefetched cursor to Kafka.
- Operator code, key functions, schemas, source identity/content, window options,
  and sink configuration must remain compatible. The topology check cannot detect
  arbitrary closure or schema changes. Custom operators must put all durable data
  in managed state and use `snapshot`/`open` for discovery metadata. Reserved
  `__rhei_*` state keys belong to the runtime and built-in operators. State TTL
  timestamps are not persistent; TTL-dependent recovery is unsupported.
- `Sink::flush()` must make preceding writes durable. Delivery is at-least-once:
  output written before an aborted checkpoint may be replayed. Non-idempotent
  external side effects inside operators are outside this guarantee.
- Propagated operator, state, sink and callback errors fail recovery, regardless
  of the legacy Skip policy. Durable DLQ coordination is not implemented, so
  SendToDlq is rejected. Synchronous callbacks must not block indefinitely.

## Alternatives considered

- Fix offsets alone: does not bind cursors to state or durable output.
- Snapshot live SlateDB roots: includes uncommitted writes on restart.
- Aligned in-band barriers: attractive for throughput, but substantially more
  channel-state machinery than a quiescent fixed-topology first implementation.
- Object-store-only metadata with conditional publication: desirable, deferred;
  state may be remote now, but publication still requires the shared filesystem.
- Transactional sinks/exactly-once or live rescaling: deferred until the basic
  durable cut and whole-job recovery are established.

## Consequences

Recovery has one authoritative decision and fail-closed compatibility checks.
Tests cover failed/cancelled persistence, timers, open windows, prefetched input,
missing snapshots, stale acknowledgements, lost commit responses, fencing, and
killing a participant during sink flush followed by whole-cluster restart.

Checkpointing pauses admission and adds durable I/O. Callbacks and protocol epochs
have a fixed 60-second deadline; timeouts abort, they do not publish partial work.
Snapshots/attempts and pinned SlateDB checkpoints accumulate: garbage collection,
retention policy, tunable deadlines, automatic job restart, Kafka replay support,
schema migration, object-store-only publication, and rescaling remain follow-ups.
The legacy state-explorer endpoints do not yet inspect coordinated snapshots.

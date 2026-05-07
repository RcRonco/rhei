---
name: Checkpoint Design
description: Checkpoint coordination protocol, manifest format, flush ordering, edge cases
type: project
---

## Checkpoint Trigger Path
1. `DataflowExecutor::build_operator` creates `TimelyErasedOperator` per operator node
2. On each frontier advance, `try_checkpoint()` is called
3. `TimelyErasedOperator::maybe_checkpoint()` checks `min_frontier > last_checkpoint_epoch`
4. If yes, calls `rt.block_on(ctx.checkpoint())` (blocks Timely worker thread)
5. Only the last operator on the first local worker sends epoch to checkpoint_notify channel

## Flush Ordering
1. `StateContext::checkpoint()` calls `memtable.flush()` -> returns dirty entries
2. Each dirty entry applied to backend one-by-one (put or delete)
3. Timer state serialized and persisted if dirty
4. `backend.checkpoint()` called (no-op for SlateDB)

## Cross-Process Coordination
- Process 0 runs `CheckpointCoordinator` (TCP listener)
- Other processes run `CheckpointParticipant` (TCP client)
- Protocol: Ready(process_id, epoch) -> collect all N -> Committed(max_epoch)
- Process 0 uses in-memory channels instead of TCP

## Manifest
- Atomic write: temp file + rename
- Contains: version, checkpoint_id, timestamp_ms, operators, source_offsets
- Partial manifests for cluster mode: per-process files merged by process 0

## Known Gaps
- Checkpoint flushes dirty entries one-by-one (no batch put API on StateBackend)
- Every operator checkpoints independently (no cross-operator coordination within a process)
- SlateDB checkpoint() is no-op, so durability depends on SlateDB's internal WAL flush timing

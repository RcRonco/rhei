---
name: State Architecture
description: L1/L2/L3 tier layout, key files, invariants, and design patterns in rhei state management
type: project
---

## Key Files
- `rhei-core/src/state/backend.rs` — `StateBackend` trait (get/put/delete/checkpoint, all async)
- `rhei-core/src/state/memtable.rs` — L1: moka-backed clean cache + HashMap dirty map
- `rhei-core/src/state/tiered_backend.rs` — L2 Foyer HybridCache + L3 SlateDB composition
- `rhei-core/src/state/slatedb_backend.rs` — L3: SlateDB wrapper, checkpoint is no-op
- `rhei-core/src/state/context.rs` — StateContext: memtable + backend, bincode serde, dirty flush
- `rhei-core/src/state/prefixed_backend.rs` — Key namespacing: `{prefix}/` prepended
- `rhei-core/src/state/fork_backend.rs` — Copy-on-write for fork-from-checkpoint mode
- `rhei-core/src/state/local_backend.rs` — JSON file backend for dev/test
- `rhei-core/src/checkpoint.rs` — CheckpointManifest: atomic save via temp+rename
- `rhei-runtime/src/checkpoint_coord.rs` — Cross-process TCP checkpoint coordination
- `rhei-runtime/src/timely_operator.rs` — TimelyErasedOperator: frontier-triggered checkpoint
- `rhei-runtime/src/async_operator.rs` — Hot/cold path: sync poll then block_in_place
- `rhei-runtime/src/executor.rs` — DataflowExecutor: source/transform/operator/sink builders
- `rhei-runtime/src/controller.rs` — PipelineController: context creation, backend selection
- `tla/RheiCore.tla` — TLA+ spec covering checkpoint, state hierarchy, watermark, crash/recovery

## Invariants Discovered
1. Dirty entries are never evicted from L1 (HashMap, not moka)
2. Clean entries use moka W-TinyLFU with configurable byte/entry limits
3. Write path is sync (memtable only), read path is async (memtable then backend)
4. Checkpoint flushes dirty to backend sequentially (not batched), then calls backend.checkpoint()
5. PrefixedBackend uses `/` separator - collision-free if operator names don't contain `/`
6. TieredBackend is write-through: L3 first, then L2 insert
7. Checkpoint triggers when frontier min > last_checkpoint_epoch AND no pending work
8. Only the "last operator" on the "first local worker" sends checkpoint notifications

## Design Patterns
- StateContext is NOT shared between operators - each operator gets its own
- MemTable separates dirty (HashMap) from clean (moka Cache) for eviction safety
- ForkBackend uses in-memory tombstone set to prevent remote fallthrough after delete
- SlateDB checkpoint is no-op (WAL is already durable)

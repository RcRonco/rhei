---
name: Known Issues
description: Correctness risks and performance concerns found during initial state management review
type: project
---

## Correctness Concerns

### C1: Non-atomic dirty flush in StateContext::checkpoint()
Dirty entries are flushed one-by-one. If the process crashes mid-flush, some keys are persisted and others are not. The checkpoint is reported as successful only if all writes complete, but a crash between writes creates partial state. SlateDB WAL may mitigate if batched internally, but the current code issues individual puts.

### C2: Checkpoint fires for EVERY operator independently
Each operator has its own `maybe_checkpoint` call. If operator A checkpoints but operator B crashes before its checkpoint, state is inconsistent across operators. Only the last operator's checkpoint notification is sent.

### C3: keys_with_prefix only scans L1 memtable
`StateContext::keys_with_prefix()` explicitly documents it does NOT scan L2/L3. This means keys that were checkpointed and evicted from L1 are invisible to prefix scans. This affects MapState iteration and timer cleanup.

### C4: ForkBackend tombstone set unbounded
Tombstones in ForkBackend accumulate in memory with no eviction. Long-running fork sessions with many deletes will grow memory usage without bound.

### C5: PrefixedBackend prefix collision if operator name contains '/'
The prefix format is `{name}/`. If operator names contain '/', keys from different operators could alias.

## Performance Concerns

### P1: Individual key flushes during checkpoint
No batch API on StateBackend. Each dirty key results in a separate put/delete call to TieredBackend, which is write-through to L3 (SlateDB). For high-cardinality state, this means N sequential S3-bound writes per checkpoint.

### P2: key.to_vec() allocation on every L2 cache lookup
`TieredBackend::get()` allocates a `Vec<u8>` from the key slice on every call, even for L2 hits.

### P3: Bincode serialization on every typed get/put
`StateContext::get<V>` and `put<V>` use bincode for every access. For hot-path L1 hits, the serialization overhead may dominate.

### P4: ListState reads entire list on every append
`ListState::append()` deserializes the full list, appends one element, re-serializes the entire list. O(n) per append.

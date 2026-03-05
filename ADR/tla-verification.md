# ADR: TLA+ Verification of Checkpoint, State, Frontier & Watermark

**Status:** Accepted
**Date:** 2026-03-05

## Context

Rhei has a complex interaction between four subsystems: the L1/L2/L3 state hierarchy, Timely frontier tracking, out-of-band watermark propagation, and cross-process checkpoint coordination. Bugs in these interactions -- dirty state loss on crash, watermark racing ahead of data, partial commits, epoch coalescing errors -- could violate at-least-once semantics. Manual reasoning about all interleavings is error-prone.

Formal verification with TLA+ and TLC exhaustively checks safety and liveness invariants on a finite model, covering every reachable state.

## Decision

Verify Rhei's core invariants using two TLA+ modules checked by the TLC model checker.

### Module structure

```
tla/
  RheiCore.tla            -- Single-process: state hierarchy + frontier + watermark + checkpoint
  RheiCore.cfg            -- TLC config
  RheiCoordination.tla    -- Multi-process: Ready/Committed protocol + cross-process consistency
  RheiCoordination.cfg    -- TLC config
```

### Diagram

```mermaid
graph TB
    subgraph "RheiCore.tla — Single Process"
        SE[SourceEmit] --> F[frontier / pending]
        P[Put] --> D[dirty]
        D --> FC[FlushAndCommit]
        FC --> B[backend]
        FC --> C[clean]
        SE --> WM[sourceWatermark]
        WM --> GW[globalWatermark]
        GW --> OW[lastOperatorWm]
        F --> BC[BeginCheckpoint]
        BC --> FC
        CR[Crash] --> REC[Recover]
        REC -->|manifestOffset + 1| SE
    end

    subgraph "RheiCoordination.tla — Multi Process"
        P0[Process 0] -->|Ready| COORD[Coordinator]
        P1[Process 1] -->|Ready| COORD
        COORD -->|Committed| P0
        COORD -->|Committed| P1
        PC[PipelineCrash] --> P0
        PC --> P1
    end

    subgraph "Safety Invariants"
        S3[S3: checkpoint => pending = {}]
        S5[S5: watermark <= sourceEpoch]
        S12[S12: AllFlushedAtCommit]
        S9[S9: backend survives crash]
    end

    subgraph "Liveness (RheiCore)"
        L1[checkpointing ~> running]
        L3[watermark propagates]
        L4[crashed ~> running]
    end
```

### RheiCore — Single-process model

Models the state hierarchy (dirty/clean/backend), Timely frontier as a set of retained epochs, pending as in-flight processing epochs, watermark propagation from source through global to operator, and the checkpoint lifecycle.

**Design decisions:**
- L2 (Foyer) collapsed into `clean` — the key invariant (evictable vs non-evictable) is captured by the dirty/clean split
- Frontier as set — Timely antichain on totally-ordered u64 simplifies to a set; `min(frontier)` gives the antichain minimum
- Pending as set of epochs — maps to `has_pending()` in `async_operator.rs`
- Single source — multi-source watermark (min of all) captured since invariant `wm <= epoch` holds per-source
- `FlushAndCommit` is atomic — partial flush safety relies on replay from manifest offset

**Parameters:** Keys={"k1","k2"}, MaxEpoch=3, MaxValue=2

**Results:** 220,379 states generated, 43,012 distinct states, depth 23. Completed in 9s. All invariants and temporal properties pass.

### RheiCoordination — Multi-process model

Models N processes each with local state and frontier, coordinated by a central coordinator via the Ready/Committed protocol from `checkpoint_coord.rs`.

**Design decisions:**
- Pipeline-wide crash (not per-process) — in the real system, TCP disconnect triggers full pipeline restart
- Per-process `procCommitNotify` flag instead of a shared message set — models TCP stream ordering where each process receives exactly one Committed per Ready
- All processes must be crashed before any can recover — models pipeline restart semantics
- Liveness properties omitted from config — with unrestricted crashes, infinite crash loops prevent progress; safety is the primary verification target

**Key finding during development:** An initial model using a shared `committedMsgs` set allowed stale committed messages from previous coordination rounds to leak into later rounds. A process could consume an old Committed, resume running, accumulate new dirty state, and then the coordinator would commit with dirty state present. Switching to per-process notification flags (modeling TCP stream ordering) resolved this. This validated that the TCP-based protocol design in `checkpoint_coord.rs` is correct *because* it uses ordered streams, not because the protocol is inherently safe with unordered delivery.

**Parameters:** Procs={0,1}, Keys={"k1","k2"}, MaxEpoch=3

**Results:** 14,177,141 states generated, 1,886,524 distinct states, depth 32. Completed in 16s. All safety invariants pass.

### Properties verified

| ID | Module | Type | Property |
|---|---|---|---|
| S3 | Core | Safety | Checkpoint only when pending = {} |
| S4 | Core | Safety | Checkpoint IDs non-negative and monotonic |
| S5 | Core | Safety | Watermark <= source epoch |
| S6 | Core | Safety | Global watermark monotonic (except crash) |
| S9 | Core | Structural | Backend survives crash |
| S10 | Core | Structural | Recovery replays from manifestOffset + 1 |
| S11 | Core | Structural | Checkpoint requires frontier advance |
| S12 | Coord | Safety | All processes have dirty = {} at commit time |
| S7 | Coord | Structural | Committed only when readySet = Procs |
| S13 | Coord | Structural | Epoch coalescing safe (full dirty flush, not per-epoch) |
| L1 | Core | Liveness | Checkpointing eventually completes |
| L3 | Core | Liveness | Watermark eventually propagates (unless crash) |
| L4 | Core | Liveness | Crashed process eventually recovers |
| PendingSubsetFrontier | Both | Safety | Pending epochs are always a subset of frontier |

### No counterexamples found

All invariants and temporal properties pass on the full state space. No bugs were discovered in the current protocol design.

## Alternatives considered

**1. Property-based testing (e.g., proptest):** Tests random interleavings but cannot guarantee exhaustive coverage. TLC explores every reachable state.

**2. Single monolithic model:** Combining single-process and multi-process into one module would explode the state space. Separating them keeps each tractable while still verifying the key invariants.

**3. SPIN/Promela:** TLA+ was chosen for its expressiveness with mathematical sets and functions, better fit for modeling state hierarchies and antichains.

## Consequences

**Positive:**
- Exhaustive verification of safety invariants across all interleavings of checkpoint, state, frontier, and watermark operations
- The `committedMsgs` finding validates that TCP stream ordering is load-bearing in the coordination protocol
- TLA+ specs serve as executable documentation of the system's safety contract
- Future protocol changes can be verified by modifying the specs and re-running TLC

**Negative:**
- TLA+ specs must be kept in sync with code changes to remain useful
- The models abstract away some details (partial flush, multi-source watermark, L2 Foyer) that may need separate verification if they become complex
- Liveness properties for the coordination model are weak due to unrestricted crash modeling

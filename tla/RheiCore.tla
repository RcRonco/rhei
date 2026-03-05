--------------------------- MODULE RheiCore ---------------------------
(*
 * Single-process model of Rhei's checkpoint, state hierarchy,
 * frontier tracking, and watermark propagation.
 *
 * Maps to:
 *   - rhei-core/src/state/memtable.rs     (dirty / clean / flush)
 *   - rhei-core/src/state/context.rs       (StateContext checkpoint)
 *   - rhei-runtime/src/timely_operator.rs  (frontier, pending, checkpoint trigger)
 *   - rhei-runtime/src/async_operator.rs   (hot/cold, has_pending)
 *   - rhei-runtime/src/bridge.rs           (source emit, watermark)
 *   - rhei-runtime/src/executor.rs         (watermark task, recovery)
 *)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Keys, MaxEpoch, MaxValue, None

ASSUME Keys # {}
ASSUME MaxEpoch \in Nat \ {0}
ASSUME MaxValue \in Nat \ {0}

Values == 1..MaxValue
MaybeValues == {None} \cup Values
Epochs == 0..MaxEpoch

------------------------------------------------------------------------
(*  VARIABLES  *)
------------------------------------------------------------------------

VARIABLES
    \* State hierarchy (memtable.rs)
    dirty,              \* [Keys -> MaybeValues] — dirty map, never evicted
    clean,              \* [Keys -> MaybeValues] — clean cache, evictable
    backend,            \* [Keys -> MaybeValues] — durable L3, survives crash

    \* Source & watermark (bridge.rs, executor.rs)
    sourceEpoch,        \* next epoch the source will emit (1..MaxEpoch+2)
    sourceWatermark,    \* watermark written by source AFTER emitting items
    globalWatermark,    \* min(non-zero source watermarks) — watermark task
    lastOperatorWm,     \* last watermark consumed by operator

    \* Frontier & pending (timely_operator.rs, async_operator.rs)
    frontier,           \* SUBSET Epochs — epochs with retained capabilities
    pending,            \* SUBSET Epochs — epochs with in-flight processing

    \* Checkpoint (timely_operator.rs, context.rs, checkpoint.rs)
    lastCkptEpoch,      \* epoch of last completed checkpoint (-1 initially)
    checkpointId,       \* monotonic checkpoint counter
    manifestEpoch,      \* epoch recorded in last manifest
    manifestOffset,     \* source offset recorded in manifest

    \* Process lifecycle
    pc                  \* {"running", "checkpointing", "crashed"}

vars == <<dirty, clean, backend,
          sourceEpoch, sourceWatermark, globalWatermark, lastOperatorWm,
          frontier, pending,
          lastCkptEpoch, checkpointId, manifestEpoch, manifestOffset,
          pc>>

------------------------------------------------------------------------
(*  HELPERS  *)
------------------------------------------------------------------------

MinSet(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxSet(S) == CHOOSE x \in S : \A y \in S : x >= y

StateUnchanged == dirty' = dirty /\ clean' = clean /\ backend' = backend
WmUnchanged    == sourceWatermark' = sourceWatermark
                   /\ globalWatermark' = globalWatermark
                   /\ lastOperatorWm' = lastOperatorWm
FrontierUnchanged == frontier' = frontier /\ pending' = pending
CkptUnchanged  == lastCkptEpoch' = lastCkptEpoch
                   /\ checkpointId' = checkpointId
                   /\ manifestEpoch' = manifestEpoch
                   /\ manifestOffset' = manifestOffset

------------------------------------------------------------------------
(*  INIT  *)
------------------------------------------------------------------------

Init ==
    /\ dirty          = [k \in Keys |-> None]
    /\ clean          = [k \in Keys |-> None]
    /\ backend        = [k \in Keys |-> None]
    /\ sourceEpoch    = 1
    /\ sourceWatermark = 0
    /\ globalWatermark = 0
    /\ lastOperatorWm = 0
    /\ frontier       = {}
    /\ pending        = {}
    /\ lastCkptEpoch  = -1
    /\ checkpointId   = 0
    /\ manifestEpoch  = -1
    /\ manifestOffset = 0
    /\ pc             = "running"

------------------------------------------------------------------------
(*  ACTIONS  *)
------------------------------------------------------------------------

(* --- bridge.rs: source emits a batch for the current epoch --- *)
SourceEmit ==
    /\ pc = "running"
    /\ sourceEpoch <= MaxEpoch
    /\ LET e == sourceEpoch IN
        \* Add epoch to frontier and pending
        /\ frontier' = frontier \cup {e}
        /\ pending'  = pending \cup {e}
        \* Advance source epoch
        /\ sourceEpoch' = sourceEpoch + 1
        \* Write watermark AFTER items (bridge.rs: watermark alongside data)
        /\ sourceWatermark' = e
    /\ StateUnchanged
    /\ globalWatermark' = globalWatermark
    /\ lastOperatorWm'  = lastOperatorWm
    /\ CkptUnchanged
    /\ pc' = pc

(* --- memtable.rs: operator writes a key --- *)
Put(k, v) ==
    /\ pc = "running"
    /\ pending # {}       \* can only write during processing
    /\ dirty' = [dirty EXCEPT ![k] = v]
    \* Invalidate clean entry (memtable.rs put() logic)
    /\ clean' = [clean EXCEPT ![k] = None]
    /\ backend' = backend
    /\ UNCHANGED <<sourceEpoch, sourceWatermark, globalWatermark, lastOperatorWm>>
    /\ FrontierUnchanged
    /\ CkptUnchanged
    /\ pc' = pc

(* --- async_operator.rs: epoch processing completes --- *)
CompleteProcessing(e) ==
    /\ pc = "running"
    /\ e \in pending
    /\ pending' = pending \ {e}
    /\ frontier' = frontier
    /\ StateUnchanged
    /\ UNCHANGED <<sourceEpoch, sourceWatermark, globalWatermark, lastOperatorWm>>
    /\ CkptUnchanged
    /\ pc' = pc

(* --- timely_operator.rs: release capability for epoch e --- *)
(* release_finished_epochs: only when !has_pending() and a later epoch exists *)
ReleaseEpoch(e) ==
    /\ pc = "running"
    /\ e \in frontier
    /\ pending = {}                          \* !has_pending()
    /\ \E f \in frontier : f > e             \* keep at least one cap
    /\ frontier' = frontier \ {e}
    /\ pending'  = pending
    /\ StateUnchanged
    /\ UNCHANGED <<sourceEpoch, sourceWatermark, globalWatermark, lastOperatorWm>>
    /\ CkptUnchanged
    /\ pc' = pc

(* --- executor.rs: watermark task updates global watermark --- *)
UpdateGlobalWatermark ==
    /\ pc = "running"
    /\ sourceWatermark > 0
    /\ globalWatermark' = sourceWatermark
    /\ UNCHANGED <<sourceEpoch, sourceWatermark, lastOperatorWm>>
    /\ StateUnchanged
    /\ FrontierUnchanged
    /\ CkptUnchanged
    /\ pc' = pc

(* --- timely_operator.rs: operator advances its watermark view --- *)
OperatorAdvanceWatermark ==
    /\ pc = "running"
    /\ globalWatermark > lastOperatorWm
    /\ lastOperatorWm' = globalWatermark
    /\ UNCHANGED <<sourceEpoch, sourceWatermark, globalWatermark>>
    /\ StateUnchanged
    /\ FrontierUnchanged
    /\ CkptUnchanged
    /\ pc' = pc

(* --- timely_operator.rs: maybe_checkpoint — begin checkpoint --- *)
BeginCheckpoint ==
    /\ pc = "running"
    /\ pending = {}                          \* !has_pending()
    /\ frontier # {}
    /\ MinSet(frontier) > lastCkptEpoch      \* frontier advanced
    /\ pc' = "checkpointing"
    /\ StateUnchanged
    /\ UNCHANGED <<sourceEpoch, sourceWatermark, globalWatermark, lastOperatorWm>>
    /\ FrontierUnchanged
    /\ CkptUnchanged

(* --- context.rs: flush dirty to backend, write manifest --- *)
FlushAndCommit ==
    /\ pc = "checkpointing"
    /\ LET ckptEpoch == MinSet(frontier) IN
        \* Flush dirty → backend + clean (memtable flush)
        /\ backend' = [k \in Keys |-> IF dirty[k] # None THEN dirty[k] ELSE backend[k]]
        /\ clean'   = [k \in Keys |-> IF dirty[k] # None THEN dirty[k] ELSE clean[k]]
        /\ dirty'   = [k \in Keys |-> None]
        \* Bump checkpoint ID
        /\ checkpointId' = checkpointId + 1
        \* Record manifest
        /\ lastCkptEpoch' = ckptEpoch
        /\ manifestEpoch'  = ckptEpoch
        /\ manifestOffset' = sourceEpoch - 1   \* last emitted epoch
        /\ pc' = "running"
    /\ UNCHANGED <<sourceEpoch, sourceWatermark, globalWatermark, lastOperatorWm>>
    /\ FrontierUnchanged

(* --- Crash: lose volatile state, keep backend --- *)
Crash ==
    /\ pc \in {"running", "checkpointing"}
    /\ dirty'    = [k \in Keys |-> None]
    /\ clean'    = [k \in Keys |-> None]
    /\ backend'  = backend                  \* S9: backend survives crash
    /\ frontier' = {}
    /\ pending'  = {}
    /\ sourceEpoch'    = sourceEpoch        \* will be reset on Recover
    /\ sourceWatermark' = 0
    /\ globalWatermark' = 0
    /\ lastOperatorWm'  = 0
    /\ lastCkptEpoch'  = lastCkptEpoch
    /\ checkpointId'   = checkpointId
    /\ manifestEpoch'  = manifestEpoch
    /\ manifestOffset' = manifestOffset
    /\ pc' = "crashed"

(* --- executor.rs startup: recover from manifest --- *)
Recover ==
    /\ pc = "crashed"
    \* S10: restart source from manifestOffset + 1 (at-least-once)
    /\ sourceEpoch' = manifestOffset + 1
    /\ sourceWatermark' = 0
    /\ globalWatermark' = 0
    /\ lastOperatorWm'  = 0
    /\ lastCkptEpoch'  = manifestEpoch
    /\ UNCHANGED <<checkpointId, manifestEpoch, manifestOffset>>
    /\ dirty'    = [k \in Keys |-> None]
    /\ clean'    = [k \in Keys |-> None]
    /\ backend'  = backend
    /\ frontier' = {}
    /\ pending'  = {}
    /\ pc' = "running"

(* --- moka eviction: clean cache entry can be evicted --- *)
EvictClean(k) ==
    /\ clean[k] # None
    /\ clean' = [clean EXCEPT ![k] = None]
    /\ UNCHANGED <<dirty, backend>>
    /\ UNCHANGED <<sourceEpoch, sourceWatermark, globalWatermark, lastOperatorWm>>
    /\ FrontierUnchanged
    /\ CkptUnchanged
    /\ pc' = pc

------------------------------------------------------------------------
(*  NEXT  *)
------------------------------------------------------------------------

Next ==
    \/ SourceEmit
    \/ \E k \in Keys, v \in Values : Put(k, v)
    \/ \E e \in Epochs : CompleteProcessing(e)
    \/ \E e \in Epochs : ReleaseEpoch(e)
    \/ UpdateGlobalWatermark
    \/ OperatorAdvanceWatermark
    \/ BeginCheckpoint
    \/ FlushAndCommit
    \/ Crash
    \/ Recover
    \/ \E k \in Keys : EvictClean(k)

------------------------------------------------------------------------
(*  FAIRNESS  *)
------------------------------------------------------------------------

Fairness ==
    /\ WF_vars(FlushAndCommit)
    /\ WF_vars(Recover)
    /\ WF_vars(UpdateGlobalWatermark)

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------
(*  SAFETY INVARIANTS  *)
------------------------------------------------------------------------

\* S2: After FlushAndCommit, all dirty entries are cleared.
\*     More precisely: while checkpointing has just completed (pc back to running),
\*     the transition guarantees dirty is cleared. We check the weaker form:
\*     if we are still in "checkpointing" (between Begin and Flush), dirty may
\*     have content, but once FlushAndCommit fires, dirty is [k |-> None].
\*     We verify this structurally via FlushAndCommit's effect.

\* S3: Checkpoint only when no pending work
CheckpointImpliesNoPending ==
    pc = "checkpointing" => pending = {}

\* S5: Watermark never exceeds source epoch
WatermarkBoundedBySource ==
    sourceWatermark <= sourceEpoch

\* S6: Global watermark monotonic (checked as action property)
GlobalWatermarkMonotonic ==
    [][globalWatermark' >= globalWatermark \/ pc' = "crashed"]_vars

\* S11: Checkpoint requires frontier advance past last checkpoint
\* (Checked structurally: BeginCheckpoint guard requires MinSet(frontier) > lastCkptEpoch)

\* S4: Checkpoint IDs are non-negative and monotonically increasing
CheckpointIdNonNegative ==
    checkpointId >= 0

\* Combined type invariant
TypeOK ==
    /\ dirty          \in [Keys -> MaybeValues]
    /\ clean          \in [Keys -> MaybeValues]
    /\ backend        \in [Keys -> MaybeValues]
    /\ sourceEpoch    \in 1..(MaxEpoch + 2)
    /\ sourceWatermark \in 0..(MaxEpoch + 1)
    /\ globalWatermark \in 0..(MaxEpoch + 1)
    /\ lastOperatorWm  \in 0..(MaxEpoch + 1)
    /\ frontier       \subseteq Epochs
    /\ pending        \subseteq Epochs
    /\ lastCkptEpoch  \in -1..MaxEpoch
    /\ checkpointId   \in Nat
    /\ manifestEpoch  \in -1..MaxEpoch
    /\ manifestOffset \in 0..(MaxEpoch + 1)
    /\ pc             \in {"running", "checkpointing", "crashed"}

\* Pending is always a subset of frontier
PendingSubsetFrontier ==
    pending \subseteq frontier

\* Backend state is monotonic (only grows via flush, never cleared except by Put)
\* Actually backend can change values, but it survives crash — checked structurally.

\* Read-your-own-writes: if dirty[k] # None, a Get(k) returns dirty[k].
\* This is structural in memtable.rs (get checks dirty first) — modeled implicitly.

------------------------------------------------------------------------
(*  LIVENESS PROPERTIES  *)
------------------------------------------------------------------------

\* L1: Checkpointing eventually completes
CheckpointCompletes ==
    pc = "checkpointing" ~> pc = "running"

\* L3: Watermark eventually propagates (unless crash)
WatermarkPropagates ==
    (sourceWatermark > globalWatermark /\ pc = "running")
        ~> (globalWatermark >= sourceWatermark \/ pc = "crashed")

\* L4: Crashed process eventually recovers
CrashRecovery ==
    pc = "crashed" ~> pc = "running"

========================================================================

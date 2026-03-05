------------------------ MODULE RheiCoordination ------------------------
(*
 * Multi-process checkpoint coordination model for Rhei.
 *
 * Maps to:
 *   - rhei-runtime/src/checkpoint_coord.rs  (Ready/Committed protocol)
 *   - rhei-runtime/src/executor.rs          (per-process checkpoint)
 *   - rhei-core/src/state/context.rs        (flush)
 *   - rhei-core/src/checkpoint.rs           (manifest merge)
 *
 * Models N processes, each with local state + frontier, coordinated
 * by a central coordinator that collects Ready messages and broadcasts
 * Committed once all processes have flushed.
 *
 * Key modeling decision: the real system uses ordered TCP streams, so
 * each process receives exactly one Committed per Ready it sent. We
 * model this with a per-process `procCommitNotify` flag that the
 * coordinator sets atomically in CoordCommit and each process clears
 * in ProcReceiveCommitted. This prevents stale committed messages
 * from previous rounds leaking into later rounds.
 *)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Procs, MaxEpoch, Keys, None

ASSUME Procs # {}
ASSUME MaxEpoch \in Nat \ {0}
ASSUME Keys # {}

Values == 1..2           \* small value domain for tractability
MaybeValues == {None} \cup Values
Epochs == 0..MaxEpoch

------------------------------------------------------------------------
(*  VARIABLES  *)
------------------------------------------------------------------------

VARIABLES
    \* Per-process state
    procState,          \* [Procs -> {"running","flushed","crashed"}]
    procFrontier,       \* [Procs -> SUBSET Epochs]
    procPending,        \* [Procs -> SUBSET Epochs]
    procLastCkptEpoch,  \* [Procs -> -1..MaxEpoch]
    procDirty,          \* [Procs -> [Keys -> MaybeValues]]
    procBackend,        \* [Procs -> [Keys -> MaybeValues]]
    procSourceEpoch,    \* [Procs -> 1..MaxEpoch+2]

    \* Coordinator state (checkpoint_coord.rs)
    readySet,           \* SUBSET Procs — processes that have sent Ready
    readyEpochs,        \* [Procs -> -1..MaxEpoch] — epoch each process reported
    committedEpoch,     \* -1..MaxEpoch — last globally committed epoch

    \* Messages
    readyMsgs,          \* set of [proc: p, epoch: e] records
    procCommitNotify    \* [Procs -> BOOLEAN] — per-process committed notification

vars == <<procState, procFrontier, procPending, procLastCkptEpoch,
          procDirty, procBackend, procSourceEpoch,
          readySet, readyEpochs, committedEpoch,
          readyMsgs, procCommitNotify>>

------------------------------------------------------------------------
(*  HELPERS  *)
------------------------------------------------------------------------

MinSet(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxSet(S) == CHOOSE x \in S : \A y \in S : x >= y

AllProcsReady == readySet = Procs

------------------------------------------------------------------------
(*  INIT  *)
------------------------------------------------------------------------

Init ==
    /\ procState        = [p \in Procs |-> "running"]
    /\ procFrontier     = [p \in Procs |-> {}]
    /\ procPending      = [p \in Procs |-> {}]
    /\ procLastCkptEpoch = [p \in Procs |-> -1]
    /\ procDirty        = [p \in Procs |-> [k \in Keys |-> None]]
    /\ procBackend      = [p \in Procs |-> [k \in Keys |-> None]]
    /\ procSourceEpoch  = [p \in Procs |-> 1]
    /\ readySet         = {}
    /\ readyEpochs      = [p \in Procs |-> -1]
    /\ committedEpoch   = -1
    /\ readyMsgs        = {}
    /\ procCommitNotify = [p \in Procs |-> FALSE]

------------------------------------------------------------------------
(*  PER-PROCESS ACTIONS  *)
------------------------------------------------------------------------

(* Source emits a batch on process p *)
ProcSourceEmit(p) ==
    /\ procState[p] = "running"
    /\ procSourceEpoch[p] <= MaxEpoch
    /\ LET e == procSourceEpoch[p] IN
        /\ procFrontier'    = [procFrontier EXCEPT ![p] = @ \cup {e}]
        /\ procPending'     = [procPending EXCEPT ![p] = @ \cup {e}]
        /\ procSourceEpoch' = [procSourceEpoch EXCEPT ![p] = @ + 1]
    /\ UNCHANGED <<procState, procLastCkptEpoch, procDirty, procBackend>>
    /\ UNCHANGED <<readySet, readyEpochs, committedEpoch, readyMsgs, procCommitNotify>>

(* Operator writes state on process p *)
ProcPut(p, k, v) ==
    /\ procState[p] = "running"
    /\ procPending[p] # {}
    /\ procDirty' = [procDirty EXCEPT ![p] = [@ EXCEPT ![k] = v]]
    /\ UNCHANGED <<procState, procFrontier, procPending, procLastCkptEpoch,
                   procBackend, procSourceEpoch>>
    /\ UNCHANGED <<readySet, readyEpochs, committedEpoch, readyMsgs, procCommitNotify>>

(* Processing completes for epoch e on process p *)
ProcCompleteProcessing(p, e) ==
    /\ procState[p] = "running"
    /\ e \in procPending[p]
    /\ procPending' = [procPending EXCEPT ![p] = @ \ {e}]
    /\ UNCHANGED <<procState, procFrontier, procLastCkptEpoch,
                   procDirty, procBackend, procSourceEpoch>>
    /\ UNCHANGED <<readySet, readyEpochs, committedEpoch, readyMsgs, procCommitNotify>>

(* Release epoch capability on process p *)
ProcReleaseEpoch(p, e) ==
    /\ procState[p] = "running"
    /\ e \in procFrontier[p]
    /\ procPending[p] = {}
    /\ \E f \in procFrontier[p] : f > e
    /\ procFrontier' = [procFrontier EXCEPT ![p] = @ \ {e}]
    /\ UNCHANGED <<procState, procPending, procLastCkptEpoch,
                   procDirty, procBackend, procSourceEpoch>>
    /\ UNCHANGED <<readySet, readyEpochs, committedEpoch, readyMsgs, procCommitNotify>>

(* Flush dirty state and send Ready to coordinator *)
(* checkpoint_coord.rs: participant sends Ready after local flush *)
ProcCheckpointAndSendReady(p) ==
    /\ procState[p] = "running"
    /\ procPending[p] = {}
    /\ procFrontier[p] # {}
    /\ MinSet(procFrontier[p]) > procLastCkptEpoch[p]
    /\ LET ckptEpoch == MinSet(procFrontier[p]) IN
        \* Flush dirty -> backend
        /\ procBackend' = [procBackend EXCEPT ![p] =
            [k \in Keys |-> IF procDirty[p][k] # None
                            THEN procDirty[p][k]
                            ELSE procBackend[p][k]]]
        /\ procDirty' = [procDirty EXCEPT ![p] = [k \in Keys |-> None]]
        /\ procLastCkptEpoch' = [procLastCkptEpoch EXCEPT ![p] = ckptEpoch]
        \* Send Ready message
        /\ readyMsgs' = readyMsgs \cup {[proc |-> p, epoch |-> ckptEpoch]}
        /\ procState' = [procState EXCEPT ![p] = "flushed"]
    /\ UNCHANGED <<procFrontier, procPending, procSourceEpoch>>
    /\ UNCHANGED <<readySet, readyEpochs, committedEpoch, procCommitNotify>>

------------------------------------------------------------------------
(*  COORDINATOR ACTIONS (checkpoint_coord.rs)  *)
------------------------------------------------------------------------

(* Coordinator receives a Ready message *)
CoordReceiveReady ==
    /\ readyMsgs # {}
    /\ LET msg == CHOOSE m \in readyMsgs : TRUE IN
        /\ readySet'    = readySet \cup {msg.proc}
        /\ readyEpochs' = [readyEpochs EXCEPT ![msg.proc] = msg.epoch]
        /\ readyMsgs'   = readyMsgs \ {msg}
    /\ UNCHANGED <<procState, procFrontier, procPending, procLastCkptEpoch,
                   procDirty, procBackend, procSourceEpoch>>
    /\ UNCHANGED <<committedEpoch, procCommitNotify>>

(* Coordinator commits when all processes are ready *)
(* checkpoint_coord.rs:151 -- committed_epoch = max(readyEpochs) *)
CoordCommit ==
    /\ AllProcsReady
    /\ LET maxEp == MaxSet({readyEpochs[p] : p \in Procs}) IN
        /\ committedEpoch' = maxEp
        \* Notify all processes (models TCP broadcast)
        /\ procCommitNotify' = [p \in Procs |-> TRUE]
        /\ readySet'       = {}
        /\ readyEpochs'    = [p \in Procs |-> -1]
    /\ UNCHANGED <<procState, procFrontier, procPending, procLastCkptEpoch,
                   procDirty, procBackend, procSourceEpoch>>
    /\ UNCHANGED <<readyMsgs>>

(* Process receives Committed and resumes running *)
ProcReceiveCommitted(p) ==
    /\ procState[p] = "flushed"
    /\ procCommitNotify[p] = TRUE
    /\ procState' = [procState EXCEPT ![p] = "running"]
    /\ procCommitNotify' = [procCommitNotify EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<procFrontier, procPending, procLastCkptEpoch,
                   procDirty, procBackend, procSourceEpoch>>
    /\ UNCHANGED <<readySet, readyEpochs, committedEpoch, readyMsgs>>

------------------------------------------------------------------------
(*  CRASH & RECOVERY  *)
------------------------------------------------------------------------

(* Pipeline-wide crash: in the real system, if any process crashes, the
   TCP coordinator detects the disconnect and the entire pipeline restarts.
   All processes transition to crashed, losing volatile state but keeping backend. *)
PipelineCrash ==
    /\ \E p \in Procs : procState[p] \in {"running", "flushed"}
    /\ procState'    = [p \in Procs |-> "crashed"]
    /\ procDirty'    = [p \in Procs |-> [k \in Keys |-> None]]
    /\ procFrontier' = [p \in Procs |-> {}]
    /\ procPending'  = [p \in Procs |-> {}]
    /\ procBackend'  = procBackend     \* backend survives crash
    /\ UNCHANGED <<procLastCkptEpoch, procSourceEpoch>>
    \* Clear all coordinator state
    /\ readySet'        = {}
    /\ readyEpochs'     = [p \in Procs |-> -1]
    /\ readyMsgs'       = {}
    /\ procCommitNotify' = [p \in Procs |-> FALSE]
    /\ UNCHANGED <<committedEpoch>>

ProcRecover(p) ==
    /\ procState[p] = "crashed"
    \* All processes must be crashed before any can recover (pipeline restart)
    /\ \A q \in Procs : procState[q] = "crashed"
    /\ procState'       = [procState EXCEPT ![p] = "running"]
    /\ procSourceEpoch' = [procSourceEpoch EXCEPT ![p] = procLastCkptEpoch[p] + 2]
    /\ procDirty'       = [procDirty EXCEPT ![p] = [k \in Keys |-> None]]
    /\ procFrontier'    = [procFrontier EXCEPT ![p] = {}]
    /\ procPending'     = [procPending EXCEPT ![p] = {}]
    /\ UNCHANGED <<procLastCkptEpoch, procBackend>>
    /\ UNCHANGED <<readySet, readyEpochs, committedEpoch, readyMsgs, procCommitNotify>>

------------------------------------------------------------------------
(*  NEXT  *)
------------------------------------------------------------------------

Next ==
    \/ \E p \in Procs : ProcSourceEmit(p)
    \/ \E p \in Procs, k \in Keys, v \in Values : ProcPut(p, k, v)
    \/ \E p \in Procs, e \in Epochs : ProcCompleteProcessing(p, e)
    \/ \E p \in Procs, e \in Epochs : ProcReleaseEpoch(p, e)
    \/ \E p \in Procs : ProcCheckpointAndSendReady(p)
    \/ CoordReceiveReady
    \/ CoordCommit
    \/ \E p \in Procs : ProcReceiveCommitted(p)
    \/ PipelineCrash
    \/ \E p \in Procs : ProcRecover(p)

------------------------------------------------------------------------
(*  FAIRNESS  *)
------------------------------------------------------------------------

Fairness ==
    /\ WF_vars(CoordReceiveReady)
    /\ WF_vars(CoordCommit)
    /\ \A p \in Procs : WF_vars(ProcReceiveCommitted(p))
    /\ \A p \in Procs : WF_vars(ProcRecover(p))
    /\ \A p \in Procs, e \in Epochs : WF_vars(ProcCompleteProcessing(p, e))
    /\ \A p \in Procs : WF_vars(ProcCheckpointAndSendReady(p))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------
(*  SAFETY INVARIANTS  *)
------------------------------------------------------------------------

\* Type invariant
TypeOK ==
    /\ procState        \in [Procs -> {"running", "flushed", "crashed"}]
    /\ procFrontier     \in [Procs -> SUBSET Epochs]
    /\ procPending      \in [Procs -> SUBSET Epochs]
    /\ procLastCkptEpoch \in [Procs -> -1..MaxEpoch]
    /\ procDirty        \in [Procs -> [Keys -> MaybeValues]]
    /\ procBackend      \in [Procs -> [Keys -> MaybeValues]]
    /\ procSourceEpoch  \in [Procs -> 1..(MaxEpoch + 2)]
    /\ readySet         \subseteq Procs
    /\ committedEpoch   \in -1..MaxEpoch
    /\ procCommitNotify \in [Procs -> BOOLEAN]

\* S7: Committed only when all processes have reported Ready
\* (Structural: CoordCommit guard requires AllProcsReady)

\* S7b: No process resumes from "flushed" until coordinator broadcasts Committed
NoResumeWithoutCommit ==
    \A p \in Procs :
        (procState[p] = "flushed" /\ procCommitNotify[p] = FALSE) =>
            (p \in readySet \/ readyMsgs # {} \/
             \E q \in Procs : procState[q] = "crashed")

\* S12: At commit time, all processes have dirty = {} (all flushed)
\* The coordinator only commits after all processes have sent Ready,
\* and sending Ready requires flushing all dirty state first.
AllFlushedAtCommit ==
    AllProcsReady =>
        \A p \in Procs : procDirty[p] = [k \in Keys |-> None]

\* S13: Epoch coalescing is safe -- each process flushes ALL dirty state
\* regardless of which epoch it reports. Structural: ProcCheckpointAndSendReady
\* flushes the entire dirty map, not filtered by epoch.

\* Per-process pending subset of frontier
ProcPendingSubsetFrontier ==
    \A p \in Procs : procPending[p] \subseteq procFrontier[p]

\* Backend survives crash (structural -- PipelineCrash doesn't modify procBackend)

------------------------------------------------------------------------
(*  LIVENESS  *)
------------------------------------------------------------------------

\* Flushed processes eventually resume or crash (pipeline restart)
\* Note: unconditional liveness (flushed ~> running) is too strong because
\* infinite crash loops can prevent progress. We verify the weaker property:
\* a flushed process never stays flushed forever -- it either resumes or crashes.
FlushedEventuallyResolves ==
    \A p \in Procs :
        procState[p] = "flushed"
            ~> (procState[p] = "running" \/ procState[p] = "crashed")

\* Crashed processes eventually recover (under fair scheduling)
CrashedEventuallyRecovers ==
    \A p \in Procs :
        procState[p] = "crashed" ~> procState[p] = "running"

========================================================================

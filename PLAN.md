# 📦 RustStream Phase 1: Local-First Engine Backlog
## 🟢 Core System
### Issue A-1: Initialize Workspace and Define Core Traits ✅
Priority: 🔴 Critical (Blocker)

#### Description
Initialize the Cargo workspace and define the fundamental traits that allow User Logic, the Runtime, and State to interact. This is the contract all other workers will build against.

#### Tasks
- [x] Create Cargo workspace with members: rs-core, rs-runtime, rs-state, rs-cli.
- [x] Define StateContext struct in rs-core.
- [x]  Define StreamFunction trait in rs-core using async_trait.
- [x]  Define StateBackend trait in rs-state.

#### Acceptance Criteria
- [x]  StateContext is decoupled from the specific backend implementation (uses Box<dyn StateBackend>).
- [x]  The following code compiles:
```rust
struct MyOp;
#[async_trait]
impl StreamFunction for MyOp {
    type Input = String;
    type Output = String;
    async fn process(&mut self, input: String, ctx: &mut StateContext) -> Vec<String> {
        let _ = ctx.get(b"key").await;
        vec![input]
    }
}
```

#### Issue A-2: Implement StreamGraph DSL (Builder API) ✅
Priority: 🟠 High

#### Description
Create the fluent API that allows users to define a logical dataflow graph. This does not execute the graph yet; it just builds the logical plan.

#### Tasks
- [x]  Create StreamGraph struct.
- [x]  Implement .source(Source) method.
- [x]  Implement .map(StreamFunction), .filter(), and .key_by() methods.
- [x]  Implement .sink(Sink) method.
- [x]  Store the graph as an adjacency list or similar structure (LogicalPlan).

#### Acceptance Criteria
- [x]  User can write: let graph = StreamGraph::new().source(src).map(op).sink(snk);
- [x]  The graph object contains the correct nodes and edges in memory.

## ⚙️ Runtime & Execution (Worker B - Runtime Engineer)
### Issue B-1: Async Operator Wrapper (The "Shell") ✅
Priority: 🔴 Critical

#### Description
Timely Dataflow operators are synchronous. We need a generic wrapper operator that can execute async closures by spawning them onto a Tokio runtime without blocking the main worker thread.

#### Tasks
- [x]  Create AsyncOperator struct (standalone, not yet wrapping a Timely scope).
- [x]  Accept a tokio::runtime::Handle.
- [x]  Implement the timely::operator::Operator trait (integrated via TimelyAsyncOperator).
- [x]  Logic to check for Future completion non-blockingly.

#### Acceptance Criteria
- [x]  Unit test: An operator that sleeps for 10ms (async) does NOT block the thread (verified by a heartbeat or second operator running concurrently).
- [x]  AsyncOperator is integrated into Timely dataflow as a real operator.

#### Note
AsyncOperator accepts an optional `tokio::runtime::Handle`. On the hot path (L1 cache hit), futures resolve synchronously. On the cold path (state miss), the runtime handle drives futures to completion via `block_in_place`. Timely integration is provided by `TimelyAsyncOperator`.

### Issue B-2: Event Stashing & Retry Logic ✅
Priority: 🟠 High

#### Description
When the AsyncOperator hits a state miss (S3 fetch), it must "stash" the incoming event and its capability. It must retry processing this event once the future completes.

#### Tasks
- [x]  Implement a Stash struct (likely VecDeque<(Capability, Data)>).
- [x]  Implement the polling loop: while let Some(res) = futures.poll() { unstash() }.
- [x]  Ensure Capabilities are retained while stashed and dropped only after successful processing.

#### Acceptance Criteria
- [x]  Ordering is preserved (Event A arrives before B; if A fetches state, B waits or is stashed behind A).
- [x]  No capabilities are dropped prematurely (which would cause downstream logic to close windows too early).

### Issue B-3: Timely Dataflow Integration ⚠️ Partial
Priority: 🔴 Critical

#### Description
Replace the sequential `Executor::run_linear()` loop with a real Timely dataflow graph. Wire `AsyncOperator` as a Timely operator, use Timely progress tracking (capabilities/frontiers), and enable multi-worker parallelism.

#### Tasks
- [ ]  Create a Timely worker that materializes a LogicalPlan into Timely operators.
- [x]  Implement an async-bridging Timely operator using AsyncOperator + Stash.
- [x]  Wire Source/Sink traits as Timely input/output operators.
- [x]  Use Timely capabilities for watermark/progress tracking.
- [ ]  Support multi-worker execution.

#### Acceptance Criteria
- [x]  Word-count example runs on Timely with correct results.
- [ ]  Multi-worker execution produces the same results as single-worker.
- [x]  Checkpoint barriers integrate with Timely progress tracking.

## 💾 State & Storage
### Issue C-1: Local File Backend Implementation ✅
Priority: 🟠 High

#### Description
Implement a simple, local-disk version of the StateBackend trait. This allows us to test statefulness without needing S3 or SlateDB yet.

#### Tasks
- [x]  Implement LocalBackend struct.
- [x]  put(): Writes to an in-memory HashMap.
- [x]  checkpoint(): Serializes the HashMap to a file (JSON/Bincode) on disk.
- [x]  fetch(): Reads from the HashMap.
- [x]  Crucial: Add a generic latency_ms configuration to simulate S3 slowness during tests.

#### Acceptance Criteria
- [x]  Data persists across restarts (if loaded from disk).
- [x]  fetch can be configured to "sleep" for 50ms to test the Async Operator's non-blocking behavior.

### Issue C-2: L1 MemTable & Dirty Tracking ✅
Priority: 🟡 Medium

#### Description
The StateContext needs a buffering layer. Writes should be instant (RAM), and only flushed to the Backend during a checkpoint.

#### Tasks
- [x]  Implement MemTable struct (wraps a HashMap).
- [x]  Track dirty_keys (keys modified since last flush).
- [x]  Implement flush(): returns the dirty key-values and clears the flag.

#### Acceptance Criteria
- [x]  ctx.put() is synchronous and fast.
- [x]  ctx.get() returns the value from MemTable if it exists (Read-Your-Own-Writes).

### Issue C-3: L2/L3 Storage Tiers (SlateDB + Foyer) ✅
Priority: 🟠 High

#### Description
Implement the full 3-tier storage hierarchy: L1 MemTable (done) → L2 Foyer HybridCache (disk/memory) → L3 SlateDB (object storage).

#### Tasks
- [x]  Add workspace dependencies: foyer, slatedb, object_store, bytes, tracing, metrics, etc.
- [x]  Implement SlateDbBackend (L3): StateBackend wrapping slatedb::Db.
- [x]  Implement TieredBackend (L2+L3): Foyer HybridCache in front of SlateDB.
- [x]  Implement PrefixedBackend: Key namespace isolation per operator.
- [x]  Add observability: tracing + metrics instrumentation in StateContext, executor, and tiered backend.
- [x]  Create telemetry module: tracing-subscriber + Prometheus exporter.
- [x]  Integrate tiered storage in Executor with `with_tiered_storage()` builder.
- [x]  Add `--json-logs` and `--log-level` CLI flags.

#### Acceptance Criteria
- [x]  Read path: StateContext.get() → L1 MemTable → TieredBackend.get() → L2 Foyer → L3 SlateDB → backfill L2.
- [x]  Write path: StateContext.put() → L1 MemTable. On checkpoint: flush dirty → TieredBackend.put() → L3 SlateDB + update L2.
- [x]  PrefixedBackend isolates keys per operator.
- [x]  All existing tests continue to pass (LocalBackend path unchanged).
- [x]  New unit tests for SlateDbBackend, TieredBackend, PrefixedBackend.

## 🛠️ Tooling & DevEx
### Issue D-1: CLI Scaffold (rs-cli) ✅
Priority: 🟡 Medium

#### Description
Create the command-line entry point for developers.

#### Tasks
- [x]  Set up clap for argument parsing.
- [x]  Implement rs-cli new <name>: Generates a project folder with Cargo.toml and main.rs template.
- [x]  Implement rs-cli run: Wraps cargo run.

#### Acceptance Criteria
- [x]  rs-cli new demo creates a compilable RustStream project.

### Issue D-2: Basic Terminal Dashboard (TUI)
Priority: 🟢 Low (Nice to have for v1)

#### Description
A simple UI to see if the job is running.

#### Tasks
- []  Use ratati crate.
- []  Create a layout with a Header (Job Status) and a Log Window (capturing stdout/stderr).

#### Acceptance Criteria
- []  Running rs-cli run --ui launches the TUI.

## 🔌 Connectors & Ecosystem
### Issue E-1: Mock Sources & Sinks ✅
Priority: 🟠 High

#### Description
We need sources and sinks to test the engine before we implement full Kafka support.

#### Tasks
- [x]  Implement VecSource: Iterates over a static Vec<T>.
- [x]  Implement PrintSink: Prints results to stdout.
- [x]  Crucial: VecSource must support "Watermark Generation" (e.g., emit a watermark every 10 records).

#### Acceptance Criteria
- [x]  Can run the "Word Count" example using VecSource and PrintSink.

### Issue E-2: Serde & Arrow Utilities ✅
Priority: 🟡 Medium

#### Description
Ensure efficient data movement. While we use Rust structs for now, we should prepare for Arrow.

#### Tasks
- [x]  Verify bincode configuration for serializing events.
- [x]  Create helper trait Event: Serialize + Deserialize.
- [x]  Write a benchmark comparing bincode vs json serialization for our internal messages.

#### Acceptance Criteria
- [x]  All user events pass the serialization round-trip test.

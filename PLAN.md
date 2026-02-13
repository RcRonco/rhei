# 📦 RustStream Phase 1: Local-First Engine Backlog
## 🟢 Core System
### Issue A-1: Initialize Workspace and Define Core Traits
Priority: 🔴 Critical (Blocker)

#### Description
Initialize the Cargo workspace and define the fundamental traits that allow User Logic, the Runtime, and State to interact. This is the contract all other workers will build against.

#### Tasks
- [] Create Cargo workspace with members: rs-core, rs-runtime, rs-state, rs-cli.
- [] Define StateContext struct in rs-core.
- []  Define StreamFunction trait in rs-core using async_trait.
- []  Define StateBackend trait in rs-state.

#### Acceptance Criteria
- []  StateContext is decoupled from the specific backend implementation (uses Box<dyn StateBackend>).
- []  The following code compiles:
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

#### Issue A-2: Implement StreamGraph DSL (Builder API)
Priority: 🟠 High

#### Description
Create the fluent API that allows users to define a logical dataflow graph. This does not execute the graph yet; it just builds the logical plan.

#### Tasks
- []  Create StreamGraph struct.
- []  Implement .source(Source) method.
- []  Implement .map(StreamFunction), .filter(), and .key_by() methods.
- []  Implement .sink(Sink) method.
- []  Store the graph as an adjacency list or similar structure (LogicalPlan).

#### Acceptance Criteria
- []  User can write: let graph = StreamGraph::new().source(src).map(op).sink(snk);
- []  The graph object contains the correct nodes and edges in memory.

## ⚙️ Runtime & Execution (Worker B - Runtime Engineer)
### Issue B-1: Async Operator Wrapper (The "Shell")
Priority: 🔴 Critical

#### Description
Timely Dataflow operators are synchronous. We need a generic wrapper operator that can execute async closures by spawning them onto a Tokio runtime without blocking the main worker thread.

#### Tasks
- []  Create AsyncOperator struct wrapping a Timely scope.
- []  Accept a tokio::runtime::Handle.
- []  Implement the timely::operator::Operator trait.
- []  Logic to check for Future completion non-blockingly.

#### Acceptance Criteria
- []  Unit test: An operator that sleeps for 10ms (async) does NOT block the thread (verified by a heartbeat or second operator running concurrently).

### Issue B-2: Event Stashing & Retry Logic
Priority: 🟠 High

#### Description
When the AsyncOperator hits a state miss (S3 fetch), it must "stash" the incoming event and its capability. It must retry processing this event once the future completes.

#### Tasks
- []  Implement a Stash struct (likely VecDeque<(Capability, Data)>).
- []  Implement the polling loop: while let Some(res) = futures.poll() { unstash() }.
- []  Ensure Capabilities are retained while stashed and dropped only after successful processing.

#### Acceptance Criteria
- []  Ordering is preserved (Event A arrives before B; if A fetches state, B waits or is stashed behind A).
- []  No capabilities are dropped prematurely (which would cause downstream logic to close windows too early).

## 💾 State & Storage
### Issue C-1: Local File Backend Implementation
Priority: 🟠 High

#### Description
Implement a simple, local-disk version of the StateBackend trait. This allows us to test statefulness without needing S3 or SlateDB yet.

#### Tasks
- []  Implement LocalBackend struct.
- []  put(): Writes to an in-memory HashMap.
- []  checkpoint(): Serializes the HashMap to a file (JSON/Bincode) on disk.
- []  fetch(): Reads from the HashMap.
- []  Crucial: Add a generic latency_ms configuration to simulate S3 slowness during tests.

#### Acceptance Criteria
- []  Data persists across restarts (if loaded from disk).
- []  fetch can be configured to "sleep" for 50ms to test the Async Operator's non-blocking behavior.

### Issue C-2: L1 MemTable & Dirty Tracking
Priority: 🟡 Medium

#### Description
The StateContext needs a buffering layer. Writes should be instant (RAM), and only flushed to the Backend during a checkpoint.

#### Tasks
- []  Implement MemTable struct (wraps a HashMap).
- []  Track dirty_keys (keys modified since last flush).
- []  Implement flush(): returns the dirty key-values and clears the flag.

#### Acceptance Criteria
- []  ctx.put() is synchronous and fast.
- []  ctx.get() returns the value from MemTable if it exists (Read-Your-Own-Writes).

## 🛠️ Tooling & DevEx
### Issue D-1: CLI Scaffold (rs-cli)
Priority: 🟡 Medium

#### Description
Create the command-line entry point for developers.

#### Tasks
- []  Set up clap for argument parsing.
- []  Implement rs-cli new <name>: Generates a project folder with Cargo.toml and main.rs template.
- []  Implement rs-cli run: Wraps cargo run.

#### Acceptance Criteria
- []  rs-cli new demo creates a compilable RustStream project.

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
### Issue E-1: Mock Sources & Sinks
Priority: 🟠 High

#### Description
We need sources and sinks to test the engine before we implement full Kafka support.

#### Tasks
- []  Implement VecSource: Iterates over a static Vec<T>.
- []  Implement PrintSink: Prints results to stdout.
- []  Crucial: VecSource must support "Watermark Generation" (e.g., emit a watermark every 10 records).

#### Acceptance Criteria
- []  Can run the "Word Count" example using VecSource and PrintSink.

### Issue E-2: Serde & Arrow Utilities
Priority: 🟡 Medium

#### Description
Ensure efficient data movement. While we use Rust structs for now, we should prepare for Arrow.

#### Tasks
- []  Verify bincode configuration for serializing events.
- []  Create helper trait Event: Serialize + Deserialize.
- []  Write a benchmark comparing bincode vs json serialization for our internal messages.

#### Acceptance Criteria
- []  All user events pass the serialization round-trip test.
# Pipeline-First Multi-Worker Execution (DRAFT)

## Context

The pipeline builder (`executor.pipeline(source).filter().operator().sink()`) is the ergonomic, type-safe API for building stream processing pipelines. However, multi-worker execution currently lives in a separate, lower-level API (`run_dataflow_parallel`) that requires manual exchange key functions, `Serialize + DeserializeOwned` bounds, and Timely-specific plumbing.

The goal is to make the pipeline builder **the** primary API for both single-worker and multi-worker execution. Multi-worker support is added via a `.key_by()` method (Flink/Kafka Streams pattern) that marks the partition point. The pipeline handles everything else automatically: exchange insertion, per-worker state contexts, worker management, and checkpoint coordination.

## API Design

```rust
// Single worker (existing, unchanged):
let executor = Executor::new(dir);
executor
    .pipeline(source)
    .operator("word_counter", word_counter, ctx)
    .sink(sink)
    .await?;

// Multi-worker with key-based partitioning:
let executor = Executor::new(dir).with_workers(4);
executor
    .pipeline(source)
    .filter(|r: &SensorReading| r.value > 0.0)
    .key_by(|r: &SensorReading| r.sensor_id.clone())
    .operator("tumbling_window", op)      // No ctx — auto-created per worker
    .map(|out| format!("{out}"))
    .sink(sink)
    .await?;
```

Key design points:
- `.key_by(fn)` takes `Fn(&T) -> String` (matches operator key function convention)
- After `.key_by()`, the type transitions to `KeyedPipeline` (new struct)
- `.operator()` on `KeyedPipeline` takes **no** `StateContext` — contexts created automatically per worker via `create_context_for_worker`
- Worker count on the executor: `Executor::new(dir).with_workers(4)`
- When `workers == 1`, `KeyedPipeline` runs the same simple async loop (key_by is a no-op for routing)
- Multiple `.key_by()` calls are a compile error (no method on `KeyedPipeline`)

## Execution Model

**Single-worker** (existing `PipelineWithSteps` or `KeyedPipeline` with workers=1): simple async `while let Some(batch) = source.next_batch()` loop, unchanged.

**Multi-worker** (`KeyedPipeline` with workers > 1): pure-async using tokio tasks and `mpsc` channels — **no Timely**:

```
Source (current task)
  → Pre-exchange steps (current task)
    → Distributor: hash(key_fn(item)) % n_workers
      → Worker 0 channel → Worker 0 task (post-steps) → output_tx
      → Worker 1 channel → Worker 1 task (post-steps) → output_tx
      → ...
      → Worker N channel → Worker N task (post-steps) → output_tx
    → Collector task (output_rx → Sink)
```

Advantages over the Timely-based `run_dataflow_parallel`:
- No `Serialize/DeserializeOwned` bounds (data stays in-process via channels)
- No `block_in_place` vs `block_on` issues (everything is native async)
- No `!Send` issues with Timely capabilities
- Simpler checkpoint coordination

The existing Timely-based methods (`run_dataflow`, `run_dataflow_parallel`, etc.) remain as lower-level APIs.

## Type System

```
PipelineSource<S>
  → .filter()/.map()/.flat_map()/.operator(name, func, ctx) → PipelineWithSteps<S, Step, Out>
  → .key_by(fn)                                              → KeyedPipeline<S, PreStep=Identity, PostStep=Identity, Mid, Out>

PipelineWithSteps<S, Step, Out>
  → .key_by(fn) → KeyedPipeline<S, PreStep=Step, PostStep=Identity, Out, Out>
  → .sink()     → execute (single-worker async loop, unchanged)

KeyedPipeline<S, PreStep, PostStep, Mid, Out>
  → .filter()/.map()/.flat_map()                 → KeyedPipeline (grows PostStep chain)
  → .operator(name, func)                        → KeyedPipeline (adds DeferredOperatorStep)
  → .sink(sink)/.sink_with_shutdown(sink, handle) → execute (single or multi-worker)
```

## Implementation Steps

### Step 1: Add `workers` field to `Executor`

**File:** `rill-runtime/src/executor.rs`

- Add `workers: usize` field to `Executor` (default `1`)
- Add `with_workers(self, n: usize) -> Self` builder method
- Add `workers(&self) -> usize` getter

### Step 2: Add `Clone` impls to pipeline step types

**File:** `rill-runtime/src/pipeline.rs`

Add conditional `Clone` impls to existing types (needed so `KeyedPipeline` can clone the post-step template per worker):

- `MapStep<F, I, O>` where `F: Clone`
- `FilterStep<F, T>` where `F: Clone`
- `FlatMapStep<F, I, O>` where `F: Clone`
- `ChainedStep<A, B>` where `A: Clone, B: Clone`

### Step 3: Add `InitializableStep` trait

**File:** `rill-runtime/src/pipeline.rs`

```rust
#[async_trait]
pub trait InitializableStep: ProcessStep {
    async fn init_worker(
        &mut self,
        executor: &Executor,
        worker_index: usize,
    ) -> anyhow::Result<()>;
}
```

Implement for all step types:
- `MapStep`, `FilterStep`, `FlatMapStep`, `IdentityStep`: no-op (`Ok(())`)
- `ChainedStep<A, B>`: calls `self.a.init_worker()` then `self.b.init_worker()`
- `DeferredOperatorStep`: creates state context via `executor.create_context_for_worker()`

### Step 4: Add `IdentityStep` and `DeferredOperatorStep`

**File:** `rill-runtime/src/pipeline.rs`

`IdentityStep<T>`: pass-through, implements `ProcessStep + InitializableStep + Clone`.

`DeferredOperatorStep<F: StreamFunction + Clone>`: stores `name + func`, no `StateContext` at construction time. `init_worker()` creates the context. `Clone` impl clones name + func but sets ctx to `None` (each clone gets its own fresh context via `init_worker`).

### Step 5: Add `.key_by()` methods

**File:** `rill-runtime/src/pipeline.rs`

Add `.key_by()` to both `PipelineSource<S>` and `PipelineWithSteps<S, Step, Out>`. Both return `KeyedPipeline` with the pre-steps captured and an empty `IdentityStep` as the initial post-step template.

### Step 6: Implement `KeyedPipeline`

**File:** `rill-runtime/src/pipeline.rs`

```rust
pub struct KeyedPipeline<'a, S, PreStep, PostStep, Mid, Out> {
    executor: &'a Executor,
    source: S,
    pre_step: PreStep,
    key_fn: Arc<dyn Fn(&Mid) -> String + Send + Sync>,
    post_step_template: PostStep,  // Cloned per worker
    _mid: PhantomData<Mid>,
    _out: PhantomData<Out>,
}
```

Methods:
- `.map(f)`, `.filter(f)`, `.flat_map(f)` — chain onto `PostStep` (require `Fn + Clone + Send + 'static`)
- `.operator(name, func)` — adds `DeferredOperatorStep` (require `StreamFunction + Clone + 'static`, no `ctx` param)
- `.sink(sink)` / `.sink_with_shutdown(sink, handle)` — execute

### Step 7: Multi-worker execution engine in `KeyedPipeline::run()`

**File:** `rill-runtime/src/pipeline.rs`

**When workers == 1:** chain pre_step + post_step, call `init_worker(executor, 0)`, run the same simple async loop as `PipelineWithSteps`.

**When workers > 1:**

Worker message protocol:
```rust
enum WorkerMessage<T> { Data(T), Checkpoint(u64), Shutdown }
enum WorkerResponse { CheckpointAck(u64) }
```

Execution:
1. Create N `mpsc` channels for worker input, one `mpsc` for collected output, one `mpsc` for checkpoint acks
2. For each worker: clone `post_step_template`, call `init_worker(executor, i)`, spawn tokio task that processes `WorkerMessage`s
3. Spawn collector task: reads output channel, writes to sink
4. Main loop on current task: read source batches → run pre-steps → hash key → route `WorkerMessage::Data` to correct worker channel
5. Every N batches: send `Checkpoint` to all workers, wait for all `CheckpointAck`s, commit source, flush sink
6. On source exhaustion or shutdown: send `Shutdown` to all workers, wait for acks, final checkpoint, join all tasks

Key routing: `hash(key_fn(&item)) % n_workers` using `DefaultHasher` (consistent within process lifetime).

Back-pressure: bounded `mpsc` channels (256 capacity). Slow workers cause the distributor to block on `send().await`.

### Step 8: Update examples

- `rill-runtime/examples/window_agg.rs` — show keyed pipeline with `with_workers()`
- `rill-runtime/examples/temporal_join.rs` — show keyed pipeline
- `rill-runtime/examples/word_count.rs` — optionally add keyed variant
- `rill-runtime/examples/kafka_transform.rs` — no key_by needed (stateful but single-key), keep as-is

### Step 9: Tests

Add to `rill-runtime/src/pipeline.rs` test module:

1. **`keyed_single_worker`** — key_by + operator with 1 worker, verify same results as non-keyed
2. **`keyed_multi_worker_word_count`** — key_by + WordCounter with 2 workers, verify correct counts
3. **`keyed_multi_worker_filter_map`** — filter + key_by + map with 2 workers, verify all items processed
4. **`keyed_key_affinity`** — verify same key always routes to same worker
5. **`keyed_with_shutdown`** — verify graceful shutdown drains all workers

## Verification

```bash
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check
```

Run the TUI demo with workers to verify end-to-end:
```bash
cargo run -p rill-cli -- run --tui --workers 4
```

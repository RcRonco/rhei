# Roadmap

## Developer Experience

- [x] Reusable operator library (`rill-core/src/operators/`)
  - [x] `TemporalJoin<L, R, K, O>` — key-based join with configurable timeout
  - [x] `TumblingWindow<T, A>` — fixed-size time windows with pluggable aggregator
  - [x] `SlidingWindow<T, A>` — overlapping time windows
  - [x] `SessionWindow<T, A>` — gap-based windows per key
  - [x] `KeyedState<K, V>` — typed state wrapper over `StateContext` with automatic serde
  - [x] `Filter`, `Map`, `FlatMap` — stateless combinators
- [x] Fluent pipeline builder API (`DataflowGraph` with `Stream<T>` / `KeyedStream<T>`)
- [x] Multi-operator chaining in `run_dataflow` (currently single-operator only)
- [ ] Hot-reload operator logic without full pipeline restart
- [x] `rill run --attach <url>` — connect to a running pipeline and stream health/metrics/logs to TUI
- [ ] `rill-cli` improvements: deploy, inspect running pipelines, replay from checkpoint

## Integrations

- [x] Kafka source/sink
  - [x] Consumer group source with partition-aware offset tracking
  - [x] Partitioned source consumption (parallel per-worker partition readers)
  - [ ] Transactional producer sink (exactly-once via checkpoint coordination)
  - [ ] Schema Registry integration (Avro, Protobuf deserialization)
- [ ] Redis Streams source/sink
  - [ ] Consumer group source with acknowledgment
  - [ ] Stream/pub-sub sink
  - [ ] Redis as lookup/enrichment source for joins
- [ ] Amazon SQS source/sink
  - [ ] Long-polling source with visibility timeout management
  - [ ] Batched sink with message deduplication
  - [ ] Dead-letter queue integration
- [ ] Google Pub/Sub source/sink
  - [ ] Streaming pull source with flow control
  - [ ] Batched publish sink with ordering keys
- [ ] File source/sink
  - [ ] JSON lines, CSV, Parquet formats
  - [ ] Local filesystem and S3/GCS/Azure via `object_store`
- [ ] HTTP webhook sink
  - [ ] Batched POST with configurable retry/backoff
- [ ] Standard I/O source/sink (stdin/stdout for unix pipe composition)

## Observability

- [x] Structured tracing spans per-operator and per-worker
- [x] Prometheus metrics exporter endpoint (`/metrics`)
- [x] Backpressure metrics (stash depth, pending future count, channel utilization)
- [x] Throughput and latency metrics (batch/element counters, p50/p99 element duration)
- [x] State size metrics (L1/L2/L3 hit rates, checkpoint duration)
- [x] Dead-letter queue for failed/dropped elements (`ErrorPolicy::DeadLetterFile`)
- [x] Pipeline topology visualization (TUI graph view with exchange point rendering)
- [x] TUI dashboard with worker count, per-worker log attribution
- [x] Health check endpoint for liveness/readiness probes (`/healthz`, `/readyz`)
- [x] JSON metrics API (`/api/metrics`) via fanout recorder
- [x] Log buffer API (`/api/logs`) with structured log capture
- [x] S3 (MinIO) E2E test for tiered storage with CI job

## Performance

- [ ] Batch-level processing in operators (process `Vec<Input>` instead of element-at-a-time)
- [ ] Zero-copy deserialization for state reads (avoid `Vec<u8>` cloning)
- [ ] Memtable compaction and eviction policies (bounded memory) (KI-7)
- [ ] Async state prefetch — predict upcoming keys and warm L2/L3 cache
- [ ] Columnar in-memory representation for windowed aggregations
- [ ] Benchmark suite with throughput/latency targets
- [ ] Profile and optimize the Timely ↔ Tokio bridge (channel sizing, wake strategy)
- [ ] Batch-level type erasure (erase `Vec<T>` once per batch instead of per element)
- [ ] Investigate `abomonation` or `flatbuffers` for Timely serialization instead of `bincode`
- [ ] Sliding window eviction for closed active windows (KI-10)

## Stability

- [ ] Exactly-once semantics with two-phase commit on source/sink
- [ ] Checkpoint versioning and backward-compatible state migration
- [x] Graceful shutdown: drain in-flight, checkpoint, then exit
- [x] Restart from checkpoint with offset tracking (Kafka consumer offsets)
  - [x] Checkpoint manifest with source offset persistence
  - [x] Reload source offsets from manifest on restart (KI-2)
  - [x] Kafka consumer seek to checkpointed offsets on restart (KI-16)
- [x] Watermark propagation for out-of-order event handling (KI-13)
- [x] Late-event policy: drop with metric and configurable `allowed_lateness` (KI-6 partial)
- [ ] Late-event side-output routing (redirect late events to a separate stream)
- [x] Operator-level error handling (skip or dead-letter file via `ErrorPolicy`)
- [ ] Propagate sink send errors instead of silently dropping (KI-1)
- [ ] Propagate DLQ write errors instead of silently dropping (KI-3)
- [x] Propagate checkpoint failures in single-worker mode (KI-15)
- [ ] Fix async stash ordering — pending elements can be overtaken by later L1 hits (KI-11)
- [x] Configurable checkpoint interval via `Executor::builder().checkpoint_interval(n)` (KI-8)
- [ ] Fuzz testing for state serialization and checkpoint restore
- [x] S3/MinIO E2E integration test for tiered storage backend
- [ ] Integration tests with simulated failures (network partitions, slow backends)
- [ ] Integration tests for backpressure, DLQ routing, late events, checkpoint recovery (KI-18)

## Topology

- [x] Support multiple exchanges (key_by) in a single pipeline (KI-4)
- [x] Merge / fan-in — combine multiple streams into one (KI-9)
- [x] Fan-out — one source feeding multiple sinks (KI-17)
- [ ] Temporal join timeout and state eviction for unmatched events (KI-5)
- [x] Single-worker source offset tracking in checkpoint manifest (KI-12)

## Clustering

- [x] Multi-worker Timely execution (single-process, multiple threads)
- [x] Key-based partitioning (hash-based exchange with per-worker Timely dataflows)
- [x] Per-worker state contexts with frontier-based checkpointing
- [x] `--workers <N>` CLI flag
- [ ] Multi-process Timely cluster with TCP communication
- [ ] Distributed state backend (shared SlateDB or S3-backed object store)
- [ ] Coordinated checkpointing across workers (Chandy-Lamport style)
- [ ] Dynamic scaling: add/remove workers with state redistribution
- [ ] Leader election and failure detection
- [ ] Cluster membership via etcd or similar coordination service

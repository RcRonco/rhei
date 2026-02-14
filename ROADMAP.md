# Roadmap

## Developer Experience

- [ ] Reusable operator library (`rill-core/src/operators/`)
  - [ ] `TemporalJoin<L, R, K, O>` — key-based join with configurable timeout
  - [ ] `TumblingWindow<T, A>` — fixed-size time windows with pluggable aggregator
  - [ ] `SlidingWindow<T, A>` — overlapping time windows
  - [ ] `SessionWindow<T, A>` — gap-based windows per key
  - [ ] `KeyedState<K, V>` — typed state wrapper over `StateContext` with automatic serde
  - [ ] `Filter`, `Map`, `FlatMap` — stateless combinators
- [ ] Fluent pipeline builder API (`stream.join(...).window(...).aggregate(...)`)
- [ ] Multi-operator chaining in `run_dataflow` (currently single-operator only)
- [ ] Real connectors: Kafka source/sink, file source/sink, HTTP sink
- [ ] Schema registry integration (Avro, Protobuf)
- [ ] Hot-reload operator logic without full pipeline restart
- [ ] `rill-cli` improvements: deploy, inspect running pipelines, replay from checkpoint

## Observability

- [ ] Structured tracing spans per-operator and per-epoch
- [ ] Prometheus metrics exporter endpoint
- [ ] Backpressure metrics (stash depth, pending future count, channel utilization)
- [ ] Throughput and latency histograms per operator
- [ ] State size metrics (memtable entries, L2/L3 hit rates, checkpoint size)
- [ ] Dead-letter queue for failed/dropped elements with diagnostics
- [ ] Pipeline topology visualization (DAG from `LogicalPlan`)
- [ ] Health check endpoint for liveness/readiness probes

## Performance

- [ ] Batch-level processing in operators (process `Vec<Input>` instead of element-at-a-time)
- [ ] Zero-copy deserialization for state reads (avoid `Vec<u8>` cloning)
- [ ] Memtable compaction and eviction policies (bounded memory)
- [ ] Async state prefetch — predict upcoming keys and warm L2/L3 cache
- [ ] Columnar in-memory representation for windowed aggregations
- [ ] Benchmark suite with throughput/latency targets
- [ ] Profile and optimize the Timely ↔ Tokio bridge (channel sizing, wake strategy)
- [ ] Investigate `abomonation` or `flatbuffers` for Timely serialization instead of `bincode`

## Stability

- [ ] Exactly-once semantics with two-phase commit on source/sink
- [ ] Checkpoint versioning and backward-compatible state migration
- [ ] Graceful shutdown: drain in-flight, checkpoint, then exit
- [ ] Restart from checkpoint with offset tracking (Kafka consumer offsets)
- [ ] Watermark propagation for out-of-order event handling
- [ ] Late-event policy (drop, redirect to side output, or update)
- [ ] Operator-level error handling (retry, skip, dead-letter)
- [ ] Fuzz testing for state serialization and checkpoint restore
- [ ] Integration tests with simulated failures (network partitions, slow backends)

## Clustering

- [ ] Multi-worker Timely execution (single-process, multiple threads)
- [ ] Multi-process Timely cluster with TCP communication
- [ ] Key-based partitioning (`Exchange` pact) for parallel stateful operators
- [ ] Distributed state backend (shared SlateDB or S3-backed object store)
- [ ] Coordinated checkpointing across workers (Chandy-Lamport style)
- [ ] Dynamic scaling: add/remove workers with state redistribution
- [ ] Leader election and failure detection
- [ ] Cluster membership via etcd or similar coordination service

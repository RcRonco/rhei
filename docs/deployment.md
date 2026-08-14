# Deployment Guide

Running a Rhei pipeline outside your editor: configuration, scaling modes, observability, and operational limits.

> **Read this first.** Rhei is pre-1.0. Delivery is at-least-once, there is no
> job manager or leader election, and several stability items are open. See
> [KNOWN-ISSUES.md](../KNOWN-ISSUES.md) before depending on it.

---

## Configuration precedence

Three layers, later overriding earlier:

```text
pipeline.toml  →  RHEI_* environment variables  →  CLI flags
```

A `#[rhei::pipeline]` binary parses flags itself. `rhei run --config pipeline.toml` loads the file.

### `pipeline.toml`

```toml
[pipeline]
name = "orders-enrichment"
workers = 4
checkpoint_dir = "/var/lib/rhei/checkpoints"
checkpoint_interval = 100          # batches between checkpoints

[metrics]
addr = "0.0.0.0:9090"
log_level = "info"                 # or "rhei_core=trace,rhei_runtime=debug"
json_logs = true

[cluster]
# Static mode
process_id = 0
peers = ["node-a:2101", "node-b:2101"]

# Or dynamic mode (mutually exclusive with process_id/peers)
discovery = "gossip"
node_id = "node-a"                 # must be stable across restarts
cluster_id = "orders-prod"
gossip_addr = "0.0.0.0:2201"
gossip_advertise_addr = "10.0.0.5:2201"
data_addr = "10.0.0.5:2101"
seeds = ["node-a:2201", "node-b:2201"]

max_parallelism = 256              # fixed for the pipeline's lifetime
rescale_debounce_secs = 30
auto_rescale = true
```

### Environment variables

| Variable | Meaning |
|----------|---------|
| `RHEI_WORKERS` | Worker threads per process |
| `RHEI_CHECKPOINT_DIR` | Local checkpoint directory |
| `RHEI_CHECKPOINT_INTERVAL` | Batches between checkpoints |
| `RHEI_CHECKPOINT_PORT` | TCP port for cross-process checkpoint coordination |
| `RHEI_PIPELINE_NAME` | Display name |
| `RHEI_METRICS_ADDR` | HTTP bind address for health/metrics |
| `RHEI_LOG_LEVEL` | Tracing filter |
| `RHEI_JSON_LOGS` | Structured JSON logs |
| `RHEI_PROCESS_ID` | 0-based process index (static cluster) |
| `RHEI_PEERS` | Comma-separated `host:port` list (static cluster) |
| `RHEI_DISCOVERY` | `static` or `gossip` |
| `RHEI_NODE_ID` | Stable node identity (gossip) |
| `RHEI_CLUSTER_ID` | Only nodes sharing this gossip together |
| `RHEI_GOSSIP_ADDR` | UDP bind address for gossip |
| `RHEI_GOSSIP_ADVERTISE_ADDR` | Address peers should use (NAT/containers) |
| `RHEI_DATA_ADDR` | Timely data-plane address advertised to peers |
| `RHEI_SEEDS` | Comma-separated gossip seeds |
| `RHEI_MAX_PARALLELISM` | Key group count — **immutable per pipeline** |
| `RHEI_RESCALE_DEBOUNCE_SECS` | Quiet period before a rescale triggers |
| `RHEI_AUTO_RESCALE` | Rescale automatically on membership change |
| `RHEI_FROM_CHECKPOINT` | Restore from a remote manifest (fork mode) |
| `RHEI_OFFSET_DELTA` | Signed offset shift in fork mode |
| `RHEI_REMOTE_BUCKET` | Object storage bucket for L3 |
| `RHEI_REMOTE_PREFIX` | Key prefix within the bucket |
| `RHEI_REMOTE_ENDPOINT` | Custom endpoint (MinIO, Azurite) |
| `RHEI_REMOTE_REGION` | Cloud region |
| `RHEI_REMOTE_ALLOW_HTTP` | Permit plain HTTP — local development only |

Object storage credentials come from the standard environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, …) or instance metadata / IAM role. Rhei never takes them as configuration.

---

## Scaling modes

### Single process, single worker

The development default. One Timely worker, local state.

```bash
cargo run
```

`key_by` is still present in the dataflow but moves no data between threads.

### Single process, multiple workers

```bash
cargo run -- --workers 4
RHEI_WORKERS=4 cargo run
```

N worker threads, shared-nothing L1/L2 per worker, key groups split across them. This is the first configuration where a missing `key_by` produces wrong results — **test here before shipping**.

Start with one worker per physical core, leaving headroom for the Tokio runtime that drives sources and sinks.

### Multi-process (static)

Every process needs the same peer list and its own index.

```bash
# node-a
RHEI_PROCESS_ID=0 RHEI_PEERS=node-a:2101,node-b:2101 RHEI_WORKERS=4 ./pipeline

# node-b
RHEI_PROCESS_ID=1 RHEI_PEERS=node-a:2101,node-b:2101 RHEI_WORKERS=4 ./pipeline
```

Total workers = processes × workers-per-process. Timely connects the mesh over TCP on the `--peers` ports; checkpoint coordination uses a **separate** port (`RHEI_CHECKPOINT_PORT`).

Requirements:

- Identical peer list and worker count on every process — mismatches produce a broken mesh, not an error.
- Shared L3 state (`remote-state` + `RHEI_REMOTE_BUCKET`). **Without it each process keeps state locally and results are wrong.**
- All processes reachable on both the data and coordination ports.
- Process 0 must be up: it is the checkpoint coordinator, chosen by convention with no failover.

### Multi-process (gossip discovery)

Requires the `chitchat` feature on `rhei-runtime`.

```bash
RHEI_DISCOVERY=gossip \
RHEI_NODE_ID=node-a \
RHEI_CLUSTER_ID=orders-prod \
RHEI_GOSSIP_ADDR=0.0.0.0:2201 \
RHEI_DATA_ADDR=10.0.0.5:2101 \
RHEI_SEEDS=node-a:2201,node-b:2201 \
RHEI_MAX_PARALLELISM=256 \
./pipeline
```

Nodes find each other by gossip, with phi-accrual failure detection. Membership changes trigger a debounced rescale — `rescale_debounce_secs` prevents a rolling restart from causing one rescale per node.

`node_id` **must be stable across restarts.** A node that picks a fresh ID each boot looks like permanent join/leave churn.

Only one seed needs to be reachable; membership propagates from there.

---

## Sizing

| Question | Guidance |
|----------|----------|
| How many workers? | Start at one per physical core. Watch per-worker throughput for skew |
| `max_parallelism`? | The highest worker count you might ever want, with headroom. **You cannot change it later** without invalidating every checkpoint |
| `checkpoint_interval`? | Lower bounds replay time and dirty L1 growth; higher reduces I/O |
| L1 size? | Large enough for the hot key set — the cold path blocks the worker thread |
| L2 (`foyer_dir`)? | **Real NVMe.** The default lives under `/tmp`, often tmpfs, which competes for the RAM you are trying to save |

More workers than `max_parallelism` is allowed but pointless for stateful work: surplus workers own no key groups. `rhei_cluster_idle_workers` reports how many.

---

## Observability

### Endpoints

Enable with `--metrics-addr 0.0.0.0:9090`.

| Endpoint | Purpose |
|----------|---------|
| `GET /healthz` | Liveness |
| `GET /readyz` | Readiness — 503 until the pipeline is running |
| `GET /metrics` | Prometheus exposition |
| `GET /api/metrics` | JSON metrics snapshot |
| `GET /api/metrics/history` | Recent metric history |
| `GET /api/logs` | Structured log buffer |
| `GET /api/health` | Health detail as JSON |
| `GET /api/topology` | Pipeline topology |
| `GET /api/info` | Build and pipeline info |
| `GET /api/state/operators` | Operators with state |
| `GET /api/state/operators/{name}` | State entries for one operator |

Use `/healthz` for liveness probes and `/readyz` for readiness — pointing both at `/healthz` means traffic arrives before the dataflow is up.

Note: `/api/topology` does **not** expose key group assignment. "Who owns key group N?" is not answerable over HTTP today.

### Metrics that matter

**Throughput**

```text
executor_batches_total       executor_elements_total       executor_workers
```

**State — the first place to look when throughput drops**

```text
state_l1_hits_total          state_l1_misses_total
state_l2_hits_total          state_l3_hits_total
state_get_duration_seconds   state_gets_total / state_puts_total
```

A falling L1 hit ratio means the blocking cold path is now on your hot path.

**Checkpoints**

```text
state_checkpoints_total              state_checkpoint_duration_seconds
state_checkpoint_dirty_keys          executor_checkpoint_duration_seconds
```

Alert when `state_checkpoint_duration_seconds` approaches the wall-clock time between checkpoints.

**Correctness signals**

```text
late_events_dropped_total    dlq_items_total
sink_send_errors_total       operator_lifecycle_errors_total
```

These are silent-data-loss counters. **Alert on all of them.** A rising `late_events_dropped_total` means results are incomplete; `sink_send_errors_total` means output never landed.

**Cluster**

```text
rhei_cluster_workers                  rhei_cluster_processes
rhei_cluster_generation               rhei_cluster_key_groups_owned
rhei_cluster_key_groups_moved_total   rhei_cluster_rescales_total
rhei_cluster_rescales_suppressed_total
rhei_cluster_rescale_duration_seconds rhei_cluster_no_quorum_total
rhei_cluster_idle_workers
rhei_gossip_members                   rhei_gossip_members_live
rhei_gossip_members_pending           rhei_gossip_members_joined_total
rhei_gossip_members_left_total        rhei_gossip_membership_changes_total
```

`rhei_cluster_key_groups_owned` per worker is your skew signal.

### Logging

```bash
./pipeline --log-level info --json-logs
./pipeline --log-level 'rhei_runtime=debug,rhei_core::state=trace'
```

Use `--json-logs` anywhere logs are collected centrally.

> **KI-14:** the tracing capture layer uses a non-blocking `try_send`. Under
> high log volume entries are silently dropped. Do not treat the log buffer as
> an audit trail.

### TUI

```bash
rhei run --tui --workers 4      # run the current project with the dashboard
rhei attach 127.0.0.1:9090      # attach to an already-running pipeline
rhei demo --workers 4           # built-in demo pipeline
```

`rhei attach` is a separate subcommand, not a flag on `run`. Install the CLI from a checkout — it is not published:

```bash
cargo install --path rhei-cli
```

---

## Error handling in production

```rust,no_run
use rhei::PipelineController;
use rhei_core::dlq::{ErrorPolicy, FileDlqSink};

fn controller() -> anyhow::Result<PipelineController> {
    PipelineController::builder()
        .checkpoint_dir("/var/lib/rhei/checkpoints")
        .workers(4)
        .error_policy(ErrorPolicy::SendToDlq)
        .dlq_sink(FileDlqSink::new("/var/log/rhei/dlq.jsonl")?)
        .build()
}
```

`ErrorPolicy::Skip` (the default) logs and drops. That is rarely what you want in production — a malformed record vanishes with only a log line. Prefer `SendToDlq` with `FileDlqSink` or `KafkaDlqSink`, and alert on `dlq_items_total`.

---

## Graceful shutdown

```rust,ignore
// not-compiled: needs a ShutdownHandle wired to a real signal source.
controller.run_with_shutdown(graph, shutdown_handle).await?;
```

On signal: finish in-flight batches, checkpoint, commit offsets, flush sinks, return. Killing a process without this loses everything since the last checkpoint — which is replayed on restart, so it costs time rather than data.

Give containers a `terminationGracePeriodSeconds` longer than your checkpoint duration.

---

## Containers

Things that bite specifically in containers:

- **`foyer_dir` must be a real volume.** The default under `/tmp` is usually tmpfs, so L2 consumes RAM.
- **`checkpoint_dir` must be persistent** unless L3 holds everything you need.
- **Advertise the right addresses.** With NAT or overlay networking, set `gossip_advertise_addr` and `data_addr` to what peers can actually reach, not the bind address.
- **Stable `node_id` in gossip mode.** In Kubernetes, use the StatefulSet pod name — a Deployment's random names look like permanent churn.
- **Stable `process_id` in static mode.** StatefulSet ordinals map naturally.
- **Memory limits must account for L1 + L2-in-memory + Arrow batches.** Dirty L1 is unbounded between checkpoints (KI-7); an OOM kill mid-interval replays from the last checkpoint.

Ports to expose: Timely data plane (`--peers`, default 2101), checkpoint coordination (`RHEI_CHECKPOINT_PORT`), gossip UDP (`gossip_addr`, e.g. 2201), and metrics HTTP.

---

## Runbook

**Throughput dropped**
Check the L1 hit ratio first — a working set that outgrew L1 puts the blocking cold path on the hot path. Then check per-worker `executor_elements_total` for skew (one hot key pins one worker; key groups do not fix key skew). Then `state_checkpoint_duration_seconds`.

**No output**
Almost always watermarks. A source that emits none produces no windowed output until exhaustion; one idle Kafka partition holds the global minimum down. See [time-and-watermarks.md](time-and-watermarks.md).

**Checkpoints stopped**
Checkpoints are frontier-driven, so a stalled watermark stalls them too. In a cluster, check that process 0 is alive — it coordinates, and there is no failover.

**Wrong results after scaling**
Look for a stateful operator with no preceding `key_by`. It compiles and is correct at one worker, wrong beyond that. Also confirm `max_parallelism` matches the manifest.

**Restore rejected**
`max_parallelism` differs from the manifest. It is immutable for a pipeline's lifetime; you cannot resume that state with a new value.

**Memory climbing between checkpoints**
KI-7. Shorten `checkpoint_interval` and watch `state_checkpoint_dirty_keys`.

---

## Operational limits

Know these before you commit to a deployment:

| Limit | Consequence |
|-------|-------------|
| **At-least-once only** | Records replay after a crash. Sinks must be idempotent |
| **No job manager / scheduler** | You start and supervise processes yourself |
| **No leader election** | Process 0 coordinates checkpoints; if it dies, coordination stops |
| **No checkpoint versioning** | Changing a state value's serialized shape breaks restore |
| **`max_parallelism` immutable** | Chosen once, forever |
| **Dirty L1 unbounded** between checkpoints | OOM risk on high write cardinality (KI-7) |
| **Blocking state cold path** | Throughput collapses rather than degrades when L1 misses (KI-11) |
| **No late-event side output** | Late data is dropped and counted, not recoverable |
| **No idle-source detection** | One idle partition stalls every window |
| **Log entries dropped under load** | KI-14 |
| **No key group ownership over HTTP** | Debugging ownership needs logs |

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

`build()` validates the result and returns an error rather than starting with a
half-understood configuration. A malformed `RHEI_WORKERS` fails the process at
startup instead of silently running at default parallelism, and every bad
variable is reported at once so one restart reveals all of them:

```rust,no_run
use rhei::PipelineController;

fn controller() -> anyhow::Result<PipelineController> {
    PipelineController::builder()
        .checkpoint_dir("/var/lib/rhei/checkpoints")
        .from_env()   // fills gaps from RHEI_* variables
        .build()      // errors on anything malformed
}
```

Rejected at `build()`: a zero worker count, a zero checkpoint interval, a
`max_parallelism` below the total worker count, a peer list with an empty or
port-less entry, and any `RHEI_*` value that does not parse. Empty values are
treated as unset, so an orchestrator injecting `VAR=""` for an absent optional
setting is not a config error.

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
| `RHEI_LIVENESS_TIMEOUT_SECS` | Worker silence tolerated by `/healthz` (default 60) |
| `RHEI_METRICS_TOKEN` | Bearer token required by `/metrics` and `/api/*` |
| `RHEI_STATE_EXPLORER` | `1` serves `/api/state/**` — off by default |
| `RHEI_ALLOWED_ORIGINS` | Comma-separated CORS origins — none by default |

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

| Endpoint | Purpose | Auth |
|----------|---------|------|
| `GET /healthz` | Liveness — 503 when a worker has stopped making progress | open |
| `GET /readyz` | Readiness — 503 unless `Running` | open |
| `GET /metrics` | Prometheus exposition | token |
| `GET /api/metrics` | JSON metrics snapshot | token |
| `GET /api/metrics/history` | Recent metric history | token |
| `GET /api/logs` | Structured log buffer | token |
| `GET /api/health` | Health detail as JSON | token |
| `GET /api/topology` | Pipeline topology | token |
| `GET /api/info` | Build and pipeline info | token |
| `GET /api/state/operators` | Operators with state | token, opt-in |
| `GET /api/state/operators/{name}` | State entries for one operator | token, opt-in |

"token" means the endpoint requires `Authorization: Bearer <token>` when
`RHEI_METRICS_TOKEN` is set, and is unauthenticated when it is not. The probes
are always open: kubelet sends no headers, so gating them would make every
authenticated deployment permanently unhealthy.

"opt-in" means the route is not served at all unless `RHEI_STATE_EXPLORER=1`.
Those endpoints return checkpointed **application data** — your keys and your
values. See [SECURITY.md](../SECURITY.md) for the full posture and a hardening
checklist.

Note: `/api/topology` does **not** expose key group assignment. "Who owns key group N?" is not answerable over HTTP today.

### The two probes answer different questions

Pointing both at `/healthz` means traffic arrives before the dataflow is up.
Pointing both at `/readyz` means a wedged pipeline is never restarted.

**`/readyz` — should this process be counted?** 200 while `Running`; 503 while
`Starting` (restoring from checkpoint), `Draining`, or `Stopped`. The `Draining`
transition happens the instant `SIGTERM` arrives, *before* the pipeline finishes
its work — so the orchestrator drops the process from endpoint lists while it is
still flushing state, rather than discovering it is gone afterwards.

**`/healthz` — is this process wedged?** 200 while every worker's dataflow loop
is turning; 503 once a worker has gone silent longer than
`RHEI_LIVENESS_TIMEOUT_SECS`, with a body naming it:

```json
{ "status": "running", "live": false,
  "detail": "worker 2 has not made progress for 94.3s (liveness timeout 60.0s)" }
```

This is a real liveness signal rather than a reachability check. A pipeline whose
Timely loop has deadlocked — on a blocked sink, a wedged state backend — still
answers TCP and would pass any probe that merely connects.

A heartbeat means "this worker's loop is turning", not "data flowed", so a
correctly-idle pipeline stays healthy. Draining and stopped pipelines always
report live, so an orderly shutdown is never interrupted by a restart.

Raise `RHEI_LIVENESS_TIMEOUT_SECS` above your worst-case checkpoint duration, or
a slow flush to object storage will look like a wedge. Check the
`state_checkpoint_duration_seconds` p99 and leave headroom.

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

Histograms are exported as buckets rather than summaries, so quantiles combine
across processes — a cluster-wide p95 is a real number, not one pod's:

```promql
histogram_quantile(0.95, sum by (le) (rate(state_get_duration_seconds_bucket[5m])))
```

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

On signal: report not-ready immediately, finish in-flight batches, checkpoint,
commit offsets, flush sinks, return. Killing a process without this loses
everything since the last checkpoint — which is replayed on restart, so it costs
time rather than data.

The not-ready flip happens first, before the work finishes, so load balancers and
endpoint lists drop the process while it is still draining.

Give containers a `terminationGracePeriodSeconds` longer than your checkpoint
duration, or `SIGKILL` lands mid-flush.

---

## Containers

A working image and Kubernetes manifests ship in `deploy/`:

```bash
docker build -f deploy/Dockerfile --build-arg BIN=my-pipeline -t my-pipeline:v1 .
kubectl apply -f deploy/kubernetes/statefulset.yaml
kubectl apply -f deploy/kubernetes/monitoring.yaml
```

`deploy/Dockerfile` is multi-stage, runs as a non-root UID, and sets no shell
entrypoint — the process must receive `SIGTERM` directly or the drain above
never runs. `deploy/kubernetes/statefulset.yaml` is a commented starting point:
a StatefulSet (ordinals give a stable `RHEI_PROCESS_ID`), a headless Service for
the data plane, a separate Service for scraping, all three probes, and a
PodDisruptionBudget. `deploy/kubernetes/monitoring.yaml` carries a ServiceMonitor
and alert rules built on the metrics above.

Things that bite specifically in containers:

- **`foyer_dir` must be a real volume.** The default under `/tmp` is usually tmpfs, so L2 consumes RAM.
- **`checkpoint_dir` must be persistent** unless L3 holds everything you need.
- **Advertise the right addresses.** With NAT or overlay networking, set `gossip_advertise_addr` and `data_addr` to what peers can actually reach, not the bind address.
- **Stable `node_id` in gossip mode.** In Kubernetes, use the StatefulSet pod name — a Deployment's random names look like permanent churn.
- **Stable `process_id` in static mode.** StatefulSet ordinals map naturally.
- **Memory limits must account for L1 + L2-in-memory + Arrow batches.** Dirty L1 is unbounded between checkpoints (KI-7); an OOM kill mid-interval replays from the last checkpoint.

Ports to expose: Timely data plane (`--peers`, default 2101), checkpoint coordination (`RHEI_CHECKPOINT_PORT`), gossip UDP (`gossip_addr`, e.g. 2201), and metrics HTTP. The first three carry no authentication or encryption — expose them *within* the cluster only.

---

## Runbook

### The pipeline will not start

Startup fails loudly and specifically rather than starting misconfigured, so the
log line identifies the cause.

| Log message contains | Cause | Fix |
|----------------------|-------|-----|
| `invalid environment configuration` | Malformed `RHEI_*` value, named in the message | Correct the value |
| `max_parallelism=... but this run is configured for` | `RHEI_MAX_PARALLELISM` changed | Restore the original value |
| `is corrupt and cannot be parsed` | Damaged checkpoint manifest | See below |
| `has schema version N but this build understands` | Manifest written by a newer Rhei | Upgrade the binary |
| `process_id ... must be less than number of peers` | Replica count and `RHEI_PEERS` disagree | Make them match |
| `max_parallelism ... is below the total worker count` | More workers than key groups | Reduce workers or raise `max_parallelism` |
| `could not reach the checkpoint coordinator` | Process 0 down, or its address wrong | Check process 0 and the first peer entry |

### A checkpoint manifest is corrupt

Rhei refuses to start. That is deliberate: treating an unreadable manifest as
"no checkpoint" would silently restart from offset zero and reprocess the whole
stream.

1. Confirm the damage: `cat $RHEI_CHECKPOINT_DIR/manifest.json`.
2. Restore that file from backup, or from object storage if remote state is
   configured, and restart.
3. With no backup, the call is yours to make explicitly. Deleting the checkpoint
   directory starts fresh — reprocessing whatever the source still retains and
   losing all accumulated state. That is a data decision, not an ops one.

Per-process partials (`manifest_p*.json`) are written before the merged
`manifest.json`, so a recent partial may still hold usable offsets.

### A pod is restarting repeatedly

`kubectl logs <pod> --previous` separates the two cases. Exiting at startup is a
configuration or checkpoint error — see the table above. Being killed by the
liveness probe means a worker is wedging; the `/healthz` body names it. Look for
a blocked sink (`sink_send_errors_total`), object storage timeouts, or a
checkpoint duration exceeding the liveness timeout.

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

## Upgrades and rollbacks

**Rolling restart.** An ordinary StatefulSet rolling update. Each pod drains on
`SIGTERM` as described above; keep the grace period above your worst-case
checkpoint duration.

**Rollback.** Manifests carry a schema version, and an older binary refuses to
read a newer manifest rather than misinterpreting it. Rolling back across a
version bump therefore needs the checkpoint directory restored to a manifest the
older binary understands.

**Changing the pipeline graph.** Adding or removing operators is allowed; Rhei
logs which operator names appeared and disappeared. State for a removed operator
is orphaned rather than deleted, and a *renamed* operator loses its state — the
name is the state's namespace.

---

## Security

Full issue register, trust boundaries, and a hardening checklist:
[SECURITY.md](../SECURITY.md). The short version:

- Set `RHEI_METRICS_TOKEN`. Without it, anyone who can reach port 9090 reads
  your metrics and logs. The process warns at startup when bound to a
  non-loopback address with no token.
- Leave `RHEI_STATE_EXPLORER` unset unless you need it. It serves application
  data.
- The **cluster data plane is unencrypted and unauthenticated**. Ports 2101 and
  the checkpoint coordination port are plaintext TCP; anyone who can reach them
  reads records in flight, injects frames, and can forge checkpoint readiness.
  Keep them on a trusted network — a private subnet, a service mesh with mTLS,
  or a NetworkPolicy restricting them to pods in the same StatefulSet.

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
| **No transport security on the data plane** | Ports 2101 and coordination are plaintext and unauthenticated (SI-4) |
| **No first-class secure-broker config** | Authenticated Kafka needs `from_consumer`/`from_producer` (SI-5) |

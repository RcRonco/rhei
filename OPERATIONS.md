# Operating Rhei

How to deploy, observe, and recover a Rhei pipeline in production.

For what Rhei *is*, see [README.md](README.md). For known gaps, see
[KNOWN-ISSUES.md](KNOWN-ISSUES.md) — read it before committing to a
deployment, since it is honest about what is not finished.

---

## Contents

- [Deployment](#deployment)
- [Configuration](#configuration)
- [Health and probes](#health-and-probes)
- [Metrics](#metrics)
- [Security](#security)
- [Capacity planning](#capacity-planning)
- [Runbook](#runbook)
- [Upgrades and rollbacks](#upgrades-and-rollbacks)

---

## Deployment

A Rhei pipeline is your own Rust binary linked against `rhei`. There is no
server to install and no cluster manager to run.

```bash
# Build an image from your pipeline crate.
docker build -f deploy/Dockerfile --build-arg BIN=my-pipeline -t my-pipeline:v1 .

# Deploy to Kubernetes.
kubectl apply -f deploy/kubernetes/statefulset.yaml
kubectl apply -f deploy/kubernetes/monitoring.yaml
```

The manifests in `deploy/kubernetes/` are a working starting point, commented
with the reasoning behind each choice. Substitute your image, bucket, and peer
count.

**Why a StatefulSet.** `RHEI_PROCESS_ID` must match the pod's position in
`RHEI_PEERS`, and only StatefulSet pod names carry a stable ordinal. The
manifest derives the ID from the pod name (`rhei-pipeline-2` → `2`).

**Scaling.** Change `replicas` and the `RHEI_PEERS` list together — they must
agree. Key groups redistribute across the new worker count on restart. What you
must *not* change is `RHEI_MAX_PARALLELISM`; see below.

---

## Configuration

Settings come from three places, in increasing precedence: a TOML file, the
environment, then explicit builder calls in your code.

```rust
let executor = Executor::builder()
    .from_env()          // fills gaps from the environment
    .build()?;           // validates, and fails on anything malformed
```

`build()` returns an error rather than starting with a half-understood
configuration. A typo in `RHEI_WORKERS` fails the process at startup instead of
silently running at default parallelism.

### Environment variables

| Variable | Meaning |
|----------|---------|
| `RHEI_WORKERS` | Worker threads per process |
| `RHEI_PROCESS_ID` | This process's index into `RHEI_PEERS` |
| `RHEI_PEERS` | Comma-separated `host:port` list of every process |
| `RHEI_CHECKPOINT_DIR` | Where checkpoints and local state live |
| `RHEI_CHECKPOINT_INTERVAL` | Batches between checkpoints (default 100) |
| `RHEI_MAX_PARALLELISM` | Key group count — **fixed for the pipeline's life** |
| `RHEI_METRICS_ADDR` | HTTP bind address, e.g. `0.0.0.0:9090` |
| `RHEI_METRICS_TOKEN` | Bearer token for `/metrics` and `/api/*` |
| `RHEI_STATE_EXPLORER` | `1` serves `/api/state/**` (off by default) |
| `RHEI_ALLOWED_ORIGINS` | Comma-separated CORS origins (none by default) |
| `RHEI_LIVENESS_TIMEOUT_SECS` | Worker silence tolerated by `/healthz` (default 60) |
| `RHEI_LOG_LEVEL` | `tracing` filter, e.g. `info`, `rhei_core=debug` |
| `RHEI_JSON_LOGS` | `1` for structured JSON logs |
| `RHEI_REMOTE_BUCKET` | Object storage bucket for durable state |
| `RHEI_REMOTE_PREFIX` | Key prefix within the bucket |
| `RHEI_REMOTE_REGION` | Region (default `us-east-1`) |
| `RHEI_REMOTE_ENDPOINT` | Custom endpoint, for MinIO and friends |
| `RHEI_REMOTE_ALLOW_HTTP` | `1` permits plaintext object storage — test only |
| `RHEI_FROM_CHECKPOINT` | Manifest path to fork from |
| `RHEI_OFFSET_DELTA` | Signed offset adjustment when forking |

Empty values are treated as unset, so an orchestrator injecting `VAR=""` for an
absent optional setting does not turn it into a config error.

### `RHEI_MAX_PARALLELISM` is permanent

Every key's group is `hash(key) % max_parallelism`. Change it and every key
lands in a different group, so the pipeline would read empty state for keys it
has records of.

Rhei refuses to start rather than let that happen: a checkpoint records the
value it was written with, and a restart at a different value fails with an
explicit error. There is no migration path — pick a value comfortably above the
largest worker count you will ever run (128 or 256 is usually right; the
default is 128), because you cannot raise it later.

Workers beyond `max_parallelism` own no key groups and sit idle. The
`RheiIdleWorkers` alert catches this.

---

## Health and probes

Two endpoints answering two different questions.

### `/readyz` — should this process be counted?

`200` while `Running`. `503` while `Starting` (restoring from checkpoint),
`Draining` (shutting down), or `Stopped`.

The `Draining` transition happens the instant a `SIGTERM` arrives, before the
pipeline finishes its work. That ordering is the point: the orchestrator stops
routing to the process and drops it from endpoint lists while it is still
flushing state, rather than finding out afterwards.

### `/healthz` — is this process wedged?

`200` while every worker's dataflow loop is turning. `503` once a worker has
gone silent longer than `RHEI_LIVENESS_TIMEOUT_SECS`, with a body naming the
worker:

```json
{ "status": "running", "live": false,
  "detail": "worker 2 has not made progress for 94.3s (liveness timeout 60.0s)" }
```

This is a real liveness signal, not a reachability check. A pipeline whose
Timely loop has deadlocked — on a blocked sink, a wedged state backend — still
answers TCP and would pass any probe that merely connects. Returning `503` is
what lets Kubernetes restart it.

A heartbeat means "this worker's loop is turning", not "data flowed". An idle
source produces no data but the loop still steps, so a correctly-idle pipeline
stays healthy.

Draining and stopped pipelines always report live, so an orderly shutdown is
never interrupted by a restart.

**Tuning.** Raise `RHEI_LIVENESS_TIMEOUT_SECS` above your worst-case checkpoint
duration, or a slow flush to object storage will look like a wedge. Check the
`state_checkpoint_duration_seconds` p99 and leave headroom.

---

## Metrics

Prometheus exposition at `GET /metrics`. Histograms are exported as buckets,
not summaries, so quantiles aggregate across pods:

```promql
histogram_quantile(0.95, sum by (le) (rate(state_get_duration_seconds_bucket[5m])))
```

### The ones worth alerting on

| Metric | Why it matters |
|--------|----------------|
| `sink_send_errors_total` | Records reached the sink and could not be written. **They are lost.** |
| `state_checkpoints_total` | Flat while running means unbounded replay on restart |
| `dlq_items_total` | Records failing their operator |
| `late_events_dropped_total` | Window results are incomplete for those keys |
| `rhei_cluster_no_quorum_total` | Cluster cannot agree on a topology |
| `rhei_cluster_idle_workers` | Workers owning no key groups — wasted capacity |

### The ones worth watching

| Metric | Reading it |
|--------|-----------|
| `state_l1_hits_total` / `state_l1_misses_total` | L1 hit rate. Below ~90% and the working set has outgrown the memtable |
| `state_l2_hits_total` / `state_l3_hits_total` | Where misses land. L3 traffic means object-storage round trips on the hot path |
| `state_get_duration_seconds` | End-to-end read latency across all tiers |
| `state_checkpoint_duration_seconds` | Sets the floor for your liveness timeout |
| `state_checkpoint_dirty_keys` | How much each checkpoint has to flush |
| `rhei_cluster_rescales_total` | Each rescale re-reads state; churn here is expensive |
| `rhei_gossip_members_live` | Should equal your replica count |
| `executor_workers` | Workers actually running on this process |

`deploy/kubernetes/monitoring.yaml` ships alert rules for all of the above.

---

## Security

The HTTP surface is closed by default and opens only where you say so.

**Authentication.** Set `RHEI_METRICS_TOKEN` and every request to `/metrics`
and `/api/*` needs `Authorization: Bearer <token>`. Probes stay open —
kubelet does not send headers, so gating them would make every authenticated
deployment permanently unhealthy.

Without a token on a non-loopback bind address, the process logs a warning at
startup naming the exposure.

**The state explorer is off by default.** `/api/state/**` reads checkpointed
application data — your keys and your values, whatever those are. It is not
served at all unless `RHEI_STATE_EXPLORER=1`, so a deployment that never
enables it cannot leak through it regardless of how the token is managed. When
you do enable it, pair it with a token.

**CORS is closed by default.** List origins explicitly in
`RHEI_ALLOWED_ORIGINS` when running the web dashboard from a different origin.
For local dashboard development that is `http://localhost:5173`; `rhei demo`
allows it automatically.

**What is not encrypted.** The Timely inter-process data plane (port 2101) and
the checkpoint coordination channel are plaintext TCP with no authentication.
Run them on a trusted network — a private subnet, a service mesh with mTLS, or
Kubernetes NetworkPolicy restricting 2101 to pods in the same StatefulSet.
Do not expose 2101 beyond the cluster.

---

## Capacity planning

**CPU.** One Timely worker thread per core, pinned. Set `RHEI_WORKERS` to the
CPU limit and keep requests equal to limits — CFS throttling on a burstable pod
shows up as latency spikes and, at the extreme, as liveness failures.

**Memory.** Dominated by L1. Dirty entries are never evicted, so peak L1 is
bounded by how many distinct keys are written between checkpoints. Shorten
`RHEI_CHECKPOINT_INTERVAL` to lower the ceiling. Clean entries are bounded by
`MemTableConfig` (32 MiB / 500k entries by default).

**Disk.** The L2 Foyer cache wants NVMe. Size it to your hot working set; the
`RheiStateCacheMissRateHigh` alert tells you when it is too small.

**Object storage.** L3 is the durable copy. Cost tracks checkpoint frequency
times state size, so a very short checkpoint interval on large state gets
expensive.

---

## Runbook

### The pipeline will not start

Check the logs first — Rhei fails startup loudly and specifically rather than
starting misconfigured.

| Log message contains | Cause | Fix |
|----------------------|-------|-----|
| `invalid environment configuration` | Malformed env var, named in the message | Correct the value |
| `max_parallelism=... but this run is configured for` | `RHEI_MAX_PARALLELISM` changed | Restore the original value |
| `is corrupt and cannot be parsed` | Damaged checkpoint manifest | See below |
| `has schema version N but this build understands` | Manifest written by a newer Rhei | Upgrade the binary |
| `process_id ... must be less than number of peers` | Replica count and `RHEI_PEERS` disagree | Make them match |
| `max_parallelism ... is below the total worker count` | More workers than key groups | Reduce workers |

### A checkpoint manifest is corrupt

Rhei refuses to start. This is deliberate: treating an unreadable manifest as
"no checkpoint" would silently restart from offset zero and reprocess the whole
stream.

1. Confirm the damage: `cat $RHEI_CHECKPOINT_DIR/manifest.json`.
2. Restore that file from backup, or from object storage if remote state is
   configured, and restart.
3. If no backup exists, you must decide explicitly. Deleting the checkpoint
   directory starts fresh — reprocessing everything the source still retains,
   and losing all accumulated state. That is a data decision, not an ops one.

Per-process partials (`manifest_p*.json`) are written before the merged
`manifest.json`, so a recent partial may hold usable offsets.

### A pod is restarting repeatedly

`kubectl logs <pod> --previous` distinguishes the two cases.

- **Exits at startup** — a configuration or checkpoint error. See the table above.
- **Killed by the liveness probe** — a worker is wedging. The `/healthz` body
  names the worker. Look for a blocked sink (`sink_send_errors_total`), object
  storage timeouts, or a checkpoint duration exceeding the liveness timeout.

### Throughput has dropped

1. `rhei_cluster_idle_workers` — are workers sitting idle for want of key groups?
2. L1 hit rate — has the working set outgrown the memtable?
3. `state_l3_hits_total` — are reads reaching object storage on the hot path?
4. `state_checkpoint_duration_seconds` — are checkpoints stalling the workers?
5. Source lag (Kafka consumer group lag) — is the pipeline the bottleneck at all?

### Records are being dead-lettered

`dlq_items_total` rising means records are failing their operator. A sudden
sustained rate is usually an upstream schema change rather than isolated bad
records. Inspect the DLQ destination for the failure reason attached to each
record.

### The cluster will not form

1. Every pod must resolve every peer. `publishNotReadyAddresses: true` on the
   headless Service matters — peers must resolve before any pod is ready.
2. `RHEI_PEERS` must be byte-identical on every pod and ordered by process ID.
3. Port 2101 must be reachable pod-to-pod; check NetworkPolicy.
4. With gossip discovery, `rhei_gossip_members_live` should equal the replica
   count. Node IDs must be stable across restarts — a changing ID reads as
   permanent join/leave churn.

---

## Upgrades and rollbacks

**Rolling restart.** Ordinary StatefulSet rolling update. Each pod drains on
`SIGTERM`: it reports not-ready immediately, finishes its current batch, flushes
L1 through to object storage, and commits source offsets. Keep
`terminationGracePeriodSeconds` above your worst-case checkpoint duration, or
`SIGKILL` lands mid-flush — state stays safe, but work since the last
checkpoint is reprocessed.

**Rollback.** Manifests carry a schema version and an older binary refuses to
read a newer manifest rather than misinterpreting it. Rolling back across a
manifest version bump therefore needs the checkpoint directory restored to a
manifest the older binary understands.

**Changing the pipeline graph.** Adding or removing operators is allowed; Rhei
logs which operator names appeared and disappeared. State for a removed
operator is orphaned, not deleted. A renamed operator loses its state — the
name is the state's namespace.

**Delivery semantics.** Rhei is **at-least-once**, not exactly-once. Source
offsets are committed after a checkpoint completes, so a crash replays
everything since the last checkpoint. Sinks must tolerate duplicates —
idempotent writes or a downstream dedupe. Do not deploy Rhei behind a sink that
assumes each record arrives once.

# Security

Security issues in Rhei: what has been fixed, what is still open, and what you
need to know before exposing a pipeline to anything you do not control.

Issue IDs are `SI-N`, stable once assigned. Resolved entries are kept rather
than deleted — what a system used to get wrong is part of judging it, and a
register that lists only open items reads as though nothing was ever broken.

Severity:

- **CRITICAL** — remote code execution, authentication bypass, or unauthenticated
  access to application data.
- **HIGH** — remote denial of service, information disclosure beyond the trust
  boundary, or a missing control that leaves data exposed by default.
- **MEDIUM** — requires local access, an enabled non-default feature, or an
  already-trusted position to exploit.
- **LOW** — defence in depth; no direct exploit path.

**Still open:** SI-4, SI-5, SI-7, SI-8, SI-9.

---

## Reporting a vulnerability

Rhei is pre-1.0 and has not had an external security audit. Report suspected
vulnerabilities through GitHub's private advisory flow on the repository
(*Security* → *Report a vulnerability*) rather than a public issue.

---

## Trust boundaries

Three surfaces, three different assumptions. Getting these wrong is the most
likely way to be bitten.

| Surface | Port | Assumption |
|---------|------|-----------|
| HTTP API | 9090 | **Semi-trusted.** Probes are open; everything else can require a bearer token. Serves operational data, and application data when the state explorer is enabled. |
| Timely data plane | 2101 | **Fully trusted.** No authentication, no encryption. Anyone who can reach it can read pipeline data and inject frames. |
| Checkpoint coordination | data port + 1000 | **Fully trusted.** Same as above. |

Rhei assumes the data plane runs on a network you control. It is not safe to
expose ports 2101 or the coordination port beyond the cluster. See SI-4.

---

## Open issues

### SI-4: Cluster data plane has no authentication or encryption

**Severity: HIGH.** Files: `rhei-runtime/src/cluster/`,
`rhei-runtime/src/checkpoint_coord.rs`

The Timely inter-process data plane and the checkpoint coordination channel are
plaintext TCP with no authentication or integrity protection. Anyone who can
reach those ports can:

- read every record in flight between workers, in Arrow IPC form;
- inject records into the dataflow;
- send `Ready` messages to the checkpoint coordinator, causing it to commit a
  checkpoint before every process has actually flushed — which can commit
  source offsets for data that was never durably written.

This is the single largest security gap in Rhei, and it is not a small fix:
it needs a TLS or mTLS transport with certificate distribution, which touches
Timely's own networking.

**Mitigation.** Treat the data plane as an internal bus. Run it on a private
subnet, inside a service mesh providing mTLS, or behind a Kubernetes
NetworkPolicy that restricts 2101 and the coordination port to pods in the same
StatefulSet. Never route these ports through a load balancer or expose them to
a shared network.

### SI-5: No first-class configuration for authenticated Kafka

**Severity: MEDIUM.** Files: `rhei-core/src/connectors/batch/kafka_source.rs`,
`kafka_sink.rs`

`KafkaSource::new` and `KafkaSink::new` set only `bootstrap.servers`,
`group.id`, and offset behaviour. There is no parameter for
`security.protocol`, `sasl.mechanism`, `sasl.username`, `sasl.password`, or
`ssl.ca.location`, so the convenience constructors can only reach a plaintext
broker. A user who reaches for the obvious constructor gets an unencrypted,
unauthenticated broker connection.

**Mitigation.** Build a configured `rdkafka` client and pass it in — this path
exists and is supported:

```rust
let mut config = rdkafka::ClientConfig::new();
config
    .set("bootstrap.servers", brokers)
    .set("group.id", group_id)
    .set("security.protocol", "SASL_SSL")
    .set("sasl.mechanism", "SCRAM-SHA-512")
    .set("sasl.username", username)
    .set("sasl.password", password)
    .set("enable.auto.commit", "false");

let source = KafkaSource::from_consumer(config.create()?);
```

`KafkaSink::from_producer` is the equivalent for the sink side. A first-class
security config surface is planned; until then this is the documented route.

### SI-7: `rhei attach` sends credentials over plaintext HTTP

**Severity: MEDIUM.** File: `rhei-cli/src/main.rs`

`rhei attach` speaks HTTP unless given an `https://` address, and Rhei's own
HTTP server does not terminate TLS. Attaching to a remote pipeline with
`--token` therefore sends the bearer token, and receives all metrics and logs,
in the clear.

The CLI warns when a token is sent over plaintext HTTP to a non-loopback host.

**Mitigation.** Attach over an SSH tunnel to localhost, or front the pipeline
with a TLS-terminating proxy and attach to the `https://` address.

### SI-8: Log and metrics APIs can expose record contents

**Severity: MEDIUM.** File: `rhei-runtime/src/http_server.rs`

`/api/logs` serves the pipeline's recent log entries. If your operators log
record fields — including at `debug` or `trace` — those values are readable by
anyone authorised to call the API. Rhei does not and cannot know which of your
fields are sensitive.

**Mitigation.** Set `RHEI_METRICS_TOKEN`. Keep `RHEI_LOG_LEVEL` at `info` or
above in production. Do not log record contents from operators handling
sensitive data.

### SI-9: No supply-chain attestation for release artifacts

**Severity: LOW.**

CI runs `cargo deny check advisories,licenses,bans`, so known-vulnerable and
unlicensed dependencies are caught. There is no signed provenance for build
artifacts (no SLSA attestation, no signed container images, no SBOM published
with releases), so a consumer cannot verify that a binary came from this
source tree.

---

## Resolved issues

### ~~SI-1: Path traversal in the state explorer~~ (RESOLVED)

**Severity: HIGH.** File: `rhei-runtime/src/http_server.rs`

`GET /api/state/operators/{name}` passed `name` straight from the URL into
`dir.join(format!("{name}.checkpoint.json"))`. Axum percent-decodes path
segments after routing, so `..%2F..%2F..%2Ftmp%2Fevil` escaped the checkpoint
directory: any file on the host whose name ends in `.checkpoint.json` could be
read, and its contents returned decoded as JSON, UTF-8, and hex.

Operator names are now resolved against the checkpoint manifest's own operator
list — an allowlist. Only names the pipeline itself recorded are reachable, and
the value that reaches the filesystem is the manifest's copy, not the caller's.
No encoding gets past it.

### ~~SI-2: Worker panic on a malformed Exchange payload~~ (RESOLVED)

**Severity: HIGH.** File: `rhei-runtime/src/erased_buffer.rs`

`ErasedBuffer`'s `Deserialize` impl decoded the selection mask with
`deserialize_batch(bytes).expect("mask deserialization cannot fail")`, followed
by an `expect` on the column downcast. Those bytes arrive from another process
over the unauthenticated data plane (SI-4), so a single frame with a malformed
mask — or a well-formed mask whose column was not boolean — panicked the Timely
worker thread. A remote denial of service requiring only reachability of port
2101.

Mask decoding is now fallible and reports a deserialization error, which the
existing error path handles.

### ~~SI-3: Regex recompiled per key from an untrusted parameter~~ (RESOLVED)

**Severity: MEDIUM.** File: `rhei-runtime/src/http_server.rs`

`GET /api/state/operators/{name}?pattern=…` compiled the caller's regex inside
the per-key filter closure, so one request performed one regex compilation per
key in the operator's state. On a large state that turns a single request into
a sustained CPU burn on the pipeline's own thread pool. An invalid pattern also
silently matched nothing, which reads as "this operator is empty" rather than
"your filter is malformed".

The pattern is now compiled once per request, and an invalid one returns 400.

### ~~SI-6: Unauthenticated state explorer with permissive CORS~~ (RESOLVED)

**Severity: CRITICAL.** File: `rhei-runtime/src/http_server.rs`

`/api/state/**` returns checkpointed application data — the keys a pipeline is
partitioned on and the values it stores — and was served to anyone who could
reach port 9090, with no authentication. `CorsLayer::permissive()` meant any
website a user visited could read that data from their browser and exfiltrate
it.

Three changes, layered so no single one has to hold:

- The state explorer is not routed at all unless `RHEI_STATE_EXPLORER=1`. A
  deployment that never enables it cannot leak through it regardless of how the
  token is managed.
- Every endpoint except the probes requires `Authorization: Bearer <token>`
  when `RHEI_METRICS_TOKEN` is set. Token comparison is constant-time.
- CORS denies all cross-origin requests unless origins are listed explicitly in
  `RHEI_ALLOWED_ORIGINS`.

Probes stay unauthenticated deliberately: kubelet sends no headers, so gating
them would make every authenticated deployment permanently unhealthy. They
reveal only whether the process is running and whether a worker is wedged.

Startup logs a warning when the server binds a non-loopback address without a
token, or when the state explorer is enabled without one.

### ~~SI-10: Unbounded memory from state inspection requests~~ (RESOLVED)

**Severity: MEDIUM.** Files: `rhei-runtime/src/http_server.rs`,
`rhei-core/src/checkpoint.rs`

`GET /api/state/operators` counted entries by deserialising every operator's
entire checkpoint file. One listing request could materialise the whole
pipeline's state in the pipeline's own address space — the cheapest available
way to OOM a healthy process from an observability endpoint.

Listing now reports on-disk size from file metadata and reads nothing. The
entry and key handlers refuse state above `MAX_INSPECTABLE_STATE_BYTES`
(256 MiB) with 413 rather than attempting the read.

---

## Hardening checklist

Before exposing a pipeline beyond your own machine:

- [ ] `RHEI_METRICS_TOKEN` set, from a secret store rather than a manifest literal.
- [ ] `RHEI_STATE_EXPLORER` unset, unless you have a specific reason and a token.
- [ ] `RHEI_ALLOWED_ORIGINS` unset, or listing exact origins — never `*`.
- [ ] Ports 2101 and the coordination port restricted to the cluster (SI-4).
- [ ] `RHEI_METRICS_ADDR` bound to `0.0.0.0` only where the network is trusted;
      otherwise bind loopback and front it with a TLS proxy.
- [ ] `RHEI_REMOTE_ALLOW_HTTP` unset — it permits plaintext object storage and
      is for local testing only.
- [ ] Object storage reached through IRSA / Workload Identity rather than
      long-lived `AWS_*` keys in the environment.
- [ ] Kafka configured through `from_consumer` / `from_producer` with TLS and
      SASL (SI-5).
- [ ] `RHEI_LOG_LEVEL` at `info` or above (SI-8).
- [ ] Container runs as non-root with a read-only root filesystem — the shipped
      `deploy/kubernetes/statefulset.yaml` does both.

---

## What Rhei does not protect against

Stated plainly, so nobody discovers these the hard way:

- **A hostile process inside the cluster.** Any process that can reach the data
  plane is trusted completely (SI-4).
- **Malicious pipeline code.** Operators are ordinary Rust running in-process
  with full privileges. Rhei is not a sandbox.
- **Resource exhaustion by a legitimate workload.** A high-cardinality key space
  can exhaust memory through the dirty half of L1 (see KI-7 in
  [KNOWN-ISSUES.md](KNOWN-ISSUES.md)).
- **Encryption at rest.** Rhei writes checkpoints and state through
  `object_store` and SlateDB without encrypting them itself. Use bucket-level
  encryption (SSE-S3, SSE-KMS, or the equivalent) and an encrypted volume for
  the local checkpoint directory and L2 cache.

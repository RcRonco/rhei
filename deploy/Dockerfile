# Container image for a Rhei pipeline.
#
# A Rhei pipeline is an ordinary Rust binary — your crate, linked against
# `rhei`. This builds that binary and packages it with the shared libraries it
# needs at runtime.
#
#   docker build -f deploy/Dockerfile --build-arg BIN=my-pipeline -t my-pipeline:v1 .
#
# Kafka support links against librdkafka. It is built from source by the
# `cmake-build` feature, so no broker libraries are needed in the final image
# beyond the C++ runtime and TLS/SASL libraries below.

# ── Build stage ─────────────────────────────────────────────────────────────

ARG RUST_VERSION=1.94
FROM rust:${RUST_VERSION}-trixie AS builder

# librdkafka's cmake build needs a toolchain; SASL/TLS headers are needed to
# link against a broker that requires authentication.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        libsasl2-dev \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependency compilation: manifests change far less often than sources,
# so a source-only edit reuses the dependency layer.
COPY Cargo.toml Cargo.lock ./
COPY rhei/Cargo.toml            rhei/
COPY rhei-core/Cargo.toml       rhei-core/
COPY rhei-runtime/Cargo.toml    rhei-runtime/
COPY rhei-macros/Cargo.toml     rhei-macros/
COPY rhei-cli/Cargo.toml        rhei-cli/

# Stub sources so `cargo build` resolves and compiles the dependency graph
# without the real code.
RUN mkdir -p rhei/src rhei-core/src rhei-runtime/src rhei-macros/src rhei-cli/src \
    && echo "" > rhei/src/lib.rs \
    && echo "" > rhei-core/src/lib.rs \
    && echo "" > rhei-runtime/src/lib.rs \
    && echo "" > rhei-macros/src/lib.rs \
    && echo "fn main() {}" > rhei-cli/src/main.rs \
    && cargo build --release --workspace 2>/dev/null || true

COPY . .

# Which binary to ship. Defaults to the CLI; override for your own pipeline.
ARG BIN=rhei-cli
ARG FEATURES=""

# `touch` defeats the stub-source mtime cache so the real sources rebuild.
RUN find . -name '*.rs' -exec touch {} + \
    && if [ -n "$FEATURES" ]; then \
         cargo build --release --bin "$BIN" --features "$FEATURES"; \
       else \
         cargo build --release --bin "$BIN"; \
       fi \
    && cp "target/release/$BIN" /build/pipeline \
    && strip /build/pipeline

# ── Runtime stage ───────────────────────────────────────────────────────────

FROM debian:trixie-slim AS runtime

# ca-certificates: TLS to S3/GCS/Azure and to Kafka brokers.
# libsasl2-2, libssl3: runtime halves of the build-stage -dev packages.
# libstdc++6: librdkafka's C++ runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsasl2-2 \
        libssl3 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user. The numeric UID lets Kubernetes enforce
# `runAsNonRoot` without resolving the name inside the image.
RUN groupadd --gid 10001 rhei \
    && useradd --uid 10001 --gid rhei --home-dir /var/lib/rhei --create-home rhei

COPY --from=builder /build/pipeline /usr/local/bin/pipeline

# Checkpoints and the L2 NVMe cache live here. Mount a volume over it: with
# remote state configured this holds only recoverable cache, but a local-state
# pipeline keeps its only copy of state here.
RUN mkdir -p /var/lib/rhei/checkpoints && chown -R rhei:rhei /var/lib/rhei
VOLUME ["/var/lib/rhei"]

USER 10001:10001
WORKDIR /var/lib/rhei

ENV RHEI_CHECKPOINT_DIR=/var/lib/rhei/checkpoints \
    RHEI_METRICS_ADDR=0.0.0.0:9090 \
    RHEI_LOG_LEVEL=info \
    RHEI_JSON_LOGS=1

# 9090 HTTP (health, metrics, API); 2101 Timely inter-process data plane.
EXPOSE 9090 2101

# No shell wrapper: the process must receive SIGTERM directly, or the graceful
# drain never starts and Kubernetes SIGKILLs it after the grace period.
ENTRYPOINT ["/usr/local/bin/pipeline"]

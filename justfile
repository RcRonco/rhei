default:
    @just --list

# ── Build & Check ───────────────────────────────────────────────────

# Type-check the entire workspace
check:
    cargo check --workspace --all-targets

# Build the entire workspace
build:
    cargo build --workspace --all-targets

# ── Formatting & Linting ────────────────────────────────────────────

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Fix formatting
fmt:
    cargo fmt --all

# Run clippy lints
clippy:
    cargo clippy --workspace --all-targets --no-deps -- -D warnings

# Run cargo deny (licenses, advisories, bans)
deny:
    cargo deny check advisories,licenses,bans

# ── Testing ─────────────────────────────────────────────────────────

# Run all unit and integration tests (no external deps)
test:
    cargo test --workspace

# Run a single test by name
test-one name:
    cargo test --workspace {{ name }} -- --nocapture

# Run tests for a single crate
test-crate crate:
    cargo test -p {{ crate }}

# ── E2E Tests ───────────────────────────────────────────────────────

# Run all E2E tests (starts docker services, runs tests, stops services)
e2e: e2e-up e2e-kafka e2e-s3 e2e-checkpoint e2e-down

# Start E2E infrastructure (Kafka + MinIO)
e2e-up:
    docker compose up -d --wait

# Stop E2E infrastructure
e2e-down:
    docker compose down

# Run Kafka E2E tests (requires: just e2e-up)
e2e-kafka:
    cargo test -p rill-runtime --features kafka --test kafka_e2e -- --nocapture
    cargo test -p rill-runtime --features kafka --test kafka_cluster_e2e -- --nocapture

# Run S3/MinIO E2E tests (requires: just e2e-up)
e2e-s3:
    S3_ENDPOINT=http://localhost:9000 \
    S3_BUCKET=rill-test \
    S3_ACCESS_KEY=minioadmin \
    S3_SECRET_KEY=minioadmin \
    S3_REGION=us-east-1 \
    cargo test -p rill-runtime --features remote-state --test s3_e2e -- --nocapture

# Run checkpoint coordination E2E test (no external deps)
e2e-checkpoint:
    cargo test -p rill-runtime --test checkpoint_coord_e2e -- --nocapture

# ── CI (mirrors GitHub Actions) ─────────────────────────────────────

# Run the full CI suite locally
ci: check fmt-check clippy deny test e2e

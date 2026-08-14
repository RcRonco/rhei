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
    cargo test -p rhei-runtime --features kafka --test kafka_e2e -- --nocapture
    cargo test -p rhei-runtime --features kafka --test kafka_cluster_e2e -- --nocapture

# Run S3/MinIO E2E tests (requires: just e2e-up)
e2e-s3:
    S3_ENDPOINT=http://localhost:9000 \
    S3_BUCKET=rhei-test \
    S3_ACCESS_KEY=minioadmin \
    S3_SECRET_KEY=minioadmin \
    S3_REGION=us-east-1 \
    cargo test -p rhei-runtime --features remote-state --test s3_e2e -- --nocapture

# Run checkpoint coordination E2E test (no external deps)
e2e-checkpoint:
    cargo test -p rhei-runtime --test checkpoint_coord_e2e -- --nocapture

# ── Benchmarks ──────────────────────────────────────────────────────

# Run the full criterion benchmark suite
bench:
    cargo bench --workspace

# Run benchmarks for a single crate (e.g. just bench-crate rhei-core)
bench-crate crate:
    cargo bench -p {{ crate }}

# Compile + single-iteration test-run of all benchmarks (fast CI smoke check)
bench-smoke:
    cargo bench --workspace -- --test

# ── Python bindings (rhei-python) ───────────────────────────────────

py_dir := "rhei-python"

# Create the venv (if missing) and install dev tooling (also builds via maturin)
py-setup:
    cd {{ py_dir }} && test -d .venv || uv venv
    cd {{ py_dir }} && uv sync --dev

# Recompile the extension into the venv (editable); runs py-setup first
py-build: py-setup
    # maturin directly, not `uv run` (which would re-sync and clobber the install)
    cd {{ py_dir }} && VIRTUAL_ENV="$PWD/.venv" .venv/bin/maturin develop

# Run the Python test suite (builds first)
py-test: py-build
    cd {{ py_dir }} && .venv/bin/python -m pytest tests/ -q

# Lint the rhei-python crate (excluded from the workspace)
py-clippy:
    cd {{ py_dir }} && cargo clippy --no-deps -- -D warnings

# Check formatting of the rhei-python crate
py-fmt-check:
    cd {{ py_dir }} && cargo fmt -- --check

# ── Documentation ───────────────────────────────────────────────────

# Verify documentation invariants (wiring, justified `ignore`, quick start sync)
docs-check:
    python3 scripts/check-doc-blocks.py

# Compile every Rust example in README.md, API.md and docs/getting-started.md
docs-test:
    cargo test --doc --workspace

# Both documentation checks
docs: docs-check docs-test

# ── CI (mirrors GitHub Actions) ─────────────────────────────────────

# Run the full CI suite locally
ci: check fmt-check clippy deny test docs e2e

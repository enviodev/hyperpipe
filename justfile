# HyperPipe dev tasks. Install `just` (cargo install just) or run the commands directly.

# Build the native workspace (engine, host, source, cli).
build:
    cargo build

# Build all guest (wasm) modules to components.
build-modules:
    cd modules && cargo build --target wasm32-wasip2

# Release artifacts ship in the Docker image — keep the chaos test fixture out.
build-modules-release:
    cd modules && cargo build --target wasm32-wasip2 --release --workspace --exclude test-chaos

# The out-of-tree example module (its own workspace — the template a user copies).
# Not part of `modules/`, so nothing else builds it; scripts/e2e/08 needs the
# .wasm, which is gitignored build output.
build-example-module:
    cd examples/modules/enrich && cargo build --target wasm32-wasip2 --release
    cp examples/modules/enrich/target/wasm32-wasip2/release/enrich.wasm examples/modules/enrich.wasm

# Everything.
build-all: build build-modules build-example-module

# Native tests (config, encoding, source, host integration) + guest module unit tests.
# Set HP_TEST_PG_DSN to also run the postgres-backed tests (they soft-skip without it):
#   docker run -d --name hp-pg -e POSTGRES_PASSWORD=hp -e POSTGRES_DB=hp -p 5436:5432 postgres:16-alpine
#   HP_TEST_PG_DSN=postgres://postgres:hp@127.0.0.1:5436/hp just test
test: build-modules
    cargo test
    cd modules && cargo test

# End-to-end suite: real binary, real wasm, scripted HyperSync, real Postgres.
# See docs/E2E_TEST_PLAN.md. `just e2e --only 05` runs one scenario.
e2e *ARGS: build build-modules build-example-module
    ./scripts/e2e/run-all.sh {{ARGS}}

# Coverage for both workspaces (needs: cargo install cargo-llvm-cov).
coverage: build-modules
    cargo llvm-cov --workspace --summary-only --ignore-filename-regex 'testutil'
    cd modules && cargo llvm-cov --summary-only

# Lint.
clippy:
    cargo clippy --all-targets -- -D warnings

# Validate a pipeline file.
validate FILE:
    cargo run -q -p hp-cli -- validate {{FILE}}

# Run a pipeline (set HYPERPIPE_MODULE_DIR if modules are elsewhere).
run FILE *ARGS:
    HYPERPIPE_MODULE_DIR="$PWD/modules/target/wasm32-wasip2/debug" cargo run -q -p hp-cli -- run {{FILE}} {{ARGS}}

# One-command launch: build-if-missing, load env, validate, run. See RUN.md.
up FILE ENV="" *ARGS:
    ./scripts/run.sh {{FILE}} {{ENV}} {{ARGS}}

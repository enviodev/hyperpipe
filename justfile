# HyperPipe dev tasks. Install `just` (cargo install just) or run the commands directly.

# Build the native workspace (engine, host, source, cli).
build:
    cargo build

# Build all guest (wasm) modules to components.
build-modules:
    cd modules && cargo build --target wasm32-wasip2

build-modules-release:
    cd modules && cargo build --target wasm32-wasip2 --release

# Everything.
build-all: build build-modules

# Native tests (config, encoding, source, host integration) + guest module unit tests.
test: build-modules
    cargo test
    cd modules && cargo test

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

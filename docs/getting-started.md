# Getting started

From zero to a running pipeline, on your machine.

## Prerequisites

- **Rust** ≥ 1.85 (the repo pins a toolchain via `rust-toolchain.toml`; `rustup` handles it)
- the **wasm32-wasip2** target: `rustup target add wasm32-wasip2`
- **Python 3** (only for the offline mock server)
- **Docker** (only for the Postgres examples and the crash test)
- a **HyperSync API token** for live chains — create one at
  <https://app.envio.dev/api-tokens>. Not needed for the offline path below.

## Build

```bash
git clone <repo> && cd datapipelines

cargo build                     # native workspace -> target/debug/hyperpipe
./scripts/build-modules.sh      # WASM modules    -> modules/target/wasm32-wasip2/debug/*.wasm
```

Two builds because there are two workspaces: the native engine (`crates/`) and the guest
modules (`modules/`, compiled to WASM components). See [codebase.md](./codebase.md).

Run the tests any time:

```bash
cargo test                      # engine, config, source, host integration
(cd modules && cargo test)      # module logic (decoder, filter, s3, postgres, webhook)
```

## First run — offline, no token

Terminal 1: a mock HyperSync server that serves one USDC transfer per block:

```bash
PORT=8799 python3 scripts/mock-hypersync.py
```

Terminal 2: a minimal pipeline through the real engine, decoder, and stdout:

```bash
# note: unquoted heredoc — $PWD expands, so the ABI path is absolute
# (relative paths in a pipeline resolve against the YAML's own directory)
cat > /tmp/first.yaml <<YAML
name: first
runtime: { resource_size: s, checkpoint: { path: /tmp/first-ck.db } }
sources:
  - name: eth
    chain_id: 1
    url: http://127.0.0.1:8799
    mode: backfill
    from_block: 19000000
    to_block: 19000050
    confirmations: 0
    query:
      logs:
        - address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
          topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]]
      field_selection:
        log: [address, topic0, topic1, topic2, data, block_number, log_index, transaction_hash]
        block: [number, timestamp, hash]
processors:
  - name: decode
    module: builtin/evm-abi-decoder@1
    inputs: [eth]
    config:
      abis: [{ file: $PWD/examples/abis/erc20.json, events: [Transfer] }]
      on_undecodable: drop
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [decode]
YAML

export HYPERPIPE_MODULE_DIR="$PWD/modules/target/wasm32-wasip2/debug"
./target/debug/hyperpipe validate /tmp/first.yaml
./target/debug/hyperpipe run /tmp/first.yaml
```

You'll see decoded `Transfer` records as NDJSON, then a clean exit at the range end (EOF).
Re-run it: it resumes from the checkpoint and exits immediately — delete `/tmp/first-ck.db`
to replay.

## First live run

```bash
cp examples/pipeline.env.example pipeline.env
$EDITOR pipeline.env            # set HYPERSYNC_BEARER_TOKEN (+ secrets your pipeline uses)

./scripts/run.sh examples/usdc-multichain.yaml pipeline.env
```

The launcher loads the env file, builds anything missing, validates, and runs. Details in
[deployment.md](./deployment.md); env file format in [operations.md](./operations.md).

More ready-made pipelines (real RWA/tokenized-stock contracts, every sink type):
[`examples/README.md`](../examples/README.md).

## The demo

[`DEMO.md`](../DEMO.md) scripts the full tour: validation errors, live multi-chain streaming,
the kill -9 crash-recovery proof (`scripts/crash-test.sh`), sink swapping, and a custom
out-of-tree WASM module.

## Common first-run problems

| Symptom | Cause / fix |
|---|---|
| `read builtin ... build modules first` | WASM modules not built or `HYPERPIPE_MODULE_DIR` wrong. Run `./scripts/build-modules.sh`. |
| Live source logs `hypersync 401` | `HYPERSYNC_BEARER_TOKEN` missing/invalid. |
| `unresolved secrets: PG_MAIN_DSN` | Export `HYPERPIPE_SECRET_PG_MAIN_DSN=...` (or put it in the env file). |
| Pipeline exits immediately with no output | Checkpoint from a previous run — the source is already past your range. Delete the checkpoint DB to replay. |
| YAML parse error at a `${secret:...}` | Put the secret ref on its own line (block style), never inside `{ ... }` flow maps. |

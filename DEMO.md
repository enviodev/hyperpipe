# HyperPipe Demo

A start-to-finish demo that runs on a clean machine with **no HyperSync token** —
it uses a local mock HyperSync server so every stage is exercised offline. Swap
the mock URL for a real chain + `HYPERSYNC_BEARER_TOKEN` to go live.

## 0. Build

```bash
rustup target add wasm32-wasip2
cargo build
./scripts/build-modules.sh
export HYPERPIPE_MODULE_DIR="$PWD/modules/target/wasm32-wasip2/debug"
```

## 1. Validate a pipeline

```bash
HYPERPIPE_SECRET_PG_MAIN_DSN=x HYPERPIPE_SECRET_SLACK_WEBHOOK=y \
  ./target/debug/hyperpipe validate examples/usdc-multichain.yaml
# -> valid: 2 source(s), 3 processor(s), 2 sink(s)
```

Typos, cycles, missing secrets, and unknown chains all fail fast with a pointed
message (see `crates/engine/src/config/tests.rs`).

## 2. Stream: HyperSync → WASM decode → WASM filter → stdout

```bash
# terminal 1: mock chain (one USDC transfer per block)
PORT=8799 python3 scripts/mock-hypersync.py

# terminal 2: run a decode+filter pipeline to stdout
# (unquoted heredoc: $PWD expands — relative paths resolve against the YAML's dir)
cat > /tmp/demo.yaml <<YAML
name: demo
runtime: { resource_size: s }
sources:
  - { name: eth, chain_id: 1, url: http://127.0.0.1:8799, mode: backfill,
      from_block: 19000000, to_block: 19000050, confirmations: 0,
      query: { logs: [{ address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
        topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]] }],
        field_selection: { log: [address,topic0,topic1,topic2,data,block_number,log_index,transaction_hash], block: [number,timestamp,hash] } } }
processors:
  - { name: decode, module: builtin/evm-abi-decoder@1, inputs: [eth],
      config: { abis: [{ file: $PWD/examples/abis/erc20.json, events: [Transfer] }], on_undecodable: drop } }
sinks:
  - { name: out, module: builtin/stdout@1, inputs: [decode] }
YAML
./target/debug/hyperpipe run /tmp/demo.yaml
```

You'll see decoded Transfer records as NDJSON — `value` as a decimal string,
block metadata joined in.

## 3. Crash recovery — kill -9, no data lost

```bash
docker run -d --name hp-pg -e POSTGRES_PASSWORD=hp -e POSTGRES_DB=hp -p 5433:5432 postgres:16-alpine
./scripts/crash-test.sh
# -> PASS: 300/300 rows, 0 gaps, survived N kill -9
```

Backfills 300 blocks into Postgres, kills the process mid-stream repeatedly, and
resumes from the SQLite checkpoint each time. Idempotent upsert + per-sink cursors
mean every block lands exactly once.

## 4. Swap a sink — Postgres, webhook, or Parquet-to-S3

The same decoded stream can fan out to any sink by editing YAML:

- **Postgres** (`builtin/postgres@1`) — upsert with auto-DDL and `column_map`.
- **Webhook** (`builtin/webhook@1`) — POST NDJSON, host allowlist-gated.
- **S3 Parquet** (`builtin/s3@1`) — buffers rows, flushes a Parquet object after
  `flush_rows`, deterministic keys. Test it with a local dir (no S3 creds):

  ```yaml
  sinks:
    - { name: archive, module: builtin/s3@1, inputs: [decode],
        connections: [lake], config: { connection: lake, format: parquet, flush_rows: 100 } }
  connections:
    lake: { type: s3, prefix: "usdc/", local_path: /tmp/lake }
  ```
  ```bash
  find /tmp/lake -name '*.parquet'   # -> usdc/1/<from>-<to>-<rows>.parquet
  ```

## 5. Custom module — bring your own WASM

```bash
cd examples/modules/enrich && cargo build --target wasm32-wasip2 --release
```

Reference it with `module: { file: .../enrich.wasm }`. It adds `usd_estimate`
and a `whale` flag to each transfer — proving any Rust (or TS/Go via the SDK)
compiled to WASM plugs into the pipeline, sandboxed by the same capability model.

## The pitch

- **Ingestion**: HyperSync — up to 2000× faster than RPC, 70+ EVM chains + Fuel.
- **Extensible**: every processor and sink is a swappable WASM module; users bring
  their own, in any language that targets WASM.
- **Sandboxed**: modules do compute only; all IO goes through capability-gated host
  imports (allowlisted HTTP, granted connections). Secrets never enter WASM.
- **Correct**: at-least-once with SQLite checkpoints + idempotent sinks; proven by
  `crash-test.sh`.
- **Self-hosted**: one binary, one YAML. No cloud lock-in.

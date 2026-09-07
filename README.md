# HyperPipe

> **Beta.** HyperPipe is under active development. The YAML schema, the module
> contract (`wit/hyperpipe.wit`) and the CLI may change between releases without
> a compatibility guarantee. It is a self-hosted tool; there is currently no
> commitment to offer it as a hosted service on Envio's cloud.

WASM data pipelines on **Envio HyperSync** — a self-hosted alternative to Goldsky Turbo.
One YAML file declares chains (sources), an ordered DAG of WASM processors (ABI decode,
filter, custom), and one or more WASM sinks. The engine streams data through, sandboxes
every module, and recovers with at-least-once delivery.

**📚 Full documentation: [docs/](./docs/README.md)** — getting started, YAML reference, module
reference + authoring, codebase guide, deployment (binary/Docker/K8s), operations.

See **[ARCHITECTURE.md](./ARCHITECTURE.md)** for the full design and **[ACTION_PLAN.md](./ACTION_PLAN.md)**
for the milestone plan. Section refs (§) point at ARCHITECTURE.md.

```
hyperpipe run pipeline.yaml
```

## Layout

```
crates/
  encoding/          batch envelope + JSON codec (shared host + guest)
  engine/            config parse/validate, chain registry, DAG model
  source-hypersync/  HyperSync /query cursor loop -> batch envelopes
  wasm-host/         wasmtime embedding: WIT bindings, host imports, pools, limits, cwasm cache
  cli/               `hyperpipe` binary + DAG runtime (channels, fan-out, backpressure)
wit/hyperpipe.wit    the module contract (processor + sink worlds)
modules/             guest (wasm) workspace
  sdk/               hyperpipe-sdk: Processor/Sink traits + export macros
  evm-abi-decoder/   raw logs -> decoded records (alloy)
  filter/            predicate filter (decimal-string aware)
  stdout-sink/  blackhole-sink/
examples/            demo pipeline + ERC20 ABI
```

## Build & run

**One command** (builds what's missing, loads secrets from an env file, validates, runs) — see [RUN.md](./RUN.md):

```bash
cp examples/pipeline.env.example pipeline.env   # fill in HYPERSYNC_BEARER_TOKEN + secrets
./scripts/run.sh examples/usdc-multichain.yaml pipeline.env
```

Or step by step:

```bash
# 1. native workspace
cargo build

# 2. guest modules -> wasm components (needs the wasm32-wasip2 target)
rustup target add wasm32-wasip2
./scripts/build-modules.sh            # or: just build-modules

# 3. validate a pipeline
./target/debug/hyperpipe validate examples/usdc-multichain.yaml

# 4. run it (needs a HyperSync token + the module dir)
export HYPERSYNC_BEARER_TOKEN=...     # create at https://app.envio.dev/api-tokens
export HYPERPIPE_MODULE_DIR="$PWD/modules/target/wasm32-wasip2/debug"
./target/debug/hyperpipe run examples/usdc-multichain.yaml --debug-stdout
```

`--debug-stdout` replaces configured sinks with a native NDJSON printer, so you can watch
decoded records without configuring Postgres/webhook or their secrets.

Secrets: `${secret:NAME}` in YAML resolves from env `HYPERPIPE_SECRET_<NAME>`.

**Docker / Kubernetes**: multi-stage [`Dockerfile`](./Dockerfile) + StatefulSet manifests in
[`deploy/k8s/`](./deploy/k8s). The pipeline is a single-writer stateful process (SQLite checkpoint
on a PVC) with `/healthz` + `/readyz` probes and SIGTERM drain. See [K8S.md](./K8S.md).

## Test

```bash
cargo test                            # native: encoding, config, source, host integration
(cd modules && cargo test)            # guest module unit tests (decoder, filter)
```

The host integration test loads the real decoder + stdout components and runs a batch
through wasmtime; it soft-skips if the wasm artifacts aren't built.

## Status (hackathon build)

Working end-to-end today — **`hyperpipe run` streams HyperSync → WASM ABI decode → WASM
filter/enrich → sink(s)**, multi-chain, with fan-out, backpressure, module sandboxing
(capability-gated host imports), epoch/memory limits, precompiled-component cache, and
at-least-once crash recovery. See [DEMO.md](./DEMO.md).

| Milestone | State |
|---|---|
| M0 scaffold, WIT contract | ✅ |
| M1 config parse + validation (19 tests) | ✅ |
| M2 HyperSync source loop (live/backfill/both, EOF) | ✅ |
| M3 DAG runtime (channels, fan-out, backpressure, status) | ✅ |
| M4 WASM host (wasmtime, imports, pools, limits, cwasm) | ✅ |
| M5 guest SDK (Processor/Sink traits + macros + host wrappers) | ✅ |
| M6 evm-abi-decoder (4 golden tests) | ✅ |
| M7 SQLite checkpointing + crash recovery | ✅ `crash-test.sh`: 300/300, 0 gaps, 15× kill -9 |
| Reorg handling: rollback_guard tracking → rollback control → sink invalidation + cursor rewind | ✅ `reorg-test.sh`: 120/120 rows, 65 forked rows replaced, 0 stale |
| M9 sinks: stdout, blackhole, postgres, webhook, **s3 (Parquet)** | ✅ verified vs real pg / webhook / Parquet file |
| M11 filter builtin (4 tests) + out-of-tree `enrich` example | ✅ |
| host imports: log, metric, kv, http, sql-exec/batch, **blob-put** | ✅ (kafka = phase 1) |
| M12 benchmark vs Turbo | ⏳ (not run this pass) |

Verified end-to-end (mock HyperSync + real backends): decode → Postgres upsert with auto-DDL;
decode → webhook POST (capability-gated, denial path tested); decode → S3 Parquet flush after
N rows (deterministic keys); kill -9 mid-backfill with exact-once row counts.

Known gaps vs Turbo (by design): EVM + Fuel only (no Solana), no SQL transforms yet
(DataFusion is the phase-3 plan). See ARCHITECTURE.md §1.2.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this project by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.

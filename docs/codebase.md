# Codebase guide

How the code is laid out, how a batch flows through it, and the invariants you must not break.
Design rationale: [ARCHITECTURE.md](../ARCHITECTURE.md) (§ refs below point there).

## Two workspaces

```
datapipelines/
├── crates/     native workspace — the engine, compiled to the `hyperpipe` binary
└── modules/    guest workspace  — WASM components, compiled with --target wasm32-wasip2
```

They are separate on purpose: guest crates must never leak into the native build (and vice
versa), and each side pins its own profile. `modules/` is excluded from the root workspace.

## Crate map (native)

| Crate | Role | Key files |
|---|---|---|
| `crates/encoding` (`hp-encoding`) | The **batch envelope** shared by host and guests: `Batch`, `BatchKind`, `BlockRange`, `ControlRecord`, JSON codec, `KvStore` trait. Target-agnostic, dependency-light. | `src/lib.rs` |
| `crates/engine` (`hp-engine`) | **Config**: typed YAML schema, `deny_unknown_fields`, secret resolution, chain registry, DAG validation (§4.1). **Checkpoints**: SQLite cursors + module KV (§8.2). | `src/config/mod.rs`, `src/config/secret.rs`, `src/config/chains.{rs,toml}`, `src/checkpoint.rs` |
| `crates/source-hypersync` (`hp-source-hypersync`) | HyperSync ingestion: HTTP client + the **cursor loop**. The loop's decisions are a pure function (`plan_step`) — that's where confirmations, `to_block`, and EOF semantics live. | `src/client.rs`, `src/lib.rs` |
| `crates/wasm-host` (`hp-wasm-host`) | wasmtime embedding: WIT bindings (`bindgen!`), **host imports** (log/metric/kv/http/sql/blob — the capability boundary, §6.3), instance pools, memory/epoch limits, `.cwasm` cache. | `src/host_impl.rs`, `src/lib.rs` |
| `crates/cli` (`hp-cli` → `hyperpipe`) | The binary: `validate`/`run`, plus the **DAG runtime** — channel wiring, fan-out, backpressure, sink acks, the checkpoint tick, shutdown, health server. | `src/main.rs`, `src/pipeline.rs`, `src/health.rs` |

## Crate map (guest)

| Crate | Role |
|---|---|
| `modules/sdk` (`hyperpipe-sdk`) | Guest SDK: `Processor`/`Sink` traits, `export_processor!`/`export_sink!` macros (generate WIT bindings in a private `__hp_bindings` module, own the envelope codec + control passthrough), typed `hp_host::*` wrappers. |
| `modules/evm-abi-decoder` | Flagship processor. Pure decode logic in `decode.rs` (native-testable), WASM glue behind `#[cfg(target_arch = "wasm32")]`. |
| `modules/filter` | Predicate filter — pure logic in `filter.rs` (sign-aware decimal compare). |
| `modules/postgres-sink` | SQL generation in `sql.rs` (DDL inference, upsert, `::numeric` casts); glue calls `hp_host::sql_batch`. |
| `modules/webhook-sink` | NDJSON body in `body.rs`; glue calls `hp_host::http`. |
| `modules/s3-sink` | `buffer.rs`: per-chain `ChainBuffers`, Parquet/NDJSON encoding, deterministic keys; glue calls `hp_host::blob_put`. |
| `modules/{stdout,blackhole}-sink` | Trivial reference sinks. |

**Pattern:** every non-trivial module keeps its logic in a plain module tested natively
(`cargo test` in `modules/`), with the WASM glue cfg-gated. Follow it for new modules.

`wit/hyperpipe.wit` is **the contract** — both `wasmtime::bindgen!` (host) and
`wit_bindgen::generate!` (guests) consume it. Treat it like a public API; version it.

## Life of a batch

1. **Source** (`source-hypersync/src/lib.rs::run`) — cursor loop: query HyperSync from
   `cursor`; `plan_step()` decides `Wait` / `Emit{effective_next}` / `Eof`, clamping to
   `head − confirmations` and `to_block`; over-window records are truncated (re-fetched once
   confirmed). Records are chunked to `batch.max_records`; each chunk becomes a `Batch` with a
   deterministic id `source:from:to:seq`. Non-final chunks set `ack_block = range.from`
   (see invariant 2).
2. **Fan-out** (`cli/src/pipeline.rs::source_task`) — the batch is cloned to every consumer's
   bounded mpsc channel. Full channels block the send → backpressure propagates to the source.
3. **Processor** (`processor_task`) — the batch crosses into WASM on a `spawn_blocking`
   thread: encoded to JSON bytes, `process()` called on a pooled wasmtime instance
   (`wasm-host/src/lib.rs::WasmProcessor::process`), outputs decoded back. A module that drops
   every record still yields an empty batch downstream (keeps acks flowing). Control batches
   pass through untouched (SDK does this).
4. **Sink** (`sink_task`) — `write()` on the sink module; the module does its IO through host
   imports, which enforce the capability grants. On `Ok`, the task records
   `ack(source, sink, batch.ack_block())` in the in-memory watermark map. On `Err`: retry ×3
   with backoff, then the branch pauses (cursor freezes → nothing is lost).
5. **Checkpoint tick** (`checkpoint_once`, every 2 s) — snapshot watermarks → `flush()` every
   sink → persist the snapshot + dirty module-KV in **one SQLite transaction**
   (`engine/src/checkpoint.rs`). Snapshot-before-flush is what makes buffering sinks safe
   (invariant 3).
6. **Restart** — each source resumes from `MIN(cursor)` over the sinks it reaches (BFS over
   the DAG, `reachable_sinks_per_source`), never below `from_block`. Everything since is
   replayed; idempotent sinks collapse duplicates. That's the at-least-once contract (§8.1).

## Invariants (do not break)

1. **Numbers ≥ 2^53 are decimal strings** in JSON records (§3.1). Float truncation of uint256
   is the classic pipeline bug; the decoder, filter, postgres sink, and parquet writer all
   assume this.
2. **`ack_block` chunk rule** (§8.2 r1): when one block range is split into chunk batches,
   only the final chunk carries the full-range ack; earlier chunks pin the cursor to
   `range.from`. `Batch::derive()` preserves it — always use `derive()` in modules.
3. **Flush-before-persist** (§8.2 r5): the checkpoint snapshot is taken *before* sinks flush,
   and persisted *after*. Reordering this loses data in buffering sinks (s3).
4. **Capability gating** (§6.3): all module IO goes through host imports checked against the
   node's YAML grants (`permissions.http` hostname exact-match, `connections` by name).
   Secrets resolve host-side; never hand a DSN or credential into guest memory.
5. **Deterministic identity**: `batch_id` and s3 object keys are pure functions of
   (source, range, seq/rows). Replays must produce identical ids/keys — no timestamps or
   randomness in them.
6. **WASM calls run on `spawn_blocking` threads.** Host imports use `handle.block_on`, which
   panics on a tokio worker thread. Never call `WasmProcessor::process`/`WasmSink::write`
   from async context directly.

## How do I…

**Add a built-in module?** New crate under `modules/` (copy `filter`), add to
`modules/Cargo.toml` members, implement the SDK trait + export macro, then register the
artifact name in `builtin_wasm_file()` in `crates/cli/src/pipeline.rs`.

**Add a host import?** Extend the `host` interface in `wit/hyperpipe.wit`; implement the new
trait method in `crates/wasm-host/src/host_impl.rs` (gate it on grants!); add a typed wrapper
in both macro bodies in `modules/sdk/src/lib.rs`; rebuild both workspaces.

**Add a chain?** One entry in `crates/engine/src/config/chains.toml` (name → chain_id + url).
Users can always bypass the registry with `chain_id:` + `url:`.

**Add a config key?** The relevant struct in `crates/engine/src/config/mod.rs`. Everything is
`deny_unknown_fields` — add the field, a default, a validation rule if needed, and a test in
`config/tests.rs`.

## Test map

| Suite | Where | Covers |
|---|---|---|
| `cargo test -p hp-encoding` | `encoding/src/lib.rs` | envelope roundtrip, `ack_block` semantics, control records |
| `cargo test -p hp-engine` | `config/tests.rs`, `checkpoint.rs` | 20 config/validation cases; cursor monotonicity, MIN-restore, snapshot-before-flush, KV atomicity |
| `cargo test -p hp-source-hypersync` | `client.rs`, `lib.rs` | response parsing/denormalization; 10 `plan_step` cases (confirmations, EOF, stalls) |
| `cargo test -p hp-wasm-host` | `host_impl.rs`, `tests/integration.rs` | allowlist security; real wasmtime round-trip through decoder + stdout components (soft-skips if modules unbuilt) |
| `(cd modules && cargo test)` | each module's logic file | decoder golden tests, filter semantics, SQL generation, parquet buffers, NDJSON |
| `scripts/crash-test.sh` | end-to-end | kill -9 ×N mid-backfill → exact row counts, no gaps (needs Docker) |

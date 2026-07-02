# HyperPipe — Engineering Action Plan

> **Implementation status (this build):** M0–M7, M9 (all sinks incl. s3 Parquet), M11 (filter +
> out-of-tree `enrich`) are **done and verified** — see [README.md](./README.md) status table and
> [DEMO.md](./DEMO.md). Crash recovery proven by `scripts/crash-test.sh` (300/300 rows, 0 gaps,
> 15× kill -9). Remaining: M12 benchmark vs Turbo; phase-1+ items below.
>
> Companion to [ARCHITECTURE.md](./ARCHITECTURE.md) — section references (§) point there.
> Structure: small milestones (M0–M12 = hackathon MVP), each independently mergeable, each with a Definition of Done and a verify command. Later phases broken down the same way.
>
> **Team assumption:** 2 engineers (A = engine/source/state, B = WASM/modules). Solo? Follow milestone order as written — it's already dependency-sorted. Tracks show what can run in parallel.

---

## Milestone map (MVP)

```
M0 scaffold
 ├── Track A: M1 config → M2 source → M3 pipeline core → M7 checkpointing → M8 recovery
 └── Track B: M4 wasm host → M5 guest SDK → M6 decoder → M9 sinks
                    (M3 + M6 integrate at M10) → M10 e2e → M11 filter+custom → M12 demo/bench
```

Critical path: M0 → M4 → M5 → M6 → M10 → M12. Protect Track B's time.

---

## M0 — Repo scaffold (½ day, both engineers)

Goal: `cargo build` green, empty crates wired, CI compiles WASM modules.

- [ ] Cargo workspace per §13 layout: `crates/{cli,engine,source-hypersync,wasm-host,encoding,sdk}`, `modules/`, `wit/`, `examples/`
- [ ] `wit/hyperpipe.wit` checked in verbatim from §6.2 (this is the contract — commit it first, change it consciously)
- [ ] Deps pinned: `tokio`, `wasmtime` (component-model feature), `hypersync-client`, `sqlx` (sqlite+postgres, runtime-tokio), `serde`/`serde_json`, `clap`, `tracing`, `anyhow`/`thiserror`; guests: `wit-bindgen`, `alloy` (decoder only)
- [ ] `modules/*` build script or `just build-modules`: `cargo build --target wasm32-wasip2` → `.wasm` artifacts land in `target/modules/`
- [ ] CLI skeleton: `hyperpipe run <file>` / `hyperpipe validate <file>` parse args, print "not implemented"
- [ ] `rust-toolchain.toml`, `.gitignore`, CI (build + clippy + one placeholder test)

**DoD:** fresh clone → `cargo build && just build-modules` succeeds.
**Verify:** `hyperpipe validate examples/usdc-multichain.yaml` prints "not implemented" (file itself lands in M1).

---

## M1 — Config: parse + validate (½–1 day, Eng A)

Goal: YAML from §4 fully parses into typed structs; §4.1 validation enforced.

- [ ] Serde structs for full schema: `runtime`, `sources[]`, `processors[]`, `sinks[]`, `connections`, `permissions` — `#[serde(deny_unknown_fields)]` everywhere (§4.1 rule 6)
- [ ] `module:` field: string form `builtin/<name>@<major>` and map form `{ file: path }`
- [ ] `${secret:NAME}` detection + resolution from `HYPERPIPE_SECRET_<NAME>` env (§9 MVP); missing secret = startup error listing all missing names at once
- [ ] DAG validation: inputs resolve, no cycles (topo sort), ≥1 source, ≥1 sink, no orphan processors
- [ ] Mode rules: `backfill` requires `to_block`, `live` forbids it
- [ ] Chain registry `chains.toml`: ethereum, base, arbitrum, optimism, polygon (name → HyperSync URL + chain_id); `url:`/`chain_id:` overrides
- [ ] Error messages name the YAML path (`sinks[0].config.unique_key: ...`) — this is UX, engineers demo with it
- [ ] Commit `examples/usdc-multichain.yaml` + `examples/abis/erc20.json`

**DoD:** valid file passes; 8+ unit tests for bad configs (cycle, typo key, missing secret, backfill w/o to_block...) each produce a targeted error.
**Verify:** `hyperpipe validate examples/usdc-multichain.yaml` → "valid: 2 sources, 3 processors, 2 sinks".

---

## M2 — HyperSync source loop (1 day, Eng A)

Goal: one source streams real chain data as envelope batches into a channel.

- [ ] `Source` task: cursor walk per §5.1 — query → split into `batch.max_records` chunks → send → `cursor = next_block`
- [ ] YAML `query` → `hypersync-client` query mapping (logs + field_selection; transactions/blocks/traces structs present, logs is the tested path)
- [ ] `confirmations` lag: target = `archive_height - confirmations`; live mode polls height (2s interval) when caught up
- [ ] `both`/`backfill` modes; backfill emits `eof` control record at `to_block`
- [ ] Block metadata denormalization: inject `block_timestamp`/`block_hash` into log records when `field_selection.block` present (§5.1)
- [ ] Envelope v1 construction in `encoding` crate: deterministic `batch_id`, uint→decimal-string rule (§3.1)
- [ ] HyperSync error retry: exponential backoff, cursor untouched, log warn
- [ ] `HYPERSYNC_BEARER_TOKEN` env passthrough

**DoD:** temp debug sink prints NDJSON; both example sources stream concurrently; kill + restart from hardcoded block works.
**Verify:** `cargo run -- run examples/usdc-multichain.yaml --debug-stdout` shows live USDC logs from both chains within seconds; backfill of 10k-block range terminates with EOF.

---

## M3 — Pipeline core: DAG runtime (1 day, Eng A)

Goal: config → running graph of tasks + channels; backpressure works.

- [ ] Build runtime DAG from validated config: spawn source tasks, stage tasks, wire bounded mpsc channels (capacity from `resource_size` table §11.1)
- [ ] Fan-out: `Arc<Bytes>` clone per consumer edge, per-edge send
- [ ] Merge: multi-input stage `select!`s its input channels
- [ ] Stage trait: `async fn handle(batch) -> Result<Vec<Batch>>` — native stub stages for now (identity processor, stdout sink); WASM plugs in at M10
- [ ] Control record passthrough in engine (processors get them at M5 via SDK; engine routes them like data)
- [ ] Graceful shutdown: ctrl-c → sources stop → channels drain → sinks flush → exit 0
- [ ] Status line every 5s per source: block / head / lag / rec-s / per-sink state (§10)

**DoD:** full example YAML runs end-to-end with stub stages; slow-sink test (sleep in stub) visibly pauses source (log proves backpressure).
**Verify:** run example 60s → status lines show lag ≈ confirmations, no unbounded memory.

---

## M4 — WASM host (1–1.5 days, Eng B) — starts right after M0, parallel to M1–M3

Goal: wasmtime loads a component, `init` + `process` round-trip works, limits enforced.

- [ ] wasmtime engine config: component model on, epoch interruption on, pooling allocator
- [ ] Host bindings for `wit/hyperpipe.wit` (wasmtime `bindgen!`): `processor` + `sink` worlds
- [ ] Host imports v1: `log` (→ tracing, tagged with node name), `metric-add` (counter map), `kv-get`/`kv-set` (in-memory now, SQLite-backed at M7), `http` (reqwest + hostname allowlist from `permissions`, deny-by-default), `sql-exec`/`sql-batch` (sqlx pool by connection name; unknown name = err)
- [ ] Instance pool per stage: N instances (§11.1), `init(ctx)` once per instance, round-robin dispatch
- [ ] Limits: memory cap via StoreLimits, epoch deadline per call (5s/10s), trap → recycle instance + retry batch ×3 → stage `degraded` + branch pause (§6.2 error contract)
- [ ] `.cwasm` precompile cache keyed by module hash (`~/.cache/hyperpipe/`)
- [ ] Test harness: hand-written echo component exercises every import

**DoD:** echo component processes a batch; infinite-loop component gets killed at deadline and pipeline survives; disallowed `http` host rejected.
**Verify:** `cargo test -p wasm-host` — round-trip, timeout, allowlist, oom tests green.

---

## M5 — Guest SDK (½–1 day, Eng B)

Goal: writing a module = implementing one trait; envelope/control plumbing invisible.

- [ ] `hyperpipe-sdk` crate: `wit-bindgen` guest bindings + ergonomic layer
- [ ] `Processor` trait: `fn init(config: serde_json::Value) -> Result<Self>`, `fn process(&mut self, batch: Batch) -> Result<Vec<Batch>>` — SDK decodes/encodes envelope, auto-passes control records through
- [ ] `Sink` trait: `init`, `write(&mut self, batch)`, `flush`
- [ ] `export_processor!(MyType)` / `export_sink!(MyType)` macros
- [ ] Host import wrappers: `host::log!()`, `host::http(req)`, `host::sql_batch(...)`, `host::kv` — typed, not raw lists of tuples
- [ ] Doc comments good enough to be the module-authoring guide seed

**DoD:** echo module from M4 rewritten in ≤30 lines on the SDK, behavior identical.
**Verify:** SDK-based echo passes M4 test suite.

---

## M6 — ABI decoder module (1 day, Eng B)

Goal: the flagship processor — raw logs → `decoded` records.

- [ ] `modules/evm-abi-decoder`: alloy `JsonAbi` parse at `init`; build topic0 → event map across all configured ABIs
- [ ] `events:` filter per ABI file; `on_undecodable: drop | passthrough | error` (§7)
- [ ] Decode indexed (topics) + non-indexed (data) params; output `decoded` record shape from §3.1 exactly — uint256 as decimal string
- [ ] ABI files read host-side at config load, contents passed inside module `config` JSON (modules have no filesystem — §6.3)
- [ ] Golden tests: fixture batch of real USDC Transfer logs → expected decoded JSON (commit fixtures)
- [ ] Malformed-log fuzz-ish tests: short data, missing topics → per `on_undecodable`, never a panic

**DoD:** golden tests green; decodes mixed-event batch (Transfer + Approval) with `events: [Transfer]` filtering correctly.
**Verify:** `cargo test -p evm-abi-decoder` + manual: fixture through wasm-host harness.

---

## M7 — Checkpointing (1 day, Eng A)

Goal: §8.2 exactly — SQLite cursors + module KV, atomic.

- [ ] SQLite schema: `cursors(source, sink, next_block, updated_at)`, `module_kv(module, key, value)`; WAL mode
- [ ] Ack plumbing: sink stage reports ack(batch_id) → coordinator; per-sink watermark tracker (in-order per source) advances `next_block`
- [ ] Cursor write + dirty `module_kv` keys in one transaction (§8.2 rule 3); flush cadence: every batch or 2s, whichever later
- [ ] **Flush-before-persist (§8.2 rule 5):** each tick snapshots watermarks → `flush()` all sinks → persist snapshot in one txn. Makes buffering sinks (s3, §7.1) at-least-once
- [ ] Restart: source resumes from `MIN(cursor)` over its **reachable** sinks (DAG BFS); missing rows = `from_block`
- [ ] Wire M4's kv host import to this store (dirty-key buffer, flushed with checkpoint txn)
- [ ] `checkpoint.store: postgres` variant behind the same trait — struct only, implementation phase 1 (don't gold-plate)

**DoD:** unit tests: fan-out watermark (fast+slow sink), restart-resume math, kv atomicity.
**Verify:** run example → `sqlite3 state/usdc.db 'select * from cursors'` shows advancing per-sink rows.

---

## M8 — Crash recovery proof (½ day, Eng A)

Goal: the demo's money moment, scripted and repeatable.

- [ ] `scripts/crash-test.sh`: start backfill pipeline → `kill -9` at random 5–15s → restart → wait for EOF → assert row count == expected count && no gaps (`generate_series` check vs sqlite cursor)
- [ ] Duplicate check: upsert sink → exact count; document duplicate-delivery behavior for webhook path
- [ ] Fix whatever this shakes out (it will shake something out — budget the half day honestly)

**DoD:** crash-test loops 10× green in CI (or locally, scripted).
**Verify:** `./scripts/crash-test.sh` prints `PASS: 48210/48210 rows, 0 gaps, survived kill -9`.

---

## M9 — Real sinks (2–2.5 days, Eng B)

Goal: postgres + webhook + s3 as real WASM modules on the SDK, plus the host IO imports they use.

- [ ] `stdout@1` + `blackhole@1` (trivial; replaces M3 native stubs — proves sink world end-to-end first)
- [ ] `postgres@1`: `mode: insert|upsert` (`ON CONFLICT (unique_key) DO UPDATE`), `column_map` incl. dotted paths (`params.from`), batch → single `sql-batch` txn
- [ ] `postgres@1` auto-DDL: opt-in `create_table: true`, infer from first batch (text/bigint/numeric/jsonb), log generated DDL loudly (§14 risk 4)
- [ ] `webhook@1`: POST NDJSON via `host::http`, `max_batch` re-chunking, retry/backoff from config, non-2xx = err (engine retry ladder applies)
- [ ] **Host `blob-put` wiring:** `object_store` client per `s3` connection (S3/R2/MinIO via env creds, or `local_path` filesystem store for dev/tests); capability-gated by granted connection name
- [ ] **`s3@1` buffered Parquet sink (§7.1):** in-memory row buffer; flush on `flush_rows` / `flush_interval_ms` / `flush()`; encode Parquet (arrow-rs in wasm) or NDJSON per `format`; deterministic key `{prefix}/{chain_id}/{first_block}-{last_block}-{rows}.{ext}`; call `blob-put`
- [ ] `s3@1` schema inference from first batch; idempotent overwrite on re-flush; relies on M7 flush-before-persist for at-least-once
- [ ] Integration tests: dockerized postgres (`docker compose up pg`) → assert rows; s3 sink → `local_path` dir → assert Parquet file with expected row count

**DoD:** postgres + webhook run against real postgres + a request-bin webhook; s3 sink writes a Parquet file to a local dir after `flush_rows`.
**Verify:** `docker compose up -d pg && cargo test -p postgres-sink --features integration`; `hyperpipe run` with an s3 sink (`local_path`) produces `<dir>/<chain>/<range>.parquet`.

---

## M10 — E2E integration (½–1 day, both)

Goal: replace every stub; full example YAML runs on real everything.

- [ ] Wire wasm-host stages into M3 DAG runtime (stage trait impl over instance pool)
- [ ] Built-in module resolution: `builtin/<name>@<major>` → `include_bytes!` embedded artifacts; `file:` → load from path
- [ ] `validate` now also: load modules, `init(config)` dry-run (§4.1 rule 3)
- [ ] Full run: 2 chains → decoder → postgres + stdout; fix integration fallout
- [ ] Re-run M8 crash test on full pipeline (not stubs)

**DoD:** `hyperpipe run examples/usdc-multichain.yaml` streams live decoded transfers from both chains into postgres.
**Verify:** `select chain_id, count(*) from usdc_transfers group by 1` shows both chains growing.

---

## M11 — Filter + custom module example (½–1 day, Eng B)

Goal: extensibility claim is demonstrable, not theoretical.

- [ ] `filter@1` built-in: MVP ops on config (field path, `eq/ne/gt/gte/lt/lte/in`, decimal-string aware numeric compare); `expr` CEL syntax deferred to phase 1
- [ ] `examples/modules/enrich/`: standalone crate on `hyperpipe-sdk`, own README with build command — this directory is the user template, treat its DX as a feature
- [ ] Example does something visible (e.g. add `usd_value` via `host::http` to a price API, or a pure-compute tag) — wired into example YAML as §4's `enrich` node
- [ ] Time a stranger-test: someone who didn't build the SDK follows the README from zero → running custom module. Target <15 min. Fix friction found.

**DoD:** demo pipeline includes a custom user module built out-of-tree.
**Verify:** `cd examples/modules/enrich && cargo build --target wasm32-wasip2 && hyperpipe run ...` — whale alerts include enriched field.

---

## M12 — Benchmark + demo package (1 day, both)

Goal: the pitch. §11.3.

- [ ] `scripts/benchmark.sh`: backfill 1M USDC Transfers (ethereum, fixed block range for reproducibility) → decode → (a) blackhole (b) postgres; report wall-clock + rec/s
- [ ] Same workload on Goldsky Turbo (their CLI, their pipeline) — record their number honestly, same range
- [ ] Demo script (literal file, `DEMO.md`): ① validate ② live multi-chain run w/ status lines ③ kill -9 + resume ④ swap sink in YAML, rerun ⑤ custom module ⑥ benchmark slide
- [ ] README: 60-second quickstart, architecture diagram (lift from §2), honest comparison table (§1.2 incl. gaps)
- [ ] Tag `v0.1.0-hackathon`

**DoD:** demo runs start-to-finish from `DEMO.md` on a clean machine.
**Verify:** dry-run the full demo once, timed, before presenting.

---

## MVP todo rollup (copy into tracker)

```
[ ] M0  scaffold: workspace, wit contract, module build, CI          (½d, A+B)
[ ] M1  config parse + validation + chain registry + example yaml    (1d, A)
[ ] M2  hypersync source loop: live/backfill/both, envelope v1       (1d, A)
[ ] M3  DAG runtime: channels, fan-out, backpressure, shutdown       (1d, A)
[ ] M4  wasm host: wasmtime, imports, pools, limits, cwasm cache     (1½d, B)
[ ] M5  guest SDK: Processor/Sink traits, macros, typed host calls   (1d, B)
[ ] M6  evm-abi-decoder module + golden tests                        (1d, B)
[ ] M7  sqlite checkpointing: cursors, watermarks, atomic kv         (1d, A)
[ ] M8  crash-test script, fix fallout                               (½d, A)
[ ] M9  postgres/webhook/s3(parquet)/stdout/blackhole sinks + blob-put (2½d, B)
[ ] M10 e2e: wasm into DAG, builtin resolution, full example runs    (1d, A+B)
[ ] M11 filter builtin + out-of-tree custom module example           (1d, B)
[ ] M12 benchmark vs Turbo + DEMO.md + README + tag                  (1d, A+B)
```

≈ 6½ dev-days per track → **3 calendar days with 2 engineers, tight but honest; 4th day = slack you'll want.** If time collapses, cut in this order: M11 filter (keep custom example), webhook sink, `both` mode (keep live+backfill separate).

---

## Phase 1 — Turbo parity (post-hackathon, ~2–3 weeks, small milestones)

Ordered by demo-value ÷ effort:

- [ ] **P1.1 `inspect`** (2d): broadcast tap per edge + unix socket + `hyperpipe inspect -n <node>`; lossy sampling, zero backpressure (§10)
- [ ] **P1.2 TS guest SDK** (3–5d, riskiest — start early): jco componentize toolchain, `@hyperpipe/sdk` npm package mirroring Rust traits, TS example module; fallback per §14 risk 2 = QuickJS interpreter module
- [ ] **P1.3 Prometheus metrics** (1–2d): `/metrics` endpoint; per-node rec/s, batch latency, retries, cursor lag vs head
- [ ] **P1.4 CEL filter + `map` module** (2–3d): `cel-interpreter` in guest; `map` select/rename/computed
- [ ] **P1.5 `secret` CLI** (1–2d): `hyperpipe secret set/list/rm`, keychain-encrypted local store (§9)
- [ ] **P1.6 kafka sink** (1–2d): `kafka-produce` host import goes live; key = `chain_id:block:log_index` for log-compaction dedupe (s3 Parquet sink moved into M9)
- [ ] **P1.7 `hyperpipe test` harness** (1–2d): fixture in → module → snapshot out; conformance for custom-module authors
- [ ] **P1.8 CBOR encoding** (1d): flip via envelope tag, benchmark delta vs JSON
- [ ] **P1.9 intra-source prefetch** (1–2d): pipeline next HyperSync query while current drains (§5.1)
- [ ] **P1.10 postgres checkpoint store** (1d): fill the trait stub from M7
- [ ] **P1.11 `dedupe` module** (1d): kv-backed, proves stateful-module replay safety publicly

## Phase 2 — Beyond parity (~3–4 weeks)

- [ ] **P2.1 Reorg handling** (1wk): block-hash ring buffer per chain, `rollback` control records, postgres sink invalidation, reorg simulation test (§5.2)
- [ ] **P2.2 Arrow IPC boundary** (1–2wk): arrow-rs in host + guests, HyperSync Arrow output end-to-end, Parquet s3 sink; benchmark vs CBOR
- [ ] **P2.3 Daemon mode** (1wk): `apply`/`status`/`logs`, pidfile or systemd docs, pipeline state machine
- [ ] **P2.4 clickhouse sink** (2–3d)
- [ ] **P2.5 Go + AssemblyScript SDKs** (1wk)
- [ ] **P2.6 Adaptive batching** (2–3d): auto-tune batch size from downstream latency
- [ ] **P2.7 Per-sink disk spill buffer** (3–5d, optional): slow sink stops stalling siblings (§14 risk 5)

## Phase 3 — Product bets (sequenced when prioritized)

- [ ] **P3.1 DataFusion SQL transforms** — SQL on Arrow batches; kills Turbo's SQL advantage. Needs P2.2 first.
- [ ] **P3.2 Module registry** — `oci://` module refs, publish/pull, semver
- [ ] **P3.3 Curated dataset presets** — `preset: erc20_transfers` sugar ≈ Turbo's named datasets
- [ ] **P3.4 Hosted control plane** — multi-tenant, org/auth, web UI

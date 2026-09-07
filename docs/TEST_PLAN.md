# HyperPipe Test Plan — Unit & Integration (target: 90% line coverage)

> Companion doc: [E2E_TEST_PLAN.md](E2E_TEST_PLAN.md) covers the end-to-end suite.
> This doc covers everything below e2e: unit tests, host-level integration tests,
> coverage measurement, and the gap-by-gap work list.

## Status

Both targets met. `just coverage` reports them; `just test` runs the suites.

| Workspace | Tests | Line coverage |
|---|---|---|
| root (`crates/*`) | 261 | **91.6%** (unit+integration alone; e2e folds in on top) |
| `modules/` | 87 | **98.7%** |

Deviations from the plan below:

- **No `httpmock`.** The tests that need a real HTTP peer use `crates/testutil`
  — a dev-only crate with a scripted server on a real socket (no new external
  dependency). Same role, same tests.
- **Postgres-backed tests** soft-skip unless `HP_TEST_PG_DSN` is set (§4 keeps
  `cargo test` green without docker). CI must set it, or those tests silently
  do nothing. Port 5436, not 5433 — see E2E_TEST_PLAN §9.
- §5.4's `HyperSyncSource::for_test` constructor proved unnecessary: `Source`'s
  fields are all public, so tests build one pointed at the mock server directly.
  No production code was changed for it.

`modules/test-chaos` (§4) exists and drives the §5.5 sandbox tests: trap →
instance recycled, graceful `Err` → instance and state kept, `loop {}` → epoch
deadline interrupts, over-cap allocation → limiter aborts the guest. It is a
processor that misbehaves on config command; batches it passes carry a
`chaos_calls` counter, which is how a test distinguishes a fresh instance
(counter restarts at 1) from a reused one — no host-side bookkeeping needed.

**Known gap §5.6 documents:** "`processor_task` process error → batch dropped,
task continues" is what the code does, and ARCHITECTURE.md §6.2 and
docs/modules.md now describe exactly that. E2E-15 measures it (30 of 100
records lost behind an advancing cursor, exit 0). Retrying processor batches
and pausing the branch, as sinks already do, is the intended improvement.

---

## 1. Goal & Scope

- **Target:** ≥ 90% line coverage across the native workspace (`crates/*`) and the
  native-testable module logic (`modules/*/src/*.rs` pure-logic files).
- **In scope:** `hp-encoding`, `hp-engine` (config + checkpoint), `hp-source-hypersync`,
  `hp-wasm-host`, `hp-cli`, and the pure-logic halves of every module
  (`sql.rs`, `buffer.rs`, `decode.rs`, `filter.rs`, `body.rs`, SDK helpers).
- **Out of numeric scope (covered by behavior, not by the % number):** the
  `#[cfg(target_arch = "wasm32")]` glue blocks in modules and the SDK export
  macros. These never compile natively, so no native coverage tool can count
  them. They are exercised for correctness by the wasm-host integration suite
  (§6) and the e2e suite — that is the honest way to "cover" them.

## 2. Current State (inventory)

~7,400 lines of Rust. ~100 existing test functions. Estimated current line
coverage: **≈ 50%** (pure logic is well tested; the runtime wiring is not).

| Area | Lines | Existing tests | Est. coverage | Verdict |
|---|---|---|---|---|
| `crates/encoding` | 352 | 7 | ~85% | Nearly done |
| `crates/engine/config` (mod+secret+chains) | 1,023 | 24 | ~75% | Gaps in `resolve_endpoint`, misc branches |
| `crates/engine/checkpoint.rs` | 409 | 8 | ~85% | Strong; few error paths left |
| `crates/source-hypersync` | 1,122 | 25 | ~55% | `plan_step`/parser/tracker great; **`run()` loop + `locate_fork` + client HTTP = 0%** |
| `crates/wasm-host` | 995 | 5 unit + 2 integration | ~35% | Allowlist tested; **Runtime, pools, host imports, services = mostly 0%** |
| `crates/cli` | 772 | **0** | **~0%** | Biggest hole: `pipeline.rs` DAG runtime, tasks, module resolution, health server |
| `modules/postgres-sink/sql.rs` | 394 | 8 | ~80% | Config error paths left |
| `modules/s3-sink/buffer.rs` | 393 | 10 | ~85% | Config error paths left |
| `modules/evm-abi-decoder/decode.rs` | 360 | 6 | ~70% | Error branches + `dyn_to_json` variants left |
| `modules/filter/filter.rs` | 260 | 7 | ~75% | Op/parse error branches left |
| `modules/webhook-sink/body.rs` | 33 | 2 | ~100% | Done |
| `modules/sdk` (native helpers) | ~60 native | 0 | 0% | `decode_batch`/`encode_batch`/`lock_state` |

## 3. Coverage Measurement (how the 90% is defined and enforced)

```bash
cargo install cargo-llvm-cov          # one-time (not currently installed)

# Native workspace, unit + integration tests. Build guest modules first so the
# wasm-host integration tests don't soft-skip (they exercise host-side lines):
cd modules && cargo build --target wasm32-wasip2 && cd ..
cargo llvm-cov --workspace --html     # report at target/llvm-cov/html

# Module pure logic (separate workspace):
cd modules && cargo llvm-cov --html
```

Rules:

1. **Two numbers, both ≥ 90%:** the root workspace and the `modules/` workspace
   are separate Cargo workspaces — report each.
2. **E2E folds into the total.** `crates/cli/src/pipeline.rs` (622 lines) is
   mostly reachable only by running the binary. Run the e2e suite against an
   instrumented binary and merge profiles (see E2E_TEST_PLAN.md §7):
   ```bash
   cargo llvm-cov --no-report run -p hp-cli -- run examples/usdc-postgres.yaml   # per scenario
   cargo llvm-cov report --html                                                  # merged
   ```
3. **Allowed exclusions** (mark with `#[cfg_attr(coverage_nightly, coverage(off))]`
   or accept as residue): `main()` clap wiring, `shutdown_signal()` (signal
   plumbing; e2e proves it), the `#[cfg(target_arch = "wasm32")]` blocks
   (never compiled natively — auto-excluded), bindgen-generated code.
4. **CI gate:** `cargo llvm-cov --workspace --fail-under-lines 90` once the work
   list below lands (start the gate at 70 and ratchet: 70 → 80 → 90).

## 4. New Test Infrastructure (prerequisites)

| Need | Choice | Used by |
|---|---|---|
| Mock HTTP server (in-process) | `httpmock` (or `wiremock`) dev-dependency | source-hypersync client + `run()` loop tests, host `http` import tests, webhook tests |
| Postgres for host `sql_batch` tests | existing `hp-pg` docker container (`postgres:16-alpine`, port 5433); tests **soft-skip** when absent, exactly like the wasm integration tests soft-skip missing `.wasm` | `wasm-host` sql tests, postgres-sink integration |
| Local object store | already supported: `BlobConnCfg.local_path` + tempdir | `blob_put` tests, s3 e2e |
| Temp dirs / sqlite | `std::env::temp_dir()` pattern already used in checkpoint tests | everywhere |
| Test guest modules | already built by `just build-modules`; add one **misbehaving module** crate (`modules/test-chaos/`) that can panic / loop forever / return Err / allocate unboundedly on command via config | trap recycling, epoch deadline, memory cap tests |

## 5. Work List — Unit Tests (per file, specific cases)

### 5.1 `crates/encoding/src/lib.rs` (~85% → ~95%)

- [ ] `decode`/`encode` with `Encoding::Cbor` and `ArrowIpc` → `CodecError::Unsupported` (both directions).
- [ ] `decode` with malformed JSON bytes → `CodecError::Json`.
- [ ] `Batch::control` for `Rollback` → `batch_id == "src:ctrl:<block>"`, `block_range == (b, b)`, `as_control()` roundtrip.
- [ ] `as_control()` on a non-control batch → `None`; on a control batch with junk record → `None`.
- [ ] `BlockRange::from()/to()`, `len()/is_empty()` accessors.

### 5.2 `crates/engine/src/config/` (~75% → ~92%)

`resolve_endpoint` branch matrix (currently only "named chain" and "unknown chain" are hit):
- [ ] `chain_id` + `url` explicit → wins even when `chain` also set.
- [ ] `chain` + `chain_id` override (registry URL, custom id).
- [ ] `chain` + `url` override (registry id, custom URL).
- [ ] `chain_id` without `url` and no `chain` → error.
- [ ] `url` without `chain_id` → error.
- [ ] none of the three → error.

Validation branches not yet exercised:
- [ ] `type: rpc` (non-hypersync) → error.
- [ ] `to_block <= from_block` → error.
- [ ] `mode: both` accepts `to_block` present *and* absent.
- [ ] sink `connections: [x]` where `x` undefined → error.
- [ ] postgres connection without `dsn` → error.
- [ ] postgres sink without `config.connection` → error (the "requires config.connection" arm).
- [ ] input that names a **sink** → "cannot be an input" error.
- [ ] node listing **itself** as input → error.
- [ ] `inputs: []` (empty) → "has no `inputs`" error.
- [ ] no sources at all → error (only no-sinks is tested today).
- [ ] `confirmations: 0` + `reorg.enabled: false` + `mode: backfill` → **allowed** (the exemption branch).
- [ ] `Config::nodes()` returns all three kinds; `summary()` string; `profile()` for `s`/`m`/`l` (assert the §11.1 table values).
- [ ] `referenced_secrets()`: none / one / repeated / unterminated `${secret:` (no closing brace).
- [ ] `ModuleRef::display()` both forms; `parse_builtin_ref` rejects `builtin/@1`, `builtin/x@notanum`, `x@1`.
- [ ] `Config::load()` (file path): missing file → `ConfigError::Io`; happy path via tempfile.
- [ ] secret.rs: unterminated ref treated as literal (the `None` arm in `replace_refs`) — currently untested.
- [ ] chains.rs: `names()` iterator non-empty; lookup case behavior documented by a test.

### 5.3 `crates/engine/src/checkpoint.rs` (~85% → ~95%)

- [ ] `open()` on an unwritable path → error (context "open checkpoint db").
- [ ] `persist_snapshot` with empty snapshot + empty dirty → early-return Ok (no txn).
- [ ] `rewind` to a value **above** current → cursor unchanged (the `if next_block < *e` guard) and SQLite row not lowered.
- [ ] KV read-through: `get` of a key persisted in a **previous** store instance populates `kv_cache`; second `get` hits cache (assert via timing-free behavior: delete the row out from under it, `get` still returns cached value).
- [ ] `restore` where **all** sinks have cursors but list contains duplicates.

### 5.4 `crates/source-hypersync` (~55% → ~90%) — the big one, part 1

`client.rs` (currently: parser only). Use `httpmock` to stand up a real HTTP endpoint:
- [ ] `archive_height()` happy path; missing `height` field → error; non-JSON body → error.
- [ ] `query()` sends `from_block`/`to_block`/`logs`/`field_selection` faithfully (assert on the received request body); bearer token header present when set, absent when not (constructor param, not env, to keep the test hermetic — see refactor note below).
- [ ] `post_query` non-2xx with `{"error": "..."}` body → error message contains it; non-2xx without error field → "unknown error".
- [ ] `block_hashes()` pagination: server returns 2 pages then completes → hashes concatenated; server stalls (`next_block <= cursor`) → returns partial without hanging.
- [ ] `new()` with `track_blocks: true` force-adds `number`+`hash` to wire block selection without duplicating existing entries (assert serialized request).
- [ ] `parse_response`: log with `block_number` **missing** from `blocks` map → no join, no panic; `data: null`; `data: 42` (non-object/array) → empty.
- [ ] `flex_u64`: negative number, non-numeric string, bool → `None`; `"0xzz"` → `None`.

`lib.rs` `run()` loop — refactor-for-testability note: `from_config` reads
`HYPERSYNC_BEARER_TOKEN` from env and the URL from config; the loop itself only
needs `client + tx + stop + kv`. Add a test constructor
(`HyperSyncSource::for_test(client, …)`) or make the endpoint URL point at
`httpmock`. Then, with a scripted mock server:
- [ ] backfill happy path: 3 pages → batches arrive in order, chunked at `max_records`, non-final chunks carry `ack_block = range.from`, EOF control emitted, `run()` returns.
- [ ] query error → backoff and retry (mock: fail twice, then succeed); cursor unmoved across failures.
- [ ] `stop` flag set → loop exits promptly, no EOF emitted.
- [ ] receiver dropped mid-stream → `run()` returns Ok (shutdown path), no panic.
- [ ] live mode with `from_block: 0` → initial cursor = `archive_height - confirmations`.
- [ ] confirmation truncation: response reaches past `head - confirmations` → records above `effective_next` retained-out (assert the `records.retain` path with real records).
- [ ] reorg path end-to-end at unit level: seed tracker via first response, second response carries mismatching guard → rollback control batch emitted with correct `invalidate_after_block`, cursor rewound to `fork+1`, tracker purged and saved to kv.
- [ ] `locate_fork`: canonical fetch succeeds → exact fork; canonical fetch fails (mock 500) → coarse rewind (`min_block - 1`); empty tracker → coarse fallback formula.
- [ ] `block_number_of`: already covered — keep.

### 5.5 `crates/wasm-host` (~35% → ~85%) — the big one, part 2

`host_impl.rs` — construct `HostState` directly (no wasm needed):
- [ ] `http` import: denied host → `Err("http denied…")` **without** any network call; allowed host → 200 via `httpmock`; response body over `HTTP_BODY_LIMIT` via `content_length` → error; chunked body exceeding limit → error; bad method string → error.
- [ ] `sql_exec`/`sql_batch`: connection not granted → error; granted but no pool → "has no open pool". With `hp-pg` container (soft-skip otherwise): multi-statement transaction commits atomically; failing statement rolls back the whole batch; affected-rows sum correct.
- [ ] `bind_json`: null / bool / i64 / f64 / huge-number-as-string / string / array (→ JSON text) — assert via a `SELECT $1::text`-style roundtrip against pg, or split the match into a testable classifier.
- [ ] `blob_put`: not granted → error; granted but no store → error; granted + `local_path` store → file lands under prefix; prefix concatenation correct.
- [ ] `kv_get`/`kv_set` route to the module's namespace (two modules, same key, no bleed).
- [ ] `metric_add` prefixes module name; `Metrics::snapshot`.
- [ ] `MemKv` get/set roundtrip + missing key.

`lib.rs` — extend `tests/integration.rs` (guest modules already build in CI):
- [ ] `build_blob`: `local_path` → LocalFileSystem; no bucket & no local_path → error; endpoint set → allow_http builder path (constructing is enough; no request).
- [ ] `compile()` cache: load same wasm twice → second load hits `.cwasm` (assert cache file exists after first, and a corrupted cache file falls back to recompile).
- [ ] `load_processor` with a module whose `init` returns Err → error surfaces with "module init:" context (drive with the filter module + empty config, which errors).
- [ ] Instance pool: `instances_per_stage = 1`, call `process` from 2 threads concurrently → second call takes the `checkout()` fresh-instantiate path; both succeed.
- [ ] Trap handling: chaos module (§4) configured to panic in `process` → `process()` returns Err, slot discarded, **next** call succeeds on a fresh instance.
- [ ] Graceful `Err` from `process` keeps the instance (chaos module: count calls in module state via kv; assert state survives an Err but not a trap).
- [ ] Epoch deadline: chaos module infinite-loops → call errors within ~deadline+1s (set `epoch_deadline_secs = 1`).
- [ ] Memory cap: chaos module allocates > `wasm_mem_bytes` → traps, host survives.
- [ ] `WasmSink::flush` propagates module flush errors; flush with poisoned/empty pool is a no-op.
- [ ] Sink `write` on control batch routes to `on_control` (via postgres/s3 module or a recording chaos sink).
- [ ] `to_wire`/`from_wire` roundtrip incl. unknown-encoding error path (`from_wire` with Cbor tag → decode error string).
- [ ] `Runtime::drop` joins the epoch thread (smoke: create + drop in a loop, no leak/panic).
- [ ] `build_services_async` with unreachable pg DSN → error contains connection name.

### 5.6 `crates/cli` (~0% → ~85%)

`pipeline.rs` pure helpers (unit tests, no runtime):
- [ ] `build_consumer_map`: linear chain, fan-out, fan-in shapes.
- [ ] `reachable_sinks_per_source`: diamond DAG (source → 2 procs → shared sink) — sink listed once; source with no path to any sink → empty vec; multi-source with disjoint sinks.
- [ ] `builtin_wasm_file`: all 7 known names + unknown → None.
- [ ] `module_dir()`: env override + default (serialize env access with a mutex or `temp-env`).
- [ ] `resolve_module`: builtin happy path (tempdir with dummy file); unknown builtin name; missing file → error message mentions `HYPERPIPE_MODULE_DIR`; `File` ref relative to base_dir.
- [ ] `preprocess_config`: decoder ref + `abis[].file` → file contents inlined under `abi`; non-decoder module → config passed through untouched; missing abi file → error; malformed abi JSON → error; abi entry already carrying `abi` and no `file` → untouched.
- [ ] `default_cache_dir` with and without `HOME`.

`pipeline.rs` task functions (tokio tests with channels — these functions take
plain `Receiver`/`Sender`/`SinkImpl`/`CheckpointStore`, so they're testable
without wasm by using `SinkImpl::Stdout` and small refactors — promote the
retry count and backoff to constants injectable in tests):
- [ ] `sink_task` acks `batch.ack_block()` (chunked batch pins to range start).
- [ ] `sink_task` rollback control → `store.rewind` called with `invalidate_after_block + 1`, only **after** sink accepted the batch.
- [ ] `sink_task` write Err ×3 → task returns (branch paused), cursor NOT advanced; write Err ×2 then Ok → acked.
- [ ] `processor_task` empty output on non-control input → **empty batch still propagated** (the quiet-branch cursor rule — this is load-bearing, test it explicitly); empty output on control input → nothing synthesized.
- [ ] `processor_task` process error → batch dropped, task continues with next batch.
- [ ] `source_task` fans out to N consumers; consumer closed → stop flag set.
- [ ] `checkpoint_once`: flush error → cursors NOT persisted (assert sqlite unchanged); flush ok → persisted.
- [ ] `SinkImpl::Stdout` skips control batches; writes NDJSON lines.

`health.rs` (tokio test, real TCP):
- [ ] `maybe_spawn` without env → None; with junk port → None + no panic.
- [ ] `/healthz` 200 in STARTING and RUNNING, 503 in STOPPING; `/readyz` 503 in STARTING, 200 in RUNNING; unknown path behaves as healthz.

`pipeline::run()` itself is covered by e2e (instrumented binary), not unit tests.

### 5.7 Modules — pure logic top-ups

`postgres-sink/sql.rs` (~80% → ~95%):
- [ ] `PgConfig::from_json`: missing `connection` / missing `table` errors; `mode: upsert` without `unique_key` → error; `rollback.block_number_column` non-string → error; `rollback.chain_id_column` non-string-non-null → error.
- [ ] `upsert_stmt` where **all** columns are in `unique_key` → `DO NOTHING` arm.
- [ ] `resolve_columns`: nested objects/arrays skipped at top level; `column_map` overwriting an existing column (the `slot.1 = v` branch); path missing → column absent.
- [ ] `col_type`: bool / float / non-decimal string / nested (jsonb) — table-driven.
- [ ] `is_decimal`: `""`, `"-"`, `"12a"`, `"-5"`.

`s3-sink/buffer.rs` (~85% → ~95%):
- [ ] `S3Config::from_json`: missing connection → error; `format: "csv"` → error; `flush_rows: 0` → clamped to 1; explicit ndjson.
- [ ] `Buffer::push` with empty records slice → state untouched (`seen` stays false).
- [ ] `to_parquet` with zero-column records (all non-objects) → "cannot infer schema" error.
- [ ] `cell_to_string`: null / bool / number / nested object → JSON text.

`evm-abi-decoder/decode.rs` (~70% → ~92%):
- [ ] `Decoder::from_config` errors: `abis` missing/not-array; entry with `file` but no `abi` (unsubstituted); invalid ABI JSON; empty event set after filtering → "no events selected".
- [ ] `decode_record` errors: malformed `topic0` (not hex32) → Err even in drop mode (parse error path); malformed `topic1`; bad `data` hex → Err; empty-string topics skipped (the `!s.is_empty()` branch).
- [ ] `on_undecodable` default (absent) = drop; unknown value → init error.
- [ ] `u64_field`: number / decimal string / hex string / missing → 0 fallback in output.
- [ ] `dyn_to_json` variants: `Int` (negative → decimal string), `Bool`, `FixedBytes`, `Bytes`, `String`, `Array`/`Tuple` (event with `bytes` + `bool` + array param, e.g. a synthetic ABI).
- [ ] Anonymous-event / missing-value zip (`let Some(val) = val else { continue }` branch): event declaring more inputs than decoded values.

`filter/filter.rs` (~75% → ~93%):
- [ ] `Op::parse` unknown op → init error; predicate missing `field` / missing `op` → error; `all` not an array → error; both `all`+`any` empty/absent → error.
- [ ] `ne` on equal + unequal; `lt`/`lte`/`gt` (only `gte` is covered); `in` with non-array value → false; `in` with numeric-string coercion (`"5"` vs `5`).
- [ ] Comparison against non-numeric lhs (string "abc" with `gt`) → false (fail closed).
- [ ] `values_eq` non-numeric equality (plain string eq); null value predicate.

`sdk` native helpers:
- [ ] `decode_batch` unknown tag → error; tag 1/2 → unsupported-encoding error; tag 0 happy path.
- [ ] `encode_batch` roundtrip with `decode_batch`.
- [ ] `lock_state` recovers a poisoned mutex (poison it in a thread, then lock).

## 6. Integration Tests — wasm-host × all modules (the wasm-glue coverage)

Extend `crates/wasm-host/tests/integration.rs` (same soft-skip pattern) into a
module conformance matrix. This is what covers the `#[cfg(wasm32)]` glue and the
SDK macros behaviorally:

- [ ] **filter.wasm**: whale config → passing/failing records; all-dropped batch → `Output::Batches([])`; control batch passes through (SDK passthrough).
- [ ] **postgres_sink.wasm** (needs `hp-pg`, soft-skip): granted conn + upsert config → rows land; rerun same batch → still N rows (upsert); `create_table: true` → auto-DDL; rollback control → rows past fork deleted; rollback control with `rollback` unset → warning, no delete; **conn not granted** → write Err (capability enforcement through the full stack).
- [ ] **s3_sink.wasm** + `local_path` blob conn: write below `flush_rows` → no object; `flush()` → object with deterministic key; rollback control → buffered rows purged; write after flush → new object.
- [ ] **webhook_sink.wasm** + `httpmock`: NDJSON body + content-type; non-2xx → Err (engine will retry); chunking at `max_batch`; rollback forwarded as single control line; `forward_rollbacks: false` → no POST; **host not in allowlist** → Err mentions allowlist.
- [ ] **stdout_sink.wasm** / **blackhole_sink.wasm**: write + flush Ok (smoke — stdout already partially covered).
- [ ] **enrich.wasm** (example module): `usd_estimate` + `whale` fields added; `process called before init` guard (call process on a fresh instantiation bypassing init is not possible via `load_processor` — cover the SDK guard instead by a chaos module whose init errs, then assert pool creation fails).
- [ ] **decoder.wasm**: already covered for happy path + control; add `on_undecodable: error` → process Err propagates as "module process error".

## 7. Execution Order & Milestones

| Phase | Work | Est. new tests | Coverage after |
|---|---|---|---|
| P1 | §5.7 module top-ups + §5.1 encoding + §5.3 checkpoint (pure, no infra) | ~45 | modules ws ≈ 93%; root ws ≈ 55% |
| P2 | §5.2 config matrix + §5.6 CLI pure helpers | ~40 | root ws ≈ 65% |
| P3 | §4 infra (httpmock, chaos module) + §5.4 source loop + client | ~25 | root ws ≈ 75% |
| P4 | §5.5 wasm-host runtime + host imports + §6 conformance matrix | ~30 | root ws ≈ 83% |
| P5 | §5.6 task functions + health + e2e coverage folding (E2E doc §7) | ~15 + e2e | root ws ≥ 90% |

## 8. CI Wiring

```yaml
# .github/workflows/test.yml (sketch)
- rustup target add wasm32-wasip2
- cd modules && cargo build --target wasm32-wasip2        # guests for integration tests
- cargo llvm-cov --workspace --no-report                  # unit + integration
- cd modules && cargo llvm-cov --no-report                # module pure logic
- ./scripts/e2e/run-all.sh --instrumented                 # E2E doc §7
- cargo llvm-cov report --fail-under-lines 90
services:
  postgres: postgres:16-alpine (5433), env POSTGRES_PASSWORD=hp, POSTGRES_DB=hp
```

Keep the soft-skip pattern locally (`cargo test` green without docker/wasm), but
CI always runs the full matrix — a skipped test in CI is a **failure** (grep for
`SKIP:` in test output and fail the job).

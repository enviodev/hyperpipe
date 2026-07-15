# HyperPipe E2E Test Plan

> Companion doc: [TEST_PLAN.md](TEST_PLAN.md) covers unit/integration tests and
> the 90% coverage methodology. This doc specifies the end-to-end suite: the
> real `hyperpipe` binary, real wasmtime running the real `.wasm` modules, a
> scripted HyperSync mock, real Postgres, a local object store, and a local
> webhook receiver.

## Status

Built and passing: **E2E-01 … 16** (`./scripts/e2e/run-all.sh`, or `just e2e`).

Not built:

- **E2E-12(c)** (SQL to an ungranted connection) — cannot be expressed in YAML:
  validation rejects a postgres sink whose `config.connection` is not granted,
  and there is no chaos sink to bypass it with. Covered instead by
  `host_impl::tests::sql_to_an_ungranted_connection_is_denied` and
  `postgres_sink_without_the_connection_grant_fails`.
- **E2E-14's STARTING/STOPPING probe windows** — sub-second races against a
  probe; asserted deterministically in `health::tests` instead.

What the suite caught:

- **E2E-09**: a paused sink branch silently truncated every sibling branch while
  the pipeline still reported a clean EOF and exit 0. Fixed in
  `pipeline.rs::fan_out`.
- **E2E-15**: a trapping processor loses its batch outright — 30 of 100 records
  never reached the sink, yet the cursor advanced past them and the run exited 0.
  **Not fixed**: the two specs disagree (see the scenario's closing note), so it
  measures and reports the loss rather than encoding an answer.

---

## 1. Philosophy

An e2e test here means: **spawn the built binary with a pipeline YAML and assert
on externally observable state** — Postgres rows, objects on disk, webhook
payloads, stdout NDJSON, the SQLite checkpoint DB, process exit codes, and
health endpoints. No mocking inside the process; the only fake is the HyperSync
server (we don't own chain data) and the webhook receiver (we are the consumer).

Everything in `mode: backfill` with a `to_block` terminates on EOF — that gives
every scenario a natural, deterministic end without sleeps-and-prayers.

## 2. Existing Assets (build on these, don't reinvent)

| Asset | What it does |
|---|---|
| `scripts/mock-hypersync.py` | Deterministic paginating `/query` + `/height` server: 1 USDC Transfer per block, `STEP` blocks/page, `DELAY` per page, `rollback_guard` on every response, `REORG_AT`/`REORG_DEPTH` fork simulation |
| `scripts/crash-test.sh` | kill -9 mid-backfill loop → restart until EOF → assert every block exactly once in Postgres |
| `scripts/reorg-test.sh` | fork mid-backfill → assert rollback detected, forked rows deleted, canonical rows re-ingested |
| `scripts/run.sh` / `justfile` | build-if-missing, env loading, validate-then-run |
| `deploy/docker-compose.yaml` | Postgres service |
| `examples/*.yaml` | 6 real pipeline configs (multichain, postgres, s3-parquet, multi-sink, whale-alerts) |
| `BlobConnCfg.local_path` | S3 sink writes to a local dir — no MinIO needed |
| `HYPERPIPE_HEALTH_PORT` | /healthz, /readyz probes |
| `--debug-stdout` | sink-less smoke runs |

New assets needed:

- `scripts/e2e/` directory with one script per scenario + `run-all.sh`.
- `scripts/mock-webhook.py` (~40 lines): records POST bodies to a file, can be
  told to fail the first N requests with HTTP 500 (retry testing), then 200.
- Per-scenario tempdir convention: `WORK=$(mktemp -d)`, checkpoint DB, module
  dir, lake dir, and YAML rendered from a template all under `$WORK`.

## 3. Environment Matrix

| Dependency | Provision | Scenarios that need it |
|---|---|---|
| Built binary + modules | `cargo build` + `just build-modules` | all |
| mock-hypersync | python3 stdlib, per-test port | all |
| Postgres | `hp-pg` container (5433) or CI service | E2E-02/03/04/09/10/13 |
| Local blob dir | tempdir | E2E-05/06 |
| mock-webhook | python3 stdlib | E2E-07/08/12 |

Every script must: pick a free port, `trap` cleanup (kill mock + pipeline,
`docker exec … DROP TABLE`), and **fail loudly with the pipeline log tail** on
assertion failure. Timeout wrapper (`timeout 120s`) on every run — a hang is a
failure, not a stuck CI job.

## 4. Scenario Catalog

Notation: `mock(FROM, END, STEP, DELAY, …)` = mock-hypersync env; every
scenario asserts **exit code 0** for terminating runs unless stated.

### E2E-01 — Backfill happy path (stdout, no external sinks)
- **Pipeline:** mock source (300 blocks) → `builtin/evm-abi-decoder@1` → `builtin/stdout@1`.
- **Assert:** exactly 300 NDJSON lines on stdout; each has `event: "Transfer"`, `params.value` = decimal-string of block number; process exits 0 on EOF; checkpoint DB contains cursor `(source, sink) = END`.
- **Also covers:** decoder ABI-file substitution (`abis[].file` → inlined), EOF control flow, `--debug-stdout` variant as a second run.

### E2E-02 — Postgres sink: upsert, auto-DDL, idempotent replay
- **Pipeline:** mock (300 blocks) → decode → `builtin/postgres@1` (`mode: upsert`, `unique_key: [chain_id, block_number, log_index]`, `create_table: true`).
- **Assert:** table auto-created; `COUNT(*) = 300`; `amount` column is `numeric` with correct values. Then **delete the checkpoint DB and rerun the identical range**: still exactly 300 rows (upsert idempotency under full replay).

### E2E-03 — Crash recovery (kill -9 loop) — *exists: `crash-test.sh`*
- Keep as-is; fold into `run-all.sh`. **Add one assertion:** after final EOF, `cursors` table in the checkpoint DB equals END for every (source, sink) pair.

### E2E-04 — Reorg rollback → Postgres — *exists: `reorg-test.sh`*
- Keep as-is. **Add assertions:** pipeline log contains "reorg detected" exactly once; every row ≥ fork block carries the post-fork tx hash prefix; row count is exactly BLOCKS (no dupes, no gaps).

### E2E-05 — S3 sink: Parquet to local dir, deterministic keys, checkpoint flush
- **Pipeline:** mock (250 blocks, `flush_rows: 100`) → decode → `builtin/s3@1` with `local_path` lake.
- **Assert:** objects exist with keys `{chain}/{first}-{last}-{rows}.parquet`; every file starts/ends with `PAR1`; sum of `{rows}` across keys = 250; final partial buffer (50 rows) flushed by the EOF/shutdown flush, not lost. Re-run full range after wiping the checkpoint → same set of keys (idempotent overwrite), file contents byte-identical.

### E2E-06 — S3 sink crash: flush-before-persist correctness
- **Pipeline:** as E2E-05 but `flush_rows: 100000` (never reached) and slow mock (`DELAY=0.3`).
- **Steps:** kill -9 at ~half range; restart; run to EOF.
- **Assert:** no records lost — union of rows across all parquet objects covers every block exactly (dupes across objects allowed *only* if keys differ; identical ranges must have overwritten). This is the §8.2-rule-5 proof for buffering sinks.

### E2E-07 — Webhook sink: delivery, chunking, retry, rollback forwarding
- **Pipeline:** mock (60 blocks, REORG_AT mid-range) → decode → `builtin/webhook@1` (`max_batch: 10`, `permissions.http: [127.0.0.1]`).
- **mock-webhook:** fail first 2 POSTs with 500.
- **Assert:** all 60 records eventually delivered as NDJSON (engine retry ×3 absorbed the 500s); no chunk exceeds 10 lines; exactly one `{"control":"rollback",…}` line received; re-delivered `batch_id`s (from the retried POSTs) are duplicates the consumer can dedupe — assert at-least-once, not exactly-once.

### E2E-08 — Custom module chain: filter → enrich → webhook (the extensibility claim)
- **Pipeline:** mock → decode → `builtin/filter@1` (`params.value gte <threshold>`) → `{file: enrich.wasm}` → webhook.
- **Assert:** only records with value ≥ threshold arrive; each carries `usd_estimate` and `whale` fields; blocks below threshold still advance the cursor (checkpoint DB reaches END even though most batches filtered to empty — the quiet-branch rule, end to end).

### E2E-09 — Multi-sink fan-out with independent cursors
- **Pipeline:** one source → decode → postgres AND s3(local) AND webhook.
- **Steps:** make webhook fail permanently (mock-webhook 500s forever) → its branch pauses after 3 retries.
- **Assert:** postgres + s3 complete the range; checkpoint DB shows postgres/s3 cursors at END, webhook cursor frozen at its last ack; process keeps running (branch pause ≠ crash) — then SIGTERM and assert graceful exit. Restart with webhook healthy → webhook replays only from its own cursor, postgres gets no duplicate writes (upsert or watch row count stays fixed).

### E2E-10 — Multi-chain fan-in (two mock sources)
- **Pipeline:** two mocks (chain_id 1 and 8453, different ports/ranges) → shared decode → postgres + s3.
- **Assert:** per-chain row counts correct; s3 objects never mix chains (`1/…` and `8453/…` key prefixes only); strict block order **within** each chain in Postgres (window over `log_index`).

### E2E-11 — `validate` CLI matrix
- Table-driven: run `hyperpipe validate` over (a) every `examples/*.yaml` with fake secrets exported → expect exit 0 + `valid: N source(s)…` summary; (b) a set of broken YAMLs (unknown key, cycle, orphan, missing secret, bad builtin ref, backfill without to_block, live with to_block, postgres sink without grant) → expect exit 1 and the specific error substring for each. Cheap, fast, covers `cmd_validate` + the error rendering users actually see.

### E2E-12 — Capability enforcement (negative security tests)
- **(a)** webhook pipeline with `permissions.http` **missing** → deliveries fail with "http denied", branch pauses, no request ever reaches mock-webhook (assert receiver saw zero).
- **(b)** custom module granted `api.example.com` attempting `127.0.0.1` (chaos/enrich variant) → denied.
- **(c)** postgres sink with `connections: []` (bypass validation by using a raw sql chaos sink or hand-edited config) → "connection not granted".
- **Assert in all:** the denial is a module-level error string, the process does not crash, other branches unaffected.

### E2E-13 — Live mode + confirmations lag
- **mock:** END far away, DELAY small → behaves like a live head advancing as fast as pagination allows; pipeline `mode: live`, `confirmations: 10`, no `to_block`.
- **Steps:** run 10s, SIGTERM.
- **Assert:** graceful shutdown (exit 0); max `block_number` in output ≤ mock's current head − 10; checkpoint persisted on the final flush; restart resumes from cursor with no gap and no duplicate beyond the confirmation window.

### E2E-14 — Health endpoints & graceful shutdown (K8s contract)
- **Pipeline:** any long-running one; `HYPERPIPE_HEALTH_PORT=8181`.
- **Assert timeline:** during startup `/readyz`=503; once running `/readyz`=200 and `/healthz`=200; send SIGTERM → `/healthz`=503 while draining; process exits 0. Also: junk `HYPERPIPE_HEALTH_PORT=abc` → pipeline still runs (probe disabled, warning logged).

### E2E-15 — Bad-module resilience (chaos module)
- **Pipeline:** mock → chaos processor (config: `trap_on_batch: 3`) → stdout.
- **Assert:** batch 3 is retried on a fresh instance (log shows trap + recycle), pipeline completes the range; variant with `error_always: true` → branch degrades after retries, process survives, SIGTERM exits cleanly.

### E2E-16 — Throughput smoke (regression guardrail, not a benchmark)
- **Pipeline:** mock (5,000 blocks, DELAY=0) → decode → `builtin/blackhole@1`.
- **Assert:** completes under a generous wall-clock bound (e.g. 60s) and records/s printed; store the number as a CI artifact for trend-watching. Fails only on order-of-magnitude regressions.

## 5. Coverage Contribution

The e2e suite is what reaches the code units can't (numbers = source lines):

| Code | Reached by |
|---|---|
| `cli/pipeline.rs::run` (~240 lines of wiring) | every scenario |
| `sink_task` retry/rollback/ack | E2E-02/04/07/09 |
| `processor_task` empty-batch propagation | E2E-08 |
| `checkpoint_once` + final flush | E2E-05/06/13 |
| `source_task` fan-out + stop | E2E-09/10/13 |
| `HyperSyncSource::run` reorg branch | E2E-04 |
| `health.rs::serve` | E2E-14 |
| wasm-host trap/pool/epoch paths | E2E-15 |
| module wasm glue (`#[cfg(wasm32)]`) + SDK macros | E2E-01…10 (behavioral) |

## 6. Harness Design

```
scripts/e2e/
├── lib.sh              # free-port picker, wait-for-port, run-with-timeout,
│                       # render-yaml-template, pg helpers, assert helpers
├── mock-webhook.py
├── 01-backfill-stdout.sh
├── 02-postgres-upsert.sh
├── ...                 # one file per scenario, numbered as above
└── run-all.sh          # runs all, honors --only <n>, --instrumented,
                        # prints per-scenario PASS/FAIL + timing summary,
                        # exits nonzero if any scenario failed or was skipped in CI
```

Conventions:
- Scenario scripts are **independent** (own ports, own tempdir, own pg table
  name) so `run-all.sh` can later parallelize.
- YAML templates live next to the scripts with `@PORT@`/`@WORK@`/`@TABLE@`
  placeholders — never mutate `examples/*.yaml`.
- `just e2e` target → `./scripts/e2e/run-all.sh`.
- Existing `crash-test.sh`/`reorg-test.sh` get thin wrappers (03/04) rather
  than rewrites.

## 7. Coverage-Instrumented Runs (folding e2e into the 90%)

`run-all.sh --instrumented` builds a coverage-instrumented binary and points the
scenarios at it, rather than launching each run through cargo:

```bash
eval "$(cargo llvm-cov show-env --export-prefix)"   # LLVM_PROFILE_FILE + rustc wrapper
cargo build -p hp-cli                               # instrumented binary
./target/debug/hyperpipe run "$YAML"                # scenarios exec this directly
cargo llvm-cov report --html                        # merged with unit/integration profiles
```

**Not** `cargo llvm-cov run -- run "$YAML"`: that wraps the pipeline in cargo, so
`$!` is cargo's pid and a `kill -TERM` hits cargo instead of the process under
test — every graceful-shutdown scenario (13, 14, 09) fails under instrumentation
while passing without it. Running the instrumented binary directly keeps the pid
and the signal semantics identical in both modes.

`run-all.sh --instrumented` deliberately does **not** clean profiles first, so it
merges with the unit/integration run that precedes it in CI (§9); pass `--clean`
for a standalone e2e-only number.

Two traps worth knowing about, both hit while building this:

- cargo's fingerprint does not notice llvm-cov's rustc wrapper, so a plain
  `cargo build` happily reuses an **uninstrumented** binary and the whole run
  reports 0% for every e2e line. `run-all.sh` detects this (counts `__llvm_prf`
  symbols), rebuilds via `cargo clean -p hp-cli`, and hard-fails rather than
  publishing an empty number.
- That symbol check must not be `nm … | grep -q` under `set -o pipefail`: grep
  quits on the first match, nm dies of SIGPIPE, and the pipeline reports failure
  for a perfectly good binary. Count with `grep -c` instead.

Notes: kill -9 scenarios lose that process's profile (profraw flushes at exit) —
acceptable, the restart run of the same scenario covers the same lines. SIGTERM
scenarios flush fine.

## 8. Flakiness Rules

1. No bare `sleep N && assert` — always poll with a deadline (`wait_for` helper).
2. Deterministic data only: the mock's value==block_number invariant is the
   assertion oracle everywhere; never assert on timing-dependent record counts
   in live-mode tests, assert on invariants (≤ head − confirmations).
3. Every scenario runs under `timeout`; on failure dump: pipeline log tail,
   mock server log, checkpoint DB dump (`sqlite3 … 'select * from cursors'`).
4. Ports are picked at runtime, never hardcoded (CI parallelism).
5. A scenario that can't provision its deps **skips locally, fails in CI**.

## 9. CI Pipeline (order matters)

```yaml
jobs:
  test:
    services: { postgres: { image: postgres:16-alpine, ports: ["5436:5432"], env: { POSTGRES_PASSWORD: hp, POSTGRES_DB: hp } } }
    env:
      HP_TEST_PG_DSN: postgres://postgres:hp@127.0.0.1:5436/hp   # else pg tests soft-skip
      PG_PORT: "5436"
      E2E_CI: "1"                                  # a skipped scenario fails the job
    steps:
      - rustup target add wasm32-wasip2
      - cd modules && cargo build --target wasm32-wasip2
      - just build-example-module                  # E2E-08; its .wasm is gitignored output
      - cargo llvm-cov clean --workspace
      - cargo llvm-cov --workspace --no-report     # unit + integration (TEST_PLAN §5–6)
      - ./scripts/e2e/run-all.sh --instrumented    # this doc; merges into the above
      - cargo llvm-cov report --fail-under-lines 90 --ignore-filename-regex 'testutil'
      - cd modules && cargo llvm-cov --fail-under-lines 90   # second workspace, own number
      - upload artifacts: e2e logs, coverage html, throughput number (E2E-16)
```

Port 5436, not 5433: 5432/5433 are routinely occupied on dev machines, and a
suite that writes into whatever answers there is a bad neighbour. `PG_PORT` /
`PG_DSN` / `HP_TEST_PG_DSN` override it.

## 10. Build-Out Order

1. `lib.sh` + `mock-webhook.py` + `run-all.sh` skeleton (half a day).
2. E2E-01, 02, 11 (fast wins; 01/11 need no docker).
3. Wrap 03/04 (existing scripts).
4. E2E-05/06/07/08 (the sink correctness core).
5. E2E-09/10/12/13/14 (resilience + security + ops).
6. Chaos module (shared with TEST_PLAN §4) → E2E-15, then E2E-16.
7. `--instrumented` mode + CI gate ratchet (70 → 80 → 90).

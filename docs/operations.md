# Operations

Everything the person running HyperPipe needs day-to-day.

## CLI

```
hyperpipe validate <pipeline.yaml>            # parse + full validation, exit 0/1
hyperpipe run <pipeline.yaml>                 # run in the foreground until EOF or signal
hyperpipe run <pipeline.yaml> --debug-stdout  # replace all sinks with an NDJSON printer
```

`run` exits 0 when every backfill source reaches EOF (job mode) or after a clean
SIGINT/SIGTERM drain. Validation failures exit 1 with the offending YAML path in the message.

## Environment variables

| Variable | Used by | Meaning |
|---|---|---|
| `HYPERSYNC_BEARER_TOKEN` | source | HyperSync API token (<https://app.envio.dev/api-tokens>). Live chains 401 without it. |
| `HYPERPIPE_SECRET_<NAME>` | config loader | Resolves `${secret:NAME}` in the YAML. |
| `HYPERPIPE_MODULE_DIR` | module loader | Directory containing builtin `.wasm` components. Default `modules/target/wasm32-wasip2/debug`; preset to `/opt/hyperpipe/modules` in the Docker image. |
| `HYPERPIPE_HEALTH_PORT` | health server | Enables `GET /healthz` + `/readyz` on this port. Unset = no health server. |
| `HYPERPIPE_HEALTH_BIND` | health server | Address to bind (default `0.0.0.0` so container probes can reach it). Set `127.0.0.1` to keep it local. |
| `RUST_LOG` | logging | e.g. `hyperpipe=info`, `hyperpipe=debug,hp_wasm_host=debug`. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` / `AWS_REGION` | s3 connections | Credentials for real S3/R2/MinIO (`local_path` needs none). |
| `HYPERPIPE_PROFILE`, `HYPERPIPE_MODULE_PROFILE`, `HYPERPIPE_REBUILD` | `scripts/run.sh` only | Launcher build knobs. |
| `HOME`/`XDG_CACHE_HOME` | wasm host | Location of the `.cwasm` precompile cache (`~/.cache/hyperpipe`). Safe to delete; rebuilt on next run. |

Env file format for the launcher (`KEY=VALUE`, `#` comments, optional `export ` prefix,
quotes stripped): [`examples/pipeline.env.example`](../examples/pipeline.env.example).

## Checkpoints & recovery

- State = one SQLite file at `runtime.checkpoint.path` (tables: `cursors`, `module_kv`).
  **Persist this file** (volume/PVC); it is the resume point.
- Every ~2 s the engine snapshots sink watermarks, flushes every sink, then commits the
  snapshot + module KV in one transaction. Kill the process at any moment — on restart each
  source resumes from the minimum durable cursor of its sinks. Delivery is **at-least-once**:
  duplicates are possible after a crash and are collapsed by idempotent sinks (postgres
  upsert, deterministic s3 keys); webhook consumers should dedupe.
- Inspect progress:

  ```bash
  sqlite3 state/checkpoint.db 'SELECT source, sink, next_block, updated_at FROM cursors;'
  ```

- **Replay from scratch**: stop, delete the checkpoint file, start.
- **Replay from a block**: stop, then lower the cursors:

  ```bash
  sqlite3 state/checkpoint.db "UPDATE cursors SET next_block=19000000 WHERE source='eth_usdc';"
  ```

  (Never edit while the pipeline runs — single writer.)

- Proof it works: `./scripts/crash-test.sh` (kill -9 loop → exact row counts).
- Reorg proof: `./scripts/reorg-test.sh` (simulated fork mid-backfill → rollback control
  record → postgres rows past the fork deleted → canonical blocks re-ingested).

## Health & lifecycle

With `HYPERPIPE_HEALTH_PORT` set:

- `GET /healthz` — liveness. 200 while running, 503 once draining.
- `GET /readyz` — readiness. 503 during startup, 200 once the DAG is running.

SIGINT and SIGTERM both trigger the same graceful path: stop sources → drain channels →
flush sinks → final checkpoint → exit 0. Give it up to 30 s before escalating; even SIGKILL
only costs replayed (deduped) work, never lost data.

## Logging & metrics

- Structured logs via `tracing` to stdout. Module `log()` calls appear tagged with the node
  name. Levels per target via `RUST_LOG`.
- A status line per source every 5 s: total records, rec/s, batch count, uptime.
- Sink failures: 2 warn-level retries, then an error and **branch pause** (cursor freezes —
  restart the process after fixing the sink; nothing is lost).
- Module `metric_add` counters are collected in-process; there is no Prometheus `/metrics`
  endpoint yet.

## Troubleshooting

| Symptom | Likely cause → fix |
|---|---|
| `invalid pipeline: …` at startup | Validation failure; message names the YAML path. |
| `unresolved secrets (set env HYPERPIPE_SECRET_<NAME>)` | Export the listed vars or add to the env file. |
| `hypersync 401` in logs | Missing/expired `HYPERSYNC_BEARER_TOKEN`. |
| `read builtin … build modules first` | `HYPERPIPE_MODULE_DIR` wrong or modules unbuilt → `./scripts/build-modules.sh`. |
| `http denied: host … not in permissions.http allowlist` | Add the exact hostname to that node's `permissions.http`. Exact match — subdomains don't inherit. |
| `connection X not granted to module Y` | Add the connection name to the sink's `connections:` list. |
| `write failed after 3 attempts … pausing branch` | Sink target down (DB unreachable, webhook 5xx). Other sinks keep flowing; cursor for this branch freezes. Fix target, restart. |
| Restart resumes "too early" / replays a lot | One sink's cursor lags (it was failing) — MIN across sinks decides. Check the `cursors` table. |
| Pipeline exits immediately after restart | Backfill already completed (cursor ≥ `to_block`). Expected; delete the checkpoint to re-run. |
| High memory | Lower `batch.max_records` / `resource_size`; channel capacity bounds in-flight batches. |
| YAML error at `${secret:…}` | Block style only — never inside `{ … }` flow maps. |
| Wasm call takes >5 s and dies | Epoch deadline (5 s on s/m, 10 s on l). Raise `resource_size` or make the module faster. |

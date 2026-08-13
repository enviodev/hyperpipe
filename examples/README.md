# Example pipelines

Real contracts + chains from [enviodev/rwa](https://github.com/enviodev/rwa) (RWA tokens and
tokenized stocks). Each example highlights a different sink or topology. All use `mode: live`, so
they need a `HYPERSYNC_BEARER_TOKEN` (create at https://app.envio.dev/api-tokens) — or swap the
source `chain` for a mock `url` to run offline (see [../DEMO.md](../DEMO.md)).

| File | Shows | Sinks | Chains |
|---|---|---|---|
| `usdc-multichain.yaml` | the reference pipeline (decode → filter → enrich → fan-out) | postgres + webhook | ethereum, base |
| `tokenized-stocks-postgres.yaml` | multi-chain → one table | **postgres** (upsert + auto-DDL) | ethereum, arbitrum |
| `ondo-stocks-s3-parquet.yaml` | data-lake archive | **s3** (buffered Parquet, flush every 5k rows) | ethereum |
| `stock-whale-alerts.yaml` | threshold filter → alert | **filter + webhook** (allowlist-gated) | ethereum |
| `rwa-multi-sink.yaml` | DAG fan-out: one decode → 3 sinks | **postgres + s3 + webhook** | ethereum |
| `robinhood-bridge-usdc.yaml` | bridge monitor: server-side topic filters (USDC ↔ Robinhood Chain canonical bridge) | postgres + webhook | ethereum |

Contracts (Backed xStocks + Ondo Global Markets), all emitting `Transfer(address,address,uint256)`:

- **Backed**: STRCx `0x1aad2179…`, TSLAx `0x8ad3c73f…`, CRCLx `0xfebded1b…` (same address across chains)
- **Ondo**: CRCLon `0x3632dea9…`, NVDAon `0x2d1f7226…`, SPYon `0xfedc5f4a…`, QQQon `0x0e397938…`, MUon, IVVon, HIMSon

## Run one

```bash
export HYPERSYNC_BEARER_TOKEN=...                 # for live chains
export HYPERPIPE_SECRET_PG_DSN=postgres://...     # for the postgres examples
export HYPERPIPE_SECRET_WEBHOOK_URL=https://hooks.slack.com/services/...

./scripts/run.sh examples/tokenized-stocks-postgres.yaml pipeline.env
# or, offline, against the mock:
./scripts/run.sh examples/<file>.yaml pipeline.env -- --debug-stdout
```

## Notes

- The **s3** examples write to a local dir (`local_path`) so they run with no cloud creds. For real
  S3/R2/MinIO, remove `local_path` and set `bucket`/`region` (+ `AWS_*` env).
- The **postgres** sink auto-creates its table (`create_table: true`) and upserts on
  `(chain_id, block_number, log_index)`, so re-runs and replays are idempotent.
- `rwa-multi-sink.yaml` is the interesting topology: `decode` fans out to Postgres and S3
  (full stream), while a `filter` branch feeds whale alerts to a webhook — all from one ingest.
- Backed xStocks use 18 decimals; the whale threshold `100000000000000000000` = 100 tokens.

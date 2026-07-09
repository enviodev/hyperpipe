# Tutorial: HyperPipe → Postgres, all local

Stream blockchain `Transfer` events through the engine, decode them with the WASM
ABI decoder, and land them in a **local Postgres** — with **no HyperSync token and no
cloud**. A built-in mock server plays the role of HyperSync, so you can watch the whole
decode → upsert path work on your laptop in a couple of minutes.

At the end you'll also see the live path (real chains → Postgres), which is the same
pipeline with two values swapped.

```
mock HyperSync ──▶ engine ──▶ evm-abi-decoder (WASM) ──▶ postgres sink (WASM) ──▶ Postgres
   (scripts/          (auto-decodes ERC-20 Transfer)      (auto-DDL + upsert)     (docker)
    mock-hypersync.py)
```

---

## 0. Prerequisites

- **Rust** ≥ 1.85 (`rustup` picks the pinned toolchain automatically)
- the **wasm32-wasip2** target — `rustup target add wasm32-wasip2`
- **Docker** (for the local Postgres)
- **Python 3** (runs the offline mock server)

No HyperSync token needed for this tutorial.

## 1. Build

Two workspaces → two builds: the native engine and the WASM modules.

```bash
git clone <repo> && cd datapipelines

cargo build                     # -> target/debug/hyperpipe
./scripts/build-modules.sh      # -> modules/target/wasm32-wasip2/debug/*.wasm
```

Sanity check the module dir has the sink you need:

```bash
ls modules/target/wasm32-wasip2/debug/postgres_sink.wasm   # must exist
```

## 2. Start a local Postgres

Same throwaway container the crash/reorg tests use — user `postgres`, password `hp`,
database `hp`, on host port **5433** (5433 avoids clashing with a default 5432):

```bash
docker run -d --name hp-pg \
  -e POSTGRES_PASSWORD=hp -e POSTGRES_DB=hp \
  -p 5433:5432 postgres:16-alpine

docker exec hp-pg pg_isready -U postgres      # -> "accepting connections"
```

Your DSN is therefore:

```
postgres://postgres:hp@localhost:5433/hp
```

## 3. Write the pipeline

One YAML declares the source (mock), the decoder, and the Postgres sink. Save as
`local-pg.yaml` in the repo root.

> The heredoc is **unquoted** so `$PWD` expands to an absolute ABI path — relative
> paths in a pipeline resolve against the YAML's own directory, and we want this to
> work from anywhere.

```bash
cat > local-pg.yaml <<YAML
name: local-pg-tutorial
version: 1

runtime:
  resource_size: s
  checkpoint:
    store: sqlite
    path: ./state/local-pg.db

sources:
  - name: eth
    type: hypersync
    chain_id: 1
    url: http://127.0.0.1:8799     # the mock server (step 4)
    mode: backfill                 # fixed range -> exits at the end (job mode)
    from_block: 19000000
    to_block: 19000050
    confirmations: 0               # mock is deterministic; no head to lag behind
    reorg: { enabled: false }      # allowed for backfill; off keeps this minimal
    query:
      logs:
        - address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]   # USDC
          topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]]  # Transfer
      field_selection:
        log: [address, topic0, topic1, topic2, data, block_number, log_index, transaction_hash]
        block: [number, timestamp, hash]

processors:
  - name: decode
    module: builtin/evm-abi-decoder@1
    inputs: [eth]
    config:
      abis:
        - file: $PWD/examples/abis/erc20.json
          events: [Transfer]
      on_undecodable: drop

sinks:
  - name: pg
    module: builtin/postgres@1
    inputs: [decode]
    connections: [pg_main]         # capability grant (must include config.connection)
    config:
      connection: pg_main
      table: usdc_transfers
      mode: upsert                 # INSERT ... ON CONFLICT -> replays are idempotent
      unique_key: [chain_id, block_number, log_index]
      create_table: true           # auto-DDL from the first batch
      column_map:                  # dotted decoded path -> column name
        params.from: from_address
        params.to: to_address
        params.value: amount

connections:
  pg_main:
    type: postgres
    dsn: \${secret:PG_MAIN_DSN}     # resolved from HYPERPIPE_SECRET_PG_MAIN_DSN
    pool: { max: 8 }
YAML
```

Note the DSN is a **secret reference**, never the literal — credentials never enter WASM.
Keep `\${secret:...}` on its own line (block style); inside a `{ }` flow map the `{`
breaks YAML parsing.

## 4. Run it

Three env vars, then validate and run. Terminal 1 = mock, terminal 2 = engine.

**Terminal 1** — the mock HyperSync server (one USDC Transfer per block, `value == block_number`):

```bash
PORT=8799 python3 scripts/mock-hypersync.py
```

**Terminal 2** — point at the modules + the DSN, validate, run:

```bash
export HYPERPIPE_MODULE_DIR="$PWD/modules/target/wasm32-wasip2/debug"
export HYPERPIPE_SECRET_PG_MAIN_DSN='postgres://postgres:hp@localhost:5433/hp'
export RUST_LOG=hyperpipe=info

./target/debug/hyperpipe validate local-pg.yaml     # -> valid: 1 source, 1 processor, 1 sink
./target/debug/hyperpipe run     local-pg.yaml
```

You'll see the engine log a loud `CREATE TABLE` (auto-DDL), stream the range, then:

```
pipeline finished (all sources reached EOF)
```

and exit 0 — backfill is job mode. Stop the mock in terminal 1 with ctrl-c.

## 5. See the rows

The sink auto-created the table and upserted 50 rows (blocks 19000000–19000049 —
`to_block` is the exclusive upper bound):

```bash
docker exec hp-pg psql -U postgres -d hp -c "SELECT count(*) FROM usdc_transfers;"

docker exec hp-pg psql -U postgres -d hp -c \
  "SELECT chain_id, block_number, log_index, from_address, amount
   FROM usdc_transfers ORDER BY block_number LIMIT 5;"
```

Because the mock encodes `value == block_number`, the `amount` column equals
`block_number` — an easy end-to-end correctness check. The schema was inferred from the
data (`bigint`, `numeric`, `text`), with the primary key on your `unique_key`:

```
 chain_id | block_number | log_index |               from_address                |  amount
----------+--------------+-----------+-------------------------------------------+----------
        1 |     19000000 |         0 | 0xaAaAaAaaAaAaAaaAaA...                    | 19000000
        1 |     19000001 |         0 | 0xaAaAaAaaAaAaAaaAaA...                    | 19000001
```

## 6. Prove idempotency (optional)

Delete the checkpoint and re-run — the upsert means **no duplicates**, count stays put:

```bash
rm -f state/local-pg.db          # forget where we were
# (restart the mock in terminal 1, then re-run the engine)
./target/debug/hyperpipe run local-pg.yaml
docker exec hp-pg psql -U postgres -d hp -tAc "SELECT count(*) FROM usdc_transfers;"   # still 50
```

If you *don't* delete the checkpoint, the re-run resumes past the range and exits
immediately with nothing to do — that's crash-recovery working.

## 7. Going live (real chains → same Postgres)

The offline path and the live path are the *same* pipeline. To hit real Ethereum instead
of the mock:

1. Get a HyperSync token → <https://app.envio.dev/api-tokens>, then
   `export HYPERSYNC_BEARER_TOKEN=...`.
2. In the source: drop `url:` + `chain_id:`, use `chain: ethereum`; switch
   `mode: backfill` → `mode: live` and delete `to_block`; set `confirmations: 10` and
   `reorg: { enabled: true }` (safe against reorgs — the sink can undo forked rows if you
   add `rollback: true` to its config).

That is exactly [`examples/usdc-postgres.yaml`](../examples/usdc-postgres.yaml). Run it with
the launcher (builds anything missing, loads the env file, validates, runs):

```bash
cp examples/pipeline.env.example pipeline.env
$EDITOR pipeline.env             # HYPERSYNC_BEARER_TOKEN + HYPERPIPE_SECRET_PG_MAIN_DSN
./scripts/run.sh examples/usdc-postgres.yaml pipeline.env
```

`live` follows the head forever — stop it with ctrl-c (it drains cleanly).

**One-command alternative** — `deploy/docker-compose.yaml` brings up Postgres *and* the
pipeline together (only needs a token in `deploy/hyperpipe.env`):

```bash
echo "HYPERSYNC_BEARER_TOKEN=..." > deploy/hyperpipe.env
cp examples/usdc-postgres.yaml deploy/pipeline.yaml
docker compose -f deploy/docker-compose.yaml up --build
```

## 8. Clean up

```bash
docker rm -f hp-pg               # drops the container + its data
rm -f local-pg.yaml state/local-pg.db
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `read builtin ... build modules first` | Modules not built or `HYPERPIPE_MODULE_DIR` wrong. Run `./scripts/build-modules.sh` and re-export the dir. |
| `unresolved secrets: PG_MAIN_DSN` | Export `HYPERPIPE_SECRET_PG_MAIN_DSN=...` before running. |
| Connection refused / can't reach Postgres | Container not up, or wrong port. `docker exec hp-pg pg_isready -U postgres`; DSN host port must be **5433**. |
| Pipeline exits immediately, table empty | Left-over checkpoint from a prior run — `rm state/local-pg.db` to replay. |
| Source logs `connection refused` on 8799 | Mock server not running (terminal 1) or a different `PORT`. |
| YAML parse error at `${secret:...}` | Put the secret ref on its own line (block style), never inside a `{ ... }` flow map. |
| `hypersync 401` (live only) | `HYPERSYNC_BEARER_TOKEN` missing/invalid. |

## Where to go next

- Full YAML schema → [pipeline-reference.md](./pipeline-reference.md)
- Every sink's config (postgres `rollback`, s3 Parquet, webhook) → [modules.md](./modules.md)
- More ready-made pipelines → [`examples/README.md`](../examples/README.md)
- The scripted demo (multi-chain, `kill -9` crash proof, reorg) → [`../DEMO.md`](../DEMO.md)

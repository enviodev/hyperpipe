# Tutorial: HyperPipe → Postgres, on your machine

Stream real blockchain `Transfer` events from **Envio HyperSync**, decode them with the
WASM ABI decoder, and land them in a **local Postgres**. The engine, modules, and database
all run on your laptop; only the chain data comes over the network.

You'll backfill a small block range first (it finishes and exits, so you can inspect the
rows), then flip one setting to follow the chain head live.

```
Envio HyperSync ──▶ engine ──▶ evm-abi-decoder (WASM) ──▶ postgres sink (WASM) ──▶ Postgres
  (real chains,       (auto-decodes ERC-20 Transfer)      (auto-DDL + upsert)     (docker)
   your API token)
```

---

## 0. Prerequisites

- **Rust** ≥ 1.85 (`rustup` picks the pinned toolchain automatically)
- the **wasm32-wasip2** target — `rustup target add wasm32-wasip2`
- **Docker** (for the local Postgres)
- a **HyperSync API token** — create one at <https://app.envio.dev/api-tokens>

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

One YAML declares the source (Ethereum USDC), the decoder, and the Postgres sink. Save as
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
  - name: eth_usdc
    type: hypersync
    chain: ethereum                # built-in chain registry name
    mode: backfill                 # fixed range -> exits at the end (job mode)
    from_block: 19000000
    to_block: 19000005             # small range; a few dozen real transfers
    confirmations: 0               # this range is long final, no need to lag the head
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
    inputs: [eth_usdc]
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

Three env vars — the HyperSync token, the module dir, and the DSN secret — then validate
and run:

```bash
export HYPERSYNC_BEARER_TOKEN='<your token from app.envio.dev/api-tokens>'
export HYPERPIPE_MODULE_DIR="$PWD/modules/target/wasm32-wasip2/debug"
export HYPERPIPE_SECRET_PG_MAIN_DSN='postgres://postgres:hp@localhost:5433/hp'
export RUST_LOG=hyperpipe=info

./target/debug/hyperpipe validate local-pg.yaml     # -> valid: 1 source, 1 processor, 1 sink
./target/debug/hyperpipe run     local-pg.yaml
```

Keep the token out of the YAML and out of your shell history — export it in the session,
or put it in an env file (step 7). The engine logs a loud `CREATE TABLE` (auto-DDL),
streams the range, then:

```
pipeline finished (all sources reached EOF)
```

and exits 0 — backfill is job mode.

## 5. See the rows

The sink auto-created the table and upserted the real transfers in blocks
19000000–19000004 (`to_block` is the exclusive upper bound):

```bash
docker exec hp-pg psql -U postgres -d hp -c "SELECT count(*) FROM usdc_transfers;"

docker exec hp-pg psql -U postgres -d hp -c \
  "SELECT block_number, log_index, from_address, to_address, amount
   FROM usdc_transfers ORDER BY block_number, log_index LIMIT 5;"
```

You'll get a few dozen rows of genuine on-chain USDC transfers — real addresses, real
amounts (USDC has 6 decimals, so `500000000` = 500 USDC). The schema was inferred from
the data (`bigint`, `numeric`, `text`), with the primary key on your `unique_key`:

```
 block_number | log_index |               from_address                |               to_address                 |  amount
--------------+-----------+-------------------------------------------+------------------------------------------+-----------
     19000000 |       116 | 0xbD098c9B3b2cffeE0083725f3545e604dD9d8De7 | 0x2Fc617E933a52713247CE25730f6695920B3befe| 500000000
     19000000 |       221 | 0xB9254341A08dA44F31B60851f038192140365e00 | 0x1ac1A8FEaAEa1900C4166dEeed0C11cC10669D36|  44042190
```

## 6. Prove idempotency (optional)

Delete the checkpoint and re-run — the upsert means **no duplicates**, the count stays put:

```bash
rm -f state/local-pg.db          # forget where we were
./target/debug/hyperpipe run local-pg.yaml
docker exec hp-pg psql -U postgres -d hp -tAc "SELECT count(*) FROM usdc_transfers;"   # unchanged
```

If you *don't* delete the checkpoint, the re-run resumes past the range and exits
immediately with nothing to do — that's crash-recovery working.

## 7. Follow the chain live

Backfill was a bounded job. To keep ingesting new blocks forever, change the source to
live mode:

- `mode: backfill` → `mode: live`, and **delete `to_block`** (live has no end),
- `confirmations: 0` → `confirmations: 10` (lag the head so short reorgs never reach you),
- `reorg: { enabled: false }` → `reorg: { enabled: true }` (track block hashes; add
  `rollback: true` to the sink's `config` and it deletes forked rows on a reorg).

That is exactly [`examples/usdc-postgres.yaml`](../examples/usdc-postgres.yaml). Instead of
exporting vars by hand, use an env file + the launcher (it builds anything missing, loads
the file, validates, runs):

```bash
cp examples/pipeline.env.example pipeline.env
$EDITOR pipeline.env             # HYPERSYNC_BEARER_TOKEN + HYPERPIPE_SECRET_PG_MAIN_DSN
./scripts/run.sh examples/usdc-postgres.yaml pipeline.env
```

`pipeline.env` is gitignored — the token lives there, not in the YAML. `live` follows the
head forever; stop it with ctrl-c (it drains cleanly).

**One-command alternative** — `deploy/docker-compose.yaml` brings up Postgres *and* the
pipeline together (only needs a token in `deploy/hyperpipe.env`):

```bash
echo "HYPERSYNC_BEARER_TOKEN=<your token>" > deploy/hyperpipe.env
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
| `hypersync 401` | `HYPERSYNC_BEARER_TOKEN` unset or invalid. Create one at <https://app.envio.dev/api-tokens>. |
| `read builtin ... build modules first` | Modules not built or `HYPERPIPE_MODULE_DIR` wrong. Run `./scripts/build-modules.sh` and re-export the dir. |
| `unresolved secrets: PG_MAIN_DSN` | Export `HYPERPIPE_SECRET_PG_MAIN_DSN=...` before running. |
| Connection refused / can't reach Postgres | Container not up, or wrong port. `docker exec hp-pg pg_isready -U postgres`; DSN host port must be **5433**. |
| Pipeline exits immediately, table empty | Left-over checkpoint from a prior run — `rm state/local-pg.db` to replay. |
| YAML parse error at `${secret:...}` | Put the secret ref on its own line (block style), never inside a `{ ... }` flow map. |

## Where to go next

- Full YAML schema → [pipeline-reference.md](./pipeline-reference.md)
- Every sink's config (postgres `rollback`, s3 Parquet, webhook) → [modules.md](./modules.md)
- More ready-made pipelines → [`examples/README.md`](../examples/README.md)
- The scripted demo (multi-chain, `kill -9` crash proof, reorg) → [`../DEMO.md`](../DEMO.md)

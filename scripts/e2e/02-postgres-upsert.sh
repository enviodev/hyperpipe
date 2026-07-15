#!/usr/bin/env bash
# E2E-02 — postgres sink: auto-DDL, typed columns, and idempotent full replay.
#
# The replay half is the real point: wiping the checkpoint and re-running the
# identical range must leave 300 rows, not 600. That is what makes at-least-once
# delivery safe to build on.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "02-postgres-upsert"
require_pg

PORT=$(free_port)
FROM=19000000
BLOCKS=300
TO=$((FROM + BLOCKS))
TABLE=e2e_upsert

pg_drop "$TABLE"
cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-postgres-upsert
runtime: { resource_size: s, checkpoint: { store: sqlite, path: $WORK/ck.db } }
sources:
  - name: eth
    chain_id: 1
    url: http://127.0.0.1:$PORT
    mode: backfill
    from_block: $FROM
    to_block: $TO
    confirmations: 0
    reorg: { enabled: true, window: 64 }
    query:
      logs: [{ address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"], topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]] }]
      field_selection: { log: [address,topic0,topic1,topic2,data,block_number,log_index,transaction_hash], block: [number,timestamp,hash] }
processors:
  - name: decode
    module: builtin/evm-abi-decoder@1
    inputs: [eth]
    config: { abis: [{ file: ./erc20.json, events: [Transfer] }], on_undecodable: drop }
sinks:
  - name: pg
    module: builtin/postgres@1
    inputs: [decode]
    connections: [pgmain]
    config:
      connection: pgmain
      table: $TABLE
      mode: upsert
      unique_key: [chain_id, block_number, log_index]
      create_table: true
      column_map: { "params.value": amount }
connections:
  pgmain:
    type: postgres
    dsn: \${secret:PG_DSN}
    pool: { max: 4 }
YAML

start_mock "$PORT" "$FROM" "$TO" 25 0.01

# ---- run 1: auto-DDL + insert ----
hp_run "$WORK/pipe.yaml"
assert_exit "$?" 0 "first run must exit 0 on EOF"

count=$(pg_q "SELECT count(*) FROM $TABLE;")
assert_eq "$count" "$BLOCKS" "row count after first run"

# Auto-DDL must have picked types from the data, not made everything text.
amount_type=$(pg_q "SELECT data_type FROM information_schema.columns WHERE table_name='$TABLE' AND column_name='amount';")
assert_eq "$amount_type" "numeric" "uint256 column type"
block_type=$(pg_q "SELECT data_type FROM information_schema.columns WHERE table_name='$TABLE' AND column_name='block_number';")
assert_eq "$block_type" "bigint" "block_number column type"

# The mock's oracle survived decode -> bind -> numeric column.
mismatched=$(pg_q "SELECT count(*) FROM $TABLE WHERE amount <> block_number::numeric;")
assert_eq "$mismatched" "0" "amount must equal block_number for every row"
distinct=$(pg_q "SELECT count(DISTINCT block_number) FROM $TABLE;")
assert_eq "$distinct" "$BLOCKS" "no duplicate or missing blocks"

# ---- run 2: full replay with the checkpoint wiped ----
rm -f "$WORK/ck.db"
: > "$LOG"
hp_run "$WORK/pipe.yaml"
assert_exit "$?" 0 "replay run must exit 0"

count2=$(pg_q "SELECT count(*) FROM $TABLE;")
assert_eq "$count2" "$BLOCKS" "replaying the identical range must not duplicate rows"

pass "$BLOCKS rows, amount numeric == block_number, full replay stayed at $BLOCKS (upsert idempotent)"

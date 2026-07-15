#!/usr/bin/env bash
# E2E-10 — multi-chain fan-in: two sources, one shared decoder, two sinks.
#
# The thing that can only break here: a sink fed by two chains must keep them
# apart. The s3 key encodes {chain}/{first}-{last}, so an object mixing chains
# would be mislabelled *and* non-deterministic across replays. Postgres must
# keep per-chain counts exact and per-chain block order intact.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "10-multichain"
require_pg

PORT_A=$(free_port)
PORT_B=$(free_port)
A_FROM=19000000; A_BLOCKS=80;  A_TO=$((A_FROM + A_BLOCKS))
B_FROM=8000000;  B_BLOCKS=120; B_TO=$((B_FROM + B_BLOCKS))
TABLE=e2e_multichain
LAKE="$WORK/lake"
mkdir -p "$LAKE"
pg_drop "$TABLE"

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-multichain
runtime: { resource_size: s, checkpoint: { store: sqlite, path: $WORK/ck.db } }
sources:
  - name: eth
    chain_id: 1
    url: http://127.0.0.1:$PORT_A
    mode: backfill
    from_block: $A_FROM
    to_block: $A_TO
    confirmations: 0
    reorg: { enabled: true, window: 64 }
    query:
      logs: [{ address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"], topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]] }]
      field_selection: { log: [address,topic0,topic1,topic2,data,block_number,log_index,transaction_hash], block: [number,timestamp,hash] }
  - name: base
    chain_id: 8453
    url: http://127.0.0.1:$PORT_B
    mode: backfill
    from_block: $B_FROM
    to_block: $B_TO
    confirmations: 0
    reorg: { enabled: true, window: 64 }
    query:
      logs: [{ address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"], topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]] }]
      field_selection: { log: [address,topic0,topic1,topic2,data,block_number,log_index,transaction_hash], block: [number,timestamp,hash] }
processors:
  - name: decode
    module: builtin/evm-abi-decoder@1
    inputs: [eth, base]
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
  - name: lake
    module: builtin/s3@1
    inputs: [decode]
    connections: [blob]
    config: { connection: blob, format: ndjson, flush_rows: 30 }
connections:
  pgmain:
    type: postgres
    dsn: \${secret:PG_DSN}
    pool: { max: 4 }
  blob:
    type: s3
    local_path: $LAKE
YAML

start_mock "$PORT_A" "$A_FROM" "$A_TO" 10 0.02
start_mock "$PORT_B" "$B_FROM" "$B_TO" 10 0.02

hp_run "$WORK/pipe.yaml"
assert_exit "$?" 0 "pipeline must exit 0 once both sources EOF"

# ---- per-chain counts ----
a_rows=$(pg_q "SELECT count(*) FROM $TABLE WHERE chain_id = 1;")
b_rows=$(pg_q "SELECT count(*) FROM $TABLE WHERE chain_id = 8453;")
assert_eq "$a_rows" "$A_BLOCKS" "chain 1 row count"
assert_eq "$b_rows" "$B_BLOCKS" "chain 8453 row count"

# ---- strict block order within each chain (no interleaving, no gaps) ----
gaps=$(pg_q "SELECT count(*) FROM (
  SELECT block_number - LAG(block_number) OVER (PARTITION BY chain_id ORDER BY block_number, log_index) AS d
  FROM $TABLE
) g WHERE d IS NOT NULL AND d <> 1;")
assert_eq "$gaps" "0" "every chain's blocks must be contiguous"

# ---- s3 objects never mix chains ----
mapfile -t OBJECTS < <(cd "$LAKE" && find . -name '*.ndjson' | sed 's|^\./||' | sort)
[ "${#OBJECTS[@]}" -gt 0 ] || fail "no objects written"
for k in "${OBJECTS[@]}"; do
  [[ "$k" =~ ^(1|8453)/ ]] || fail "object key is not chain-prefixed: $k"
done
mixed=$(python3 - "$LAKE" <<'PY'
import json, pathlib, sys
bad = []
for f in pathlib.Path(sys.argv[1]).rglob("*.ndjson"):
    prefix = int(f.parent.name)
    chains = {json.loads(l)["chain_id"] for l in f.read_text().splitlines() if l.strip()}
    if chains != {prefix}:
        bad.append((str(f), sorted(chains)))
print(json.dumps(bad))
PY
)
assert_eq "$mixed" "[]" "objects must contain only their key's chain"

pass "chain 1: $a_rows rows, chain 8453: $b_rows rows, contiguous per chain, ${#OBJECTS[@]} objects with no chain mixing"

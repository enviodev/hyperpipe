#!/usr/bin/env bash
# Reorg-recovery proof (§5.2 phase 2): backfill through a simulated reorg and
# assert the pipeline detected it via the rollback guard, deleted the forked
# rows in Postgres, and re-ingested the canonical (post-fork) blocks.
#
# The mock flips blocks >= REORG_AT-REORG_DEPTH to a new fork (hashes and tx
# hashes prefixed 0xbeef) once the pipeline's cursor passes REORG_AT. A correct
# run ends with exactly BLOCKS rows where every block >= fork carries the
# new-fork tx hash. Requires the `hp-pg` container from the demo.
#
# Usage: ./scripts/reorg-test.sh
#   PG_CONTAINER / PG_DSN / PG_USER / PG_DB override the postgres target.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT=8798
FROM=19000000
BLOCKS=120
TO=$((FROM + BLOCKS))
REORG_AT=$((FROM + 60))
REORG_DEPTH=5
FORK=$((REORG_AT - REORG_DEPTH))   # first block on the new fork
PG_CONTAINER="${PG_CONTAINER:-hp-pg}"
PG_USER="${PG_USER:-postgres}"
PG_DB="${PG_DB:-hp}"
PG_DSN="${PG_DSN:-postgres://postgres:hp@127.0.0.1:5436/hp}"

export HYPERPIPE_MODULE_DIR="$ROOT/modules/target/wasm32-wasip2/debug"
export HYPERPIPE_SECRET_PG_DSN="$PG_DSN"
# The "reorg detected" warning comes from the source layer, so that target has
# to be enabled for the grep below to show anything. An inherited RUST_LOG wins
# (scripts/e2e/04 asserts on these lines).
: "${RUST_LOG:=hyperpipe=info,hp_source_hypersync=info}"
export RUST_LOG

command -v docker >/dev/null || { echo "docker required"; exit 1; }
docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER" >/dev/null 2>&1 || {
  echo "start postgres first: docker run -d --name hp-pg -e POSTGRES_PASSWORD=hp -e POSTGRES_DB=hp -p 5436:5432 postgres:16-alpine"
  exit 1
}

BIN="$ROOT/target/debug/hyperpipe"
[ -x "$BIN" ] || { echo "build first: cargo build"; exit 1; }

WORK=/tmp/hpreorg
rm -rf "$WORK"; mkdir -p "$WORK"
cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "DROP TABLE IF EXISTS reorg_test;" >/dev/null

cat > "$WORK/pipe.yaml" <<YAML
name: reorg-test
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
      table: reorg_test
      mode: upsert
      unique_key: [chain_id, block_number, log_index]
      create_table: true
      rollback: true
      column_map: { "params.value": amount }
connections:
  pgmain:
    type: postgres
    dsn: \${secret:PG_DSN}
    pool: { max: 4 }
YAML

PORT=$PORT FROM=$FROM END=$TO STEP=10 DELAY=0.05 \
  REORG_AT=$REORG_AT REORG_DEPTH=$REORG_DEPTH \
  python3 "$ROOT/scripts/mock-hypersync.py" &
MOCK=$!
trap 'kill $MOCK 2>/dev/null' EXIT
sleep 1

"$BIN" run "$WORK/pipe.yaml" 2>&1 | grep -Ei 'reorg|rollback' || true

psql_q() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "$1" | tr -d '[:space:]'; }
count=$(psql_q "SELECT count(*) FROM reorg_test;")
distinct=$(psql_q "SELECT count(DISTINCT block_number) FROM reorg_test;")
forked=$(psql_q "SELECT count(*) FROM reorg_test WHERE transaction_hash LIKE '0xbeef%';")
stale=$(psql_q "SELECT count(*) FROM reorg_test WHERE block_number >= $FORK AND transaction_hash NOT LIKE '0xbeef%';")
expect_forked=$((TO - FORK))

echo "----------------------------------------"
if [ "$count" = "$BLOCKS" ] && [ "$distinct" = "$BLOCKS" ] && [ "$forked" = "$expect_forked" ] && [ "$stale" = "0" ]; then
  echo "PASS: $count/$BLOCKS rows, $forked post-fork rows replaced, 0 stale pre-reorg rows"
  exit 0
else
  echo "FAIL: count=$count/$BLOCKS distinct=$distinct forked=$forked/$expect_forked stale=$stale"
  exit 1
fi

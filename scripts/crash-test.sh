#!/usr/bin/env bash
# Crash-recovery proof (§8.3): backfill a range into Postgres, kill -9 the
# pipeline mid-stream, restart, repeat until EOF, then assert every block landed
# exactly once (idempotent upsert). Requires the `hp-pg` container from the demo.
#
# Usage: ./scripts/crash-test.sh
#   PG_CONTAINER / PG_DSN / PG_USER / PG_DB override the postgres target
#   (same knobs as reorg-test.sh; scripts/e2e/03 passes its own).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT=8799
FROM=19000000
BLOCKS=300
TO=$((FROM + BLOCKS))
PG_CONTAINER="${PG_CONTAINER:-hp-pg}"
PG_USER="${PG_USER:-postgres}"
PG_DB="${PG_DB:-hp}"
PG_DSN="${PG_DSN:-postgres://postgres:hp@127.0.0.1:5436/hp}"

export HYPERPIPE_MODULE_DIR="$ROOT/modules/target/wasm32-wasip2/debug"
export HYPERPIPE_SECRET_PG_DSN="$PG_DSN"
export RUST_LOG=hyperpipe=error

command -v docker >/dev/null || { echo "docker required"; exit 1; }
docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER" >/dev/null 2>&1 || {
  echo "start postgres first: docker run -d --name $PG_CONTAINER -e POSTGRES_PASSWORD=hp -e POSTGRES_DB=$PG_DB -p 5436:5432 postgres:16-alpine"
  exit 1
}

BIN="$ROOT/target/debug/hyperpipe"
[ -x "$BIN" ] || { echo "build first: cargo build"; exit 1; }

# Private scratch dir: the pipeline YAML written here carries the Postgres
# DSN, so it must not live at a fixed, world-writable path.
WORK=$(mktemp -d "${TMPDIR:-/tmp}/hpcrash.XXXXXX")
echo "workdir: $WORK"
cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "DROP TABLE IF EXISTS crash_test;" >/dev/null

cat > "$WORK/pipe.yaml" <<YAML
name: crash-test
runtime: { resource_size: s, checkpoint: { store: sqlite, path: $WORK/ck.db } }
sources:
  - name: eth
    chain_id: 1
    url: http://127.0.0.1:$PORT
    mode: backfill
    from_block: $FROM
    to_block: $TO
    confirmations: 0
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
      table: crash_test
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

PORT=$PORT FROM=$FROM END=$TO STEP=10 DELAY=0.12 python3 "$ROOT/scripts/mock-hypersync.py" &
MOCK=$!
trap 'kill $MOCK 2>/dev/null' EXIT
sleep 1

pg_count() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "SELECT count(*) FROM crash_test;" 2>/dev/null | tr -d '[:space:]'; }

count=0; iter=0; kills=0
while [ "${count:-0}" -lt "$BLOCKS" ] && [ "$iter" -lt 15 ]; do
  iter=$((iter + 1))
  "$BIN" run "$WORK/pipe.yaml" >/dev/null 2>&1 &
  HP=$!
  sleep "$(awk 'BEGIN{srand('"$iter"');print 0.6+rand()*1.6}')"
  if kill -0 "$HP" 2>/dev/null; then
    kill -9 "$HP" 2>/dev/null; wait "$HP" 2>/dev/null
    kills=$((kills + 1))
    echo "iter $iter: kill -9 (rows so far: $(pg_count)/$BLOCKS)"
  else
    wait "$HP" 2>/dev/null
    echo "iter $iter: finished (rows: $(pg_count)/$BLOCKS)"
  fi
  count=$(pg_count)
done

# one clean run to guarantee EOF
"$BIN" run "$WORK/pipe.yaml" >/dev/null 2>&1
count=$(pg_count)
distinct=$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "SELECT count(DISTINCT block_number) FROM crash_test;" | tr -d '[:space:]')

echo "----------------------------------------"
if [ "$count" = "$BLOCKS" ] && [ "$distinct" = "$BLOCKS" ]; then
  echo "PASS: $count/$BLOCKS rows, 0 gaps, survived $kills kill -9"
  exit 0
else
  echo "FAIL: count=$count distinct=$distinct expected=$BLOCKS (kills=$kills)"
  exit 1
fi

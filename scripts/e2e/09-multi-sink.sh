#!/usr/bin/env bash
# E2E-09 — fan-out to three sinks with independent cursors, one of them broken.
#
# The webhook receiver 500s forever, so that branch exhausts its retries and
# pauses. The contract: a paused branch is not a crash — postgres and s3 must
# still complete the range and reach END, the webhook's cursor stays frozen at
# its last ack, and the process keeps running until SIGTERM (exit 0). Restarting
# with a healthy receiver must replay only the webhook's own backlog, leaving
# postgres row-count-stable (upsert).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "09-multi-sink"
require_pg
command -v sqlite3 >/dev/null || skip "sqlite3 needed to read the checkpoint db"

PORT=$(free_port)
HOOK_PORT=$(free_port)
FROM=19000000
BLOCKS=100
TO=$((FROM + BLOCKS))
TABLE=e2e_fanout
LAKE="$WORK/lake"
mkdir -p "$LAKE"
pg_drop "$TABLE"

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-multi-sink
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
  - name: lake
    module: builtin/s3@1
    inputs: [decode]
    connections: [blob]
    config: { connection: blob, format: ndjson, flush_rows: 25 }
  - name: hook
    module: builtin/webhook@1
    inputs: [decode]
    permissions: { http: ["127.0.0.1"] }
    config: { url: http://127.0.0.1:$HOOK_PORT/hook, max_batch: 50 }
connections:
  pgmain:
    type: postgres
    dsn: \${secret:PG_DSN}
    pool: { max: 4 }
  blob:
    type: s3
    local_path: $LAKE
YAML

# ---- run 1: the webhook is down for good ----
start_webhook "$HOOK_PORT" 0 1        # 500 forever
# Paced so the source is still streaming when the webhook gives up (~1.5s in:
# two backoffs of 500ms and 1s), which is what makes the liveness check below
# meaningful rather than a race against a finished backfill.
start_mock "$PORT" "$FROM" "$TO" 10 0.3

hp_start "$WORK/pipe.yaml"; pid=$HP_PID
wait_for "the webhook branch to pause after its retries" \
  "grep -q 'pausing branch' '$LOG'" 60

# A paused branch is not a crash.
kill -0 "$pid" 2>/dev/null || fail "the process died when one branch paused"

# ...and the healthy branches must finish the range regardless. This is the
# assertion that caught the fan-out bug: they used to stop dead at the pause.
wait_for "postgres to receive every block despite the paused webhook branch" \
  "[ \"\$(pg_q 'SELECT count(*) FROM $TABLE;')\" = '$BLOCKS' ]" 60

# The backfill may now finish on its own (EOF); if it is still going, SIGTERM it.
# Either way the process must exit 0 — a paused branch is not a failed run.
if kill -0 "$pid" 2>/dev/null; then
  kill -TERM "$pid" 2>/dev/null
fi
wait_for "the process to exit" "! kill -0 $pid 2>/dev/null" 30
wait "$pid" 2>/dev/null
rc=$?
assert_exit "$rc" 0 "the process must exit 0 with one branch paused"

pg_cursor=$(ck_cursor "$WORK/ck.db" eth pg)
hook_cursor=$(ck_cursor "$WORK/ck.db" eth hook)
assert_eq "$pg_cursor" "$TO" "the postgres cursor must reach END"
[ "${hook_cursor:-0}" != "$TO" ] || fail "the broken webhook's cursor must not have reached END"
echo "cursors: pg=$pg_cursor hook=${hook_cursor:-<none>}" >> "$LOG"

pg_rows=$(pg_q "SELECT count(*) FROM $TABLE;")
assert_eq "$pg_rows" "$BLOCKS" "postgres completed the range"

# ---- run 2: the webhook is healthy again ----
: > "$LOG"
rm -f "$WORK/webhook.ndjson"
HOOK_PORT2=$HOOK_PORT
kill -9 "${PIDS[0]}" 2>/dev/null       # drop the failing receiver
sleep 0.3
start_webhook "$HOOK_PORT2"            # same port, now answering 200

hp_run "$WORK/pipe.yaml"
assert_exit "$?" 0 "the restart must reach EOF"

pg_rows2=$(pg_q "SELECT count(*) FROM $TABLE;")
assert_eq "$pg_rows2" "$BLOCKS" "the replay must not duplicate postgres rows"
hook_cursor2=$(ck_cursor "$WORK/ck.db" eth hook)
assert_eq "$hook_cursor2" "$TO" "the webhook must catch up to END on the restart"

pass "pg+s3 completed while the webhook branch paused; SIGTERM exit 0; restart caught the webhook up with no pg dupes"

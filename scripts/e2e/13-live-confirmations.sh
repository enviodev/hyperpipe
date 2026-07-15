#!/usr/bin/env bash
# E2E-13 — live mode: never ingest past the confirmation window, shut down
# gracefully, and resume without a gap.
#
# Live-mode assertions must be invariants, not counts: how far a 10-second run
# gets is timing. The invariant is that nothing above (head - confirmations) is
# ever emitted, and that a restart picks up exactly where the cursor stopped.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "13-live-confirmations"
command -v sqlite3 >/dev/null || skip "sqlite3 needed to read the checkpoint db"

PORT=$(free_port)
FROM=19000000
HEAD=$((FROM + 100000))    # far away: the mock behaves like an advancing head
CONFIRMATIONS=10

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-live
runtime: { resource_size: s, checkpoint: { store: sqlite, path: $WORK/ck.db } }
sources:
  - name: eth
    chain_id: 1
    url: http://127.0.0.1:$PORT
    mode: live
    from_block: $FROM
    confirmations: $CONFIRMATIONS
    reorg: { enabled: true, window: 64 }
    query:
      logs: [{ address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"], topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]] }]
      field_selection: { log: [address,topic0,topic1,topic2,data,block_number,log_index,transaction_hash], block: [number,timestamp,hash] }
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [eth]
YAML

start_mock "$PORT" "$FROM" "$HEAD" 50 0.02

# ---- run for a few seconds, then SIGTERM ----
hp_start "$WORK/pipe.yaml"; pid=$HP_PID
wait_for "the pipeline to start streaming" "grep -q 'running (' '$LOG'" 20
sleep 6
kill -TERM "$pid" 2>/dev/null
wait_for "the process to exit after SIGTERM" "! kill -0 $pid 2>/dev/null" 30
wait "$pid" 2>/dev/null
rc=$?
assert_exit "$rc" 0 "SIGTERM must drain and exit 0"

# The mock reports archive_height = HEAD, so nothing above HEAD - confirmations
# may ever have been emitted.
max_block=$(grep -oE '"block_number":[0-9]+' "$LOG" | cut -d: -f2 | sort -n | tail -1)
[ -n "$max_block" ] || fail "no records were emitted at all"
limit=$((HEAD - CONFIRMATIONS))
[ "$max_block" -le "$limit" ] || fail "emitted block $max_block is past the confirmed head ($limit)"

cursor1=$(ck_cursor "$WORK/ck.db" eth out)
[ -n "$cursor1" ] || fail "no cursor was persisted on the final flush"
[ "$cursor1" -gt "$FROM" ] || fail "the cursor never advanced past from_block"

# ---- restart: resume from the cursor, no gap, no rewind ----
: > "$LOG"
hp_start "$WORK/pipe.yaml"; pid=$HP_PID
wait_for "the restarted pipeline to emit" "grep -q '\"block_number\"' '$LOG'" 30
kill -TERM "$pid" 2>/dev/null
wait_for "the restart to exit" "! kill -0 $pid 2>/dev/null" 30
wait "$pid" 2>/dev/null

grep -q "resuming from checkpoint" "$LOG" || fail "the restart did not resume from the checkpoint"
first_block=$(grep -oE '"block_number":[0-9]+' "$LOG" | head -1 | cut -d: -f2)
# At-least-once: the restart may replay from the cursor, never skip past it.
[ "$first_block" -le "$cursor1" ] || fail "restart skipped blocks: resumed at $first_block, cursor was $cursor1"
cursor2=$(ck_cursor "$WORK/ck.db" eth out)
[ "$cursor2" -ge "$cursor1" ] || fail "the cursor went backwards across a clean restart ($cursor1 -> $cursor2)"

pass "live run stayed <= head-$CONFIRMATIONS (max $max_block <= $limit), SIGTERM exit 0, resumed at $first_block from cursor $cursor1"

#!/usr/bin/env bash
# E2E-15 — bad-module resilience: a trapping / erroring processor must not take
# the pipeline down.
#
#   mock -> decode -> chaos processor -> stdout
#
# (a) trap_on_batch: 3 — the guest panics on its 3rd call. The host must recycle
#     the poisoned instance and the run must still reach EOF and exit 0.
# (b) error_always  — every call returns Err. The stage degrades, but the
#     process survives and SIGTERM still exits 0.
#
# Records: see the note on batch loss at the bottom — this scenario measures it
# rather than asserting a number, because the two specs disagree about what
# should happen (ARCHITECTURE §13 says a failed batch retries; TEST_PLAN §5.6
# says it is dropped, which is what the code does).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "15-bad-module"

CHAOS="$ROOT/modules/target/wasm32-wasip2/debug/test_chaos.wasm"
[ -f "$CHAOS" ] || skip "test_chaos.wasm not built (cd modules && cargo build --target wasm32-wasip2)"

FROM=19000000
BLOCKS=100
TO=$((FROM + BLOCKS))
STEP=10   # 10 pages -> 10 chaos calls

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cp "$CHAOS" "$WORK/chaos.wasm"

write_pipe() {   # write_pipe <file> <port> <ck-suffix> <chaos-config>
  cat > "$1" <<YAML
name: e2e-bad-module
runtime: { resource_size: s, checkpoint: { store: sqlite, path: $WORK/ck-$3.db } }
sources:
  - name: eth
    chain_id: 1
    url: http://127.0.0.1:$2
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
  - name: chaos
    module: { file: ./chaos.wasm }
    inputs: [decode]
    config: $4
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [chaos]
YAML
}

# ---- (a) a trapping module ----
PORT=$(free_port)
write_pipe "$WORK/trap.yaml" "$PORT" "trap" '{ trap_on_batch: 3 }'
start_mock "$PORT" "$FROM" "$TO" "$STEP" 0.01

hp_run "$WORK/trap.yaml"
rc=$?
assert_exit "$rc" 0 "a trapping module must not stop the pipeline reaching EOF"

grep -q 'process error' "$LOG" || fail "(a) expected the trap to be logged as a process error"
grep -q 'backfill complete (EOF)' "$LOG" || fail "(a) the run did not reach EOF"

# The instance was recycled: `chaos_calls` restarts at 1 after the trapped call,
# and later batches keep flowing.
delivered=$(grep -cE '^\{.*"chaos_calls"' "$LOG")
[ "$delivered" -gt 0 ] || fail "(a) nothing was delivered after the trap"
recycled=$(python3 - "$LOG" <<'PY'
import json, re, sys
calls = []
for line in open(sys.argv[1], errors="replace"):
    if '"chaos_calls"' in line and line.startswith("{"):
        try:
            calls.append(json.loads(line)["chaos_calls"])
        except Exception:
            pass
# A counter that goes back down proves a fresh instance took over.
print(1 if any(b <= a for a, b in zip(calls, calls[1:])) else 0)
PY
)
assert_eq "$recycled" "1" "(a) the counter must restart, proving a fresh instance served later batches"

# How much data did the trapped batch cost? (Measured, not asserted — see below.)
lost=$((BLOCKS - delivered))
echo "(a) delivered $delivered/$BLOCKS records; the trapped batch cost $lost" >> "$LOG"

# ---- (b) a module that fails every call ----
: > "$LOG"
PORT_B=$(free_port)
write_pipe "$WORK/err.yaml" "$PORT_B" "err" '{ error_always: true }'
start_mock "$PORT_B" "$FROM" "$TO" "$STEP" 0.05

hp_start "$WORK/err.yaml"; pid=$HP_PID
wait_for "the chaos stage to start refusing batches" "grep -q 'process error' '$LOG'" 30
kill -0 "$pid" 2>/dev/null || fail "(b) the process died when a stage failed every call"

if kill -0 "$pid" 2>/dev/null; then
  kill -TERM "$pid" 2>/dev/null
fi
wait_for "the process to exit" "! kill -0 $pid 2>/dev/null" 30
wait "$pid" 2>/dev/null
assert_exit "$?" 0 "(b) SIGTERM must still exit 0 with a permanently failing stage"

delivered_b=$(grep -cE '^\{.*"chaos_calls"' "$LOG" || true)
assert_eq "$delivered_b" "0" "(b) a module that refuses everything must deliver nothing"

pass "trap recycled and the run reached EOF (a: $delivered/$BLOCKS records, $lost lost to the trapped batch); error_always degraded without crashing"

# NOTE — the records lost in (a) are real and not asserted on purpose.
# `processor_task` logs a failed batch and moves on, so the trapped batch's
# records never reach the sink while the cursor still advances past them.
# TEST_PLAN §5.6, ARCHITECTURE.md §6.2 and docs/modules.md all describe this
# skip-on-error behaviour as a known gap; this scenario reports the loss so the
# number stays visible until processor batches are retried like sink batches.

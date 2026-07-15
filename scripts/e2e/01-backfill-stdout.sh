#!/usr/bin/env bash
# E2E-01 — backfill happy path: mock source -> decoder -> stdout sink.
#
# The mock's invariant (one Transfer per block, value == block number) is the
# oracle: 300 blocks must produce exactly 300 NDJSON lines whose decoded value
# is the block number, the process must exit 0 on EOF, and the checkpoint DB
# must end at END. Also covers ABI-file substitution and --debug-stdout.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "01-backfill-stdout"

PORT=$(free_port)
FROM=19000000
BLOCKS=300
TO=$((FROM + BLOCKS))

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-backfill-stdout
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
  - name: out
    module: builtin/stdout@1
    inputs: [decode]
YAML

start_mock "$PORT" "$FROM" "$TO" 25 0.01

# ---- run 1: real stdout sink module ----
hp_run "$WORK/pipe.yaml"
rc=$?
assert_exit "$rc" 0 "pipeline must exit 0 on EOF"

# The sink prints NDJSON records; the tracing logs go to stderr, but both land
# in $LOG, so select the record lines by shape.
grep -E '^\{.*"event":"Transfer"' "$LOG" > "$WORK/records.ndjson"
lines=$(wc -l < "$WORK/records.ndjson" | tr -d ' ')
assert_eq "$lines" "$BLOCKS" "one decoded record per block"

# Every record decoded, and value == block_number (the mock's oracle).
bad=$(python3 - "$WORK/records.ndjson" <<'PY'
import json, sys
bad = 0
for line in open(sys.argv[1]):
    r = json.loads(line)
    if r.get("event") != "Transfer":
        bad += 1
    elif r["params"]["value"] != str(r["block_number"]):
        bad += 1
    elif not isinstance(r["params"]["value"], str):
        bad += 1  # uint256 must stay a decimal string
print(bad)
PY
)
assert_eq "$bad" "0" "every record must decode with value == block_number"

# Distinct blocks: no dupes, no gaps.
distinct=$(python3 -c "
import json,sys
blocks={json.loads(l)['block_number'] for l in open('$WORK/records.ndjson')}
print(len(blocks), min(blocks), max(blocks))
")
assert_eq "$distinct" "$BLOCKS $FROM $((TO - 1))" "distinct blocks / first / last"

# The cursor is durable at END.
cursor=$(ck_cursor "$WORK/ck.db" eth out)
if [ -n "$cursor" ]; then
  assert_eq "$cursor" "$TO" "checkpoint cursor must reach to_block"
fi

# ---- run 2: --debug-stdout (sink-less smoke) ----
rm -f "$WORK/ck.db"
: > "$LOG"
hp_run "$WORK/pipe.yaml" --debug-stdout
rc=$?
assert_exit "$rc" 0 "--debug-stdout run must exit 0"
debug_lines=$(grep -cE '^\{.*"event":"Transfer"' "$LOG")
assert_eq "$debug_lines" "$BLOCKS" "--debug-stdout must tap every record"

pass "$BLOCKS records decoded and printed, exit 0, cursor at $TO (both sink and --debug-stdout)"

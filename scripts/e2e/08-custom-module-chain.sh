#!/usr/bin/env bash
# E2E-08 — the extensibility claim, end to end:
#   mock -> decode -> builtin/filter -> {file: enrich.wasm} -> webhook
#
# Two things are under test. First, an out-of-tree module (built against the SDK,
# referenced by path) runs in the same chain as the builtins. Second, the
# quiet-branch rule: the filter drops most batches entirely, and the cursor must
# still reach END — otherwise a selective pipeline would replay its whole range
# on every restart.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "08-custom-module-chain"

ENRICH="$ROOT/examples/modules/enrich.wasm"
[ -f "$ENRICH" ] || skip "examples/modules/enrich.wasm not built (cd examples/modules/enrich && cargo build --target wasm32-wasip2 --release)"
command -v sqlite3 >/dev/null || skip "sqlite3 needed to read the checkpoint db"

PORT=$(free_port)
HOOK_PORT=$(free_port)
FROM=19000000
BLOCKS=100
TO=$((FROM + BLOCKS))
# value == block number, so this keeps the top 40 blocks and drops the first 60.
THRESHOLD=$((FROM + 60))
EXPECTED=$((TO - THRESHOLD))

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cp "$ENRICH" "$WORK/enrich.wasm"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-custom-chain
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
  - name: whales
    module: builtin/filter@1
    inputs: [decode]
    config:
      all: [{ field: params.value, op: gte, value: "$THRESHOLD" }]
  - name: enrich
    module: { file: ./enrich.wasm }
    inputs: [whales]
    config: { whale_usd: 19.0 }
sinks:
  - name: hook
    module: builtin/webhook@1
    inputs: [enrich]
    permissions: { http: ["127.0.0.1"] }
    config: { url: http://127.0.0.1:$HOOK_PORT/hook, max_batch: 50 }
YAML

start_webhook "$HOOK_PORT"
# STEP=10 with a threshold 60 blocks in means the first 6 pages filter to empty.
start_mock "$PORT" "$FROM" "$TO" 10 0.02

hp_run "$WORK/pipe.yaml"
assert_exit "$?" 0 "pipeline must exit 0 on EOF"

report=$(python3 - "$WORK/webhook.ndjson" "$THRESHOLD" <<'PY'
import json, sys
threshold = int(sys.argv[2])
blocks, missing_fields, below = set(), 0, 0
for r in (json.loads(l) for l in open(sys.argv[1])):
    for line in r["lines"]:
        o = json.loads(line)
        if o.get("control"):
            continue
        blocks.add(o["block_number"])
        if int(o["params"]["value"]) < threshold:
            below += 1
        if "usd_estimate" not in o or "whale" not in o:
            missing_fields += 1
print(json.dumps({"n": len(blocks), "below": below, "missing_fields": missing_fields,
                  "lo": min(blocks) if blocks else 0}))
PY
)
get() { echo "$report" | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }

assert_eq "$(get n)" "$EXPECTED" "only records at or above the threshold should arrive"
assert_eq "$(get below)" "0" "no record below the filter threshold may pass"
assert_eq "$(get lo)" "$THRESHOLD" "the first delivered block is the threshold"
assert_eq "$(get missing_fields)" "0" "every record must carry the custom module's fields"

# The quiet branch: batches that filtered to empty still advanced the cursor.
cursor=$(ck_cursor "$WORK/ck.db" eth hook)
assert_eq "$cursor" "$TO" "the cursor must reach END even though most batches were filtered empty"

pass "$EXPECTED/$BLOCKS records survived filter+enrich with usd_estimate/whale; cursor at $TO despite empty batches"

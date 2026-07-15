#!/usr/bin/env bash
# E2E-07 — webhook sink: delivery, chunking, retry, and rollback forwarding.
#
# The receiver 500s the first two POSTs, so the engine's retry has to absorb
# them; a reorg mid-range must arrive as exactly one control line. The contract
# asserted here is at-least-once, not exactly-once: retried POSTs legitimately
# re-deliver a chunk, and the consumer dedupes on batch_id.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "07-webhook"

PORT=$(free_port)
HOOK_PORT=$(free_port)
FROM=19000000
BLOCKS=60
TO=$((FROM + BLOCKS))
REORG_AT=$((FROM + 30))

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-webhook
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
  - name: hook
    module: builtin/webhook@1
    inputs: [decode]
    permissions: { http: ["127.0.0.1"] }
    config:
      url: http://127.0.0.1:$HOOK_PORT/hook
      max_batch: 10
YAML

start_webhook "$HOOK_PORT" 2         # 500 the first two POSTs, then 200
start_mock "$PORT" "$FROM" "$TO" 10 0.02 "$REORG_AT" 5

hp_run "$WORK/pipe.yaml"
assert_exit "$?" 0 "pipeline must exit 0 on EOF despite the 500s"

[ -s "$WORK/webhook.ndjson" ] || fail "the receiver recorded nothing"

report=$(python3 - "$WORK/webhook.ndjson" "$FROM" "$TO" <<'PY'
import json, sys
recs, frm, to = [json.loads(l) for l in open(sys.argv[1])], int(sys.argv[2]), int(sys.argv[3])
delivered, rollbacks, oversized, failed = set(), 0, 0, 0
for r in recs:
    if len(r["lines"]) > 10:
        oversized += 1
    if r["status"] != 200:
        failed += 1
    for line in r["lines"]:
        obj = json.loads(line)
        if obj.get("control") == "rollback":
            if r["status"] == 200:
                rollbacks += 1
        elif r["status"] == 200:
            delivered.add(obj["block_number"])
print(json.dumps({
    "posts": len(recs), "failed_posts": failed, "oversized": oversized,
    "rollbacks": rollbacks, "n_delivered": len(delivered),
    "missing": sorted(set(range(frm, to)) - delivered)[:5],
}))
PY
)
get() { echo "$report" | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }

assert_eq "$(get failed_posts)" "2" "the receiver 500'd exactly the first two POSTs"
assert_eq "$(get oversized)" "0" "no POST may exceed max_batch (10) lines"
assert_eq "$(get n_delivered)" "$BLOCKS" "every block must be delivered eventually (missing: $(get missing))"
assert_eq "$(get rollbacks)" "1" "the reorg must arrive as exactly one control line"

# The retried chunks were re-POSTed: that is at-least-once working as designed.
posts=$(get posts)
grep -qi 'retrying' "$LOG" || fail "expected the engine to log a retry after the 500s"

pass "$BLOCKS blocks delivered over $posts POSTs (2 x 500 absorbed by retry), chunks <= 10, 1 rollback line"

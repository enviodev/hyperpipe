#!/usr/bin/env bash
# E2E-12 — capability enforcement (negative security tests).
#
# The claim under test: a module can only reach what the YAML grants it, and a
# denial is a module-level error — not a crash, and not a silent success.
#
# (a) webhook sink with `permissions.http` missing -> every delivery is denied
#     and the receiver sees zero requests.
# (b) a granted-but-different host -> still denied (no prefix/suffix matching).
#
# The plan's third case (a sink writing SQL to a connection it was not granted)
# cannot be built from YAML: validation rejects a postgres sink whose
# config.connection is not in `connections`, and there is no chaos module to
# bypass it with. That path is covered instead by
# `host_impl::tests::sql_to_an_ungranted_connection_is_denied` (unit) and
# `postgres_sink_without_the_connection_grant_fails` (wasm-host integration).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "12-capabilities"

FROM=19000000
BLOCKS=20
TO=$((FROM + BLOCKS))

write_pipe() {   # write_pipe <file> <port> <hook_port> <permissions-block>
  cat > "$1" <<YAML
name: e2e-capabilities
runtime: { resource_size: s, checkpoint: { store: sqlite, path: $WORK/ck-$4.db } }
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
sinks:
  - name: hook
    module: builtin/webhook@1
    inputs: [decode]
$5
    config: { url: http://127.0.0.1:$3/hook, max_batch: 50 }
YAML
}

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"

# ---- (a) no permissions.http at all ----
PORT=$(free_port); HOOK_PORT=$(free_port)
write_pipe "$WORK/a.yaml" "$PORT" "$HOOK_PORT" "a" ""
start_webhook "$HOOK_PORT"
start_mock "$PORT" "$FROM" "$TO" 10 0.02

hp_run "$WORK/a.yaml"
rc=$?
grep -q 'http denied' "$LOG" || fail "(a) expected an 'http denied' module error"
grep -q 'pausing branch' "$LOG" || fail "(a) the denied branch should pause after its retries"
assert_exit "$rc" 0 "(a) a denied capability must not crash the process"

posts=$(python3 -c "
import json,urllib.request
print(json.load(urllib.request.urlopen('http://127.0.0.1:$HOOK_PORT/'))['count'])
")
assert_eq "$posts" "0" "(a) no request may reach the receiver"
[ ! -s "$WORK/webhook.ndjson" ] || fail "(a) the receiver recorded a body it should never have seen"

# ---- (b) allowlisted, but a different host than the URL targets ----
: > "$LOG"
PORT_B=$(free_port); HOOK_PORT_B=$(free_port)
write_pipe "$WORK/b.yaml" "$PORT_B" "$HOOK_PORT_B" "b" "    permissions: { http: [\"api.example.com\"] }"
start_webhook "$HOOK_PORT_B"
start_mock "$PORT_B" "$FROM" "$TO" 10 0.02

hp_run "$WORK/b.yaml"
rc=$?
assert_exit "$rc" 0 "(b) a denied capability must not crash the process"
grep -q 'http denied' "$LOG" || fail "(b) expected 127.0.0.1 to be denied under an api.example.com grant"
posts_b=$(python3 -c "
import json,urllib.request
print(json.load(urllib.request.urlopen('http://127.0.0.1:$HOOK_PORT_B/'))['count'])
")
assert_eq "$posts_b" "0" "(b) no request may reach the receiver"

pass "http denied without a grant and under a mismatched grant; receiver saw 0 requests; process survived both"

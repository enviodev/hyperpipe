#!/usr/bin/env bash
# E2E-14 — the Kubernetes contract: probes track the lifecycle, SIGTERM drains.
#
# Asserted here against a live process: /readyz and /healthz answer 200 once
# running, SIGTERM drains to exit 0, and a junk port disables the probes without
# taking the pipeline down.
#
# The STARTING (/readyz 503) and STOPPING (/healthz 503) windows are NOT asserted
# here: both are sub-second races against a probe, and polling for them would be
# flaky in exactly the way §8 forbids. They are covered deterministically by
# `health::tests::probes_track_the_pipeline_lifecycle`, which drives the same
# server through every state directly.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "14-health-shutdown"

PORT=$(free_port)
HEALTH_PORT=$(free_port)
FROM=19000000
HEAD=$((FROM + 100000))   # long-running: live mode with a far head

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-health
runtime: { resource_size: s, checkpoint: { store: sqlite, path: $WORK/ck.db } }
sources:
  - name: eth
    chain_id: 1
    url: http://127.0.0.1:$PORT
    mode: live
    from_block: $FROM
    confirmations: 10
    reorg: { enabled: true, window: 64 }
    query:
      logs: [{ address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"], topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]] }]
      field_selection: { log: [address,topic0,topic1,topic2,data,block_number,log_index,transaction_hash], block: [number,timestamp,hash] }
sinks:
  - name: out
    module: builtin/blackhole@1
    inputs: [eth]
YAML

# probe <port> <path> -> the HTTP status code
probe() {
  python3 - "$1" "$2" <<'PY'
import socket, sys
port, path = int(sys.argv[1]), sys.argv[2]
try:
    s = socket.create_connection(("127.0.0.1", port), 2)
    s.sendall(f"GET {path} HTTP/1.1\r\nhost: l\r\nconnection: close\r\n\r\n".encode())
    data = s.recv(200).decode("utf-8", "replace")
    s.close()
    print(data.split()[1] if data else "0")
except Exception:
    print("0")
PY
}

start_mock "$PORT" "$FROM" "$HEAD" 50 0.05

# ---- probes over the lifecycle ----
HYPERPIPE_HEALTH_PORT="$HEALTH_PORT" hp_start "$WORK/pipe.yaml"
pid=$HP_PID
wait_for "the health server to listen" "[ \"\$(probe $HEALTH_PORT /healthz)\" = '200' ]" 30

assert_eq "$(probe "$HEALTH_PORT" /healthz)" "200" "/healthz while running"
assert_eq "$(probe "$HEALTH_PORT" /readyz)" "200" "/readyz once running"
assert_eq "$(probe "$HEALTH_PORT" /anything)" "200" "an unknown path behaves like /healthz"

# ---- SIGTERM: drain, then exit 0 ----
kill -TERM "$pid" 2>/dev/null
wait_for "the process to exit after SIGTERM" "! kill -0 $pid 2>/dev/null" 30
wait "$pid" 2>/dev/null
rc=$?
assert_exit "$rc" 0 "SIGTERM must drain and exit 0"

# ---- a junk port disables the probes but must not stop the pipeline ----
: > "$LOG"
HYPERPIPE_HEALTH_PORT="not-a-port" hp_start "$WORK/pipe.yaml"
pid=$HP_PID
wait_for "the pipeline to run without a health server" "grep -q 'running (' '$LOG'" 30
grep -q 'not a valid port' "$LOG" || fail "expected a warning about the junk HYPERPIPE_HEALTH_PORT"
kill -0 "$pid" 2>/dev/null || fail "a junk health port must not kill the pipeline"
kill -TERM "$pid" 2>/dev/null
wait_for "the process to exit" "! kill -0 $pid 2>/dev/null" 30
wait "$pid" 2>/dev/null
assert_exit "$?" 0 "the probe-less run must still exit 0 on SIGTERM"

pass "/healthz+/readyz 200 while running, SIGTERM drained to exit 0, junk port degraded to a warning"

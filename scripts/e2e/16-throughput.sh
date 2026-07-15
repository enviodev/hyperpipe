#!/usr/bin/env bash
# E2E-16 — throughput smoke: a regression guardrail, NOT a benchmark.
#
# 5,000 blocks through decode -> blackhole with the mock serving as fast as it
# can. The bound is deliberately generous: this fails on an order-of-magnitude
# regression (a sync point in the hot path, a per-record allocation storm), not
# on a noisy machine. The measured rate is written to $E2E_ARTIFACTS for trend
# watching if that is set.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "16-throughput"

PORT=$(free_port)
FROM=19000000
BLOCKS=5000
TO=$((FROM + BLOCKS))
BOUND=60   # seconds

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-throughput
runtime: { resource_size: m, checkpoint: { store: sqlite, path: $WORK/ck.db } }
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
  - name: void
    module: builtin/blackhole@1
    inputs: [decode]
YAML

start_mock "$PORT" "$FROM" "$TO" 500 0

start=$(date +%s.%N)
hp_run "$WORK/pipe.yaml"
rc=$?
end=$(date +%s.%N)
assert_exit "$rc" 0 "the throughput run must reach EOF"

elapsed=$(python3 -c "print(f'{$end - $start:.2f}')")
rate=$(python3 -c "print(int($BLOCKS / max($end - $start, 0.001)))")
over=$(python3 -c "print(1 if $end - $start > $BOUND else 0)")
[ "$over" = "0" ] || fail "took ${elapsed}s for $BLOCKS blocks (bound ${BOUND}s) — likely an order-of-magnitude regression"

if [ -n "${E2E_ARTIFACTS:-}" ]; then
  mkdir -p "$E2E_ARTIFACTS"
  echo "{\"blocks\": $BLOCKS, \"seconds\": $elapsed, \"records_per_sec\": $rate}" \
    > "$E2E_ARTIFACTS/throughput.json"
fi

pass "$BLOCKS blocks in ${elapsed}s (~$rate rec/s, bound ${BOUND}s)"

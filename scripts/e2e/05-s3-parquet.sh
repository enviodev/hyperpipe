#!/usr/bin/env bash
# E2E-05 — s3 sink: Parquet objects to a local lake, deterministic keys, and the
# EOF flush of the final partial buffer.
#
# 250 blocks at flush_rows=100 means two full objects plus a 50-row remainder
# that only the shutdown flush can save. The rows encoded in the keys must sum
# to 250 — a lost remainder shows up as 200.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "05-s3-parquet"

PORT=$(free_port)
FROM=19000000
BLOCKS=250
TO=$((FROM + BLOCKS))
LAKE="$WORK/lake"
mkdir -p "$LAKE"

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-s3-parquet
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
  - name: lake
    module: builtin/s3@1
    inputs: [decode]
    connections: [blob]
    config: { connection: blob, format: parquet, flush_rows: 100 }
connections:
  blob:
    type: s3
    local_path: $LAKE
YAML

start_mock "$PORT" "$FROM" "$TO" 25 0.01

hp_run "$WORK/pipe.yaml"
assert_exit "$?" 0 "pipeline must exit 0 on EOF"

keys() { (cd "$LAKE" && find . -name '*.parquet' | sed 's|^\./||' | sort); }
mapfile -t OBJECTS < <(keys)
[ "${#OBJECTS[@]}" -gt 0 ] || fail "no parquet objects were written to $LAKE"

# Keys are {chain}/{first}-{last}-{rows}.parquet, and every file is real parquet.
total=0
for k in "${OBJECTS[@]}"; do
  [[ "$k" =~ ^1/([0-9]+)-([0-9]+)-([0-9]+)\.parquet$ ]] || fail "key does not match {chain}/{first}-{last}-{rows}.parquet: $k"
  rows="${BASH_REMATCH[3]}"
  total=$((total + rows))
  head -c 4 "$LAKE/$k" | grep -q PAR1 || fail "$k does not start with the PAR1 magic"
  tail -c 4 "$LAKE/$k" | grep -q PAR1 || fail "$k does not end with the PAR1 magic"
done
assert_eq "$total" "$BLOCKS" "rows summed across object keys (a lost tail flush shows up here)"

# The 50-row remainder was flushed by shutdown, not dropped.
echo "${OBJECTS[*]}" | grep -qE -- '-50\.parquet' || fail "expected a 50-row remainder object, got: ${OBJECTS[*]}"

# ---- idempotent overwrite: same range, wiped checkpoint -> same objects ----
declare -A SUMS
for k in "${OBJECTS[@]}"; do SUMS["$k"]=$(md5sum "$LAKE/$k" | cut -d' ' -f1); done

rm -f "$WORK/ck.db"
: > "$LOG"
hp_run "$WORK/pipe.yaml"
assert_exit "$?" 0 "replay run must exit 0"

mapfile -t OBJECTS2 < <(keys)
assert_eq "${OBJECTS2[*]}" "${OBJECTS[*]}" "replay must produce the same key set"
for k in "${OBJECTS2[@]}"; do
  sum=$(md5sum "$LAKE/$k" | cut -d' ' -f1)
  assert_eq "$sum" "${SUMS[$k]}" "replay must overwrite $k with byte-identical contents"
done

pass "${#OBJECTS[@]} parquet objects, $total rows total incl. the shutdown-flushed remainder, replay byte-identical"

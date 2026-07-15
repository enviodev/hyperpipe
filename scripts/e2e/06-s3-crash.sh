#!/usr/bin/env bash
# E2E-06 — buffering sink + kill -9: the §8.2-rule-5 proof.
#
# flush_rows is set beyond the range, so rows only reach an object via the
# checkpoint barrier's flush or the shutdown flush. A cursor that advanced past
# rows still sitting in the module's buffer would lose them on kill -9: the
# restart would resume above them and no object would ever contain them. So the
# assertion is coverage, not counts — every block must appear in some object.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "06-s3-crash"

PORT=$(free_port)
FROM=19000000
BLOCKS=120
TO=$((FROM + BLOCKS))
LAKE="$WORK/lake"
mkdir -p "$LAKE"

cp "$ROOT/examples/abis/erc20.json" "$WORK/erc20.json"
cat > "$WORK/pipe.yaml" <<YAML
name: e2e-s3-crash
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
    config: { connection: blob, format: ndjson, flush_rows: 100000 }
connections:
  blob:
    type: s3
    local_path: $LAKE
YAML

# Slow pagination so there is a mid-flight moment to kill.
start_mock "$PORT" "$FROM" "$TO" 10 0.25

# ---- run 1: kill -9 partway through ----
hp_start "$WORK/pipe.yaml"; pid=$HP_PID
# Wait until real progress exists, then kill without warning.
wait_for "the pipeline to ingest some blocks" "grep -q 'running (' '$LOG'" 20
sleep 2.5
kill -9 "$pid" 2>/dev/null
wait "$pid" 2>/dev/null
killed_cursor=$(ck_cursor "$WORK/ck.db" eth lake)
echo "killed at cursor=${killed_cursor:-<none>}" >> "$LOG"

# ---- restart until EOF ----
for attempt in 1 2 3 4 5; do
  hp_run "$WORK/pipe.yaml"
  rc=$?
  [ $rc -eq 0 ] && break
  [ $attempt -eq 5 ] && fail "pipeline never reached EOF after $attempt restarts (last rc=$rc)"
done

# ---- every block must live in some object ----
missing=$(python3 - "$LAKE" "$FROM" "$TO" <<'PY'
import json, pathlib, sys
lake, frm, to = pathlib.Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
seen = {}
for f in lake.rglob("*.ndjson"):
    for line in f.read_text().splitlines():
        if not line.strip():
            continue
        b = json.loads(line)["block_number"]
        seen.setdefault(b, []).append(f.name)
want = set(range(frm, to))
missing = sorted(want - set(seen))
dupes = {b: v for b, v in seen.items() if len(v) > 1}
# A block duplicated *within one object* would mean a buffer was flushed twice
# without being cleared — that is a bug, unlike a cross-object replay dupe.
intra = {b: v for b, v in dupes.items() if len(set(v)) != len(v)}
print(json.dumps({"missing": missing[:10], "n_missing": len(missing),
                  "n_dupes": len(dupes), "intra_object_dupes": len(intra)}))
PY
)
n_missing=$(echo "$missing" | python3 -c 'import json,sys; print(json.load(sys.stdin)["n_missing"])')
intra=$(echo "$missing" | python3 -c 'import json,sys; print(json.load(sys.stdin)["intra_object_dupes"])')
n_dupes=$(echo "$missing" | python3 -c 'import json,sys; print(json.load(sys.stdin)["n_dupes"])')

[ "$n_missing" = "0" ] || fail "records lost across the crash: $missing"
assert_eq "$intra" "0" "a block repeated inside one object means a buffer flushed twice"

pass "all $BLOCKS blocks present after kill -9 (cursor at kill: ${killed_cursor:-none}; $n_dupes replayed across objects — at-least-once)"

#!/usr/bin/env bash
# E2E-11 — the `validate` CLI, table-driven.
#
# Cheap and fast: no mock, no docker. Covers the shipped examples (they must all
# validate with secrets present) and the error rendering users actually hit —
# each broken config must fail with *its own* message, not a generic one.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "11-validate-matrix"

# Fake secrets: validation resolves refs, it never connects.
export HYPERPIPE_SECRET_PG_DSN="postgres://u:p@127.0.0.1:5432/db"
export HYPERPIPE_SECRET_PG_MAIN_DSN="postgres://u:p@127.0.0.1:5432/db"
export HYPERPIPE_SECRET_SLACK_WEBHOOK="https://hooks.slack.com/services/T/B/X"
export HYPERPIPE_SECRET_WEBHOOK_URL="https://example.com/hook"

failures=0

# ---- (a) every shipped example must validate ----
for yaml in "$ROOT"/examples/*.yaml; do
  out=$(hp_validate "$yaml")
  rc=$?
  if [ $rc -ne 0 ]; then
    echo "  FAIL $(basename "$yaml"): exit $rc — $out"
    failures=$((failures + 1))
  elif ! grep -qE '^valid: [0-9]+ source\(s\), [0-9]+ processor\(s\), [0-9]+ sink\(s\)$' <<<"$out"; then
    echo "  FAIL $(basename "$yaml"): unexpected summary — $out"
    failures=$((failures + 1))
  else
    echo "  ok   $(basename "$yaml"): $out"
  fi
done

# ---- (b) each broken config must fail with its own error ----
BASE='name: broken
sources:
  - name: eth
    chain: ethereum
    query: {}
processors:
  - name: decode
    module: builtin/evm-abi-decoder@1
    inputs: [eth]
    config: { abis: [] }
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [decode]
'

# case <name> <expected-substring> <yaml>
case_fails() {
  local name="$1" want="$2" yaml="$3"
  local f="$WORK/$name.yaml"
  printf '%s' "$yaml" > "$f"
  local out rc
  out=$(hp_validate "$f")
  rc=$?
  if [ $rc -eq 0 ]; then
    echo "  FAIL $name: expected a non-zero exit, got 0 — $out"
    failures=$((failures + 1))
    return
  fi
  case "$out" in
    *"$want"*) echo "  ok   $name: rejected with '$want'" ;;
    *)
      echo "  FAIL $name: expected '$want', got: $out"
      failures=$((failures + 1))
      ;;
  esac
}

case_fails "unknown-key" "unknown field" \
  "${BASE/name: broken/name: broken
typo_key: 3}"

case_fails "cycle" "cycle in DAG" 'name: cyc
sources:
  - name: eth
    chain: ethereum
    query: {}
processors:
  - name: a
    module: builtin/filter@1
    inputs: [eth, b]
  - name: b
    module: builtin/filter@1
    inputs: [a]
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [b]
'

case_fails "orphan-processor" "orphan" 'name: orphan
sources:
  - name: eth
    chain: ethereum
    query: {}
processors:
  - name: dead
    module: builtin/filter@1
    inputs: [eth]
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [eth]
'

case_fails "missing-secret" "unresolved secrets" 'name: sec
sources:
  - name: eth
    chain: ethereum
    query: {}
sinks:
  - name: out
    module: builtin/postgres@1
    inputs: [eth]
    connections: [pg]
    config: { connection: pg, table: t }
connections:
  pg:
    type: postgres
    dsn: ${secret:DEFINITELY_NOT_SET_ANYWHERE}
'

case_fails "bad-builtin-ref" "not a valid" \
  "${BASE/builtin\/stdout@1/builtin\/stdout}"

case_fails "backfill-without-to-block" "requires \`to_block\`" 'name: backfill
sources:
  - name: eth
    chain: ethereum
    mode: backfill
    query: {}
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [eth]
'

case_fails "live-with-to-block" "forbids \`to_block\`" 'name: live
sources:
  - name: eth
    chain: ethereum
    mode: live
    to_block: 100
    query: {}
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [eth]
'

case_fails "postgres-sink-without-grant" "must also be listed in \`connections\`" 'name: grant
sources:
  - name: eth
    chain: ethereum
    query: {}
sinks:
  - name: out
    module: builtin/postgres@1
    inputs: [eth]
    config: { connection: pg, table: t }
connections:
  pg:
    type: postgres
    dsn: postgres://u:p@h/db
'

case_fails "unknown-chain" "unknown chain" \
  "${BASE/chain: ethereum/chain: narnia}"

case_fails "input-does-not-exist" "does not name any node" \
  "${BASE/inputs: \[decode\]/inputs: [nope]}"

case_fails "no-sinks" "no sinks" 'name: nosinks
sources:
  - name: eth
    chain: ethereum
    query: {}
sinks: []
'

# `validate` on a path that does not exist must be a clean Io error, not a panic.
out=$(timeout 60 "$BIN" validate "$WORK/does-not-exist.yaml" 2>&1)
rc=$?
if [ $rc -eq 0 ]; then
  echo "  FAIL missing-file: expected a non-zero exit, got 0"
  failures=$((failures + 1))
else
  case "$out" in
    *"reading"*) echo "  ok   missing-file: rejected with 'reading'" ;;
    *) echo "  FAIL missing-file: expected 'reading', got: $out"; failures=$((failures + 1)) ;;
  esac
fi

[ "$failures" = "0" ] || fail "$failures validate case(s) behaved unexpectedly"
pass "all examples validate; every broken config rejected with its own error"

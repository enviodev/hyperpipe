#!/usr/bin/env bash
# E2E-04 — reorg: fork mid-backfill, assert the rollback was detected, the
# forked rows deleted, and the canonical blocks re-ingested.
#
# scripts/reorg-test.sh owns the scenario; this wrapper adds the assertions the
# plan calls for: the reorg is reported exactly once (not a detection loop), and
# the final table has no dupes and no gaps.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "04-reorg-rollback"
require_pg

PG_CONTAINER="$PG_CONTAINER" PG_USER="$PG_USER" PG_DB="$PG_DB" PG_DSN="$PG_DSN" \
  timeout 300 "$ROOT/scripts/reorg-test.sh" >>"$LOG" 2>&1
rc=$?
grep -q '^PASS' "$LOG" || { grep -E '^FAIL' "$LOG" >&2 || true; fail "reorg-test.sh exited $rc without PASS"; }

# Detected once — a reorg that re-triggers every poll would mean the tracker
# never converged on the new chain.
detections=$(grep -ci 'reorg detected' "$LOG")
assert_eq "$detections" "1" "number of 'reorg detected' log lines"

# And the rollback was actually applied downstream, not just logged.
grep -qi 'rollback applied\|postgres rollback' "$LOG" || fail "no evidence the sink applied the rollback"

FROM=19000000
BLOCKS=120
TO=$((FROM + BLOCKS))
FORK=$((FROM + 60 - 5))   # REORG_AT - REORG_DEPTH, per reorg-test.sh

count=$(pg_q "SELECT count(*) FROM reorg_test;")
distinct=$(pg_q "SELECT count(DISTINCT block_number) FROM reorg_test;")
assert_eq "$count" "$BLOCKS" "row count (no dupes)"
assert_eq "$distinct" "$BLOCKS" "distinct blocks (no gaps)"

# Every row at or past the fork carries the post-fork tx hash; nothing stale.
stale=$(pg_q "SELECT count(*) FROM reorg_test WHERE block_number >= $FORK AND transaction_hash NOT LIKE '0xbeef%';")
assert_eq "$stale" "0" "stale pre-reorg rows past the fork"
forked=$(pg_q "SELECT count(*) FROM reorg_test WHERE transaction_hash LIKE '0xbeef%';")
assert_eq "$forked" "$((TO - FORK))" "post-fork rows re-ingested"

pass "reorg detected once, $forked forked rows replaced, $count rows with no dupes or gaps"

#!/usr/bin/env bash
# E2E-03 — crash recovery: kill -9 mid-backfill, restart until EOF, assert every
# block landed exactly once.
#
# The proof itself lives in scripts/crash-test.sh (kept as the demo-able script);
# this is the run-all wrapper, plus the assertion the plan adds on top: after the
# final EOF the persisted cursor must equal END for every (source, sink) pair —
# i.e. the checkpoint, not just the rows, survived the crashes.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup "03-crash-recovery"
require_pg
command -v sqlite3 >/dev/null || skip "sqlite3 needed to inspect the checkpoint db"

# crash-test.sh picks its own pg target from these; hand it ours, plus a workdir
# inside this scenario's private $WORK so the checkpoint db can be inspected
# afterwards (and is cleaned up with everything else).
CRASH_WORK="$WORK/crash"
WORK="$CRASH_WORK" PG_CONTAINER="$PG_CONTAINER" PG_USER="$PG_USER" PG_DB="$PG_DB" PG_DSN="$PG_DSN" \
  timeout 300 "$ROOT/scripts/crash-test.sh" >>"$LOG" 2>&1
rc=$?
if [ $rc -ne 0 ]; then
  grep -E '^(PASS|FAIL)' "$LOG" >&2 || true
  fail "crash-test.sh exited $rc"
fi
grep -q '^PASS' "$LOG" || fail "crash-test.sh did not report PASS"

# The added assertion: the cursor is durable at END, so a further restart would
# fetch nothing rather than replaying the range.
CK="$CRASH_WORK/ck.db"
[ -f "$CK" ] || fail "expected a checkpoint db at $CK"
END=19000300
rows=$(sqlite3 "$CK" 'select source, sink, next_block from cursors;')
[ -n "$rows" ] || fail "checkpoint db has no cursors"
while IFS='|' read -r source sink next; do
  [ -z "$source" ] && continue
  assert_eq "$next" "$END" "cursor for ($source, $sink) after the final EOF"
done <<< "$rows"

pass "every block exactly once across kill -9 restarts; all cursors durable at $END"

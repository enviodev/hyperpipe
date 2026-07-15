#!/usr/bin/env bash
# Shared helpers for the e2e scenarios (see docs/E2E_TEST_PLAN.md).
#
# Every scenario sources this, then uses:
#   setup <name>            — tempdir, log paths, cleanup trap
#   free_port               — an unused TCP port (never hardcode: CI runs parallel)
#   start_mock ...          — mock-hypersync with env, waits until it answers
#   hp_run <yaml> [args...] — run the pipeline under a timeout, log to $LOG
#   wait_for <desc> <cmd>   — poll a condition to a deadline (never `sleep && assert`)
#   require_pg / pg_q       — postgres, soft-skip locally / hard-fail in CI
#   pass / fail             — verdict; fail dumps the logs that explain it
#
# Conventions: no bare sleeps, no hardcoded ports, no writes outside $WORK.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_DIR="$ROOT/scripts/e2e"

# `run-all.sh --instrumented` builds a coverage-instrumented binary and exports
# its path plus LLVM_PROFILE_FILE, so scenarios exec the real binary either way.
# (Not `cargo llvm-cov run`: that wraps the pipeline in cargo, so $! would be
# cargo's pid and a SIGTERM would never reach the process under test.)
BIN="${E2E_BIN:-$ROOT/target/debug/hyperpipe}"
: "${E2E_INSTRUMENTED:=0}"
# CI sets this: a scenario that cannot provision its deps must fail, not skip.
: "${E2E_CI:=0}"
: "${E2E_TIMEOUT:=120}"

# Postgres target. hp-pg lives on a non-default port: 5432/5433 are routinely
# taken by other projects on a dev box, and a test suite that writes into
# whatever answers on 5433 is a bad neighbour. Override PG_PORT if 5436 is busy.
: "${PG_CONTAINER:=hp-pg}"
: "${PG_USER:=postgres}"
: "${PG_DB:=hp}"
: "${PG_PORT:=5436}"
: "${PG_DSN:=postgres://postgres:hp@127.0.0.1:${PG_PORT}/${PG_DB}}"

SCENARIO=""
WORK=""
LOG=""
MOCK_LOG=""
PIDS=()

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

setup() {
  SCENARIO="$1"
  WORK="$(mktemp -d "/tmp/hp-e2e-${SCENARIO}.XXXXXX")"
  LOG="$WORK/pipeline.log"
  MOCK_LOG="$WORK/mock.log"
  : > "$LOG"
  : > "$MOCK_LOG"
  export HYPERPIPE_MODULE_DIR="${HYPERPIPE_MODULE_DIR:-$ROOT/modules/target/wasm32-wasip2/debug}"
  export RUST_LOG="${RUST_LOG:-hyperpipe=info,hp_source_hypersync=info}"
  trap cleanup EXIT
  need_bin
}

# The redirect is on the function: bash announces reaped background jobs
# ("Killed  python3 ...") on stderr, which would look like a scenario failure.
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill -9 "$pid" 2>/dev/null
  done
  wait 2>/dev/null
  [ -n "${KEEP_WORK:-}" ] || rm -rf "$WORK"
} 2>/dev/null

track() { PIDS+=("$1"); }

need_bin() {
  [ -x "$BIN" ] || { echo "build first: cargo build -p hp-cli"; exit 1; }
  [ -d "$HYPERPIPE_MODULE_DIR" ] || {
    echo "build modules first: (cd modules && cargo build --target wasm32-wasip2)"
    exit 1
  }
}

# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------

pass() {
  echo "PASS: $SCENARIO — $*"
  exit 0
}

# Dump everything needed to explain the failure: no re-running to find out why.
fail() {
  echo "FAIL: $SCENARIO — $*"
  echo "---- pipeline log (tail 40) ----"
  tail -40 "$LOG" 2>/dev/null
  if [ -s "$MOCK_LOG" ]; then
    echo "---- mock log (tail 10) ----"
    tail -10 "$MOCK_LOG" 2>/dev/null
  fi
  if [ -f "$WORK/ck.db" ] && command -v sqlite3 >/dev/null; then
    echo "---- checkpoint cursors ----"
    sqlite3 "$WORK/ck.db" 'select * from cursors;' 2>/dev/null
  fi
  exit 1
}

# A dependency is missing: locally that is a skip, in CI it is a failure.
skip() {
  if [ "$E2E_CI" = "1" ]; then
    echo "FAIL: $SCENARIO — dependency missing in CI: $*"
    exit 1
  fi
  echo "SKIP: $SCENARIO — $*"
  exit 0
}

# ---------------------------------------------------------------------------
# Ports / polling
# ---------------------------------------------------------------------------

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# wait_for <desc> <shell-cmd> [timeout_s] — poll until the command succeeds.
wait_for() {
  local desc="$1" cmd="$2" timeout="${3:-20}"
  local deadline=$(( SECONDS + timeout ))
  while [ $SECONDS -lt $deadline ]; do
    if eval "$cmd" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  fail "timed out after ${timeout}s waiting for: $desc"
}

wait_for_port() {
  wait_for "port $1 to accept connections" \
    "python3 -c 'import socket,sys; socket.create_connection((\"127.0.0.1\", $1), 1).close()'" \
    "${2:-20}"
}

# ---------------------------------------------------------------------------
# Mocks
# ---------------------------------------------------------------------------

# start_mock PORT FROM END [STEP] [DELAY] [REORG_AT] [REORG_DEPTH]
start_mock() {
  local port="$1" from="$2" end="$3" step="${4:-10}" delay="${5:-0.02}"
  local reorg_at="${6:-0}" reorg_depth="${7:-3}"
  PORT="$port" FROM="$from" END="$end" STEP="$step" DELAY="$delay" \
    REORG_AT="$reorg_at" REORG_DEPTH="$reorg_depth" \
    python3 "$ROOT/scripts/mock-hypersync.py" >>"$MOCK_LOG" 2>&1 &
  track $!
  wait_for_port "$port"
}

# start_webhook PORT [FAIL_FIRST] [FAIL_ALWAYS] — records POST bodies to
# $WORK/webhook.ndjson; prints its recorded file path.
start_webhook() {
  local port="$1" fail_first="${2:-0}" fail_always="${3:-0}"
  PORT="$port" FAIL_FIRST="$fail_first" FAIL_ALWAYS="$fail_always" \
    RECORD="$WORK/webhook.ndjson" \
    python3 "$E2E_DIR/mock-webhook.py" >>"$MOCK_LOG" 2>&1 &
  track $!
  wait_for_port "$port"
}

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

# hp_run <yaml> [extra args...] — foreground, under a timeout. Returns the exit
# code; output appends to $LOG.
hp_run() {
  local yaml="$1"; shift
  timeout "$E2E_TIMEOUT" "$BIN" run "$yaml" "$@" >>"$LOG" 2>&1
}

# hp_start <yaml> [args...] — background; sets $HP_PID.
#
# Deliberately not `pid=$(hp_start ...)`: command substitution forks a subshell,
# so the pipeline would not be a child of the script (`wait` returns 127) and the
# `track` below would register it in a shell that is about to disappear, leaving
# the process alive after cleanup.
HP_PID=""
hp_start() {
  local yaml="$1"; shift
  "$BIN" run "$yaml" "$@" >>"$LOG" 2>&1 &
  HP_PID=$!
  track "$HP_PID"
}

hp_validate() {
  timeout 60 "$BIN" validate "$1" 2>&1
}

# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

require_pg() {
  command -v docker >/dev/null || skip "docker not available"
  docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER" >/dev/null 2>&1 || skip \
    "postgres container '$PG_CONTAINER' not running — start it with:
    docker run -d --name $PG_CONTAINER -e POSTGRES_PASSWORD=hp -e POSTGRES_DB=$PG_DB -p $PG_PORT:5432 postgres:16-alpine"
  export HYPERPIPE_SECRET_PG_DSN="$PG_DSN"
}

pg_q() {
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "$1" | tr -d '[:space:]'
}

pg_drop() {
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "DROP TABLE IF EXISTS $1;" >/dev/null
}

# ---------------------------------------------------------------------------
# Checkpoint DB
# ---------------------------------------------------------------------------

# ck_cursor <db> <source> <sink> — the persisted watermark, or empty.
ck_cursor() {
  command -v sqlite3 >/dev/null || return 0
  sqlite3 "$1" "select next_block from cursors where source='$2' and sink='$3';" 2>/dev/null | tr -d '[:space:]'
}

# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

assert_eq() {
  [ "$1" = "$2" ] || fail "${3:-assertion}: expected '$2', got '$1'"
}

assert_contains() {
  case "$1" in
    *"$2"*) : ;;
    *) fail "${3:-assertion}: expected output to contain '$2'" ;;
  esac
}

assert_exit() {
  assert_eq "$1" "$2" "${3:-exit code}"
}

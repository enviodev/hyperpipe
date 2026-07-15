#!/usr/bin/env bash
# Run the e2e suite. See docs/E2E_TEST_PLAN.md.
#
#   ./scripts/e2e/run-all.sh                 # everything
#   ./scripts/e2e/run-all.sh --only 05       # one scenario (prefix match)
#   ./scripts/e2e/run-all.sh --instrumented  # coverage-instrumented binary
#   ./scripts/e2e/run-all.sh --instrumented --clean   # ...ignoring earlier profiles
#   ./scripts/e2e/run-all.sh --ci            # a skipped scenario is a failure
#
# --instrumented merges into whatever profiles already exist, so the CI order in
# docs/E2E_TEST_PLAN.md §9 (unit tests --no-report, then this, then
# `cargo llvm-cov report`) yields one number covering both.
#
# Each scenario is independent (own ports, own tempdir, own pg table), so this
# can be parallelized later without touching the scenarios themselves.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_DIR="$ROOT/scripts/e2e"

ONLY=""
CLEAN_PROFILES=0
export E2E_INSTRUMENTED="${E2E_INSTRUMENTED:-0}"
export E2E_CI="${E2E_CI:-0}"

while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="$2"; shift 2 ;;
    --instrumented) E2E_INSTRUMENTED=1; shift ;;
    --clean) CLEAN_PROFILES=1; shift ;;
    --ci) E2E_CI=1; shift ;;
    -h|--help) sed -n '2,14p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown flag: $1"; exit 2 ;;
  esac
done

# ---- preflight ----
if [ "$E2E_INSTRUMENTED" = "1" ]; then
  command -v cargo-llvm-cov >/dev/null || { echo "cargo llvm-cov not installed: cargo install cargo-llvm-cov"; exit 1; }
  # Deliberately no `llvm-cov clean` here: CI runs the unit/integration suites
  # with --no-report first, and cleaning would throw their profiles away before
  # this run's could merge with them. Pass --clean for a standalone e2e-only
  # coverage number.
  if [ "$CLEAN_PROFILES" = "1" ]; then
    echo "==> clearing previous coverage profiles"
    ( cd "$ROOT" && cargo llvm-cov clean --workspace )
  fi
  # show-env gives the profile path + rustc wrapper; building under it yields a
  # binary that writes profraw when run directly (so signals and pids stay ours).
  # If the caller already sourced it (the CI order in §9 does), re-evaluating
  # would ask the rustc wrapper to wrap itself and every build fails.
  if [ "${CARGO_LLVM_COV:-0}" != "1" ]; then
    eval "$( cd "$ROOT" && cargo llvm-cov show-env --export-prefix 2>/dev/null )"
  fi
  export LLVM_PROFILE_FILE
  export E2E_BIN="$ROOT/target/debug/hyperpipe"

  # Count rather than `grep -q`: under `set -o pipefail` a quitting grep closes
  # the pipe, nm dies of SIGPIPE, and the pipeline reports failure even on a
  # match — which reads as "not instrumented" for a perfectly good binary.
  prf_symbols() { nm "$1" 2>/dev/null | grep -c '__llvm_prf' || true; }

  # cargo's fingerprint does not notice llvm-cov's rustc wrapper, so a plain
  # `cargo build` here would happily reuse an *uninstrumented* binary and the
  # run would produce no profiles at all. Force a rebuild when the binary lacks
  # coverage symbols. In the CI order (§9) the test run already built everything
  # under this same env, so this is a no-op — which is what keeps the unit
  # profiles intact for the merge.
  if [ "$(prf_symbols "$E2E_BIN")" -eq 0 ]; then
    echo "==> building an instrumented binary"
    ( cd "$ROOT" && cargo clean -p hp-cli && cargo build -p hp-cli ) || exit 1
  else
    echo "==> reusing the already-instrumented binary"
  fi
  [ "$(prf_symbols "$E2E_BIN")" -gt 0 ] || {
    echo "the binary is still not instrumented; coverage would be empty"; exit 1
  }
else
  [ -x "$ROOT/target/debug/hyperpipe" ] || { echo "==> building the binary"; ( cd "$ROOT" && cargo build -p hp-cli ) || exit 1; }
fi
[ -d "$ROOT/modules/target/wasm32-wasip2/debug" ] || {
  echo "==> building guest modules"
  ( cd "$ROOT/modules" && cargo build --target wasm32-wasip2 ) || exit 1
}

mapfile -t SCENARIOS < <(find "$E2E_DIR" -maxdepth 1 -name '[0-9][0-9]-*.sh' | sort)
[ "${#SCENARIOS[@]}" -gt 0 ] || { echo "no scenarios found in $E2E_DIR"; exit 1; }

passed=0; failed=0; skipped=0
FAILED_NAMES=()
SUMMARY=()

for s in "${SCENARIOS[@]}"; do
  name="$(basename "$s" .sh)"
  if [ -n "$ONLY" ] && [[ "$name" != "$ONLY"* ]]; then
    continue
  fi
  echo "==> $name"
  start=$SECONDS
  out=$(bash "$s" 2>&1)
  rc=$?
  took=$((SECONDS - start))
  verdict=$(grep -E '^(PASS|FAIL|SKIP):' <<<"$out" | tail -1)

  case "$verdict" in
    PASS:*)
      passed=$((passed + 1))
      SUMMARY+=("  PASS  ${name}  (${took}s)")
      echo "${verdict}"
      ;;
    SKIP:*)
      skipped=$((skipped + 1))
      SUMMARY+=("  SKIP  ${name}  (${took}s)")
      echo "${verdict}"
      ;;
    *)
      failed=$((failed + 1))
      FAILED_NAMES+=("$name")
      SUMMARY+=("  FAIL  ${name}  (${took}s)")
      echo "$out"
      ;;
  esac
done

# ---- coverage report ----
if [ "$E2E_INSTRUMENTED" = "1" ]; then
  echo "==> merging e2e profiles into the coverage report"
  ( cd "$ROOT" && cargo llvm-cov report --summary-only --ignore-filename-regex 'testutil' )
fi

echo
echo "======================= e2e summary ======================="
printf '%s\n' "${SUMMARY[@]}"
echo "-----------------------------------------------------------"
echo "  $passed passed, $failed failed, $skipped skipped"
[ "${#FAILED_NAMES[@]}" -eq 0 ] || echo "  failed: ${FAILED_NAMES[*]}"
echo "==========================================================="

# In CI a skip means a dependency silently went missing: fail the job.
if [ "$failed" -gt 0 ]; then
  exit 1
fi
if [ "$E2E_CI" = "1" ] && [ "$skipped" -gt 0 ]; then
  echo "CI requires every scenario to run; $skipped were skipped"
  exit 1
fi
exit 0

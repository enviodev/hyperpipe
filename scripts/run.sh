#!/usr/bin/env bash
# HyperPipe launcher: load an env file, build what's missing, validate, and run.
#
#   ./scripts/run.sh <pipeline.yaml> [env-file] [-- extra hyperpipe args]
#
# Examples:
#   ./scripts/run.sh examples/usdc-multichain.yaml examples/pipeline.env
#   ./scripts/run.sh my.yaml my.env -- --debug-stdout
#
# Env knobs:
#   HYPERPIPE_PROFILE=release|debug   binary build profile (default: release)
#   HYPERPIPE_MODULE_PROFILE=debug|release  module build profile (default: debug)
#   HYPERPIPE_REBUILD=1               force a rebuild of the binary + modules
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

die() { echo "error: $*" >&2; exit 1; }
info() { echo ">> $*" >&2; }

# ---- args ----
[ $# -ge 1 ] || die "usage: run.sh <pipeline.yaml> [env-file] [-- extra args]"
PIPELINE="$1"; shift
[ -f "$PIPELINE" ] || die "pipeline file not found: $PIPELINE"

ENV_FILE=""
if [ $# -ge 1 ] && [ "$1" != "--" ]; then
  ENV_FILE="$1"; shift
elif [ -f "${PIPELINE%/*}/.env" ]; then
  ENV_FILE="${PIPELINE%/*}/.env"
elif [ -f ".env" ]; then
  ENV_FILE=".env"
fi
# drop a leading `--` separator before pass-through args
[ "${1:-}" = "--" ] && shift || true
EXTRA=("$@")

# ---- load env ----
load_env() {
  local f="$1" line key val
  [ -f "$f" ] || die "env file not found: $f"
  info "loading env from $f"
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"                       # strip CR (Windows files)
    case "$line" in ''|'#'*) continue ;; esac  # skip blank / comment
    line="${line#export }"                     # allow `export KEY=VAL`
    key="${line%%=*}"; val="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"       # rtrim key
    key="${key#"${key%%[![:space:]]*}"}"       # ltrim key
    case "$key" in ''|*[!A-Za-z0-9_]*) continue ;; esac  # valid var names only
    case "$val" in                             # strip matching surrounding quotes
      \"*\") val="${val#\"}"; val="${val%\"}" ;;
      \'*\') val="${val#\'}"; val="${val%\'}" ;;
    esac
    export "$key=$val"
  done < "$f"
}
[ -n "$ENV_FILE" ] && load_env "$ENV_FILE" || info "no env file (proceeding without secrets)"

# ---- build binary if missing ----
PROFILE="${HYPERPIPE_PROFILE:-release}"
case "$PROFILE" in release) FLAG=(--release) ;; debug) FLAG=() ;; *) die "HYPERPIPE_PROFILE must be release|debug" ;; esac
BIN="$ROOT/target/$PROFILE/hyperpipe"
if [ ! -x "$BIN" ] || [ "${HYPERPIPE_REBUILD:-0}" = 1 ]; then
  info "building hyperpipe ($PROFILE)"
  ( cd "$ROOT" && cargo build "${FLAG[@]}" -p hp-cli )
fi

# ---- build modules if missing ----
MODPROFILE="${HYPERPIPE_MODULE_PROFILE:-debug}"
MODDIR="$ROOT/modules/target/wasm32-wasip2/$MODPROFILE"
need_modules=0
if [ ! -d "$MODDIR" ] || [ "$(ls "$MODDIR"/*.wasm 2>/dev/null | wc -l)" -lt 7 ] || [ "${HYPERPIPE_REBUILD:-0}" = 1 ]; then
  need_modules=1
fi
if [ "$need_modules" = 1 ]; then
  rustup target list --installed 2>/dev/null | grep -q wasm32-wasip2 || {
    info "adding wasm32-wasip2 target"; rustup target add wasm32-wasip2
  }
  info "building wasm modules ($MODPROFILE)"
  MODFLAG=(); [ "$MODPROFILE" = release ] && MODFLAG=(--release)
  ( cd "$ROOT/modules" && cargo build --target wasm32-wasip2 "${MODFLAG[@]}" )
fi
export HYPERPIPE_MODULE_DIR="$MODDIR"

# ---- token hint ----
if [ -z "${HYPERSYNC_BEARER_TOKEN:-}" ] && ! printf '%s\0' "${EXTRA[@]}" | grep -qz -- --debug-stdout; then
  info "note: HYPERSYNC_BEARER_TOKEN not set — live sources will 401. Create one at https://app.envio.dev/api-tokens (or pass -- --debug-stdout with a mock)."
fi

# ---- validate then run ----
info "validating $PIPELINE"
"$BIN" validate "$PIPELINE" || die "validation failed"
info "starting pipeline (ctrl-c to stop)"
exec "$BIN" run "$PIPELINE" "${EXTRA[@]}"

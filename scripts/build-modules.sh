#!/usr/bin/env bash
# Build all HyperPipe guest (wasm) modules to components.
set -euo pipefail
cd "$(dirname "$0")/../modules"
PROFILE_FLAG=""
[ "${1:-}" = "--release" ] && PROFILE_FLAG="--release"
cargo build --target wasm32-wasip2 $PROFILE_FLAG
echo "modules built ->  modules/target/wasm32-wasip2/${1:+release}${1:-debug}/"

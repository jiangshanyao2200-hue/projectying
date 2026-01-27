#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

"$ROOT/scripts/bootstrap.sh"

profile="--release"
if [[ "${1:-}" == "--debug" ]]; then
  profile=""
  shift
fi

exec cargo run ${profile} -- "$@"


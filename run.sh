#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STATE_HOME="${XDG_STATE_HOME:-$HOME/.local/state}"
CACHE_HOME="${XDG_CACHE_HOME:-$HOME/.cache}"
STATE_DIR="$STATE_HOME/projectying"
CACHE_DIR="$CACHE_HOME/projectying"
MARKER_STATE="$STATE_DIR/first_run_selfcheck_v1.done"
MARKER_CACHE="$CACHE_DIR/first_run_selfcheck_v1.done"
MARKER="$MARKER_STATE"

force_selfcheck=0
skip_selfcheck=0
selfcheck_only=0

rest=()
for arg in "$@"; do
  case "$arg" in
    --force-selfcheck)
      force_selfcheck=1
      ;;
    --skip-selfcheck)
      skip_selfcheck=1
      ;;
    --selfcheck)
      selfcheck_only=1
      ;;
    *)
      rest+=("$arg")
      ;;
  esac
done

if ! mkdir -p "$STATE_DIR" 2>/dev/null; then
  MARKER="$MARKER_CACHE"
  mkdir -p "$CACHE_DIR" 2>/dev/null || true
fi

write_marker() {
  local ts
  ts="$(date '+%Y-%m-%d %H:%M:%S')"
  mkdir -p "$(dirname "$MARKER")" 2>/dev/null || return 1
  { printf '%s\n' "$ts" > "$MARKER"; } 2>/dev/null || return 1
}

if (( selfcheck_only )); then
  "$ROOT/scripts/selfcheck.sh"
  write_marker || true
  exit 0
fi

if (( !skip_selfcheck )); then
  if (( force_selfcheck )) || [[ ! -f "$MARKER" ]]; then
    echo "[run] 首次启动/强制模式：先执行自检..."
    "$ROOT/scripts/selfcheck.sh"
    write_marker || true
  fi
fi

exec "$ROOT/scripts/run.sh" "${rest[@]}"

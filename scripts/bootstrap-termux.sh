#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v pkg >/dev/null 2>&1; then
  echo "[bootstrap-termux] 未检测到 pkg（这看起来不像 Termux 环境）。" >&2
  exit 1
fi

declare -a need_pkgs=()
declare -A seen_pkg=()

need_pkg() {
  local pkg="$1"
  if [[ -z "${seen_pkg[$pkg]:-}" ]]; then
    need_pkgs+=("$pkg")
    seen_pkg[$pkg]=1
  fi
}

need_cmd_pkg() {
  local cmd="$1"
  local pkg="$2"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    need_pkg "$pkg"
  fi
}

# Rust 构建 + rusqlite(bundled) 需要 C 编译器
need_cmd_pkg cargo rust
need_cmd_pkg rustc rust
need_cmd_pkg clang clang

# 运行时/工具链常用
need_cmd_pkg git git
need_cmd_pkg timeout coreutils
need_cmd_pkg adb android-tools
need_cmd_pkg rg ripgrep

# Termux:API（用于 wake-lock 与 termux_api 工具）
need_cmd_pkg termux-wake-lock termux-api
need_cmd_pkg termux-wake-unlock termux-api
need_cmd_pkg termux-battery-status termux-api

if ((${#need_pkgs[@]})); then
  echo "[bootstrap-termux] 将自动安装缺失依赖：${need_pkgs[*]}"
  pkg update -y
  pkg install -y "${need_pkgs[@]}"
else
  echo "[bootstrap-termux] 依赖已齐全"
fi

if command -v termux-battery-status >/dev/null 2>&1; then
  if ! termux-battery-status >/dev/null 2>&1; then
    cat >&2 <<'EOF'
[bootstrap-termux] 注意：termux-api 命令已存在，但调用失败。
这通常意味着：
1) 未安装 Android 端的 Termux:API APP；或
2) 未授予对应权限；或
3) Termux:API 与 termux-api 包版本不匹配。
请先在手机上安装 Termux:API 并授予权限，然后重试。
EOF
  fi
fi

echo "[bootstrap-termux] ok"

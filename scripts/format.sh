#!/usr/bin/env bash
#
# Apply every formatter the repository enforces, in place. This is the single
# fix entry point: clang-format over C/H and ruff format + ruff check --fix
# over Python. Run it when a gate reports formatting violations; rerun
# scripts/verify.sh afterward to confirm the gate is green.
#
# Idempotent: a second run is a no-op on already-compliant files. Never fails
# a build: it only fixes. Verification lives in scripts/verify.sh and the
# check-* CMake targets; this script is their fix-side counterpart.
#
# Usage:
#   scripts/format.sh
#
# Run from the repository root. Operates on git-tracked files only, so
# untracked scratch files are left alone.

set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v clang-format-22 >/dev/null 2>&1; then
    echo "format.sh: 'clang-format-22' not found; install the clang-format 22" >&2
    echo "toolchain the pre-commit hook pins" >&2
    exit 1
fi
if ! command -v ruff >/dev/null 2>&1; then
    echo "format.sh: 'ruff' not found; run 'uv sync' or 'pip install ruff'" >&2
    exit 1
fi

# git ls-files keeps the list current without a reconfigure and excludes
# untracked scratch files. third_party/ is excluded to match the
# .pre-commit-config.yaml and cmake/checks.cmake exclude rules.
c_h_files=$(git ls-files "*.c" "*.h" | grep -v "^third_party/" || true)
py_files=$(git ls-files "*.py" || true)

echo "==> format.sh: clang-format (C/H)"
if [ -n "$c_h_files" ]; then
    # shellcheck disable=SC2086
    clang-format-22 -i $c_h_files
fi

echo "==> format.sh: ruff format + ruff check --fix (Python)"
if [ -n "$py_files" ]; then
    # shellcheck disable=SC2086
    ruff format $py_files
    # shellcheck disable=SC2086
    ruff check --fix $py_files
fi

echo "format.sh: done. Run ./scripts/verify.sh to confirm the gate is green."

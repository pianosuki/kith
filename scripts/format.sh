#!/usr/bin/env bash
#
# Apply every formatter the repository enforces, in place. This is the single
# fix entry point: clang-format over C/H, ruff format + ruff check --fix over
# Python, and the trivial whitespace/line-ending/BOM fixers over both. Run it
# when a gate reports formatting violations; rerun scripts/verify.sh afterward
# to confirm the gate is green.
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

if ! command -v clang-format-23 >/dev/null 2>&1; then
    echo "format.sh: 'clang-format-23' not found; install the clang-format 23" >&2
    echo "toolchain the pre-commit hook pins" >&2
    exit 1
fi
if ! command -v ruff >/dev/null 2>&1; then
    echo "format.sh: 'ruff' not found; run 'uv sync' or 'pip install ruff'" >&2
    exit 1
fi

# git ls-files keeps the list current without a reconfigure and excludes
# untracked scratch files. third_party/ and tools/fixtures/ are excluded to
# match the .pre-commit-config.yaml and cmake/checks.cmake exclude rules.
c_h_files=$(git ls-files "*.c" "*.h" | grep -v "^third_party/" || true)
py_files=$(git ls-files "*.py" | grep -v "^tools/fixtures/" || true)

echo "==> format.sh: clang-format (C/H)"
if [ -n "$c_h_files" ]; then
    # shellcheck disable=SC2086
    clang-format-23 -i $c_h_files
fi

echo "==> format.sh: ruff format + ruff check --fix (Python)"
if [ -n "$py_files" ]; then
    # shellcheck disable=SC2086
    ruff format $py_files
    # shellcheck disable=SC2086
    ruff check --fix $py_files
fi

echo "==> format.sh: trivial whitespace/line-ending/BOM fixers"
# Apply the same four invariants check_trivial_fixers.py verifies. These are
# cheap to do in shell and avoid a Python dependency for the fix path; the
# checker remains the source of truth for what "compliant" means.
fix_trivial() {
    local file="$1"
    [ -f "$file" ] || return 0
    # Skip binary files (NUL byte in leading chunk).
    if head -c 8192 "$file" | LC_ALL=C grep -q $'\x00'; then
        return 0
    fi
    # Strip UTF-8 BOM, normalize CRLF/CR to LF, strip trailing whitespace,
    # and ensure exactly one trailing newline. perl handles all four in one
    # pass over the file content read as bytes.
    perl -0777 -pi -e '
        s/^\xEF\xBB\xBF//;          # strip leading BOM
        s/\r\n/\n/g;                # CRLF -> LF
        s/\r/\n/g;                  # bare CR -> LF
        s/[ \t]+\n/\n/g;            # trailing whitespace before newline
        s/\n\z/\n/;                 # ensure exactly one trailing newline
    ' "$file"
}

all_tracked=$(git ls-files "*.c" "*.h" "*.py" \
    | grep -v "^third_party/" | grep -v "^tools/fixtures/" || true)
if [ -n "$all_tracked" ]; then
    while IFS= read -r f; do
        fix_trivial "$f"
    done <<<"$all_tracked"
fi

echo "format.sh: done. Run ./scripts/verify.sh to confirm the gate is green."

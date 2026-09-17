#!/usr/bin/env bash
#
# Run clang-tidy over every translation unit recorded in the CMake
# compilation database, failing on any displayed warning.
#
# Usage:
#   scripts/run-clang-tidy.sh [-p <build-dir>]
#
# The optional -p <build-dir> selects the directory containing
# compile_commands.json (default: build/debug). The driver upgrades
# displayed warnings to errors so the script exits non-zero when clang-tidy
# reports a violation in user code; warnings filtered out by .clang-tidy
# (HeaderFilterRegex, suppressed checks) do not cause failure.
#
# The compilation database is produced by `cmake --preset debug`; run that
# first. Requires the LLVM run-clang-tidy driver on PATH.

set -euo pipefail

build_dir="build/debug"
while [ "$#" -gt 0 ]; do
    case "$1" in
        -p)
            if [ "$#" -lt 2 ]; then
                echo "usage: $0 [-p <build-dir>]" >&2
                exit 2
            fi
            build_dir="$2"
            shift 2
            ;;
        -p*)
            build_dir="${1#-p}"
            shift
            ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "usage: $0 [-p <build-dir>]" >&2
            exit 2
            ;;
    esac
done

if ! command -v run-clang-tidy >/dev/null 2>&1; then
    echo "error: run-clang-tidy not found on PATH" >&2
    exit 2
fi

db="$build_dir/compile_commands.json"
if [ ! -f "$db" ]; then
    echo "info: $db not found; no translation units to check"
    exit 0
fi

if ! grep -q '"' "$db"; then
    echo "info: $db contains no translation units; nothing to check"
    exit 0
fi

exec run-clang-tidy -p "$build_dir" -quiet -hide-progress -warnings-as-errors '*'

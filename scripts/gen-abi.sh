#!/usr/bin/env bash
#
# Regenerate the ABI snapshots in tests/abi/ from one build directory. The
# snapshot set is the ABI gate's baseline (scripts/verify.sh diffs every
# built library against its snapshot); run this after an intended ABI
# change and commit the snapshots in the same commit as the source change
# that produced them.
#
# The debug preset is the one consistent generation config: unoptimized,
# no LTO, full DWARF in every library. An optimized or stripped build
# yields corpora without type records (abidw falls back to the ELF symbol
# table alone, which leaves signature and layout changes undetected); the
# per-snapshot check below refuses that output.
#
# abidw runs from the repository root, so source paths land repo-relative
# ('./src/...', comp-dir-path='.'), and --no-corpus-path drops the
# build-directory attribute, keeping the snapshots machine-independent.
#
# Usage:
#   scripts/gen-abi.sh [build-dir]
#
# Arguments default to build/debug. Confirm the result with
# scripts/verify.sh (its ABI-diff stage must stay green).

set -euo pipefail

cd "$(dirname "$0")/.."

build_dir="${1:-build/debug}"

if ! command -v abidw >/dev/null 2>&1; then
    echo "gen-abi.sh: 'abidw' not found; install 'abigail-tools'" >&2
    exit 1
fi
if [ ! -d "$build_dir" ]; then
    echo "gen-abi.sh: build directory '$build_dir' does not exist;" >&2
    echo "configure it with 'cmake --preset debug && cmake --build --preset debug'" >&2
    exit 1
fi

count=0
for snapshot in tests/abi/*.abi; do
    [ -e "$snapshot" ] || continue
    name="$(basename "$snapshot")"
    soname="$(sed -n "s/.*soname='\([^']*\)'.*/\1/p" "$snapshot" | head -1)"
    if [ -z "$soname" ]; then
        echo "gen-abi.sh: $name does not name a soname" >&2
        exit 1
    fi
    lib="$build_dir/$soname"
    if [ ! -f "$lib" ]; then
        echo "gen-abi.sh: '$soname' is not built in '$build_dir'" >&2
        exit 1
    fi
    tmp="$(mktemp)"
    trap 'rm -f "$tmp"' EXIT
    abidw --no-corpus-path --out-file "$tmp" "$lib"
    if ! grep -q "<abi-instr " "$tmp"; then
        echo "gen-abi.sh: $name came out symbol-only (no type records);" >&2
        echo "'$lib' was built without debug info — generate from the debug preset" >&2
        exit 1
    fi
    mv "$tmp" "$snapshot"
    trap - EXIT
    echo "gen-abi.sh: regenerated $name from $soname"
    count=$((count + 1))
done

if [ "$count" -eq 0 ]; then
    echo "gen-abi.sh: no snapshots in tests/abi/" >&2
    exit 1
fi
echo "gen-abi.sh: regenerated $count snapshots from $build_dir"

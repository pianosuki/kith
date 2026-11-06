#!/usr/bin/env bash
# Resolve the preload runtime through a configured native build's compiler.
set -euo pipefail

fail() {
    echo "sanitizer-runtime.sh: $*" >&2
    exit 1
}

[ "$#" -eq 2 ] || fail 'usage: sanitizer-runtime.sh asan|tsan <build-dir>'
case "${1:-}" in
    asan|tsan) kind="$1" ;;
    *) fail 'usage: sanitizer-runtime.sh asan|tsan <build-dir>' ;;
esac

cache="$2/CMakeCache.txt"
[ -f "$cache" ] || fail "configured build cache not found: $cache"
compiler=""
compiler_seen=false
while IFS= read -r entry || [ -n "$entry" ]; do
    case "$entry" in
        CMAKE_C_COMPILER:=*) fail 'compiler cache entry has no type' ;;
        CMAKE_C_COMPILER:*=*)
            [ "$compiler_seen" = false ] || fail 'ambiguous compiler cache entries'
            compiler="${entry#*=}"
            compiler_seen=true
            ;;
        CMAKE_C_COMPILER_ARG1:*=*|CMAKE_C_COMPILER_TARGET:*=*)
            [ -z "${entry#*=}" ] || fail "unsupported native compiler setting: ${entry%%:*}"
            ;;
    esac
done < "$cache"

[[ "$compiler" = /* && -f "$compiler" && -x "$compiler" ]] ||
    fail "configured compiler is not an absolute executable: $compiler"

# Official LLVM archives use target-directory names; distro packages may
# include the architecture in the filename. Compiler arguments and targets
# require separate resolution; these callers use the native Clang presets.
for name in "libclang_rt.$kind.so" "libclang_rt.$kind-x86_64.so"; do
    runtime="$("$compiler" -print-file-name="$name")" || fail 'compiler runtime query failed'
    if [[ "$runtime" = /* && -f "$runtime" && -r "$runtime" ]]; then
        printf 'Configured compiler: %s\n' "$compiler" >&2
        "$compiler" --version >&2
        sha256sum -- "$compiler" "$runtime" >&2
        printf '%s\n' "$runtime"
        exit 0
    fi
done
fail "$kind shared runtime unavailable for $compiler"

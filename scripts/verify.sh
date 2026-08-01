#!/usr/bin/env bash
#
# Run every verification gate the repository enforces in one invocation, so
# a change cannot pass one check and fail another.
#
# This script is the single source of truth for "what must pass".
# Contributing developers run the same commands through this script before
# every commit and before every push.
#
# Usage:
#   scripts/verify.sh [stages...]
#
# Stages (run in order when listed):
#   lint          pre-commit (verify-only: format compliance, lint) + mypy.
#                 Hooks never mutate the working tree; the fix path is
#                 `cmake --build --target format` or `scripts/format.sh`.
#   build         configure, build, ctest, pytest, the ABI diff against the
#                 committed snapshots, and the aggregated check-* targets
#   all           lint + build  (default when no stages given)
#
# Examples:
#   scripts/verify.sh                       # everything (default: all)
#   scripts/verify.sh lint                  # only the lint/pre-commit stage
#   scripts/verify.sh build                 # only build + tests + check-*
#
# Environment:
#   KITH_BUILD_DIR    build output directory for the build stage
#                     (default build/debug)
#   KITH_C_COMPILER   configure the build stage with this C compiler instead
#                     of the debug preset's pinned clang (-DCMAKE_C_COMPILER
#                     override; pair with KITH_BUILD_DIR when switching
#                     compilers on an existing checkout)
#   KITH_PYTEST_JOBS  worker count for the parallel pytest leg (xdist -n;
#                     default 4; serial when xdist is not importable)
#
# Every stage exits non-zero on failure and prints the failing step. Run from
# the repository root.

set -euo pipefail

stage_lint=0
stage_build=0

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        lint)      stage_lint=1; shift ;;
        build)     stage_build=1; shift ;;
        all)       stage_lint=1; stage_build=1; shift ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "error: unknown stage or option: $1" >&2
            usage >&2
            exit 2 ;;
    esac
done

if [ "$stage_lint" -eq 0 ] && [ "$stage_build" -eq 0 ]; then
    stage_lint=1; stage_build=1
fi

cd "$(dirname "$0")/.."

fail() {
    printf '\nverify.sh: FAILED at: %s\n' "$1" >&2
    exit 1
}

# Redirect python bytecode caches out of the source tree so the working tree
# stays clean.
export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-${TMPDIR:-/tmp}/kith-pycache}"
# Clear any pre-existing source-tree __pycache__ left by older runs that wrote
# bytecode in place; the prefix above keeps new caches out of the tree.
find . -name __pycache__ -type d -not -path "./.venv*" -exec rm -rf {} + 2>/dev/null || true

# --- lint ---------------------------------------------------------------

if [ "$stage_lint" -eq 1 ]; then
    echo "==> verify.sh: lint (pre-commit, verify-only)"
    if ! command -v pre-commit >/dev/null 2>&1; then
        echo "verify.sh: 'pre-commit' not found; run 'uv sync' or install it" >&2
        fail "lint (pre-commit missing)"
    fi
    # Hooks are configured verify-only (no fixers); --show-diff-on-failure
    # surfaces what is wrong without rewriting files. The fix path is
    # `cmake --build --target format` or `scripts/format.sh`.
    pre-commit run --all-files --show-diff-on-failure || fail "lint (pre-commit)"

    echo "==> verify.sh: lint (license compliance, manual stage)"
    # The reuse hook is a manual-stage hook, which the all-files run above
    # skips by design; invoke it explicitly so license compliance is
    # enforced by the same gate that enforces everything else.
    pre-commit run reuse --all-files --hook-stage manual || fail "lint (reuse)"

    echo "==> verify.sh: lint (mypy, pre-push stage)"
    pre-commit run mypy --all-files --hook-stage pre-push || fail "lint (mypy)"
fi

# --- build --------------------------------------------------------------

if [ "$stage_build" -eq 1 ]; then
    # The build output directory. Defaults to the preset's build/debug; an
    # overriding KITH_BUILD_DIR lets a second build context whose source root
    # differs keep its build tree separate so the two caches do not clobber
    # each other.
    build_dir="${KITH_BUILD_DIR:-build/debug}"

    echo "==> verify.sh: build (configure)"
    # KITH_C_COMPILER retargets the configure: a cache variable beats the
    # debug preset's pinned CC=clang environment entry, which an exported CC
    # alone cannot override. It scopes to this preset line only.
    cc_args=()
    if [ -n "${KITH_C_COMPILER:-}" ]; then
        cc_args+=("-DCMAKE_C_COMPILER=${KITH_C_COMPILER}")
    fi
    cmake --preset debug -B "$build_dir" "${cc_args[@]}" || fail "build (configure)"

    echo "==> verify.sh: build (compile)"
    cmake --build "$build_dir" || fail "build (compile)"

    echo "==> verify.sh: build (ctest)"
    # --quiet suppresses ctest's failure summary along with the per-test
    # chatter, so a failing run re-executes the failed set with full
    # output; the first exit code stays the verdict.
    if ! ctest --test-dir "$build_dir" --output-on-failure --quiet; then
        ctest --test-dir "$build_dir" --output-on-failure --rerun-failed || true
        fail "build (ctest)"
    fi

    echo "==> verify.sh: build (pytest)"
    # Prefer the project venv (uv sync) so the suite runs under the same
    # pinned interpreter and dependencies the developers share; fall back
    # to the system python3 where no venv is provisioned.
    pytest_python=""
    if [ -x ".venv/bin/python" ] && ".venv/bin/python" -c "import pytest" >/dev/null 2>&1; then
        pytest_python=".venv/bin/python"
    elif python3 -c "import pytest" >/dev/null 2>&1; then
        pytest_python="python3"
        echo "verify.sh: no .venv with pytest; using system python3 (run 'uv sync' to pin it)." >&2
    else
        echo "verify.sh: 'pytest' not found; run 'uv sync' or install it" >&2
        fail "build (pytest missing)"
    fi
    # Parallel only when xdist is importable; the default worker count is
    # bounded well below the logical CPU count.
    pytest_args=()
    if "$pytest_python" -c "import xdist" >/dev/null 2>&1; then
        pytest_args+=(-n "${KITH_PYTEST_JOBS:-4}")
    else
        echo "verify.sh: pytest-xdist not importable; running the suite serially" >&2
    fi
    "$pytest_python" -m pytest -q "${pytest_args[@]}" || fail "build (pytest)"

    echo "==> verify.sh: build (check-all: clang-tidy, clang-format, ruff)"
    cmake --build "$build_dir" --target check-all || fail "build (check-all)"

    echo "==> verify.sh: build (ABI diff vs committed snapshots)"
    if command -v abidiff >/dev/null 2>&1; then
        ok=1
        snapshots=0
        for snapshot in tests/abi/*.abi; do
            [ -e "$snapshot" ] || continue
            snapshots=$((snapshots + 1))
            soname="$(sed -n 's/.*soname='"'"'\([^'"'"']*\)'"'"'.*/\1/p' "$snapshot" | head -1)"
            lib="$build_dir/$soname"
            if [ -z "$soname" ] || [ ! -f "$lib" ]; then
                echo "verify.sh: ABI snapshot $snapshot does not name a built library" >&2
                ok=0
                continue
            fi
            if ! abidiff "$lib" "$snapshot" >/dev/null 2>&1; then
                echo "verify.sh: ABI mismatch in $lib vs $snapshot" >&2
                echo "verify.sh: for an intended ABI change, regenerate with scripts/gen-abi.sh" >&2
                ok=0
            fi
        done
        [ "$snapshots" -gt 0 ] || fail "build (ABI diff: no snapshots in tests/abi)"
        [ "$ok" -eq 1 ] || fail "build (ABI diff)"
    else
        fail "build (abidiff not found; install 'abigail-tools' to verify ABI)"
    fi

    echo "==> verify.sh: build (checksec hardening verification)"
    if command -v checksec >/dev/null 2>&1; then
        # The hardening contract mandates PIE, full RELRO, NX, stack
        # canary, and CFI (Intel CET on x86-64, BTI on aarch64) on every
        # artifact: the test executables and the shipped libkith_*.so.*
        # libraries.
        # checksec ships two incompatible CLIs: the classic bash script
        # ('checksec --file=<bin>') and the Go tool ('checksec file <bin>').
        # The Go tool gates all five features via --fail-if; the classic
        # script reports RELRO/canary/NX/PIE but not CFI, so CFI is verified
        # separately via readelf when only the classic flavor is present.
        flavor=""
        if checksec --file=/bin/true >/dev/null 2>&1; then flavor="classic"; fi
        if [ -z "$flavor" ] && checksec file /bin/true >/dev/null 2>&1; then flavor="go"; fi
        if [ -n "$flavor" ]; then
            hardening_failed=0
            artifacts="$(
                find "$build_dir/tests/c" -maxdepth 1 -type f -executable 2>/dev/null
                find "$build_dir" -maxdepth 1 -name 'libkith_*.so.*' -type f -executable 2>/dev/null
            )"
            # An empty scan set greens the loop below without inspecting
            # anything — a build whose artifact layout broke must fail the
            # gate, not skip hardening verification.
            [ -n "$artifacts" ] || fail "checksec: no built artifacts found to scan"
            while IFS= read -r bin; do
                [ -x "$bin" ] || continue
                if [ "$flavor" = "go" ]; then
                    # --fail-if exits non-zero with a per-check violation
                    # report on stderr when any listed check is not green.
                    # Suppress the table (stdout); let the report through
                    # so the failing feature is visible in the log. PIE
                    # binds executables — checksec classifies a shared
                    # object as "DSO" — so the gate list omits pie there.
                    gates="relro,nx,canary,cfi"
                    case "$bin" in
                        *.so | *.so.*) ;;
                        *) gates="$gates,pie" ;;
                    esac
                    # shellcheck disable=SC2069
                    if ! checksec file --no-banner --no-headers \
                            --fail-if="$gates" "$bin" \
                            2>&1 >/dev/null; then
                        hardening_failed=1
                    fi
                else
                    out="$(checksec --file="$bin" 2>&1)"
                    if ! printf '%s' "$out" | grep -q "Full RELRO"; then
                        echo "verify.sh: RELRO not full for $bin" >&2
                        hardening_failed=1
                    fi
                    if ! printf '%s' "$out" | grep -qi "canary found"; then
                        echo "verify.sh: stack canary missing for $bin" >&2
                        hardening_failed=1
                    fi
                    if ! printf '%s' "$out" | grep -q "NX enabled"; then
                        echo "verify.sh: NX disabled for $bin" >&2
                        hardening_failed=1
                    fi
                    case "$bin" in
                        *.so | *.so.*) ;;
                        *)
                            if ! printf '%s' "$out" | grep -qi "PIE enabled"; then
                                echo "verify.sh: PIE not enabled for $bin" >&2
                                hardening_failed=1
                            fi
                            ;;
                    esac
                    # CFI is not reported by classic checksec; verify the
                    # .note.gnu.property CET (IBT+SHSTK) or BTI marker via
                    # readelf. Fail closed when readelf is unavailable
                    # rather than silently skipping the CFI check.
                    if command -v readelf >/dev/null 2>&1; then
                        if ! readelf -n "$bin" 2>/dev/null \
                                | grep -qiE "IBT|SHSTK|BTI"; then
                            echo "verify.sh: CFI markers not found for $bin" >&2
                            hardening_failed=1
                        fi
                    else
                        echo "verify.sh: cannot verify CFI (readelf missing) for $bin" >&2
                        hardening_failed=1
                    fi
                fi
            done <<< "$artifacts"
            if [ "$hardening_failed" -ne 0 ]; then
                fail "checksec hardening (mandated feature missing)"
            fi
        else
            fail "checksec present but neither CLI syntax worked"
        fi
    else
        fail "build (checksec not found; install 'checksec' to verify hardening)"
    fi
fi

printf 'verify.sh: all requested stages passed.\n'

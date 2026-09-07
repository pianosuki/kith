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
#   lint          pre-commit (verify-only: format compliance, lint,
#                 architectural checkers) + mypy. Hooks never mutate the
#                 working tree; the fix path is `cmake --build --target
#                 format` or `scripts/format.sh`.
#   build         configure, build, ctest, pytest, and the aggregated check-* targets
#   commits       conventional-commits header + DCO + signing over
#                 a commit range; when KITH_ALLOWED_SIGNERS points at a
#                 git allowedSignersFile, signatures are cryptographically
#                 verified via git verify-commit (otherwise presence-only)
#   free-threaded run the Python suite under the free-threaded interpreter
#                 (python3.14t, GIL disabled); self-installs the interpreter
#                 via uv when absent, and fails if it cannot be obtained
#   all           lint + commits + build + free-threaded  (default when no stages given)
#
# Examples:
#   scripts/verify.sh                       # everything (default: all)
#   scripts/verify.sh lint                   # only the lint/pre-commit stage
#   scripts/verify.sh build                  # only build + tests + check-*
#   scripts/verify.sh free-threaded          # only the python3.14t suite
#   scripts/verify.sh --base <sha> commits   # validate a specific range
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
stage_commits=0
stage_ft=0
base=""
range_head="HEAD"

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        lint)      stage_lint=1; shift ;;
        build)     stage_build=1; shift ;;
        commits)   stage_commits=1; shift ;;
        free-threaded) stage_ft=1; shift ;;
        all)       stage_lint=1; stage_commits=1; stage_build=1; stage_ft=1; shift ;;
        --base)
            base="$2"; shift 2 ;;
        --head)
            range_head="$2"; shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "error: unknown stage or option: $1" >&2
            usage >&2
            exit 2 ;;
    esac
done

if [ "$stage_lint" -eq 0 ] && [ "$stage_build" -eq 0 ] && [ "$stage_commits" -eq 0 ] && [ "$stage_ft" -eq 0 ]; then
    stage_lint=1; stage_commits=1; stage_build=1; stage_ft=1
fi

cd "$(dirname "$0")/.."

fail() {
    printf '\nverify.sh: FAILED at: %s\n' "$1" >&2
    exit 1
}

# Redirect python bytecode caches out of the source tree so the working tree
# stays clean and the free-threaded and standard interpreters never share a
# .pyc cache. A shared cache compiles co_filename against one absolute path
# and then fails inspect.getsourcelines under another (a different CWD),
# which surfaces as spurious test errors.
# Each stage that drives a different interpreter overrides the prefix below.
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

# --- commits ------------------------------------------------------------

if [ "$stage_commits" -eq 1 ]; then
    echo "==> verify.sh: commits (conventional, DCO, signing)"
    # The commits checker confirms each commit carries a signature. When
    # KITH_ALLOWED_SIGNERS points at a git allowedSignersFile (set in CI
    # from a secret, never committed), signatures are cryptographically
    # verified via git verify-commit. Otherwise the check is presence-only
    # (the gpgsig header exists), relying on the hosting platform's own
    # verification. No per-repo signing keyring is tracked (AGENTS.md §5.2.8).

    range_args=(--head "$range_head")
    base_sha="$(git rev-parse --verify --quiet origin/main || true)"
    head_sha="$(git rev-parse --verify --quiet "$range_head" || true)"
    if [ -n "$base" ]; then
        range_args+=(--base "$base")
    elif [ -n "$base_sha" ] && [ -n "$head_sha" ] \
            && fork_point="$(git merge-base "$base_sha" "$head_sha" 2>/dev/null)"; then
        # Validate exactly the commits not yet on the shared ref:
        # origin/main..head for linear work, and fork-point..head once
        # a rebase diverges local history from the fetched ref. A
        # depth-1 shallow clone whose fork point falls below the
        # boundary is a CI shape; CI passes an explicit --base. When head
        # equals the fetched ref the range is empty and the checker
        # validates nothing — each pushed commit was already validated
        # over its push range.
        range_args+=(--base "$fork_point")
        if [ "$fork_point" = "$head_sha" ]; then
            # No unpushed commits exist (head equals origin/main, or head
            # is behind the fetched ref). Passing here depends on the
            # printed reason staying explicit — do not soften it to a
            # bare success line.
            echo "verify.sh: commits: no unpushed commits to validate (origin/main == ${fork_point})"
        fi
    fi
    # With origin/main unresolved the checker applies its documented
    # fallback: scan recent history reachable from head, capped at
    # --max-commits (default 256). Deriving --base main instead would
    # make direct-to-main a no-op: HEAD equals main, the range is empty,
    # and the stage passes without inspecting anything.

    signers_args=()
    if [ -n "${KITH_ALLOWED_SIGNERS:-}" ] && [ -f "${KITH_ALLOWED_SIGNERS}" ]; then
        signers_args+=(--allowed-signers "${KITH_ALLOWED_SIGNERS}")
    fi

    python3 tools/check_commit_messages.py "${range_args[@]}" "${signers_args[@]}" \
        || fail "commits (${range_args[*]})"

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
    # Parallel only when xdist is importable; --dist loadgroup honors the
    # postgres cohort's xdist_group marks, and a crashed worker fails the
    # run instead of restarting behind xdist's default retry budget. The
    # default worker count is bounded well below the logical CPU count:
    # the integration suite boots real servers, and each worker's resident
    # footprint must fit the machine's free memory alongside them — a
    # worker count near the CPU count drives the kernel-backed reactor
    # resources into transient allocation failures on a loaded dev box.
    pytest_args=()
    if "$pytest_python" -c "import xdist" >/dev/null 2>&1; then
        pytest_args+=(-n "${KITH_PYTEST_JOBS:-4}" --dist loadgroup --max-worker-restart 0)
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
        # artifact: the test executables, the shipped libkith_*.so.*
        # libraries, and the example DSOs.
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
                find "$build_dir/examples" -maxdepth 1 -name '*.so' -type f -executable 2>/dev/null
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

    # --- out-of-tree downstream consumer (find_package + pkg-config) -----
    # A real downstream CMake project consumes an installed kith via
    # find_package(kith) and links the imported targets the export set
    # generates. This step installs the in-tree build to a throwaway prefix,
    # configures tests/consumer/ against it, builds the consumer, and runs
    # the resulting executable so the find_package story is exercised in CI
    # rather than only claimed. The pkg-config channel gets the same
    # treatment further down: the installed .pc file is queried, its link
    # line is checked against the installed library set, and a probe
    # consumer is compiled, linked, and run with exactly the reported flags.
    echo "==> verify.sh: build (out-of-tree consumer: find_package + pkg-config)"
    consumer_prefix="$(mktemp -d -t kith-consumer-XXXXXX)"
    consumer_build="$(mktemp -d -t kith-consumer-build-XXXXXX)"
    # shellcheck disable=SC2064
    trap "rm -rf '$consumer_prefix' '$consumer_build'" EXIT
    if ! cmake --install "$build_dir" --prefix "$consumer_prefix" \
            >"$consumer_prefix/install.log" 2>&1; then
        echo "verify.sh: cmake --install failed (see $consumer_prefix/install.log)" >&2
        fail "build (consumer install)"
    fi
    if ! cmake -B "$consumer_build" -S tests/consumer \
            -Dkith_DIR="$consumer_prefix/lib/cmake/kith" \
            >"$consumer_prefix/configure.log" 2>&1; then
        echo "verify.sh: consumer configure failed (see $consumer_prefix/configure.log)" >&2
        fail "build (consumer configure)"
    fi
    if ! cmake --build "$consumer_build" >"$consumer_prefix/build.log" 2>&1; then
        echo "verify.sh: consumer build failed (see $consumer_prefix/build.log)" >&2
        fail "build (consumer build)"
    fi
    if ! "$consumer_build/kith_consumer" >"$consumer_prefix/run.log" 2>&1; then
        echo "verify.sh: consumer executable failed (see $consumer_prefix/run.log)" >&2
        fail "build (consumer run)"
    fi
    # pkg-config .pc file: --modversion reports the Version field, proving
    # the installed pkg-config entry point is queryable. The .pc file's
    # Version is generated from @PROJECT_VERSION@ by configure_file(@ONLY),
    # so it cannot diverge from the project version by construction; the
    # gate verifies the file parses and reports a valid dotted triple.
    #
    # The link story is exercised, not just queried: every installed kith
    # shared library must be named on `pkg-config --libs kith` (the .pc is
    # the one place a pkg-config consumer gets the full set), and a probe
    # consumer must compile, link, and run against exactly the reported
    # flags. The leg assumes the default CONTROL_PLANE_ENABLED=ON
    # configuration the .pc template enumerates. libdir is read back from
    # the installed .pc itself, so multiarch layouts (lib/<triplet>) are
    # discovered rather than assumed. pkg-config is optional (some minimal
    # CI images omit it), so a missing binary skips rather than failing; a
    # present-but-broken one fails.
    if command -v pkg-config >/dev/null 2>&1; then
        pc_modversion="$(PKG_CONFIG_PATH="$consumer_prefix/lib/pkgconfig" \
            pkg-config --modversion kith 2>&1)"
        if ! printf '%s' "$pc_modversion" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
            echo "verify.sh: pkg-config --modversion kith = '$pc_modversion', expected a dotted triple" >&2
            fail "build (consumer pkg-config modversion)"
        fi

        pc_libdir="$(PKG_CONFIG_PATH="$consumer_prefix/lib/pkgconfig" \
            pkg-config --variable=libdir kith 2>&1)"
        # shellcheck disable=SC2086
        pc_libs="$(PKG_CONFIG_PATH="$consumer_prefix/lib/pkgconfig" \
            pkg-config --libs kith 2>&1)"
        for pc_so in "$pc_libdir"/libkith_*.so; do
            if [ ! -e "$pc_so" ]; then
                echo "verify.sh: pkg-config libdir '$pc_libdir' installs no libkith_*.so" >&2
                fail "build (consumer pkg-config lib enumeration)"
            fi
            pc_lflag="-l$(basename "$pc_so" | sed 's/^lib//; s/\.so$//')"
            case " $pc_libs " in
                *" $pc_lflag "*) ;;
                *)
                    echo "verify.sh: pkg-config --libs kith omits $pc_lflag (installed: $pc_so)" >&2
                    fail "build (consumer pkg-config lib enumeration)"
                    ;;
            esac
        done

        pc_cc="${CC:-}"
        if [ -z "$pc_cc" ]; then
            for pc_candidate in cc clang gcc; do
                if command -v "$pc_candidate" >/dev/null 2>&1; then
                    pc_cc="$pc_candidate"
                    break
                fi
            done
        fi
        if [ -z "$pc_cc" ]; then
            fail "build (consumer pkg-config probe: no C compiler found)"
        fi
        # The pkg-config output is a flag list meant to be word-split; the
        # probe links with exactly the reported flags, nothing added.
        # shellcheck disable=SC2046,SC2086
        if ! "$pc_cc" \
                $(PKG_CONFIG_PATH="$consumer_prefix/lib/pkgconfig" \
                    pkg-config --cflags kith) \
                -o "$consumer_build/pkgconfig_probe" \
                tests/consumer/pkgconfig_probe.c \
                $pc_libs >"$consumer_prefix/probe-build.log" 2>&1; then
            echo "verify.sh: pkg-config probe compile/link failed (see $consumer_prefix/probe-build.log)" >&2
            fail "build (consumer pkg-config probe link)"
        fi
        # The .pc carries no runtime search path (that is the deploying
        # consumer's concern), so the run resolves the framework's own
        # libraries through LD_LIBRARY_PATH pointed at the install.
        if ! LD_LIBRARY_PATH="$pc_libdir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
                "$consumer_build/pkgconfig_probe" >"$consumer_prefix/probe-run.log" 2>&1 \
                || ! grep -q . "$consumer_prefix/probe-run.log"; then
            echo "verify.sh: pkg-config probe run failed (see $consumer_prefix/probe-run.log)" >&2
            fail "build (consumer pkg-config probe run)"
        fi
    fi
fi

# --- free-threaded ------------------------------------------------------

if [ "$stage_ft" -eq 1 ]; then
    echo "==> verify.sh: free-threaded (python3.14t, GIL disabled)"

    # Resolve the free-threaded interpreter. uv manages a standalone
    # cpython-3.14.x+freethreaded build; fall back to a python3.14t on PATH.
    ft_python=""
    if command -v python3.14t >/dev/null 2>&1; then
        ft_python="$(command -v python3.14t)"
    elif command -v uv >/dev/null 2>&1; then
        ft_python="$(uv python find python3.14t 2>/dev/null || true)"
    fi
    # Self-bootstrap: if no interpreter is available, ask uv to fetch one so
    # the stage runs instead of failing. A green gate that skipped the
    # free-threaded suite would hide a concurrency regression until a user
    # ran python3.14t, so the stage fails rather than skipping when the
    # interpreter cannot be obtained.
    if { [ -z "$ft_python" ] || [ ! -x "$ft_python" ]; } && command -v uv >/dev/null 2>&1; then
        if uv python install 3.14t >/dev/null 2>&1; then
            ft_python="$(uv python find python3.14t 2>/dev/null || true)"
        fi
    fi
    if [ -z "$ft_python" ] || [ ! -x "$ft_python" ]; then
        fail "free-threaded (python3.14t unavailable and uv could not fetch it)"
    else
        # Provision a dedicated venv for the free-threaded interpreter so the
        # standard project .venv is never replaced. pytest and ruff run the
        # suite; clang2 supplies the LLVM 22 python bindings the drift tests
        # regenerate with, matching the checked-in bindings byte for byte.
        # libclang1-22 (the C library) and clang-22 (the driver) come from the
        # system.
        ft_venv="$(pwd)/.venv-ft"
        need_venv=0
        if [ ! -x "$ft_venv/bin/python3.14t" ]; then
            need_venv=1
        elif ! "$ft_venv/bin/python3.14t" -c "import pytest" >/dev/null 2>&1 \
             || ! "$ft_venv/bin/python3.14t" -c "import xdist" >/dev/null 2>&1 \
             || ! "$ft_venv/bin/python3.14t" -c "import clang.cindex" >/dev/null 2>&1 \
             || ! "$ft_venv/bin/python3.14t" -c "import yaml" >/dev/null 2>&1 \
             || [ ! -x "$ft_venv/bin/ruff" ]; then
            need_venv=1
        fi
        if [ "$need_venv" -eq 1 ]; then
            if ! command -v uv >/dev/null 2>&1; then
                echo "verify.sh: 'uv' not found; cannot provision free-threaded venv" >&2
                fail "free-threaded (uv missing)"
            fi
            uv venv --python "$ft_python" --clear "$ft_venv" >/dev/null 2>&1 \
                || fail "free-threaded (venv create)"
            # ruff is pinned so regenerated bindings are formatted
            # byte-identical to the checked-in set; an unpinned/latest ruff
            # could reformat output and make every drift check spuriously
            # fail. The pin matches the pre-commit ruff hook rev, so one
            # ruff version formats the corpus on every gate path. clang2 is
            # the LLVM 22 python bindings (same version the drift hook uses).
            # pytest and pytest-xdist are pinned for the same reason: this
            # venv installs outside uv.lock, and an unpinned latest would
            # drift from the standard leg's runner.
            uv pip install --python "$ft_venv" \
                pytest==9.1.1 pytest-xdist==3.8.0 ruff==0.16.0 clang2==22.1.8.post0 pyyaml==6.0.3 >/dev/null 2>&1 \
                || fail "free-threaded (pytest+xdist+ruff+clang2+pyyaml install)"
        fi

        # Confirm the interpreter really runs GIL-free before driving the suite.
        if ! "$ft_venv/bin/python3.14t" -c "import sys; sys.exit(0 if not sys._is_gil_enabled() else 1)"; then
            echo "verify.sh: $ft_python is not a free-threaded build (GIL enabled)" >&2
            fail "free-threaded (interpreter is not GIL-free)"
        fi

        # Use a per-interpreter pycache prefix so the free-threaded run never
        # shares .pyc files with the standard interpreter's cache. The
        # separation is load-bearing, not redundant: both interpreters are
        # 3.14.7 and share the identical cache tag and bytecode magic number.
        # This leg runs serial by default: its teardown-sensitive tests
        # (finalizers, asyncio loop shutdown) are load-timing sensitive, and
        # parallel workers surface that sensitivity on a loaded host.
        # KITH_FT_JOBS>1 opts into parallel workers under the same shape as
        # the standard leg; that mode assumes a quiet host. pytest-xdist
        # stays installed in the venv so the leg's runner dependencies match
        # the standard leg's whether or not it runs parallel.
        ft_pycache="${TMPDIR:-/tmp}/kith-pycache-ft"
        ft_jobs="${KITH_FT_JOBS:-0}"
        case "$ft_jobs" in (*[!0-9]*|'') ft_jobs=0 ;; esac
        ft_args=()
        if [ "$ft_jobs" -gt 1 ]; then
            if "$ft_venv/bin/python3.14t" -c "import xdist" >/dev/null 2>&1; then
                ft_args+=(-n "$ft_jobs" --dist loadgroup --max-worker-restart 0)
            else
                echo "verify.sh: pytest-xdist not importable; running the free-threaded suite serially" >&2
            fi
        fi
        env PATH="$ft_venv/bin:$PATH" \
            PYTHONPYCACHEPREFIX="$ft_pycache" \
            "$ft_venv/bin/python3.14t" -m pytest -q "${ft_args[@]}" \
            || fail "free-threaded (pytest)"
        echo "verify.sh: free-threaded Python suite passed (GIL disabled)."
    fi
fi

printf 'verify.sh: all requested stages passed.\n'

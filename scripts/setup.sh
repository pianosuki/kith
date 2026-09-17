#!/usr/bin/env bash
#
# One-command bootstrap from a fresh clone: preflight the host toolchain,
# apply the project git configuration, sync the Python dev environment,
# install the free-threaded interpreter the test suite runs under, and wire
# the pre-commit hooks. Running this then `scripts/verify.sh` takes a fresh
# checkout to a fully provisioned, gate-green working tree.
#
# The underlying steps (setup-git-config.sh, uv sync, pre-commit install) are
# each idempotent and reusable on their own; this script only orchestrates them
# and adds the preflight checks that catch a missing host tool before it fails
# mid-build. Re-running is safe: every step is a no-op when already satisfied.
#
# Usage:
#   scripts/setup.sh [options]
#
# Options:
#   --yes     run non-interactively; pass --yes to setup-git-config.sh so the
#             discovered git identity and signing key are applied without a
#             prompt (for CI and agents)
#   -h, --help   show this help and exit

set -euo pipefail

assume_yes=0

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --yes) assume_yes=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

cd "$(dirname "$0")/.."

# --- preflight: required host tools ----------------------------------------

# Each entry: "binary|install-hint". The hint is printed when the binary is
# absent so a contributor knows exactly what to install before re-running.
required_tools=(
    "clang|clang 22+ (apt: clang-22, brew: llvm@22, or https://releases.llvm.org)"
    "clang-format-23|clang-format 23 (uv tool install clang-format==23.1.0, then ln -s ~/.local/bin/clang-format ~/.local/bin/clang-format-23; or apt.llvm.org: clang-format-23)"
    "cmake|cmake 4.4+ (pip install cmake==4.4.3, or https://cmake.org/download)"
    "ninja|ninja 1.13+ (apt: ninja-build, brew: ninja, pip: ninja==1.13.2)"
    "python3|python 3.14+ (https://www.python.org, or uv: uv python install 3.14)"
    "uv|uv 0.12+ (pip install uv, or https://docs.astral.sh/uv/getting-started/installation/)"
    "pre-commit|pre-commit 3.0+ (pip install pre-commit, or brew: pre-commit)"
)

missing=0
printf '==> setup.sh: preflight\n'
for entry in "${required_tools[@]}"; do
    bin="${entry%%|*}"
    hint="${entry#*|}"
    if command -v "$bin" >/dev/null 2>&1; then
        printf '  ok   %s\n' "$bin"
    else
        printf '  MISS %s -- install: %s\n' "$bin" "$hint" >&2
        missing=1
    fi
done
if [ "$missing" -ne 0 ]; then
    echo "setup.sh: one or more required host tools are missing; install them and re-run." >&2
    exit 1
fi

# --- git configuration -----------------------------------------------------

printf '\n==> setup.sh: git config\n'
gc_args=()
[ "$assume_yes" -eq 1 ] && gc_args+=(--yes)
./scripts/setup-git-config.sh "${gc_args[@]}"

# --- python dev environment -----------------------------------------------

printf '\n==> setup.sh: uv sync\n'
uv sync

# --- free-threaded interpreter --------------------------------------------

# The test suite runs under the free-threaded (GIL-less) interpreter. uv fetches
# a standalone cpython-3.14+freethreaded build on demand; installing it here
# means the first `scripts/verify.sh free-threaded` run does not pause to
# download. Best-effort: on platforms without a freethreaded build, warn and
# continue (verify.sh's free-threaded stage auto-installs when it can too).
printf '\n==> setup.sh: free-threaded interpreter (3.14t)\n'
if uv python install 3.14t >/dev/null 2>&1; then
    printf '  ok   free-threaded interpreter ready\n'
else
    echo "  warn: could not install a 3.14t build on this platform;" \
         "the free-threaded stage will attempt it on demand." >&2
fi

# --- pre-commit hooks ------------------------------------------------------

printf '\n==> setup.sh: pre-commit hooks\n'
pre-commit install --install-hooks -t pre-commit -t pre-push -t commit-msg

printf '\nsetup.sh: bootstrap complete. Run ./scripts/verify.sh to confirm the gate is green.\n'

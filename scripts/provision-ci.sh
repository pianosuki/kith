#!/usr/bin/env bash
#
# Single source of truth for CI provisioning: the apt packages, pip installs,
# and clang tool symlinks that every GitHub Actions job needs. Both the CI
# workflow (.github/workflows/ci.yml) and the local Docker CI image
# (ci/ubuntu-24.04/Dockerfile) call this script, so a dependency added here
# appears in both environments with no drift.
#
# Usage:
#   scripts/provision-ci.sh <job>
#
# Jobs (install only that job's deps):
#   lint           clang 22 driver + libclang C library, pip: pre-commit
#                  (vermin, PyYAML, ruff, and clang2 python bindings are
#                  provisioned by pre-commit into each hook's own env; only
#                  the C library and driver are needed from apt here)
#   build          clang 22 toolchain + dev libs + the compiler-rt runtimes
#                  (libclang-rt-22-dev) + liburing built from source, pip:
#                  cmake, ninja, pytest, pytest-xdist, PyYAML, ruff,
#                  clang-format 23; abigail-tools + checksec (best effort)
#   build-gcc      everything build installs plus gcc-14, the second
#                  compiler leg of the CI matrix (noble's default gcc is 13,
#                  below the GCC 14 floor in cmake/framework.cmake; g++ is
#                  not needed, the framework is pure C)
#   coverage       everything build installs plus llvm-22 (ships llvm-cov
#                  and llvm-profdata, version-suffixed only), pip: coverage
#   free-threaded  clang 22 + libclang C library + the compiler-rt runtimes
#                  (libclang-rt-22-dev) + liburing built from source, pip:
#                  cmake, ninja, uv, clang-format 23
#   sdist          the from-source contract floor: clang 22 + lld + liburing
#                  built from source + the dev libraries (libpq, libhiredis)
#                  only. cmake and ninja are deliberately absent — pip's
#                  build isolation provides them — and the dev-checker
#                  toolchain (clang-tidy, clang-format, ruff) is not part of
#                  a from-source build
#   all            lint + build + build-gcc + coverage + free-threaded in
#                  sequence (for the Docker image); apt dedupes across
#                  sections
#
# noble's liburing-dev is 2.5, below the 2.7 floor the io_uring reactor
# backend enforces at configure, so every job that configures the framework
# builds liburing from source instead (build_liburing).
#
# The clang python bindings (clang.cindex) are not installed system-wide:
# the pre-commit drift hook installs the clang2 pip wheel (LLVM 22 bindings)
# into its own isolated env. libclang1-22 (the C library the bindings load)
# and clang-22 (the driver the generator invokes for system-include discovery)
# are the only system packages needed.
#
# $HOME/.local/bin (pip --user install location) is added to $GITHUB_PATH when
# that variable exists (GitHub Actions), and to $PATH otherwise (Docker), so
# cmake, ninja, pre-commit, and uv land on PATH in both environments.

set -euo pipefail

job="${1:-}"
if [ -z "$job" ]; then
        echo "usage: $0 <lint|build|build-gcc|coverage|free-threaded|sdist|all>" >&2
    exit 2
fi

# pip --user installs into $HOME/.local/bin; put it on PATH for the rest of
# this script and for the caller. In GitHub Actions also append to
# $GITHUB_PATH so subsequent workflow steps inherit it.
export PATH="$HOME/.local/bin:$PATH"
if [ -n "${GITHUB_PATH:-}" ]; then
    echo "$HOME/.local/bin" >> "$GITHUB_PATH"
fi

LLVM_KEY="/usr/share/keyrings/llvm-snapshot.gpg"
LLVM_LIST="/etc/apt/sources.list.d/llvm.list"

# In Docker (no sudo) the script runs as root; in GitHub Actions it runs as
# the runner user, so apt/sudo wrap the privileged commands.
run_priv() {
    if command -v sudo >/dev/null 2>&1 && [ "$(id -u)" -ne 0 ]; then
        sudo "$@"
    else
        "$@"
    fi
}

# apt sets no read timeout on package fetches: a connection that stalls
# mid-transfer holds apt open indefinitely. Cap each connection and retry
# the interrupted download, so a blip recovers instead of hanging the job.
run_priv tee /etc/apt/apt.conf.d/99kith-acquire-limits >/dev/null <<'CONF'
Acquire::http::Timeout "30";
Acquire::https::Timeout "30";
Acquire::Retries "3";
CONF

setup_llvm_apt() {
    # The LLVM apt source is added once; the first section to run apt does it,
    # and subsequent sections reuse the cached apt update. Idempotent. The
    # keyring and source-list writes go through run_priv so they work under the
    # non-root GitHub Actions runner, not just the root Docker image.
    if [ ! -f "$LLVM_LIST" ]; then
        local key_tmp
        key_tmp="$(mktemp)"
        # Fetch the key to a temporary file before dearmoring so a transient
        # network failure surfaces as wget's non-zero exit instead of an
        # empty pipe into gpg, whose "no valid OpenPGP data found" error
        # masks the real cause. --tries retries transient blips; the
        # armored-block check rejects an empty, truncated, or HTML error
        # response.
        wget -q --tries=3 --timeout=30 -O "$key_tmp" \
            https://apt.llvm.org/llvm-snapshot.gpg.key
        if ! grep -q '^-----BEGIN PGP PUBLIC KEY BLOCK-----' "$key_tmp"; then
            rm -f "$key_tmp"
            echo "error: failed to fetch LLVM apt key" >&2
            exit 1
        fi
        gpg --dearmor < "$key_tmp" | run_priv tee "$LLVM_KEY" >/dev/null
        rm -f "$key_tmp"
        echo "deb [signed-by=$LLVM_KEY] http://apt.llvm.org/noble/ llvm-toolchain-noble-22 main" \
            | run_priv tee "$LLVM_LIST" >/dev/null
        run_priv apt-get update
    fi
}

symlink_clang_tools() {
    # Force-link so clang-22 wins even when the base image ships an older
    # default clang at /usr/bin/clang. Idempotent across `all` sections.
    for tool in "$@"; do
        if [ -e "/usr/bin/${tool}-22" ]; then
            run_priv ln -sf "/usr/bin/${tool}-22" "/usr/bin/${tool}"
        fi
    done
}

pip_user() {
    python3 -m pip install --user --break-system-packages "$@"
}

# liburing 2.14, built from source into /usr/local. The version and sha256
# are pinned together with .devcontainer/Dockerfile: bump both files in the
# same change. noble's liburing-dev is 2.5, below the 2.7 floor
# cmake/lib_reactor.cmake enforces at configure, so the archive package
# cannot satisfy the reactor backend.
LIBURING_VERSION=2.14
LIBURING_SHA256=5f80964108981c6ad979c735f0b4877d5f49914c2a062f8e88282f26bf61de0c

build_liburing() {
    # /usr/local precedes /usr in pkg-config's default search path, so the
    # source-built installation wins over any archive copy without extra
    # environment. Idempotent: a host already at >= 2.7 (the rig, or a
    # second `all` section) skips the build.
    if pkg-config --modversion liburing >/dev/null 2>&1; then
        local found
        found="$(pkg-config --modversion liburing)"
        if dpkg --compare-versions "$found" ge 2.7; then
            echo "==> provision-ci.sh: liburing ${found} >= 2.7 present; source build skipped"
            return 0
        fi
    fi
    local tarball srcdir
    tarball="$(mktemp -t liburing-XXXXXX.tar.gz)"
    srcdir="$(mktemp -d -t liburing-src-XXXXXX)"
    wget -q --tries=3 --timeout=30 \
        -O "$tarball" \
        "https://github.com/axboe/liburing/archive/refs/tags/liburing-${LIBURING_VERSION}.tar.gz"
    echo "${LIBURING_SHA256}  ${tarball}" | sha256sum -c - >&2
    tar -xzf "$tarball" -C "$srcdir" --strip-components=1
    # Explicit compilers and the library-only target: the default target
    # also builds liburing's tests and examples, which hardcode gcc. clang
    # builds the library; nothing else is installed.
    (cd "$srcdir" \
        && ./configure --prefix=/usr/local --cc=clang-22 --cxx=clang++-22 >/dev/null \
        && make -j"$(nproc)" library >/dev/null)
    run_priv make -C "$srcdir" install >/dev/null
    run_priv ldconfig
    rm -rf "$tarball" "$srcdir"
    pkg-config --modversion liburing
}

setup_libclang_env() {
    # libclang1-22 installs a versioned soname (e.g. libclang-22.so.1) under
    # the multiarch directory; clang.cindex searches for the unversioned
    # libclang.so by default and fails on a clean runner where no dev symlink
    # exists. Point the bindings at the versioned library file directly so the
    # ctypes drift hook, the build job's pytest, and the free-threaded venv all
    # load the system libclang. Idempotent; the env var is inherited by all
    # child processes (pre-commit hook envs, pytest, uv venvs).
    local lib
    lib="$(ldconfig -p 2>/dev/null | grep -o '/[^ ]*libclang-[0-9]*\.so\.[0-9]*' | head -1)"
    if [ -z "$lib" ]; then
        lib="$(find /usr/lib /usr/local/lib -name 'libclang-[0-9]*.so.*' 2>/dev/null | head -1)"
    fi
    if [ -n "$lib" ]; then
        export LIBCLANG_LIBRARY_FILE="$lib"
        if [ -n "${GITHUB_ENV:-}" ]; then
            echo "LIBCLANG_LIBRARY_FILE=$lib" >> "$GITHUB_ENV"
        fi
    fi
}

provision_lint() {
    setup_llvm_apt
    # libclang1-22 is the C library the clang2 python bindings load at
    # runtime; clang-22 (the driver) is required because the generator
    # discovers system include paths via `clang -E -v` — without it
    # libclang can't resolve <stdint.h> and regenerated bindings diverge.
    # The python bindings themselves (clang.cindex) come from the clang2
    # pip wheel provisioned by the pre-commit drift hook's own env, so
    # python3-clang-22 is not installed system-wide.
    # python3-pip provisions the system interpreter's pip for the --user install.
    run_priv apt-get install -y --no-install-recommends \
        clang-22 libclang1-22 python3-pip
    symlink_clang_tools clang clang-tidy ld.lld
    # Only pre-commit is pip-installed here: the lint hooks (vermin, PyYAML,
    # ruff, mypy, codespell, clang2) each provision their own isolated env
    # from the .pre-commit-config.yaml additional_dependencies, so the
    # runner's system python does not need them on PATH.
    pip_user pre-commit
    setup_libclang_env
    clang --version
}

provision_build() {
    setup_llvm_apt
    # libclang1-22 is the C library the drift checker loads; the clang2
    # python bindings are provisioned by pre-commit, so python3-clang-22 is
    # not needed. python3-pip provisions the system interpreter's pip for
    # the --user installs. PyYAML and ruff are installed here for the build
    # job's own use (the CMake check-all targets drive the checkers via
    # system python, not via pre-commit hook envs).
    #
    # libclang-rt-22-dev carries the compiler-rt runtimes the asan-ubsan and
    # fuzz presets link (static ASan/UBSan/libFuzzer archives and the shared
    # ASan runtime the pytest legs preload). clang-22 only Recommends it deep
    # in its chain, so --no-install-recommends drops it unless named here.
    run_priv apt-get install -y --no-install-recommends \
        clang-22 clang++-22 clang-tidy-22 lld-22 ninja-build \
        libclang-rt-22-dev \
        libclang1-22 libhiredis-dev libpq-dev \
        python3-pip pkg-config
    build_liburing
    symlink_clang_tools clang clang++ clang-tidy ld.lld run-clang-tidy
    pip_user cmake==4.4.3 ninja==1.13.2 PyYAML==6.0.3 pytest==9.1.1 pytest-xdist==3.8.0 \
        ruff==0.16.7 clang2==22.1.8.post0 clang-format==23.1.0
    # The formatter ships as the clang-format PyPI wheel; expose it under the
    # versioned name the CMake check and scripts/format.sh resolve.
    ln -sf "$HOME/.local/bin/clang-format" "$HOME/.local/bin/clang-format-23"
    clang --version
    run-clang-tidy --version || true
    setup_libclang_env
    # ABI and hardening audit tools: required by verify.sh's build stage,
    # which fails closed when either is missing (no silent skip). A failed
    # install fails the job rather than letting the checks pass vacuously.
    run_priv apt-get install -y --no-install-recommends abigail-tools checksec
}

provision_build_gcc() {
    # Everything the build section installs, plus gcc-14: the CI matrix's
    # second compiler leg builds the framework with it, while the drift
    # checker and the binding generator keep driving the clang stack above.
    provision_build
    run_priv apt-get install -y --no-install-recommends gcc-14
}

provision_coverage() {
    # Everything the build section installs, plus the source-based coverage
    # reporters. llvm-22 ships llvm-cov and llvm-profdata as
    # version-suffixed binaries only, and none of the toolchain above
    # depends on that package, so it is installed by name. The profile
    # runtime -fprofile-instr-generate links against rides the compiler-rt
    # package the build section already names.
    provision_build
    run_priv apt-get install -y --no-install-recommends llvm-22
    symlink_clang_tools llvm-cov llvm-profdata
    pip_user coverage==7.15.4
}

provision_free_threaded() {
    setup_llvm_apt
    # libclang1-22 is the C library the ctypes drift checker loads; the
    # bindings themselves are installed by verify.sh into the free-threaded
    # venv (clang2 pip wheel), so python3-clang-22 is not needed here. The
    # free-threaded sanitizer leg builds the asan-ubsan preset through this
    # provisioning, so libclang-rt-22-dev is named here as in build.
    run_priv apt-get install -y --no-install-recommends \
        clang-22 clang++-22 lld-22 ninja-build \
        libclang-rt-22-dev \
        libclang1-22 libhiredis-dev libpq-dev \
        python3-pip pkg-config
    build_liburing
    symlink_clang_tools clang clang++ ld.lld
    # uv provisions the free-threaded interpreter (cpython-3.14+freethreaded)
    # so verify.sh can drive the suite under python3.14t without a separate
    # install step. Both the GitHub Actions free-threaded job and the Docker
    # CI image (via `provision-ci.sh all`) call this, so the interpreter is
    # present in both environments. The pip wheel is the official
    # distribution, pinned like every other tool above; the interpreter pin
    # names the exact CPython build uv must resolve.
    pip_user cmake==4.4.3 ninja==1.13.2 uv==0.12.15
    clang --version
    setup_libclang_env
    uv python install cpython-3.14.7+freethreaded-linux-x86_64-gnu
}

provision_sdist() {
    setup_llvm_apt
    # The from-source contract floor. The host supplies the C compiler, the
    # linker the CMake layer selects, and the dev libraries; everything else
    # the build needs (cmake, ninja) arrives through pip's isolated build
    # environment. The dev-checker toolchain is deliberately absent so the
    # sdist configure path runs exactly as a from-source user sees it: no
    # clang-tidy, no clang-format, no ruff.
    run_priv apt-get install -y --no-install-recommends \
        clang-22 clang++-22 lld-22 libhiredis-dev libpq-dev pkg-config
    build_liburing
    symlink_clang_tools clang ld.lld
    clang --version
}

case "$job" in
    lint)          provision_lint ;;
    build)         provision_build ;;
    build-gcc)     provision_build_gcc ;;
    coverage)      provision_coverage ;;
    free-threaded) provision_free_threaded ;;
    sdist)         provision_sdist ;;
    all)
        provision_lint
        provision_build
        provision_build_gcc
        provision_coverage
        provision_free_threaded
        ;;
    *)
        echo "error: unknown job: $job" >&2
    echo "usage: $0 <lint|build|build-gcc|coverage|free-threaded|sdist|all>" >&2
        exit 2
        ;;
esac

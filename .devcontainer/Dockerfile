# One-click development environment for kith: Clang 22, clangd, lld, CMake
# 4.4, Ninja, Python 3.14, uv, pre-commit, plus the native build dependencies
# (liburing for the io_uring reactor backend, libpq and libhiredis for the db
# and state libraries, libabigail for local ABI diffs). Built on the
# devcontainers base image so Codespaces and VS Code Dev Containers get a
# non-root "vscode" user, git, and common utilities out of the box. The
# accompanying compose stack also starts a Postgres service reachable as
# host "db", shaped like the CI one, so the db-backed tests and examples
# run with no further setup. Cloning the repo into this container and
# running the documented bootstrap (uv sync, pre-commit install,
# cmake --preset debug) works with no further setup.

# The digest pins the image bytes; the tag documents the intended version.
# Bumping is deliberate: re-resolve the tag's digest and update both.
FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04@sha256:d94c97dd9cacf183d0a6fd12a8e87b526e9e928307674ae9c94139139c0c6eae

ARG LLVM_VERSION=22
ARG NINJA_VERSION=1.13.2
ARG LIBURING_VERSION=2.14

# --- Base packages -------------------------------------------------------
# ca-certificates/curl/wget/gnupg/unzip/xz-utils: key and asset fetching and
# source-tarball extraction. build-essential/pkg-config: compiling the C core
# and probing liburing. software-properties-common: add-apt-repository for the
# deadsnakes PPA (Python 3.14). libpq-dev/libhiredis-dev: the db and state
# libraries' pkg-config probes (lib_db.cmake, lib_state.cmake) require the
# matching dev headers. libabigail-dev/abigail-tools: local ABI diffs.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates curl wget gnupg unzip xz-utils \
        build-essential pkg-config \
        software-properties-common \
        libpq-dev libhiredis-dev \
        libabigail-dev abigail-tools \
 && rm -rf /var/lib/apt/lists/*

# --- Third-party APT repositories + their packages ------------------------
# LLVM apt (apt.llvm.org): Clang, clangd, lld, clang-format, clang-tidy, and
# the matching dev headers, all at ${LLVM_VERSION}.
# Kitware apt: recent CMake (>= 4.4); noble's stock cmake is too old.
# GitHub CLI apt: recent gh; noble's stock gh is older.
# deadsnakes PPA: Python 3.14 (noble ships 3.12).
#
# The versioned LLVM tools are exposed under their unversioned names via
# update-alternatives so the build (CC=clang) and clangd resolve to this
# toolchain. run-clang-tidy is included so scripts/run-clang-tidy.sh works.
RUN wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key \
    | gpg --dearmor -o /usr/share/keyrings/llvm-snapshot.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/noble/ llvm-toolchain-noble-${LLVM_VERSION} main" \
    > /etc/apt/sources.list.d/llvm.list \
 && wget -qO- https://apt.kitware.com/keys/kitware-archive-latest.asc \
    | gpg --dearmor -o /usr/share/keyrings/kitware-archive-keyring.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ noble main" \
    > /etc/apt/sources.list.d/kitware.list \
 && wget -qO /usr/share/keyrings/githubcli-archive-keyring.gpg \
    https://cli.github.com/packages/githubcli-archive-keyring.gpg \
 && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    > /etc/apt/sources.list.d/github-cli.list \
 && add-apt-repository -y ppa:deadsnakes/ppa \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
        clang-${LLVM_VERSION} clangd-${LLVM_VERSION} lld-${LLVM_VERSION} \
        clang-format-${LLVM_VERSION} clang-tidy-${LLVM_VERSION} \
        libclang-${LLVM_VERSION}-dev llvm-${LLVM_VERSION}-dev \
        cmake \
        python3.14 python3.14-dev python3.14-venv \
        gh \
 && rm -rf /var/lib/apt/lists/* \
 && for tool in clang clang++ clangd clang-format clang-tidy ld.lld \
                llvm-config run-clang-tidy; do \
        if [ -e "/usr/bin/${tool}-${LLVM_VERSION}" ]; then \
            update-alternatives --install "/usr/bin/${tool}" "${tool}" \
                "/usr/bin/${tool}-${LLVM_VERSION}" 100; \
        fi; \
    done

# --- Ninja (pinned, checksummed) ------------------------------------------
# noble's ninja-build is 1.11, below the 1.12 floor; pull the official binary.
# The checksum belongs to the pinned version: bump NINJA_VERSION and the
# hash together.
RUN curl -fsSL "https://github.com/ninja-build/ninja/releases/download/v${NINJA_VERSION}/ninja-linux.zip" \
    -o /tmp/ninja.zip \
 && echo "5749cbc4e668273514150a80e387a957f933c6ed3f5f11e03fb30955e2bbead6  /tmp/ninja.zip" | sha256sum -c - \
 && unzip -o /tmp/ninja.zip -d /usr/local/bin \
 && chmod +x /usr/local/bin/ninja \
 && rm /tmp/ninja.zip

# --- liburing (pinned, checksummed, from source) ---------------------------
# noble's liburing2 is 2.5, below the 2.7 floor the io_uring reactor backend
# requires. The checksum belongs to the pinned version: bump LIBURING_VERSION
# and the hash together. Installed to /usr/local so pkg-config (its default
# search path includes /usr/local/lib/pkgconfig) and ldconfig (/usr/local/lib)
# find it.
RUN curl -fsSL "https://github.com/axboe/liburing/archive/refs/tags/liburing-${LIBURING_VERSION}.tar.gz" \
    -o /tmp/liburing.tar.gz \
 && echo "5f80964108981c6ad979c735f0b4877d5f49914c2a062f8e88282f26bf61de0c  /tmp/liburing.tar.gz" | sha256sum -c - \
 && mkdir -p /tmp/liburing-build \
 && tar -xzf /tmp/liburing.tar.gz -C /tmp/liburing-build --strip-components=1 \
 && cd /tmp/liburing-build \
 && ./configure --prefix=/usr/local \
 && make -j"$(nproc)" \
 && make install \
 && ldconfig \
 && rm -rf /tmp/liburing-build /tmp/liburing.tar.gz

# --- uv (digest-pinned) ----------------------------------------------------
# Project Python environment manager, copied from the official tool image so
# no remote script executes at build time. The digest pins the image bytes;
# the tag documents the intended version. Bumping is deliberate: re-resolve
# the tag's digest and update both. Installed to /usr/local/bin so it is
# available to every user; postCreateCommand runs as the non-root "vscode"
# user.
COPY --from=ghcr.io/astral-sh/uv:0.12.15@sha256:62f8c047d0a0e9ece6b53fc63df902585a67a47a7f318ddec4a37db586edc8e3 \
     /uv /uvx /usr/local/bin/

# --- pre-commit ----------------------------------------------------------
# Installed as a uv tool into a world-readable tool directory so the binary
# symlink (/usr/local/bin/pre-commit) is executable by the "vscode" user even
# though the tool venv was created as root.
RUN UV_TOOL_DIR=/opt/uv-tools UV_TOOL_BIN_DIR=/usr/local/bin \
    uv tool install --python python3.14 pre-commit \
 && chmod -R a+rX /opt/uv-tools

# --- Interactive Python defaults -----------------------------------------
# Point PATH-resolved "python3"/"python" at Python 3.14 so a contributor's
# terminal and CMake's find_package(Python3) match the project target, while
# system tooling that calls /usr/bin/python3 (3.12) directly is unaffected.
RUN ln -sf /usr/bin/python3.14 /usr/local/bin/python3 \
 && ln -sf /usr/bin/python3.14 /usr/local/bin/python

# --- Build defaults ------------------------------------------------------
ENV CC=clang \
    CXX=clang++

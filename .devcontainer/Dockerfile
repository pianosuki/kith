# One-click development environment for kith: Clang 23.1.5, clangd, lld, CMake
# 4.4, Ninja, Python 3.14/3.15, uv, pre-commit, plus the native build dependencies
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

ARG BINDINGS_LLVM_VERSION=22
ARG NINJA_VERSION=1.13.2
ARG LIBURING_VERSION=2.15

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
# LLVM apt supplies the matching binding parser and headers. The stable
# native compiler and analysis tools come from the verified archive below.
# Kitware apt: recent CMake (>= 4.4); noble's stock cmake is too old.
# GitHub CLI apt: recent gh; noble's stock gh is older.
# deadsnakes PPA: Python 3.14 (noble ships 3.12).
#
# The binding parser is selected explicitly; PATH selects the native tools.
RUN wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key \
    | gpg --dearmor -o /usr/share/keyrings/llvm-snapshot.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/noble/ llvm-toolchain-noble-${BINDINGS_LLVM_VERSION} main" \
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
        clang-${BINDINGS_LLVM_VERSION} clang-tidy-${BINDINGS_LLVM_VERSION} libclang-${BINDINGS_LLVM_VERSION}-dev \
        cmake \
        python3.14 python3.14-dev python3.14-venv \
        gh libzstd1 zlib1g libtinfo6 libedit2 libxml2 \
 && rm -rf /var/lib/apt/lists/*

# Stable compiler/tools use the same verified archive as CI. The parser stays
# on LLVM 22 to match the supported clang2 package.
COPY scripts/install-llvm.sh /tmp/install-llvm.sh
RUN bash /tmp/install-llvm.sh /opt/kith/llvm-23.1.5 /tmp/llvm-cache \
 && rm -rf /tmp/llvm-cache /tmp/install-llvm.sh
ENV PATH="/opt/kith/llvm-23.1.5/native-bin:${PATH}" \
    KITH_BINDINGS_CLANG=/usr/bin/clang-22 \
    LIBCLANG_LIBRARY_FILE=/usr/lib/llvm-22/lib/libclang.so

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
 && echo "8d052f2622dcb3678cbaee5ff582a87572672a6c0a56533cdda5b65cb636120a  /tmp/liburing.tar.gz" | sha256sum -c - \
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
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 \
     /uv /uvx /usr/local/bin/

# Exact compatibility runtimes are shared read-only; project environments stay
# owned by the contributor. System tools still use /usr/bin/python3.
ENV UV_PYTHON_INSTALL_DIR=/opt/kith-python
RUN uv python install --no-bin 3.14.10 3.15.2 3.14.10t 3.15.2t \
 && chmod -R a+rX /opt/kith-python \
 && ln -sf "$(uv python find 3.14.10)" /usr/local/bin/python3 \
 && ln -sf "$(uv python find 3.14.10)" /usr/local/bin/python \
 && ln -sf "$(uv python find 3.14.10)" /usr/local/bin/python3.14 \
 && ln -sf "$(uv python find 3.15.2)" /usr/local/bin/python3.15

# --- pre-commit ----------------------------------------------------------
# Installed as a uv tool into a world-readable tool directory so the binary
# symlink (/usr/local/bin/pre-commit) is executable by the "vscode" user even
# though the tool venv was created as root.
RUN UV_TOOL_DIR=/opt/uv-tools UV_TOOL_BIN_DIR=/usr/local/bin \
    uv tool install --python python3.14 pre-commit==4.7.0 \
 && UV_TOOL_DIR=/opt/uv-tools UV_TOOL_BIN_DIR=/usr/local/bin \
    uv tool install --python python3.14 clang-format==23.1.5 \
 && ln -s /usr/local/bin/clang-format /usr/local/bin/clang-format-23 \
 && chmod -R a+rX /opt/uv-tools

# --- Build defaults ------------------------------------------------------
ENV CC=clang \
    CXX=clang++

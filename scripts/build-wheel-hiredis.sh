#!/usr/bin/env bash
# Build the release wheel's reviewed Redis client in a private prefix.
set -euo pipefail

prefix="$(realpath -m "${1:?usage: build-wheel-hiredis.sh PREFIX SCRATCH}")"
scratch="$(realpath -m "${2:?usage: build-wheel-hiredis.sh PREFIX SCRATCH}")"
version=1.5.0
sha256=eec7ef8641d221e144a58d5ef8d733c8c75c7fd487097a233aacb4fa33981cfb
url="https://github.com/redis/hiredis/archive/refs/tags/v${version}.tar.gz"
hardening_cflags="-O2 -fPIC -fstack-protector-strong -fstack-clash-protection -D_FORTIFY_SOURCE=3"
if [ "$(uname -m)" = x86_64 ]; then
    hardening_cflags="$hardening_cflags -fcf-protection=full"
fi
hardening_ldflags="-Wl,-z,relro,-z,now,-z,noexecstack"
mkdir -p "$scratch" "$prefix"
build="$(mktemp -d "$scratch/hiredis-XXXXXX")"
wget -q "$url" -O "$build/hiredis.tar.gz"
printf '%s  %s\n' "$sha256" "$build/hiredis.tar.gz" | sha256sum -c -
tar -xzf "$build/hiredis.tar.gz" -C "$build"
cd "$build/hiredis-${version}"
make -j "${CMAKE_BUILD_PARALLEL_LEVEL:-2}" CC="${CC:-clang}" USE_SSL=0 PREFIX="$prefix" \
    CFLAGS="${CFLAGS:-} $hardening_cflags" LDFLAGS="${LDFLAGS:-} $hardening_ldflags"
make CC="${CC:-clang}" USE_SSL=0 \
    CFLAGS="${CFLAGS:-} $hardening_cflags" LDFLAGS="${LDFLAGS:-} $hardening_ldflags" PREFIX="$prefix" install
mkdir -p "$prefix/share/kith-hiredis"
cp COPYING "$prefix/share/kith-hiredis/COPYING"
printf '%s\n' "$version" > "$prefix/share/kith-hiredis/VERSION"
printf '%s\n' "$url" > "$prefix/share/kith-hiredis/SOURCE_URL"
printf '%s\n' "$sha256" > "$prefix/share/kith-hiredis/SOURCE_SHA256"

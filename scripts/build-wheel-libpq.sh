#!/usr/bin/env bash
# Build the release wheel's TLS-enabled PostgreSQL client in a private prefix.
set -euo pipefail

prefix="$(realpath -m "${1:?usage: build-wheel-libpq.sh PREFIX SCRATCH}")"
scratch="$(realpath -m "${2:?usage: build-wheel-libpq.sh PREFIX SCRATCH}")"
version=18.6
sha256=555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f
url="https://ftp.postgresql.org/pub/source/v${version}/postgresql-${version}.tar.bz2"
hardening_cflags="-O2 -fPIC -fstack-protector-strong -fstack-clash-protection -D_FORTIFY_SOURCE=3"
if [ "$(uname -m)" = x86_64 ]; then
    hardening_cflags="$hardening_cflags -fcf-protection=full"
fi
hardening_ldflags="-Wl,-z,relro,-z,now,-z,noexecstack"
mkdir -p "$scratch" "$prefix"
build="$(mktemp -d "$scratch/libpq-XXXXXX")"
wget -q "$url" -O "$build/postgresql.tar.bz2"
printf '%s  %s\n' "$sha256" "$build/postgresql.tar.bz2" | sha256sum -c -
tar -xjf "$build/postgresql.tar.bz2" -C "$build"
cd "$build/postgresql-${version}"
CC="${CC:-clang}" CFLAGS="${CFLAGS:-} $hardening_cflags" \
    LDFLAGS="${LDFLAGS:-} $hardening_ldflags" ./configure --prefix="$prefix" --with-ssl=openssl --without-ldap \
    --without-gssapi --without-icu --without-readline --without-zlib \
    --without-zstd --without-lz4 --disable-rpath
make -j "${CMAKE_BUILD_PARALLEL_LEVEL:-2}" -C src/interfaces/libpq
make -C src/interfaces/libpq install
make -C src/include install
mkdir -p "$prefix/share/kith-libpq"
cp COPYRIGHT "$prefix/share/kith-libpq/COPYRIGHT"
printf '%s\n' "$version" > "$prefix/share/kith-libpq/VERSION"
printf '%s\n' "$url" > "$prefix/share/kith-libpq/SOURCE_URL"
printf '%s\n' "$sha256" > "$prefix/share/kith-libpq/SOURCE_SHA256"

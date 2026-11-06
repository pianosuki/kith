#!/usr/bin/env bash
# Install the stable native toolchain into a private, checksummed prefix.
set -euo pipefail

version=23.1.5
sha256=f72edb0b36580ece7f52e23858f00c18fad4e87395ea7ad05d7e55c39f8fc78a
prefix="$(realpath -m "${1:?usage: install-llvm.sh PREFIX CACHE}")"
cache="$(realpath -m "${2:?usage: install-llvm.sh PREFIX CACHE}")"
archive="LLVM-${version}-Linux-X64.tar.xz"
url="https://github.com/llvm/llvm-project/releases/download/llvmorg-${version}/${archive}"

if [ "$(uname -s)" != Linux ] || [ "$(uname -m)" != x86_64 ]; then
    echo 'install-llvm.sh: the reviewed archive requires Linux x86_64' >&2
    exit 1
fi
if [ -f "$prefix/.source-sha256" ] && [ "$(cat "$prefix/.source-sha256")" = "$sha256" ]; then
    test -x "$prefix/native-bin/clang"
    "$prefix/bin/clang" --version
    exit 0
fi
if [ -e "$prefix" ]; then
    echo "install-llvm.sh: refusing to replace an unrecognized prefix: $prefix" >&2
    exit 1
fi
mkdir -p "$cache" "$(dirname "$prefix")"
if [ ! -f "$cache/$archive" ]; then
    wget --tries=3 --timeout=30 -q "$url" -O "$cache/$archive.partial"
    printf '%s  %s\n' "$sha256" "$cache/$archive.partial" | sha256sum -c -
    mv "$cache/$archive.partial" "$cache/$archive"
else
    printf '%s  %s\n' "$sha256" "$cache/$archive" | sha256sum -c -
fi
stage="$(mktemp -d "${prefix}.XXXXXX")"
trap 'rm -rf "$stage"' EXIT
tar -xJf "$cache/$archive" -C "$stage" --strip-components=1
for tool in clang clang++ ld.lld clang-tidy llvm-cov llvm-profdata; do
    test -x "$stage/bin/$tool"
done
if [ ! -x "$stage/bin/run-clang-tidy" ]; then
    test -f "$stage/share/clang/run-clang-tidy.py"
    ln -s ../share/clang/run-clang-tidy.py "$stage/bin/run-clang-tidy"
fi
# Keep the separately pinned formatter on PATH; compiler archives may ship a
# newer formatter than the Python/pre-commit distribution channel.
mkdir "$stage/native-bin"
for binary in "$stage/bin/"*; do
    name="$(basename "$binary")"
    case "$name" in clang-format*) continue ;; esac
    if [ -x "$binary" ] && [ ! -d "$binary" ]; then
        ln -s "../bin/$name" "$stage/native-bin/$name"
    fi
done
"$stage/bin/clang" --version
printf '%s\n' "$sha256" > "$stage/.source-sha256"
printf '%s\n' "$url" > "$stage/.source-url"
chmod 755 "$stage"
mv "$stage" "$prefix"
trap - EXIT

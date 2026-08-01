#!/usr/bin/env python3
"""Generate a linker version script (.map) for a kith shared library.

Parses the library's public headers under include/kith/<module>/ and
emits a GNU ld version script exporting only the KITH_API symbols. This
keeps the exported surface minimal and stable per library. The generated
.map is committed alongside the source change that produced it.

A KITH_API symbol is a function declaration whose declarator is
preceded (directly or via a leading attribute) by KITH_API. The checker
uses a lightweight line-oriented parser: it scans for lines containing
KITH_API and extracts the function name from the following declarator.
This avoids a hard libclang dependency at build time; a libclang-based
generator (gen_ctypes.py) handles the richer parsing for Python
bindings.

Usage:
    gen_symbols.py <module> [--out <path>] [--include-root <dir>]

    module         the library's module name (e.g. "util", "reactor")
    --out          output .map file path (default: stdout)
    --include-root public headers root (default: include)

Example output:
    KITH_UTIL_1 {
        global:
            kith_util_ulid_next;
            kith_util_time_now;
        local:
            *;
    };
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


# Match a function declaration line that carries KITH_API.
# Captures the function name. Handles:
#   KITH_API int kith_foo_create(...);
#   KITH_API kith_foo_t *kith_foo_alloc(...);
#   [[nodiscard]] KITH_API int kith_foo_query(...);
KITH_API_FUNC_RE = re.compile(
    r"""
    \bKITH_API\b                    # visibility macro
    .*?                             # return type / attributes (non-greedy)
    \b(kith_[A-Za-z_][A-Za-z0-9_]*) # function name starting with kith_
    \s*\(                           # opening paren of parameter list
    """,
    re.VERBOSE | re.DOTALL,
)


def extract_api_symbols(header_path: Path) -> list[str]:
    """Extract KITH_API function names from a public header."""
    text = header_path.read_text(encoding="utf-8", errors="ignore")

    # Strip block comments to avoid matching in doc prose.
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    # Strip line comments.
    text = re.sub(r"//[^\n]*", "", text)

    symbols: list[str] = []
    for m in KITH_API_FUNC_RE.finditer(text):
        name = m.group(1)
        if name not in symbols:
            symbols.append(name)
    return symbols


def generate_version_script(module: str, symbols: list[str], abi_version: int = 1) -> str:
    """Generate a GNU ld version script."""
    tag = f"KITH_{module.upper()}_{abi_version}"
    lines = [f"{tag} {{"]
    lines.append("    global:")
    if symbols:
        for sym in symbols:
            lines.append(f"        {sym};")
    else:
        lines.append("        /* no KITH_API symbols found */")
    lines.append("    local:")
    lines.append("        *;")
    lines.append("};")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a linker version script from public headers."
    )
    parser.add_argument("module", help="module name (e.g. util, reactor)")
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output .map path (default: stdout)",
    )
    parser.add_argument(
        "--include-root",
        type=Path,
        default=Path("include"),
        help="public headers root (default: include)",
    )
    parser.add_argument(
        "--abi-version",
        type=int,
        default=1,
        help="ABI version number (default: 1)",
    )
    args = parser.parse_args(argv[1:])

    module_dir = args.include_root / "kith" / args.module
    if not module_dir.is_dir():
        print(
            f"error: module header directory not found: {module_dir}",
            file=sys.stderr,
        )
        return 2

    symbols: list[str] = []
    for header in sorted(module_dir.rglob("*.h")):
        symbols.extend(extract_api_symbols(header))

    script = generate_version_script(args.module, symbols, args.abi_version)

    if args.out is not None:
        args.out.write_text(script, encoding="utf-8")
        print(
            f"wrote {args.out} ({len(symbols)} symbols)",
            file=sys.stderr,
        )
    else:
        sys.stdout.write(script)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

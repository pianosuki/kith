#!/usr/bin/env python3
"""Cross-module internal-header isolation checker for the kith framework.

A module's private headers live under `internal/` or are named
`internal.h` (`module/internal/name.h` or
`module/submodule/internal.h`). These headers are private to the module
that owns them; another module may not include them directly. This
checker flags any `#include "X/.../internal..."` or
`#include "X/internal.h"` where X differs from the source file's module.

The public API surface of a module is its headers under
`include/kith/<module>/` excluding any `internal/` subtree. Consumers
reaching into another module's `internal/` are breaking encapsulation and
must instead depend on the module's public entry header.

Usage:
    check_internal_includes.py <root> [<root> ...]

The committed pre-commit hook passes `include src`.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


INCLUDE_RE = re.compile(r'^\s*#\s*include\s+"([^"]+)"')


def is_internal_header(include_path: str) -> bool:
    """True if the include path targets a private/internal header."""
    return include_path.endswith("internal.h") or "/internal/" in include_path


def module_for(path: str) -> str:
    """First path segment, or the filename stem for a bare file."""
    if "/" in path:
        return path.split("/", 1)[0]
    return path.rsplit(".", 1)[0]


def source_module_for(src_rel: str) -> str:
    """Map a source-relative path to its module name.

    `include/kith/net/conn.c` -> `net`
    `src/net/conn.c` -> `net`
    `net/conn.c` -> `net`
    """
    parts = src_rel.split("/")
    if len(parts) >= 2 and parts[0] == "include" and parts[1] == "kith":
        return parts[2] if len(parts) >= 3 else parts[-1].split(".", 1)[0]
    if len(parts) == 1:
        return src_rel.split(".", 1)[0]
    return parts[0]


def scan(root: Path) -> list[tuple[str, int, str]]:
    """Return [(src_rel, line_no, include_path), ...] for violations."""
    violations: list[tuple[str, int, str]] = []
    files = sorted(list(root.rglob("*.c")) + list(root.rglob("*.h")))
    for file_path in files:
        src_rel = file_path.relative_to(root).as_posix()
        if src_rel.startswith(("third_party/", "cmake/", "tools/")):
            continue
        src_mod = source_module_for(src_rel)

        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line_no, line in enumerate(f, start=1):
                m = INCLUDE_RE.match(line)
                if not m:
                    continue
                include_path = m.group(1)
                if not is_internal_header(include_path):
                    continue
                if include_path.startswith("third_party/"):
                    continue

                inc_mod = module_for(include_path)
                if inc_mod != src_mod:
                    violations.append((src_rel, line_no, include_path))
    return violations


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <root> [<root> ...]", file=sys.stderr)
        return 2

    all_violations: list[tuple[str, int, str]] = []
    for root_arg in argv[1:]:
        root = Path(root_arg)
        if not root.exists() or not root.is_dir():
            print(f"error: source root does not exist: {root_arg}", file=sys.stderr)
            return 2
        all_violations.extend(scan(root))

    if not all_violations:
        print("OK: no cross-module internal header includes found.")
        return 0

    print("Cross-module internal header includes detected:")
    for rel, line_no, include_path in all_violations:
        print(f"  {rel}:{line_no} -> {include_path}")
    print(f"Total violations: {len(all_violations)}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

#!/usr/bin/env python3
"""File-layout enforcement for the kith framework.

Verifies repository layout and file-naming conventions:
  - No `.c` files under `include/` (public API surface is headers only).
  - Public headers under `include/kith/` live in a module directory,
    except the four framework-level headers (kith.h, api.h, version.h,
    types.h).
  - Public headers open with the path-derived traditional include guard
    (`kith/util/rng.h` guards as `KITH_UTIL_RNG_H`) and never use
    `#pragma once`, which stays a private-header convenience.
  - Private headers (`internal.h` or under an `internal/` directory)
    have at least one `.c` file in their subtree (no orphan private
    headers for dead modules).
  - Prefixed sources: `src/<module>/<sub>.c` where `src/<module>/<sub>/`
    exists as a directory should move into that subdirectory (the prefix
    indicates submodule ownership).

Usage:
    check_layout_consistency.py <root> [<root> ...]

The committed pre-commit hook passes `include src` so both the public
API surface and the private implementation are checked.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


# Framework-level public headers that live directly under include/kith/,
# not under a module subdirectory.
FRAMEWORK_HEADERS: set[str] = {"kith.h", "api.h", "version.h", "types.h"}

_PRAGMA_ONCE = re.compile(r"^\s*#\s*pragma\s+once\b", re.MULTILINE)


def is_skipped(rel: str) -> bool:
    return rel.startswith(("third_party/", "cmake/"))


def scan_c_files_in_include(root: Path) -> list[str]:
    """No `.c` files under include/ (public API surface is headers only)."""
    violations: list[str] = []
    for c_file in sorted(root.rglob("*.c")):
        rel = c_file.relative_to(root).as_posix()
        if is_skipped(rel):
            continue
        violations.append(f"{rel}: .c file in include/ (public API is headers only)")
    return violations


def scan_public_header_placement(root: Path) -> list[str]:
    """Headers directly under include/kith/ must be framework-level."""
    violations: list[str] = []
    kith_root = root / "kith"
    if not kith_root.is_dir():
        return violations
    for header in sorted(kith_root.glob("*.h")):
        if header.name in FRAMEWORK_HEADERS:
            continue
        rel = header.relative_to(root).as_posix()
        violations.append(
            f"{rel}: public header directly under include/kith/ "
            f"(move to include/kith/<module>/ or add to FRAMEWORK_HEADERS)"
        )
    return violations


def scan_public_header_guards(root: Path) -> list[str]:
    """Public headers open with their path-derived traditional guard.

    The guard name derives from the header's path below include/kith/:
    kith/util/rng.h guards as KITH_UTIL_RNG_H, kith/types.h as
    KITH_TYPES_H.
    """
    violations: list[str] = []
    kith_root = root / "kith"
    if not kith_root.is_dir():
        return violations
    for header in sorted(kith_root.rglob("*.h")):
        rel = header.relative_to(root).as_posix()
        if is_skipped(rel):
            continue
        text = header.read_text(encoding="utf-8")
        if _PRAGMA_ONCE.search(text):
            violations.append(
                f"{rel}: #pragma once in a public header (traditional guards required)"
            )
        kith_parts = header.relative_to(kith_root).parts
        guard = "KITH_" + "_".join((*kith_parts[:-1], header.stem)).upper() + "_H"
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        first = lines[0] if lines else ""
        second = lines[1] if len(lines) > 1 else ""
        if first != f"#ifndef {guard}" or second != f"#define {guard}":
            found = first if first else "no content"
            violations.append(
                f"{rel}: public header must open with the include guard {guard} (found: {found})"
            )
    return violations


def is_private_header(path: Path) -> bool:
    return path.name == "internal.h" or "internal" in path.parts


def scan_orphan_private_headers(root: Path) -> list[str]:
    """Private headers with no .c file in their module subtree.

    A private header (`internal.h` or under `internal/`) belongs to the
    nearest enclosing module directory (the first ancestor that is a
    direct child of `src/`). That module subtree must contain at least
    one `.c` file, or the header is orphaned.
    """
    violations: list[str] = []
    for header in sorted(root.rglob("*.h")):
        rel = header.relative_to(root).as_posix()
        if is_skipped(rel):
            continue
        if not is_private_header(header):
            continue

        # Find the module root: the first ancestor directory that is a
        # direct child of the scan root (e.g. src/net for
        # src/net/internal/conn.h).
        parts = header.relative_to(root).parts
        if len(parts) < 2:
            continue
        module_dir = root / parts[0]

        impl_files = [
            p for p in module_dir.rglob("*.c") if not is_skipped(p.relative_to(root).as_posix())
        ]
        if impl_files:
            continue

        violations.append(f"{rel}: private header with no .c file in module {parts[0]}/ (orphan)")
    return violations


def scan_prefixed_sources(root: Path) -> list[str]:
    """`<module>_<sub>.c` should live under `<module>/<sub>/` if that
    directory exists (the prefix indicates submodule ownership).

    Only applies to `src/` (implementation files), not `include/`.
    """
    violations: list[str] = []
    src_root = root if root.name == "src" else None
    if src_root is None:
        return violations

    for c_file in sorted(root.rglob("*.c")):
        rel = c_file.relative_to(root).as_posix()
        if is_skipped(rel):
            continue
        name = c_file.name
        if "_" not in name:
            continue
        # Only check files directly under a module dir (not already in
        # a submodule dir).
        parts = c_file.relative_to(root).parts
        if len(parts) != 2:
            continue

        prefix = name.split("_", 1)[0]
        subdir = c_file.parent / prefix
        if subdir.is_dir():
            violations.append(
                f"{rel}: prefixed source should live under "
                f"{subdir.relative_to(root).as_posix()}/ "
                f"(submodule directory exists)"
            )
    return violations


def scan_root(root: Path) -> list[str]:
    """Scan one root and return all layout violations."""
    violations: list[str] = []

    is_include = root.name == "include" or (root / "kith").is_dir()
    is_src = root.name == "src"

    if is_include:
        violations.extend(scan_c_files_in_include(root))
        violations.extend(scan_public_header_placement(root))
        violations.extend(scan_public_header_guards(root))

    # Private-header orphans and prefixed-source checks apply to both
    # roots, but prefixed-source only makes sense under src/.
    violations.extend(scan_orphan_private_headers(root))
    if is_src:
        violations.extend(scan_prefixed_sources(root))

    return violations


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <root> [<root> ...]", file=sys.stderr)
        return 2

    all_violations: list[str] = []
    for root_arg in argv[1:]:
        root = Path(root_arg)
        if not root.exists() or not root.is_dir():
            print(f"error: source root does not exist: {root_arg}", file=sys.stderr)
            return 2
        all_violations.extend(scan_root(root))

    if all_violations:
        print("Layout violations detected:")
        for v in all_violations:
            print(f"  {v}")
        print(f"Total violations: {len(all_violations)}")
        return 1

    print("OK: layout consistency checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

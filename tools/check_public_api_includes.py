#!/usr/bin/env python3
"""Public-API include-hygiene checker for the kith framework.

Detects cross-module "deep includes": a source file in module A including
a header from module B at a path that reaches past B's public API surface.
A module's public API lives under `include/kith/<module>/` as flat
one-level-deep headers (`<module>/<module>.h` entry header and
`<module>/<submodule>.h` public submodule headers). A cross-module include
whose path contains a non-public depth segment (`core`, `impl`, `detail`,
`private`, `internal`) is a deep include: the consumer is reaching into
the other module's implementation structure rather than its public contract.

The `internal` segment is also enforced by `check_internal_includes.py`;
this checker catches the broader set of depth indicators. Both fire
independently; a single bad include may produce two violations, which is
intentional (different fix guidance).

A `DEPRECATED_INCLUDES` dict maps specific deep paths to their recommended
public entry-header replacements. It starts empty (greenfield: no legacy
paths to deprecate yet) and is extended as the framework matures and old
paths are retired.

Usage:
    check_public_api_includes.py <root> [<root> ...]

The committed pre-commit hook passes `include src`.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


INCLUDE_RE = re.compile(r'^\s*#\s*include\s+"([^"]+)"')

# Non-public depth segments in an include path. A cross-module include
# whose path contains any of these is a deep include.
DEPTH_SEGMENTS: set[str] = {"core", "impl", "detail", "private", "internal"}

# Specific deep paths to deprecate, mapped to their recommended public
# replacement. Starts empty (greenfield). Extended by appending entries
# as old include paths are retired.
DEPRECATED_INCLUDES: dict[str, str] = {}

# One-off deep-include exceptions, tracked explicitly: source file and
# include pair, with a reason and removal target. Starts empty.
TEMP_EXCEPTIONS: dict[tuple[str, str], str] = {}


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


def include_module_for(include_path: str) -> str | None:
    if "/" not in include_path:
        return None
    return include_path.split("/", 1)[0]


def has_depth_segment(include_path: str) -> bool:
    parts = include_path.replace("\\", "/").split("/")
    return any(seg in DEPTH_SEGMENTS for seg in parts)


def scan(
    root: Path,
) -> tuple[
    list[tuple[str, int, str, str | None]],
    list[tuple[str, int, str, str]],
]:
    """Return (violations, debt).

    violations: (src_rel, line_no, include_path, replacement_or_None)
    debt: (src_rel, line_no, include_path, reason)
    """
    violations: list[tuple[str, int, str, str | None]] = []
    debt: list[tuple[str, int, str, str]] = []

    files = sorted(list(root.rglob("*.c")) + list(root.rglob("*.h")))
    for file_path in files:
        src_rel = file_path.relative_to(root).as_posix()
        if src_rel.startswith(("third_party/", "cmake/")):
            continue

        src_mod = source_module_for(src_rel)
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line_no, line in enumerate(f, start=1):
                m = INCLUDE_RE.match(line)
                if not m:
                    continue

                include_path = m.group(1)
                inc_mod = include_module_for(include_path)
                if inc_mod is None or inc_mod == src_mod:
                    continue

                key = (src_rel, include_path)

                # Explicit deprecation mapping takes priority.
                replacement = DEPRECATED_INCLUDES.get(include_path)
                if replacement is not None:
                    if key in TEMP_EXCEPTIONS:
                        debt.append((src_rel, line_no, include_path, TEMP_EXCEPTIONS[key]))
                    else:
                        violations.append((src_rel, line_no, include_path, replacement))
                    continue

                # Structural deep-include detection: non-public depth
                # segment in a cross-module include path.
                if has_depth_segment(include_path):
                    if key in TEMP_EXCEPTIONS:
                        debt.append((src_rel, line_no, include_path, TEMP_EXCEPTIONS[key]))
                    else:
                        entry = f"{inc_mod}/{inc_mod}.h"
                        violations.append((src_rel, line_no, include_path, entry))

    return violations, debt


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <root> [<root> ...]", file=sys.stderr)
        return 2

    all_violations: list[tuple[str, int, str, str | None]] = []
    all_debt: list[tuple[str, int, str, str]] = []

    for root_arg in argv[1:]:
        root = Path(root_arg)
        if not root.exists() or not root.is_dir():
            print(f"error: source root does not exist: {root_arg}", file=sys.stderr)
            return 2
        v, d = scan(root)
        all_violations.extend(v)
        all_debt.extend(d)

    if all_debt:
        print("Tracked deep-include debt (allowed temporarily):")
        for src_rel, line_no, include_path, reason in all_debt:
            print(f"  {src_rel}:{line_no} -> {include_path}")
            print(f"    reason: {reason}")

    if all_violations:
        print("Cross-module deep includes detected:")
        for src_rel, line_no, include_path, replacement in all_violations:
            print(f"  {src_rel}:{line_no} -> {include_path}")
            if replacement is not None:
                print(f"    use: {replacement}")
            else:
                print("    use the module's public entry header")
        print(f"Total violations: {len(all_violations)}")
        return 1

    if all_debt:
        print(f"OK: no new deep includes; tracked debt: {len(all_debt)}")
    else:
        print("OK: no cross-module deep includes found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

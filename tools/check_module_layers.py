#!/usr/bin/env python3
"""Dependency-layer enforcement for the kith framework.

Scans C source/header trees and verifies that every `#include "mod/..."`
edge points same-layer or downward per the dependency-layer map.
A module may only include headers from the same or lower dependency
layer, regardless of runtime plane. Plane orthogonality is enforced by
`check_runtime_planes.py`.

Usage:
    check_module_layers.py <root> [<root> ...]

Each <root> is scanned independently; results are merged. The committed
pre-commit hook passes `include src` so both the public API surface and
the private implementation are checked.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


INCLUDE_RE = re.compile(r'^\s*#\s*include\s+"([^"]+)"')

# Dependency layers. A module may include same-or-lower layer only.
# Layer 0 (foundation): framework, util, config, logger, metrics
# Layer 1 (infrastructure): net, proto, worker
# Layer 2 (services): reactor, aoi, sim, fabric, gateway, coord,
#                     control, client, state, db
# Layer 3 (composition): server
# Layer 4 (game): examples/*, user code (not scanned here)
#
# "framework" is a pseudo-module for the cross-cutting public headers that
# live directly under include/kith/ (api.h, version.h, types.h, kith.h).
# Every real module depends on them, so they sit at the foundation layer.
MODULE_LAYERS: dict[str, int] = {
    "framework": 0,
    "util": 0,
    "config": 0,
    "logger": 0,
    "metrics": 0,
    "net": 1,
    "proto": 1,
    "worker": 1,
    "db": 2,
    "reactor": 2,
    "state": 2,
    "aoi": 2,
    "sim": 2,
    "fabric": 2,
    "gateway": 2,
    "coord": 2,
    "control": 2,
    "client": 2,
    "server": 3,
}

# Framework-level public headers that live directly under include/kith/,
# not under a module subdirectory. They form the cross-cutting foundation
# every module depends on and map to the "framework" pseudo-module above.
FRAMEWORK_HEADERS: set[str] = {"api.h", "version.h", "types.h", "kith.h"}

# One-off layering exceptions, tracked explicitly. Each entry maps
# (src_module, inc_module) -> reason.
# Starts empty; add an entry only with a removal target in mind.
TEMP_EXCEPTIONS: dict[tuple[str, str], str] = {}

# First-segment modules that are not part of the layer map (third-party
# vendored libraries, generated code). Includes into these are skipped.
EXTERNAL_MODULES: set[str] = set()


def source_module_for(src_rel: str) -> str:
    """Map a source-relative path to its module name.

    The committed hook passes the `include` and `src` directories as the
    scan roots, so paths are root-relative:
      - include root: `kith/net/net.h` -> `net`, `kith/api.h` -> `framework`
      - src root:     `net/conn.c`     -> `net`

    A leading `include/` segment (when a path is repo-root-relative instead
    of include-root-relative) is stripped first. The `kith` segment is the
    public-API namespace, not a module. Framework-level headers that live
    directly under `include/kith/` (api.h, version.h, types.h, kith.h) map
    to the "framework" pseudo-module rather than the namespace segment.
    """
    parts = src_rel.split("/")
    if parts[0] == "include":
        parts = parts[1:]
    if len(parts) >= 2 and parts[0] == "kith":
        if len(parts) == 2 and parts[1] in FRAMEWORK_HEADERS:
            return "framework"
        if len(parts) >= 3:
            return parts[1]
        return parts[-1].split(".", 1)[0]
    if len(parts) == 1:
        return src_rel.split(".", 1)[0]
    return parts[0]


def target_module_for(include_path: str) -> str | None:
    """Map an include path to its target module, or None if the include
    is a bare filename (no module namespace).

    The ``kith/`` prefix is the public-API namespace, not a module.
    Framework-level headers that live directly under ``include/kith/``
    (api.h, version.h, types.h, kith.h) map to the "framework"
    pseudo-module so any real module may depend on them at the
    foundation layer. Deeper ``kith/<module>/<...>`` paths map to
    ``<module>``. Non-kith includes keep their first path segment.
    """
    if "/" not in include_path:
        return None
    parts = include_path.split("/")
    if parts[0] == "kith":
        if len(parts) == 2 and parts[1] in FRAMEWORK_HEADERS:
            return "framework"
        if len(parts) >= 3:
            return parts[1]
        return parts[-1].split(".", 1)[0]
    return parts[0]


def scan_root(
    root: Path,
) -> tuple[
    list[tuple[str, str, str]],
    list[tuple[str, str, str, str]],
    set[str],
]:
    """Scan one root, returning (violations, debt_edges, unknown_modules)."""
    violations: list[tuple[str, str, str]] = []
    debt_edges: list[tuple[str, str, str, str]] = []
    unknown_modules: set[str] = set()

    files = sorted(list(root.rglob("*.c")) + list(root.rglob("*.h")))
    for file_path in files:
        src_rel = file_path.relative_to(root).as_posix()
        if src_rel.startswith(("third_party/", "cmake/")):
            continue

        src_mod = source_module_for(src_rel)
        src_layer = MODULE_LAYERS.get(src_mod)
        if src_layer is None:
            unknown_modules.add(src_mod)
            continue

        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line_no, line in enumerate(f, start=1):
                m = INCLUDE_RE.match(line)
                if not m:
                    continue
                include_path = m.group(1)
                inc_mod = target_module_for(include_path)
                if inc_mod is None:
                    continue
                if inc_mod in EXTERNAL_MODULES or inc_mod == src_mod:
                    continue

                inc_layer = MODULE_LAYERS.get(inc_mod)
                if inc_layer is None:
                    unknown_modules.add(inc_mod)
                    continue

                # Allowed: same-layer or downward dependency.
                if src_layer >= inc_layer:
                    continue

                edge = (src_mod, inc_mod)
                if edge in TEMP_EXCEPTIONS:
                    debt_edges.append(
                        (
                            f"{src_rel}:{line_no}",
                            include_path,
                            f"{src_mod}->{inc_mod}",
                            TEMP_EXCEPTIONS[edge],
                        )
                    )
                else:
                    violations.append(
                        (f"{src_rel}:{line_no}", include_path, f"{src_mod}->{inc_mod}")
                    )

    return violations, debt_edges, unknown_modules


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <root> [<root> ...]", file=sys.stderr)
        return 2

    all_violations: list[tuple[str, str, str]] = []
    all_debt: list[tuple[str, str, str, str]] = []
    all_unknown: set[str] = set()

    for root_arg in argv[1:]:
        root = Path(root_arg)
        if not root.exists() or not root.is_dir():
            print(f"error: source root does not exist: {root_arg}", file=sys.stderr)
            return 2
        v, d, u = scan_root(root)
        all_violations.extend(v)
        all_debt.extend(d)
        all_unknown.update(u)

    if all_unknown:
        print("Unknown modules (not in MODULE_LAYERS):")
        for mod in sorted(all_unknown):
            print(f"  {mod}")
        print(
            "Add the module to MODULE_LAYERS in "
            "tools/check_module_layers.py or list it in EXTERNAL_MODULES."
        )
        return 2

    if all_debt:
        print("Tracked layer-debt edges (allowed temporarily):")
        for rel, include_path, edge, reason in all_debt:
            print(f"  {rel} -> {include_path} [{edge}]")
            print(f"    reason: {reason}")

    if all_violations:
        print("Dependency-layer violations detected:")
        for rel, include_path, edge in all_violations:
            print(f"  {rel} -> {include_path} [{edge}]")
        print(f"Total violations: {len(all_violations)}")
        return 1

    if all_debt:
        print(f"OK: no new layer violations; tracked debt edges: {len(all_debt)}")
    else:
        print("OK: no dependency-layer violations detected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

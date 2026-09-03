#!/usr/bin/env python3
"""Runtime-plane invariant enforcer for the kith framework.

The dependency-layer checker (check_module_layers.py) governs #include
direction. This checker governs data-flow ownership expressed through
cross-plane includes and through cross-plane writes. It is seeded with the
plane invariants from the plane contracts via tools/plane_rules.yaml.

Include-based rules (implemented):
  - A sim source file may not #include gateway/* or fabric/* (sim must
    not know about sockets or delivery).
  - A fabric source file may not #include gateway/* or net/* (fabric
    must not mutate authoritative state or talk directly to sockets).

Data-flow rules (implemented via libclang AST over src/ translation units):
  - A gateway/view translation unit must not call authoritative-state
    mutators on another plane (e.g. kith_sim_set_actor_*). The plane
    contracts define the forbidden target modules in plane_rules.yaml
    under ``forbidden_writes``; the checker flags a call to
    ``kith_<target>_set_*`` from a translation unit under the source
    path pattern. This is a static AST check, not a runtime assertion.

Usage:
    check_runtime_planes.py [<root> ...]

With no arguments, scans include/ and src/ relative to the current
working directory. The committed pre-commit hook (pass_filenames: false)
invokes with no arguments; it runs only when .c/.h files are staged.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any


try:
    import yaml
except ImportError:
    yaml = None

try:
    from clang.cindex import CursorKind, Index, TranslationUnit
except ImportError:  # pragma: no cover - the hook provisions clang2
    Index = None
    TranslationUnit = None
    CursorKind = None

INCLUDE_RE = re.compile(r'^\s*#\s*include\s+"([^"]+)"')

RULES_PATH = Path(__file__).parent / "plane_rules.yaml"

# Default roots scanned when no arguments are given.
DEFAULT_ROOTS = ["include", "src"]


def load_rules(path: Path) -> dict[str, Any]:
    """Load plane rules from YAML. Returns a dict with keys:
    'planes', 'forbidden_includes', 'forbidden_writes'.
    """
    if yaml is None:
        print(
            "error: pyyaml is required to parse plane rules; install with: pip install pyyaml",
            file=sys.stderr,
        )
        sys.exit(2)
    with path.open("r", encoding="utf-8") as f:
        loaded: dict[str, Any] = yaml.safe_load(f)
        return loaded


def source_module_for(src_rel: str) -> str:
    """Map a source-relative path to its module name.

    include/kith/net/conn.c -> net
    src/net/conn.c -> net
    net/conn.c -> net
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


def scan_root(
    root: Path, planes: dict[str, Any], forbidden_includes: dict[str, Any]
) -> list[tuple[str, int, str, str]]:
    """Scan one root. Returns [(src_rel, line_no, include_path, reason)]."""
    violations: list[tuple[str, int, str, str]] = []
    files = sorted(list(root.rglob("*.c")) + list(root.rglob("*.h")))

    for file_path in files:
        src_rel = file_path.relative_to(root).as_posix()
        if src_rel.startswith(("third_party/", "cmake/")):
            continue

        src_mod = source_module_for(src_rel)
        src_plane = planes.get(src_mod)
        if src_plane is None or src_plane in ("shared", "client", "composition"):
            continue

        forbidden = forbidden_includes.get(src_mod, [])
        if not forbidden:
            continue

        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line_no, line in enumerate(f, start=1):
                m = INCLUDE_RE.match(line)
                if not m:
                    continue
                include_path = m.group(1)
                inc_mod = include_module_for(include_path)
                if inc_mod is None:
                    continue

                if inc_mod in forbidden:
                    inc_plane = planes.get(inc_mod, "unknown")
                    reason = (
                        f"{src_mod} ({src_plane} plane) includes "
                        f"{inc_mod} ({inc_plane} plane) — "
                        f"forbidden cross-plane dependency"
                    )
                    violations.append((f"{src_rel}:{line_no}", 0, include_path, reason))

    return violations


# ---------------------------------------------------------------------------
# data-flow (writes) checker
# ---------------------------------------------------------------------------


def _system_include_args() -> list[str]:
    """Return the toolchain's system include directory arguments."""
    import subprocess

    out = subprocess.run(
        ["clang", "-E", "-v", "-xc", "/dev/null", "-o", "/dev/null"],
        capture_output=True,
        text=True,
        check=False,
    )
    args: list[str] = []
    capture = False
    for line in out.stderr.splitlines():
        if "search starts here" in line:
            capture = True
            continue
        if "End of search" in line:
            capture = False
            continue
        if capture and line.strip():
            args.append(f"-I{line.strip()}")
    return args


def _callee_name(call_cursor: Any) -> str:
    """Return the function name a CALL_EXPR invokes, or "" if unresolvable."""
    ref = call_cursor.referenced
    if ref is not None and ref.spelling:
        return str(ref.spelling)
    # Fall back to the identifier token preceding the opening paren.
    tokens = [t.spelling for t in call_cursor.get_tokens()]
    if tokens:
        return str(tokens[0])
    return ""


def _mutator_prefix_regex(targets: list[str]) -> re.Pattern[str]:
    """Match ``kith_<target>_set_`` for any forbidden target module."""
    alternation = "|".join(re.escape(t) for t in targets if t)
    return re.compile(rf"^kith_(?:{alternation})_set_")


def scan_forbidden_writes(
    src_root: Path, forbidden_writes: dict[str, Any], include_root: Path
) -> list[tuple[str, int, str, str]]:
    """Flag cross-plane mutator calls from forbidden source translation units.

    ``forbidden_writes`` maps a source path pattern (e.g. ``gateway/view``) to
    a list of target modules whose mutators the source must not call. A
    translation unit under ``src/<pattern>/`` calling ``kith_<target>_set_*``
    is flagged.
    """
    if Index is None or not forbidden_writes:
        return []

    parse_args = ["-std=c23", f"-I{include_root}", f"-I{src_root}", *_system_include_args()]
    index = Index.create()
    violations: list[tuple[str, int, str, str]] = []

    for pattern, targets in forbidden_writes.items():
        if not isinstance(targets, list) or not targets:
            continue
        tu_dir = src_root / pattern
        if not tu_dir.is_dir():
            continue
        mutator_re = _mutator_prefix_regex(targets)
        target_list = ", ".join(targets)
        for src_file in sorted(tu_dir.rglob("*.c")):
            src_rel = src_file.relative_to(src_root).as_posix()
            tu = index.parse(
                str(src_file),
                args=parse_args,
                options=TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD,
            )
            for diag in tu.diagnostics:
                if diag.severity >= 3:
                    print(f"warning: {src_rel}: {diag.spelling}", file=sys.stderr)
            for cursor in tu.cursor.walk_preorder():
                if cursor.kind != CursorKind.CALL_EXPR:
                    continue
                callee = _callee_name(cursor)
                if callee and mutator_re.match(callee):
                    line = cursor.location.line if cursor.location else 0
                    reason = (
                        f"gateway/view translation unit calls {callee} "
                        f"(a {target_list} plane mutator) — "
                        f"forbidden cross-plane write"
                    )
                    violations.append((f"{src_rel}:{line}", 0, callee, reason))
    return violations


def main(argv: list[str]) -> int:
    rules = load_rules(RULES_PATH)
    planes = rules.get("planes", {})
    forbidden_includes = rules.get("forbidden_includes", {})
    forbidden_writes = rules.get("forbidden_writes", {})

    roots: list[Path] = []
    if len(argv) > 1:
        for arg in argv[1:]:
            root = Path(arg)
            if not root.exists() or not root.is_dir():
                print(f"error: invalid root: {root}", file=sys.stderr)
                return 2
            roots.append(root)
    else:
        for name in DEFAULT_ROOTS:
            root = Path(name)
            if root.is_dir():
                roots.append(root)

    if not roots:
        print(
            "error: no source roots to scan (no include/ or src/ found)",
            file=sys.stderr,
        )
        return 2

    all_violations: list[tuple[str, int, str, str]] = []
    for root in roots:
        all_violations.extend(scan_root(root, planes, forbidden_includes))

    # The data-flow (writes) check scans src/ against the include root. When
    # src/ is one of the scanned roots, reuse it; otherwise resolve it from
    # the working directory.
    src_root = Path("src")
    if src_root.is_dir():
        include_root = Path("include") if Path("include").is_dir() else src_root.parent / "include"
        all_violations.extend(scan_forbidden_writes(src_root, forbidden_writes, include_root))

    if all_violations:
        print("Runtime-plane invariant violations detected:")
        for src_loc, _, include_path, reason in all_violations:
            print(f"  {src_loc} -> {include_path}")
            print(f"    {reason}")
        print(f"Total violations: {len(all_violations)}")
        return 1

    print("OK: no runtime-plane invariant violations detected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

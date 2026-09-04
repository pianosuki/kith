#!/usr/bin/env python3
"""Stdlib-only import enforcement for the shared replay codec modules.

examples/_common/replay_format.py and examples/_common/replay_recorder.py
are imported by example composition roots and by tools/replay.py, which run
with PYTHONPATH=python:examples where the repo root and the kith package may
be absent. Their documented contract is standard-library imports plus
sibling imports within examples._common; this checker enforces that contract
mechanically so an accidental ``kith`` or third-party import fails the gate
instead of surfacing as an ImportError in a bare checkout.

Usage:
    check_replay_codec_imports.py

Scans examples/_common/replay_*.py relative to the repository root. Imports
guarded by ``if TYPE_CHECKING:`` are exempt: they exist only for type hints
and never execute.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Iterator
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
_TARGET_GLOB = "examples/_common/replay_*.py"
_SIBLING_PREFIX = "examples._common"

# Top-level import roots the codec modules may use, pruned to actual usage:
# a new dependency is a deliberate edit to this set, not silent drift.
# Sibling modules under examples._common are handled separately.
_ALLOWED_ROOTS = frozenset(
    {
        "__future__",
        "collections",
        "dataclasses",
        "json",
        "pathlib",
        "struct",
        "threading",
        "typing",
    }
)


def _is_type_checking(test: ast.expr) -> bool:
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    if isinstance(test, ast.Attribute):
        return test.attr == "TYPE_CHECKING"
    return False


def _collect_imports(body: list[ast.stmt]) -> Iterator[ast.Import | ast.ImportFrom]:
    for stmt in body:
        if isinstance(stmt, ast.If) and _is_type_checking(stmt.test):
            continue
        if isinstance(stmt, ast.Import | ast.ImportFrom):
            yield stmt
            continue
        if isinstance(stmt, ast.If | ast.For | ast.AsyncFor | ast.While):
            yield from _collect_imports(stmt.body)
            yield from _collect_imports(stmt.orelse)
        elif isinstance(stmt, ast.With | ast.AsyncWith):
            yield from _collect_imports(stmt.body)
        elif isinstance(stmt, ast.Try):
            yield from _collect_imports(stmt.body)
            for handler in stmt.handlers:
                yield from _collect_imports(handler.body)
            yield from _collect_imports(stmt.orelse)
            yield from _collect_imports(stmt.finalbody)
        elif isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            yield from _collect_imports(stmt.body)


def _check_dotted(dotted: str, lineno: int, rel_path: str, violations: list[str]) -> None:
    root = dotted.split(".")[0]
    if root in _ALLOWED_ROOTS:
        return
    if root == "examples" and dotted.startswith(_SIBLING_PREFIX + "."):
        return
    violations.append(
        f"{rel_path}:{lineno}: import of {dotted!r} is outside the stdlib "
        f"allowlist and the {_SIBLING_PREFIX} sibling prefix"
    )


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(f"usage: {argv[0]}", file=sys.stderr)
        return 2

    targets = sorted(_REPO_ROOT.glob(_TARGET_GLOB))
    if not targets:
        print(f"check_replay_codec_imports: no modules matched {_TARGET_GLOB}", file=sys.stderr)
        return 2

    violations: list[str] = []
    for path in targets:
        rel_path = str(path.relative_to(_REPO_ROOT))
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for stmt in _collect_imports(tree.body):
            if isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    _check_dotted(alias.name, stmt.lineno, rel_path, violations)
            elif stmt.level == 0 and stmt.module is not None:
                # level > 0 is a relative import between siblings: allowed.
                _check_dotted(stmt.module, stmt.lineno, rel_path, violations)

    if violations:
        print("Replay codec import violations:")
        for violation in violations:
            print(f"  {violation}")
        print(f"Total violations: {len(violations)}")
        return 1

    print(f"OK: {len(targets)} replay codec modules import stdlib + examples._common only.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

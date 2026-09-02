#!/usr/bin/env python3
"""Check generated Python ctypes bindings for drift.

Regenerates the bindings into a temporary directory using ``gen_ctypes.py``
and diffs the result against the checked-in ``python/kith/_generated/``
files. Fails (exit 1) if they differ, indicating that a header change was
not accompanied by a regeneration of its bindings.

This eliminates hand-mirror drift (silent memory corruption from a missed
struct-field update): the checked-in bindings are always exactly what the
generator produces from the current headers, or the commit fails.

Usage:
    check_ctypes_drift.py [--include-root <dir>] [--generated <dir>]

    --include-root  public headers root (default: include)
    --generated     checked-in generated bindings directory
                    (default: python/kith/_generated)
"""

from __future__ import annotations

import argparse
import filecmp
import sys
import tempfile
from pathlib import Path


def _diff_dirs(expected: Path, actual: Path) -> list[str]:
    """Return a list of relative paths that differ between expected and actual."""
    diffs: list[str] = []
    expected_files = {p.relative_to(expected) for p in expected.rglob("*.py")}
    actual_files = {p.relative_to(actual) for p in actual.rglob("*.py")}

    for rel in sorted(expected_files | actual_files):
        exp = expected / rel
        act = actual / rel
        if not exp.is_file():
            diffs.append(f"+ {rel} (generated but not checked in)")
        elif not act.is_file():
            diffs.append(f"- {rel} (checked in but not generated)")
        elif not filecmp.cmp(exp, act, shallow=False):
            diffs.append(f"~ {rel} (content differs)")

    return diffs


def check_drift(include_root: Path, generated: Path) -> int:
    """Return 0 when bindings are current, 1 on drift, 2 on setup error."""
    kith_root = include_root / "kith"
    if not kith_root.is_dir():
        print(f"error: no public headers under {kith_root}", file=sys.stderr)
        return 2

    if not generated.is_dir():
        print(
            f"error: no generated bindings directory at {generated}; "
            "run `python3 tools/gen_ctypes.py` to create it.",
            file=sys.stderr,
        )
        return 2

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    try:
        import gen_ctypes
    except ImportError as exc:
        print(f"error: could not import gen_ctypes: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="kith-ctypes-drift-") as tmp:
        tmp_out = Path(tmp) / "_generated"
        try:
            gen_ctypes.generate(include_root, tmp_out)
        except Exception as exc:
            print(f"error: generation failed: {exc}", file=sys.stderr)
            return 2

        diffs = _diff_dirs(generated, tmp_out)

    if not diffs:
        print("OK: generated ctypes bindings are current.", file=sys.stderr)
        return 0

    print("FAIL: generated ctypes bindings are stale.", file=sys.stderr)
    print(
        "  A header changed but python/kith/_generated/ was not regenerated.",
        file=sys.stderr,
    )
    print("  Run `python3 tools/gen_ctypes.py` and commit the result.", file=sys.stderr)
    print("", file=sys.stderr)
    for d in diffs:
        print(f"  {d}", file=sys.stderr)
    return 1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Check generated ctypes bindings for drift.")
    parser.add_argument(
        "--include-root",
        type=Path,
        default=Path("include"),
        help="public headers root (default: include)",
    )
    parser.add_argument(
        "--generated",
        type=Path,
        default=Path("python/kith/_generated"),
        help="checked-in generated bindings directory",
    )
    args = parser.parse_args(argv[1:])

    repo_root = Path(__file__).resolve().parents[1]
    include_root = args.include_root
    if not include_root.is_absolute():
        include_root = repo_root / include_root
    generated = args.generated
    if not generated.is_absolute():
        generated = repo_root / generated

    return check_drift(include_root, generated)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

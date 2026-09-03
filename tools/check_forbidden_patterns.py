#!/usr/bin/env python3
"""Identifier scanner for the kith framework.

Scans .c, .h, and .py files for game-specific terms, and scans those
files plus the .md prose surfaces for additional identifiers supplied
through the KITH_FORBIDDEN_PATTERNS_LOCAL environment variable. Also
flags #define'd numeric tunables in include/kith/*.h public headers.

Rule sources:

  1. tools/game_terms.txt (committed) — game-specific terms, enforced
     only in src/ and include/ (not examples/, where game vocabulary
     is expected).
  2. KITH_FORBIDDEN_PATTERNS_LOCAL (optional) — path to an additional
     one-identifier-per-line file, applied to every scanned file and
     to file names and paths.

Checks performed:
  - Identifier scan (word-boundary): fails if any identifier from the
    active rule sources appears in text, file names, or paths.
    Word-boundary matching prevents false positives (a forbidden foo
    matches, foobar does not).
  - #define'd tunables in public headers (heuristic): flags #define
    followed by a numeric literal in include/kith/*.h, excluding
    version, visibility, and ABI macros. Tunables belong in config
    structs, not headers.

Usage:
    check_forbidden_patterns.py <file> [<file> ...]

The committed pre-commit hook passes changed filenames (pass_filenames:
true) and excludes tools/fixtures/.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path


HERE = Path(__file__).parent

# ---------------------------------------------------------------------------
# rule loading
# ---------------------------------------------------------------------------


def load_identifiers(path: Path) -> list[str]:
    """Load one-identifier-per-line rules from a file. Lines starting
    with # are comments; blank lines are skipped."""
    if not path.exists():
        return []
    terms: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            term = line.strip()
            if not term or term.startswith("#"):
                continue
            terms.append(term)
    return terms


def load_game_terms() -> list[str]:
    """Load the committed game-term list."""
    return load_identifiers(HERE / "game_terms.txt")


GAME_TERMS: list[str] = load_game_terms()


def load_local_identifiers() -> list[str]:
    """Load additional identifiers from the file the
    KITH_FORBIDDEN_PATTERNS_LOCAL environment variable points at. The
    variable unset or the file missing yields an empty list."""
    local_path = os.environ.get("KITH_FORBIDDEN_PATTERNS_LOCAL")
    if not local_path:
        return []
    return load_identifiers(Path(local_path))


def merge_identifiers(*groups: list[str]) -> list[str]:
    """Concatenate identifier groups, dropping duplicates in order."""
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for ident in group:
            if ident not in seen:
                seen.add(ident)
                merged.append(ident)
    return merged


def is_game_terms_file(file_rel: str) -> bool:
    """Game terms are enforced only in src/ and include/."""
    return file_rel.startswith(("src/", "include/"))


# ---------------------------------------------------------------------------
# forbidden identifier scan
# ---------------------------------------------------------------------------


def scan_identifiers(
    file_path: Path, identifiers: list[str], root: Path
) -> list[tuple[int, str, str]]:
    """Scan a file for forbidden identifiers in all text (code, comments,
    strings). Returns [(line_no, matched, identifier), ...]."""
    try:
        file_rel = file_path.resolve().relative_to(root).as_posix()
    except ValueError:
        file_rel = file_path.as_posix()

    # Check file name and path for forbidden identifiers.
    path_violations: list[tuple[int, str, str]] = []
    for ident in identifiers:
        if re.search(rf"\b{re.escape(ident)}\b", file_rel):
            path_violations.append((0, file_rel, ident))

    if not file_path.exists() or file_path.suffix not in (".c", ".h", ".py", ".md"):
        return path_violations

    text = file_path.read_text(encoding="utf-8", errors="ignore")
    violations: list[tuple[int, str, str]] = path_violations

    for ident in identifiers:
        pattern = re.compile(rf"\b{re.escape(ident)}\b")
        for line_no, line in enumerate(text.splitlines(), start=1):
            for m in pattern.finditer(line):
                violations.append((line_no, m.group(), ident))

    return violations


# ---------------------------------------------------------------------------
# #define tunables in public headers
# ---------------------------------------------------------------------------

# Macros excluded from the tunable check: visibility, version, ABI, and
# deprecation markers. Tunables belong in config structs, not headers.
TUNABLE_EXCLUDES: set[str] = {
    "KITH_API",
    "KITH_LOCAL",
    "KITH_ABI_VERSION",
    "KITH_DEPRECATED",
}

DEFINE_TUNABLE_RE = re.compile(
    r"^\s*#\s*define\s+([A-Z_][A-Z0-9_]*)\s+"
    r"(?:0x[0-9A-Fa-f]+|\d+[uUlL]*|-\d+)",
)


def scan_define_tunables(file_path: Path, root: Path) -> list[tuple[int, str, str]]:
    """Flag #define'd numeric tunables in include/kith/*.h public headers,
    excluding version/visibility/ABI macros."""
    try:
        file_rel = file_path.resolve().relative_to(root).as_posix()
    except ValueError:
        file_rel = file_path.as_posix()

    if not file_rel.startswith("include/kith/"):
        return []
    if file_path.suffix != ".h":
        return []

    violations: list[tuple[int, str, str]] = []
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    for line_no, line in enumerate(text.splitlines(), start=1):
        m = DEFINE_TUNABLE_RE.match(line)
        if not m:
            continue
        macro_name = m.group(1)
        if macro_name in TUNABLE_EXCLUDES:
            continue
        # Version macros (e.g. KITH_VERSION_MAJOR) are allowed.
        if "VERSION" in macro_name:
            continue
        violations.append((line_no, macro_name, "tunable #define in public header"))
    return violations


# ---------------------------------------------------------------------------
# exceptions
# ---------------------------------------------------------------------------

# Key: (file_rel, identifier_lower), value: reason + removal target.
FORBIDDEN_EXCEPTIONS: dict[tuple[str, str], str] = {}


def is_exception(file_rel: str, identifier: str) -> bool:
    return (file_rel, identifier.lower()) in FORBIDDEN_EXCEPTIONS


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <file> [<file> ...]", file=sys.stderr)
        return 2

    root = Path.cwd()
    local_identifiers = load_local_identifiers()

    all_violations: list[tuple[str, int, str, str]] = []

    for file_arg in argv[1:]:
        file_path = Path(file_arg)
        if not file_path.exists():
            print(f"error: invalid file: {file_path}", file=sys.stderr)
            return 2

        try:
            file_rel = file_path.resolve().relative_to(root).as_posix()
        except ValueError:
            file_rel = file_path.as_posix()

        # Game terms stay confined to src/ and include/; the local
        # identifiers apply everywhere the scanner runs, prose included.
        game_terms = GAME_TERMS if is_game_terms_file(file_rel) else []
        identifiers = merge_identifiers(game_terms, local_identifiers)

        # Identifier scan
        for line_no, matched, ident in scan_identifiers(file_path, identifiers, root):
            if is_exception(file_rel, ident):
                continue
            if line_no == 0:
                all_violations.append(
                    (file_rel, 0, matched, f"forbidden identifier in path: {ident}")
                )
            else:
                all_violations.append(
                    (file_rel, line_no, matched, f"forbidden identifier: {ident}")
                )

        # #define tunables
        for line_no, macro_name, _ in scan_define_tunables(file_path, root):
            if is_exception(file_rel, macro_name):
                continue
            all_violations.append(
                (file_rel, line_no, macro_name, "tunable #define in public header")
            )

    if all_violations:
        print("Forbidden patterns detected:")
        for file_rel, line_no, matched, description in all_violations:
            if line_no == 0:
                print(f"  {file_rel}: {description}")
                print(f"    match: {matched}")
            else:
                print(f"  {file_rel}:{line_no}: {description}")
                print(f"    match: {matched}")
        print(f"Total violations: {len(all_violations)}")
        return 1

    print("OK: no forbidden patterns detected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

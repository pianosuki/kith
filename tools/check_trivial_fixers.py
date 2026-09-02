#!/usr/bin/env python3
"""Trivial-format verifier for the kith framework.

Checks the four whitespace/line-ending invariants that the fixer hooks of a
typical pre-commit setup would correct automatically, but in verify-only
mode so a gate never mutates the working tree:

  - trailing whitespace on any line
  - file does not end with exactly one newline
  - UTF-8 BOM at start of file
  - line endings other than LF (CRLF or bare CR)

Binary files (detected by a NUL byte in the first 8 KiB) are skipped. On any
violation the script prints the file and the failing invariant, then exits
non-zero with a hint to run the fix command.

Usage:
    check_trivial_fixers.py <file> [<file> ...]

Exits 0 when every input file passes, 1 when any file violates an invariant.
"""

from __future__ import annotations

import sys
from pathlib import Path


_READ_CHUNK = 8192


def _is_binary(data: bytes) -> bool:
    """Heuristic: a NUL byte in the leading chunk indicates a binary file."""
    return b"\x00" in data[:_READ_CHUNK]


def _check_file(path: Path) -> list[str]:
    """Return a list of invariant-violation messages for path, or []."""
    raw = path.read_bytes()
    if not raw:
        return []
    if _is_binary(raw):
        return []

    violations: list[str] = []
    text = raw.decode("utf-8", errors="surrogateescape")

    # Mixed line endings: any CRLF or bare CR fails the LF-only contract.
    if b"\r\n" in raw or b"\r" in raw.replace(b"\r\n", b""):
        violations.append("non-LF line ending (CRLF or bare CR present)")

    # Trailing whitespace: a line ending in space or tab before the newline.
    # Split keeps the line bodies; the final fragment (after the last \n) is
    # checked too because it has no following newline to hide behind.
    for line in text.splitlines():
        if line and line[-1] in (" ", "\t"):
            violations.append(f"trailing whitespace: {line!r}")
            break

    # End of file: exactly one trailing newline. No newline, or more than
    # one (blank trailing line), both fail.
    if not raw.endswith(b"\n"):
        violations.append("file does not end with a newline")
    elif raw.endswith(b"\n\n"):
        violations.append("file ends with a blank line")

    # UTF-8 BOM at start: the fixer strips it; presence fails the check.
    if raw.startswith(b"\xef\xbb\xbf"):
        violations.append("UTF-8 BOM at start of file")

    return violations


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: check_trivial_fixers.py <file> [<file> ...]", file=sys.stderr)
        return 2

    any_failed = False
    for arg in argv:
        path = Path(arg)
        if not path.is_file():
            print(f"error: invalid file: {path}", file=sys.stderr)
            return 2
        violations = _check_file(path)
        if violations:
            any_failed = True
            for msg in violations:
                print(f"{path}: {msg}")

    if any_failed:
        print(
            "\ntrivial-format violations found. Run "
            "'cmake --build --target format' or 'scripts/format.sh' to fix.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

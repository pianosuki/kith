#!/usr/bin/env python3
"""Changelog version-lockstep verifier for the kith framework.

One rule: the newest released section heading in CHANGELOG.md must equal
the project version declared on CMakeLists.txt's project(kith VERSION ...)
line. That line is the version source of truth — pyproject reads it by
regex, the header macro is held to it by the version test, and the Python
binding constants are generated from the header — so this one check closes
the lockstep chain.

A heading that is not a dotted triple (an "Unreleased" section) is
skipped: the rule binds the newest released version, whatever sits above
it.

Usage:
    check_changelog_version.py [--changelog PATH] [--cmake PATH]

Exits 0 when the heading matches the project version, 1 on a mismatch, 2
when an input is missing or malformed.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CHANGELOG = _REPO_ROOT / "CHANGELOG.md"
_DEFAULT_CMAKE = _REPO_ROOT / "CMakeLists.txt"

_PROJECT_VERSION_RE = re.compile(r"project\(\s*kith\s+VERSION\s+(\d+\.\d+\.\d+)")
_RELEASE_HEADING_RE = re.compile(r"^##\s+\[(\d+\.\d+\.\d+)\]", re.MULTILINE)


def _project_version(cmake_path: Path) -> str | None:
    """The version from the project() line, or None when absent."""
    match = _PROJECT_VERSION_RE.search(cmake_path.read_text(encoding="utf-8"))
    return match.group(1) if match else None


def _newest_release_heading(changelog_path: Path) -> str | None:
    """The newest semver-shaped section heading, or None when absent."""
    match = _RELEASE_HEADING_RE.search(changelog_path.read_text(encoding="utf-8"))
    return match.group(1) if match else None


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--changelog", type=Path, default=_DEFAULT_CHANGELOG, help="path to CHANGELOG.md"
    )
    parser.add_argument("--cmake", type=Path, default=_DEFAULT_CMAKE, help="path to CMakeLists.txt")
    args = parser.parse_args(argv)

    for path, what in ((args.changelog, "changelog"), (args.cmake, "CMakeLists")):
        if not path.is_file():
            print(f"error: {what} not found: {path}", file=sys.stderr)
            return 2

    version = _project_version(args.cmake)
    if version is None:
        print(f"error: no project(kith VERSION ...) line in {args.cmake}", file=sys.stderr)
        return 2

    released = _newest_release_heading(args.changelog)
    if released is None:
        print(f"error: no '## [x.y.z]' release heading in {args.changelog}", file=sys.stderr)
        return 2

    if released != version:
        print(
            f"{args.changelog}: newest release heading [{released}] != "
            f"project version {version} ({args.cmake})"
        )
        print(
            "\nchangelog version mismatch: align the newest '## [x.y.z]' "
            "heading with the project version.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

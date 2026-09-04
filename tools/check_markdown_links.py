#!/usr/bin/env python3
"""Markdown relative-link checker for the kith repository.

One rule: every markdown link that points inside the repository must
resolve. Inline links and images, reference-style definitions and
usages, and cross-file fragments are checked. Absolute URLs (http,
https, mailto) are out of scope: they are never fetched and never
validated.

Relative targets resolve against the linking file's directory. A target
ending in "/" must name an existing directory; any other target must
name an existing file or directory. A "#fragment" on a markdown target
must match a heading in that file, slugged the way the repository host
renders anchors (ATX headings; lowercase; punctuation dropped; spaces
replaced with hyphens; repeated headings suffixed -1, -2, ...). A
target starting with "/" resolves against a site root the repository
does not control, so it is always a finding.

Code fences and inline code spans are stripped before scanning: a
link-shaped string inside a code example is prose, not a link.

Usage:
    check_markdown_links.py [--root PATH]

Exits 0 when every link resolves, 1 with one file:line finding per
broken link on stdout, 2 when the root is unusable.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import urllib.parse
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parent.parent

_FENCE_RE = re.compile(r"^\s{0,3}(?:```|~~~)")
_CODE_SPAN_RE = re.compile(r"`[^`]*`")
_INLINE_LINK_RE = re.compile(r"!?\[([^\]]*)\]\(\s*(?:<([^<>]*)>|([^)\s]+))(?:\s+\"[^\"]*\")?\s*\)")
_REF_DEF_RE = re.compile(
    r"^\s{0,3}!?\[([^\]]+)\]:\s*(?:<([^<>]*)>|([^)\s]+))(?:\s+\"[^\"]*\")?\s*$"
)
_REF_USE_RE = re.compile(r"\[([^\]]+)\]\[([^\]]+)\]")
_SHORTCUT_USE_RE = re.compile(r"\[([^\]]+)\]")
_ATX_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")

_SCHEMES = ("http://", "https://", "mailto:")


def _clean_line(line: str) -> str:
    """The line with inline code spans blanked, so their contents never scan."""
    return _CODE_SPAN_RE.sub(" ", line)


def _slug(text: str) -> str:
    """The anchor slug for a heading's text, per the host's rendering."""
    cleaned = re.sub(r"[^\w\s-]", "", text.strip().lower())
    return cleaned.replace(" ", "-")


def _heading_slugs(text: str) -> set[str]:
    """Every heading slug a file exposes, duplicates suffixed -1, -2, ...."""
    slugs: list[str] = []
    seen: dict[str, int] = {}
    in_fence = False
    for line in text.splitlines():
        if _FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = _ATX_HEADING_RE.match(_clean_line(line))
        if match is None:
            continue
        base = _slug(match.group(2))
        count = seen.get(base, 0)
        seen[base] = count + 1
        slugs.append(base if count == 0 else f"{base}-{count}")
    return set(slugs)


class _LinkScanner:
    """Extracts the markdown link targets of one file with their line numbers."""

    def __init__(self, text: str) -> None:
        self._text = text

    def targets(self) -> list[tuple[int, str]]:
        """(line number, target) for every link the file makes, in file order."""
        lines: list[tuple[int, str]] = []
        in_fence = False
        for number, raw in enumerate(self._text.splitlines(), start=1):
            if _FENCE_RE.match(raw):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            lines.append((number, _clean_line(raw)))

        # Definitions are file-scoped: a usage resolves wherever the
        # definition sits, so they are collected before any usage scans.
        definitions: dict[str, str] = {}
        for _, line in lines:
            for match in _REF_DEF_RE.finditer(line):
                label, target = match.group(1).lower(), match.group(2) or match.group(3)
                definitions.setdefault(label, target)

        found: list[tuple[int, str]] = []
        for number, line in lines:
            for match in _INLINE_LINK_RE.finditer(line):
                target = match.group(2) if match.group(2) is not None else match.group(3)
                if target is not None:
                    found.append((number, target))
            used_spans = [match.span() for match in _REF_USE_RE.finditer(line)]
            for match in _REF_USE_RE.finditer(line):
                label = match.group(2).lower()
                if label in definitions:
                    found.append((number, definitions[label]))
            for match in _SHORTCUT_USE_RE.finditer(line):
                label = match.group(1).lower()
                start, end = match.span()
                if any(
                    start < used_end and end > used_start for used_start, used_end in used_spans
                ):
                    continue
                tail = line[end : end + 1]
                if tail in ("(", "[") or label.startswith("^"):
                    continue
                if label in definitions:
                    found.append((number, definitions[label]))
        return found


class _LinkResolver:
    """Resolves link targets against the repository, caching heading sets."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._headings: dict[Path, set[str]] = {}

    def reason(self, file: Path, target: str) -> str | None:
        """None when the target resolves, else the one-line reason it does not."""
        if target.startswith(_SCHEMES):
            return None
        if target.startswith("/"):
            return "resolves against a site root, not the repository"
        path, _, fragment = target.partition("#")
        path = urllib.parse.unquote(path)
        if path == "":
            if fragment and fragment not in self._file_headings(file):
                return f"missing heading #{fragment}"
            return None
        resolved = file.parent / path
        if path.endswith("/"):
            if not resolved.is_dir():
                return "no such directory"
            return None
        if not resolved.exists():
            return "no such file"
        if fragment and resolved.suffix == ".md" and fragment not in self._file_headings(resolved):
            return f"missing heading #{fragment} in {path}"
        return None

    def _file_headings(self, file: Path) -> set[str]:
        if file not in self._headings:
            self._headings[file] = _heading_slugs(file.read_text(encoding="utf-8"))
        return self._headings[file]


def _markdown_files(root: Path) -> list[Path]:
    """The markdown files to check: git-tracked when the root is a checkout."""
    if (root / ".git").exists():
        listed = subprocess.run(
            ["git", "-C", str(root), "ls-files", "*.md"],
            capture_output=True,
            text=True,
            check=False,
        )
        if listed.returncode == 0:
            return [root / name for name in listed.stdout.split()]
    return sorted(path for path in root.rglob("*.md") if ".git" not in path.parts)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=_REPO_ROOT,
        help="repository root to scan (default: this checkout)",
    )
    args = parser.parse_args(argv)
    if not args.root.is_dir():
        print(f"error: root not found: {args.root}", file=sys.stderr)
        return 2

    resolver = _LinkResolver(args.root)
    broken = 0
    for file in _markdown_files(args.root):
        for number, target in _LinkScanner(file.read_text(encoding="utf-8")).targets():
            reason = resolver.reason(file, target)
            if reason is not None:
                relative = file.relative_to(args.root)
                print(f"{relative}:{number}: {target} — {reason}")
                broken += 1
    if broken:
        print(
            f"\n{broken} broken markdown link{'s' if broken != 1 else ''}: fix the "
            "target paths relative to the linking file's directory.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

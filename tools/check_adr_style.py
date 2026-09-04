#!/usr/bin/env python3
"""Architecture-record house style enforcer.

Scans the architecture decision records under docs/architecture/adr/ for
mechanical house-style violations (AGENTS.md §7.2):

  - Filename shape: NNNN-lowercase-slug.md; the number matches the title.
  - Title: first heading is `# ADR-NNNN: <title>`.
  - Status: exactly one `**Status:**` line whose value is exactly
    `Accepted`. (After v1.0.0 a superseded record reads `Superseded by
    ADR-NNNN`; the vocabulary extends when the first supersession lands.)
  - Skeleton: a record carries `## Context`, `## Decision`, and
    `## Consequences`, in that order, and no other H2 sections.
  - No citation trailers (trailing `Refs` lines) and no blockquotes:
    cross-references live inline in the prose.
  - Voice: no first- or second-person pronouns.
  - Vocabulary: no plan, roadmap, workstream, sprint, or milestone
    terms, no internal-workstream identifiers, and no aspirational
    markers.
  - No code coupling: no code identifiers (kith-prefixed names, macros,
    snake_case, CamelCase, CLI flag spellings). A contract's concrete
    surface is owned by its public header; records point at owning
    headers and docs by path (spans containing `/` are paths and are
    exempt), and the decision-content tokens in the allowlist — names
    that are themselves the decision — stay.
  - No client/actor/session/instance counts: gate language uses class
    and operating-point phrasing. Digit-adjacent counts are flagged;
    spelled-out counts ("hundreds of instances") are a documented gap
    left to review, as are non-digit measurers generally.

Comparable length across substantive records is a review concern, not a
mechanical one, and is deliberately not checked here.

Usage:
    check_adr_style.py <file> [<file> ...]

The committed pre-commit hook passes changed filenames (pass_filenames:
true); verify.sh runs pre-commit with --all-files, so every invocation
checks the whole corpus passed to it.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


_TITLE_RE = re.compile(r"^# ADR-(\d{4}): \S.*$")
_FILENAME_RE = re.compile(r"^(\d{4})-[a-z0-9][a-z0-9.-]*\.md$")
_STATUS_RE = re.compile(r"^\*\*Status:\*\* (.+)$")
_H2_RE = re.compile(r"^## (.+)$")
_SKELETON = ("Context", "Decision", "Consequences")
_CODE_SPAN_RE = re.compile(r"`[^`]*`")

_PRONOUN_RE = re.compile(
    r"\b(i|we|our|ours|ourselves|us|you|your|yours|yourself|"
    r"let's|let us)\b",
    re.IGNORECASE,
)
_BANNED_RE = re.compile(
    r"\b(plan|roadmap|workstream|sprint|milestone|todo|fixme)\b"
    r"|\b07i\b|\bgolden-history\b",
    re.IGNORECASE,
)
_TRAILER_RE = re.compile(r"^Refs\b")
_BLOCKQUOTE_RE = re.compile(r"^>")

# Decision-content tokens: the name is itself the decision, so the token
# stays while every other code identifier moves to the owning header.
_SNAKE_ALLOWLIST = frozenset(
    {
        "io_uring",
        "update_seq",
        "move_missing_ratio",
        "move_missing_ratio_certified",
        "kith_control_response_overflow_total",
        "kith_gateway_broadcast_refused_total",
        "kith_gateway_broadcast_dropped_total",
    }
)
_UPPER_ALLOWLIST = frozenset({"FLT_EVAL_METHOD"})
_CAMEL_ALLOWLIST = frozenset({"OpenTelemetry"})
_FLAG_ALLOWLIST = frozenset({"-ffp-contract=off"})

# Identifier shapes, longest-family first. Mixed-case snake tails
# (PyErr_CheckSignals-class) get their own rule because neither the
# snake nor the CamelCase rule spans the underscore boundary.
_SNAKE_RE = re.compile(r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b")
_UPPER_SNAKE_RE = re.compile(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b")
_CAMEL_RE = re.compile(r"\b[A-Z][a-z]+(?:[A-Z][a-z0-9]*)+\b")
_MIXED_SNAKE_RE = re.compile(r"\b[a-zA-Z0-9]*[a-z]_[A-Z][A-Za-z0-9]*\b")
_FLAG_RE = re.compile(r"(?:^|[\s(\"`])--?[a-zA-Z][a-zA-Z0-9-]*(?:=\S*)?")

# Counts: client/actor/session/instance counts and profile names
# are measurement garnish; gate language is class + operating point.
_PROFILE_COUNT_RE = re.compile(r"\b(?:distributed|dense)-\d+\b")
_ACTOR_COUNT_RE = re.compile(
    r"\b\d+\s+(?:(?:distributed|dense|concurrent|single)\s+)?"
    r"(?:clients?|actors?|sessions?|instances?)\b"
)
_THREAD_COUNT_RE = re.compile(r"\b\d+\s+(?:threads?|workers?)\b")

# Founding voice: no self-demotion. Conformance framing names the
# founding set; "additive per/under ADR-NNNN" reads as a junior addition.
_SELF_DEMOTION_RE = re.compile(r"\badditive (?:per|under) ADR-\d{4}\b|\bis an additive component\b")


def _token_surface(line: str) -> str:
    """Line with path-like code spans blanked, other spans left intact.

    Paths (spans containing `/`) are the owning-header pointers the house
    style allows; identifiers inside other spans are still checked.
    """
    return _CODE_SPAN_RE.sub(lambda match: " " if "/" in match.group(0) else match.group(0), line)


def _token_violations(line: str) -> list[tuple[str, str]]:
    """Returns (matched, description) for code-coupling hits on a line."""
    surface = _token_surface(line)
    violations: list[tuple[str, str]] = []
    for regex, allowlist, description in (
        (_SNAKE_RE, _SNAKE_ALLOWLIST, "snake_case code identifier"),
        (
            _UPPER_SNAKE_RE,
            _UPPER_ALLOWLIST,
            "upper-snake code identifier",
        ),
        (_CAMEL_RE, _CAMEL_ALLOWLIST, "CamelCase code identifier"),
        (_MIXED_SNAKE_RE, frozenset(), "mixed-case code identifier"),
        (_FLAG_RE, _FLAG_ALLOWLIST, "CLI flag spelling"),
    ):
        for match in regex.finditer(surface):
            token = match.group(0).strip().strip("`")
            if token not in allowlist:
                violations.append((token, description))
    return violations


def _count_violations(line: str) -> list[tuple[str, str]]:
    """Returns (matched, description) for count hits on a line."""
    violations: list[tuple[str, str]] = []
    for regex, description in (
        (_PROFILE_COUNT_RE, "profile-name count"),
        (_ACTOR_COUNT_RE, "client/actor/session/instance count"),
        (_THREAD_COUNT_RE, "thread/worker count"),
    ):
        for match in regex.finditer(line):
            violations.append((match.group(0), description))
    return violations


def check_text(text: str, number: str) -> list[tuple[int, str, str]]:
    """Check one record's text; returns (line_no, matched, description)."""
    violations: list[tuple[int, str, str]] = []
    lines = text.splitlines()

    title_seen = False
    status_line: str | None = None
    status_no = 0
    h2_seen: list[tuple[int, str]] = []

    for line_no, line in enumerate(lines, start=1):
        if not title_seen:
            title = _TITLE_RE.match(line)
            if title:
                title_seen = True
                if title.group(1) != number:
                    violations.append((line_no, line, "title number differs from filename"))
                continue
            if line.startswith("#"):
                violations.append((line_no, line, "first heading is not `# ADR-NNNN: title`"))
                title_seen = True
                continue

        status = _STATUS_RE.match(line)
        if status:
            if status_line is not None:
                violations.append((line_no, line, "duplicate **Status:** line"))
            else:
                status_line = status.group(1)
                status_no = line_no
            continue

        h2 = _H2_RE.match(line)
        if h2:
            h2_seen.append((line_no, h2.group(1).strip()))

    if not title_seen:
        violations.append((0, "", "missing `# ADR-NNNN: title` heading"))

    if status_line is None:
        violations.append((0, "", "missing `**Status:**` line"))
    elif status_line != "Accepted":
        violations.append((status_no, status_line, "status is not exactly `Accepted`"))

    names = [name for _, name in h2_seen]
    for expected in _SKELETON:
        if expected not in names:
            violations.append((0, expected, f"missing `## {expected}` section"))
    order = [names.index(name) for name in _SKELETON if name in names]
    if order != sorted(order):
        violations.append((0, ", ".join(_SKELETON), "skeleton sections out of order"))
    for line_no, name in h2_seen:
        if name not in _SKELETON:
            violations.append((line_no, name, "H2 section outside the house skeleton"))

    for line_no, line in enumerate(lines, start=1):
        if _TRAILER_RE.match(line):
            violations.append((line_no, line, "trailing citation trailer; cite inline"))
        if _BLOCKQUOTE_RE.match(line):
            violations.append((line_no, line, "blockquote; amendments are inline prose"))
        # Inline code spans quote identifiers, not prose; the voice and
        # vocabulary rules do not apply inside them.
        prose = _CODE_SPAN_RE.sub("``", line)
        # "I/O" is the abbreviation, not the pronoun.
        prose = re.sub(r"\bi/o\b", "io", prose, flags=re.IGNORECASE)
        hit = _PRONOUN_RE.search(prose)
        if hit:
            violations.append((line_no, hit.group(0), "first/second-person pronoun"))
        hit = _BANNED_RE.search(prose)
        if hit:
            violations.append((line_no, hit.group(0), "forbidden vocabulary"))
        for matched, description in _token_violations(line):
            violations.append((line_no, matched, description))
        for matched, description in _count_violations(line):
            violations.append((line_no, matched, description))
        hit = _SELF_DEMOTION_RE.search(prose)
        if hit:
            violations.append((line_no, hit.group(0), "self-demotion phrasing"))

    return violations


def check_file(path: Path) -> list[tuple[int, str, str]]:
    """Check one ADR file; returns (line_no, matched, description)."""
    if not _FILENAME_RE.match(path.name) or path.stem.endswith("."):
        return [(0, path.name, "filename is not NNNN-lowercase-slug.md")]
    number = path.name[:4]
    return check_text(path.read_text(encoding="utf-8"), number)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <file> [<file> ...]", file=sys.stderr)
        return 2

    all_violations: list[tuple[str, int, str, str]] = []
    for file_arg in argv[1:]:
        file_path = Path(file_arg)
        if not file_path.is_file():
            print(f"error: invalid file: {file_path}", file=sys.stderr)
            return 2
        for line_no, matched, description in check_file(file_path):
            all_violations.append((file_path.as_posix(), line_no, matched, description))

    if all_violations:
        print("Architecture-record house style violations detected:")
        for file_rel, line_no, matched, description in all_violations:
            location = f"{file_rel}:{line_no}" if line_no else f"{file_rel}:"
            print(f"  {location}: {description}: '{matched}'")
        print(f"Total violations: {len(all_violations)}")
        return 1

    print("OK: no architecture-record house style violations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

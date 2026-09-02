#!/usr/bin/env python3
"""Comment philosophy enforcer for the kith framework.

Scans .c, .h, and .py files for forbidden comment patterns, plus the
#-comment surfaces of YAML and CMake files (.yml/.yaml/.cmake and
CMakeLists.txt). Shell scripts stay review-enforced: their heredocs
and quoting make mechanical comment extraction unsound. The
implementation is regex word-boundary, comment-scoped, dictionary-file-
driven with an exceptions allowlist. Comment text is extracted via a
single-pass state machine that skips string literals, char literals,
and identifiers — so forbidden tokens in code or strings are never
flagged; Python docstrings are additionally extracted from the parse
tree and checked with the doc-comment vocabulary.

The forbidden vocabulary lives in the literal lists below, not in this
docstring, so the file can live under the floor it enforces. The
categories:

  - Aspirational markers and scheduling language
  - First- and second-person pronouns
  - Modal verbs in implementation comments (allowed in doc comments,
    where contract prose legitimately hedges)
  - Emoji and non-ASCII decorative characters
  - Authorship markers
  - Issue-tracker references
  - Structural divider shape: the rule-title-rule sandwich, one form
    per file, uniform rule width, dash-only fills, lowercase titles
  - Record citations outside docs/ and the record-subject files

One aspirational token doubles as a doc-comment tag prefix; the ban
matches the bare word only, never its at-prefixed tag form, so a
format-defined tag never reads as a violation.

Game-specific terms are loaded from tools/game_terms.txt and enforced
only in files under src/ or include/ (not examples/). Hash-comment
surfaces carry no doc-comment split (every # line is an implementation
comment) and no divider check (the structural forms §1.7 defines are C
and Python only).

Allowed:
  - Doxygen doc comments (/** ... */) with the tag vocabulary the
    doc-comment format defines (@param, @return, @note, @warning,
    @thread_safety, @ownership)
  - Implementation-detail comments explaining non-obvious behavior
  - NOLINT, NOLINTNEXTLINE (clang-tidy suppression)

Exceptions: TEMP_EXCEPTIONS dict for legitimate one-off cases, each with
a comment and removal target. Starts empty.

Usage:
    check_comments.py <file> [<file> ...]

The committed pre-commit hook passes changed filenames (pass_filenames:
true) and excludes tools/fixtures/.
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path


# ---------------------------------------------------------------------------
# forbidden tokens
# ---------------------------------------------------------------------------


@dataclass
class ForbiddenToken:
    """A forbidden comment pattern.

    Attributes:
        pattern: Compiled regex (word-boundary where applicable).
        description: Human-readable label for violation messages.
        skip_in_doc: If True, do not check this token in doc comments
            (/** ... */). Used for modal verbs allowed in contract docs.
    """

    pattern: re.Pattern[str]
    description: str
    skip_in_doc: bool = False


def _word(pattern: str, ignorecase: bool = True) -> re.Pattern[str]:
    flags = re.IGNORECASE if ignorecase else 0
    return re.compile(rf"\b{pattern}\b", flags)


FORBIDDEN_TOKENS: list[ForbiddenToken] = [
    # Aspirational language
    ForbiddenToken(_word("TODO"), "aspirational TODO"),
    ForbiddenToken(_word("FIXME"), "aspirational FIXME"),
    ForbiddenToken(_word("HACK"), "aspirational HACK"),
    # The bare word is noise; the at-prefixed form is the doc-comment
    # tag, not the noise marker.
    ForbiddenToken(_word(r"(?<!@)note"), "aspirational NOTE"),
    ForbiddenToken(_word("XXX"), "aspirational XXX"),
    ForbiddenToken(_word("TEMP"), "aspirational TEMP"),
    ForbiddenToken(_word("PLACEHOLDER"), "aspirational PLACEHOLDER"),
    ForbiddenToken(_word("WIP"), "aspirational WIP"),
    ForbiddenToken(_word("someday"), "aspirational someday"),
    ForbiddenToken(_word("eventually"), "aspirational eventually"),
    ForbiddenToken(_word("in the future"), "aspirational 'in the future'"),
    ForbiddenToken(_word("later"), "aspirational later"),
    ForbiddenToken(_word("for now"), "aspirational 'for now'"),
    # First/second person
    ForbiddenToken(_word(r"I(?!/)", ignorecase=False), "first-person I"),
    ForbiddenToken(_word("we"), "first-person we"),
    ForbiddenToken(_word("you"), "second-person you"),
    ForbiddenToken(_word(r"let['’]s"), "first-person let's"),
    ForbiddenToken(_word("let me"), "first-person 'let me'"),
    # Modal verbs (implementation comments only; allowed in doc comments)
    ForbiddenToken(_word("should"), "modal should", skip_in_doc=True),
    ForbiddenToken(_word("would"), "modal would", skip_in_doc=True),
    ForbiddenToken(_word("could"), "modal could", skip_in_doc=True),
    # Authorship markers
    ForbiddenToken(re.compile(r"@author\b"), "authorship @author"),
    ForbiddenToken(re.compile(r"@date\b"), "authorship @date"),
    ForbiddenToken(re.compile(r"@since\b"), "authorship @since"),
    ForbiddenToken(_word("Written by"), "authorship 'Written by'"),
    # Issue-tracker references
    ForbiddenToken(_word(r"JIRA-\w+"), "ticket JIRA-"),
    ForbiddenToken(_word(r"GH-\d+"), "ticket GH-"),
    ForbiddenToken(re.compile(r"#\d+\b"), "ticket #N"),
    ForbiddenToken(_word("ticket"), "ticket reference"),
]

# ---------------------------------------------------------------------------
# emoji and decorative non-ASCII characters
# ---------------------------------------------------------------------------

ZERO_WIDTH_CHARS: set[int] = {0x200B, 0x200C, 0x200D, 0x2060, 0xFEFF}

EMOJI_RANGES: list[tuple[int, int]] = [
    (0x1F600, 0x1F64F),
    (0x1F300, 0x1F5FF),
    (0x1F680, 0x1F6FF),
    (0x1F1E0, 0x1F1FF),
    (0x1F900, 0x1F9FF),
    (0x1FA00, 0x1FA6F),
    (0x1FA70, 0x1FAFF),
    (0x2600, 0x26FF),
    (0x2700, 0x27BF),
]


def is_decorative(char: str) -> bool:
    cp = ord(char)
    if cp in ZERO_WIDTH_CHARS:
        return True
    return any(lo <= cp <= hi for lo, hi in EMOJI_RANGES)


# ---------------------------------------------------------------------------
# exceptions allowlist
# ---------------------------------------------------------------------------

# Key: (file_rel, matched_text_lower), value: reason + removal target.
# Starts empty. Add entries for legitimate one-off cases only.
TEMP_EXCEPTIONS: dict[tuple[str, str], str] = {}

# ---------------------------------------------------------------------------
# game-specific terms
# ---------------------------------------------------------------------------

GAME_TERMS_PATH = Path(__file__).parent / "game_terms.txt"


def load_game_terms() -> list[re.Pattern[str]]:
    """Load game-specific terms from game_terms.txt as word-boundary
    patterns (case-insensitive)."""
    if not GAME_TERMS_PATH.exists():
        return []
    terms: list[re.Pattern[str]] = []
    with GAME_TERMS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            term = line.strip()
            if not term or term.startswith("#"):
                continue
            terms.append(re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE))
    return terms


GAME_TERMS = load_game_terms()


def is_game_terms_file(file_rel: str) -> bool:
    """Game terms are enforced only in src/ and include/."""
    return file_rel.startswith(("src/", "include/"))


# ---------------------------------------------------------------------------
# record citations
# ---------------------------------------------------------------------------

# The repo law scopes record citations to docs/, the records file, the
# checkers that validate the records, and their fixtures; code comments
# and docstrings restate the binding fact instead. The token below is
# the record-id shape: the prefix, a dash, four digits, case-insensitive,
# word-bounded (a longer digit run is not a record id).
RECORD_CITATION = re.compile(r"\bADR-\d{4}\b", re.IGNORECASE)

# Files whose subject is the records themselves. Exact repo-relative
# paths; the record-validating checkers may name record ids in their own
# comments, and the style checker's fixtures carry record-shaped text as
# its test inputs.
RECORD_SUBJECT_FILES: frozenset[str] = frozenset(
    {
        "tools/check_adr_style.py",
        "tools/check_comments.py",
        "tests/python/test_check_adr_style.py",
    }
)


def is_record_subject_file(file_rel: str) -> bool:
    """Record citations are enforced outside docs/ and the record-subject
    files."""
    return file_rel.startswith("docs/") or file_rel in RECORD_SUBJECT_FILES


# ---------------------------------------------------------------------------
# comment extraction
# ---------------------------------------------------------------------------


def _char_literal_end(text: str, i: int, n: int) -> int:
    """Index just past a char literal opened at ``i``, or -1 when the
    quote is an apostrophe: a char quote with no closing quote on its
    line. Newline-terminated, so C23 digit separators (1'000'000'000)
    leave an odd quote count that cannot dangle the scan across lines
    and swallow subsequent comments. Mirrors elide_strings.
    """
    j = i + 1
    while j < n and text[j] != "\n":
        if text[j] == "\\" and j + 1 < n and text[j + 1] != "\n":
            j += 2
            continue
        if text[j] == "'":
            return j + 1
        j += 1
    return -1


def extract_c_comments(text: str) -> list[tuple[int, str, bool]]:
    """Extract comments from C source text.

    Skips string literals and char literals. Returns
    [(line_no, comment_text, is_doc), ...] where is_doc is True for
    /** ... */ doc comments and False for // and /* ... */ comments.
    The literal scans are newline-terminated and a char quote with no
    closing quote on its line reads as an apostrophe, so C23 digit
    separators (1'000'000'000) cannot dangle the scan across lines and
    swallow subsequent comments.
    """
    comments: list[tuple[int, str, bool]] = []
    i = 0
    line_no = 1
    n = len(text)

    while i < n:
        if text[i] == '"':
            i += 1
            closed = False
            while i < n and not closed:
                if text[i] == "\\" and i + 1 < n:
                    if text[i + 1] == "\n":
                        line_no += 1
                    i += 2
                elif text[i] == "\n":
                    line_no += 1
                    i += 1
                    break
                elif text[i] == '"':
                    i += 1
                    closed = True
                else:
                    i += 1
        elif text[i] == "'":
            end = _char_literal_end(text, i, n)
            if end < 0:
                i += 1
            else:
                i = end
        elif text[i : i + 2] == "//":
            start_line = line_no
            start = i + 2
            i += 2
            while i < n and text[i] != "\n":
                i += 1
            comments.append((start_line, text[start:i], False))
        elif text[i : i + 2] == "/*":
            start_line = line_no
            is_doc = text[i : i + 3] == "/**" and text[i : i + 4] != "/**/"
            start = i + 3 if is_doc else i + 2
            i += 2
            while i < n and text[i : i + 2] != "*/":
                if text[i] == "\n":
                    line_no += 1
                i += 1
            comments.append((start_line, text[start:i], is_doc))
            i += 2
        elif text[i] == "\n":
            line_no += 1
            i += 1
        else:
            i += 1

    return comments


def extract_py_comments(text: str) -> list[tuple[int, str, bool]]:
    """Extract comments and docstrings from Python source text.

    ``#`` comments come from the single-pass scan (which skips string
    literals, so tokens in code or ordinary strings are never flagged).
    Docstrings come from the parse tree: the first statement of a module,
    class, or function is the file-level header or contract prose, and it
    is checked with the doc-comment vocabulary (``is_doc=True``, matching
    the C doc-comment split). A file that does not parse yields comments
    only — the floor never fails open on a broken file, it just narrows.
    Returns [(line_no, comment_text, is_doc), ...].
    """
    comments: list[tuple[int, str, bool]] = []
    i = 0
    line_no = 1
    n = len(text)

    while i < n:
        if text[i : i + 3] in ('"""', "'''"):
            quote = text[i : i + 3]
            i += 3
            while i < n and text[i : i + 3] != quote:
                if text[i] == "\n":
                    line_no += 1
                i += 1
            i += 3
        elif text[i] in ('"', "'"):
            quote = text[i]
            i += 1
            while i < n and text[i] != quote:
                if text[i] == "\\" and i + 1 < n:
                    i += 2
                else:
                    if text[i] == "\n":
                        line_no += 1
                    i += 1
            i += 1
        elif text[i] == "#":
            start_line = line_no
            start = i
            while i < n and text[i] != "\n":
                i += 1
            comments.append((start_line, text[start:i], False))
        elif text[i] == "\n":
            line_no += 1
            i += 1
        else:
            i += 1

    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError, RecursionError) as _exc:
        return comments
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = getattr(node, "body", [])
        if not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            comments.append((first.lineno, first.value.value, True))
    return sorted(comments, key=lambda entry: entry[0])


# ---------------------------------------------------------------------------
# hash-comment surfaces (YAML, CMake)
# ---------------------------------------------------------------------------

# The comment style system scopes its taxonomy to C, Python, shell, and
# CMake/config files by analogy; the mechanical floor extends to the
# surfaces where extraction is sound: line-oriented # comments with no
# heredocs and no nested quoting dialects. Shell scripts stay
# review-enforced — their heredocs and quoting make mechanical comment
# extraction unsound.
HASH_COMMENT_SUFFIXES = frozenset({".yml", ".yaml", ".cmake"})


def is_hash_surface(file_path: Path) -> bool:
    """YAML and CMake files carry # comments the checker scans."""
    return file_path.suffix in HASH_COMMENT_SUFFIXES or file_path.name == "CMakeLists.txt"


def extract_hash_comments(text: str) -> list[tuple[int, str, bool]]:
    """Extract # comments from a YAML or CMake file.

    A comment opens at line start (except the #! shebang) or at a
    whitespace-preceded #; quoting is not tracked, so a quoted " #"
    sequence reads as a comment opener. Returns
    [(line_no, comment_text, False), ...] — hash surfaces carry no doc
    comment split and no divider convention (§1.7 defines structural
    forms for C and Python only).
    """
    comments: list[tuple[int, str, bool]] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#") and not stripped.startswith("#!"):
            comments.append((line_no, stripped, False))
            continue
        opener = line.find(" #")
        if opener != -1:
            comment = line[opener + 1 :].strip()
            if comment:
                comments.append((line_no, comment, False))
    return comments


def extract_comments(
    file_path: Path,
) -> list[tuple[int, str, bool]]:
    """Extract comments from a .c, .h, or .py file."""
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    if file_path.suffix in (".c", ".h"):
        return extract_c_comments(text)
    if file_path.suffix == ".py":
        return extract_py_comments(text)
    return []


# ---------------------------------------------------------------------------
# structural dividers
# ---------------------------------------------------------------------------

# A divider rule line is a comment marker, an optional space, a dashes-only
# fill, and for the C star form the closing token. The three-dash minimum
# bounds the grammar: prose like "x ---- y" never parses as a rule.
_RULE_FILL = r"-{3,}"
_SHORT_FILL = r"-{2,}"

_PY_RULE_LINE = re.compile(rf"^#[ \t]*{_RULE_FILL}[ \t]*$")
_C_SLASH_RULE_LINE = re.compile(rf"^// ?{_RULE_FILL}[ \t]*$")
_C_STAR_OPEN_LINE = re.compile(rf"^/\*{_RULE_FILL}[ \t]*$")
_C_STAR_CLOSE_LINE = re.compile(rf"^\*[ \t]*{_RULE_FILL}\*/[ \t]*$")

# A one-line divider carries the title inside the rule itself: a dash run,
# a label, a dash run (the short-label form), or a pure fill one-liner.
_FLANKED_LABEL = re.compile(rf"^{_SHORT_FILL}[^-].*{_SHORT_FILL}$")
_PURE_FILL = re.compile(rf"^{_RULE_FILL}$")

# Rule-shaped lines whose fill is not all dashes: the dash is the only
# canonical fill metal.
_PY_BANNER_LINE = re.compile(r"^#[ \t]*[-=*_~]{3,}[ \t]*$")
_C_SLASH_BANNER_LINE = re.compile(r"^// ?[-=*_~]{3,}[ \t]*$")
_C_STAR_OPEN_BANNER_LINE = re.compile(r"^/\*[-=*_~]{3,}[ \t]*$")
_C_STAR_CLOSE_BANNER_LINE = re.compile(r"^\*[ \t]*[-=*_~]{3,}\*/[ \t]*$")


def elide_strings(text: str) -> str:
    """Blank string- and char-literal contents, keeping line geometry.

    Comment regions survive so the divider scan sees comment text. The
    literal scans are newline-terminated (an unescaped newline ends an
    open literal) and a char quote with no closing quote on its line
    reads as an apostrophe, so C23 digit separators (1'000'000'000)
    cannot dangle the scan across lines. Mirrors extract_c_comments.
    """
    out = list(text)
    i = 0
    n = len(text)
    while i < n:
        if text[i : i + 2] == "//":
            while i < n and text[i] != "\n":
                i += 1
        elif text[i : i + 2] == "/*":
            i += 2
            while i < n and text[i : i + 2] != "*/":
                i += 1
            i = min(i + 2, n)
        elif text[i] == '"':
            i += 1
            while i < n and text[i] not in ('"', "\n"):
                if text[i] == "\\" and i + 1 < n and text[i + 1] != "\n":
                    out[i] = " "
                    out[i + 1] = " "
                    i += 2
                else:
                    out[i] = " "
                    i += 1
            i = i + 1 if i < n and text[i] == '"' else i
        elif text[i] == "'":
            end = _char_literal_end(text, i, n)
            if end < 0:
                i += 1
            else:
                for k in range(i + 1, end - 1):
                    out[k] = " "
                i = end
        else:
            i += 1
    return "".join(out)


def _classify_divider_lines(lines: list[str], is_c: bool) -> list[tuple[str, str]]:
    """Classify elided source lines for the divider scan.

    Kinds: "rule" (a divider rule line, opener or closer), "single" (a
    one-line divider), "banner" (rule-shaped, non-dash fill), "content"
    (a whole-line comment that is neither), and "code" (everything else,
    blank lines included). C block comments carry state across lines: a
    comment that opens with a rule line is a divider candidate, and its
    ``*/`` line closes the block; a comment that opens with prose is an
    ordinary comment and its ``*/`` line is content, never a rule.
    """
    kinds: list[tuple[str, str]] = []
    in_star = False
    star_rule = False
    for raw in lines:
        rstripped = raw.rstrip()
        s = rstripped.strip()
        if is_c and in_star:
            if "*/" in s:
                if _C_STAR_CLOSE_LINE.match(s) and star_rule:
                    kinds.append(("rule", rstripped))
                else:
                    kinds.append(("content", rstripped))
                in_star = False
                star_rule = False
            else:
                kinds.append(("content", rstripped))
        elif is_c and s.startswith("/*"):
            if s.endswith("*/"):
                inner = s[2:-2].strip()
                if _PURE_FILL.match(inner) or _FLANKED_LABEL.match(inner):
                    kinds.append(("single", rstripped))
                else:
                    kinds.append(("code", rstripped))
            elif _C_STAR_OPEN_LINE.match(s):
                kinds.append(("rule", rstripped))
                in_star = True
                star_rule = True
            elif _C_STAR_OPEN_BANNER_LINE.match(s):
                kinds.append(("banner", rstripped))
                in_star = True
                star_rule = False
            else:
                kinds.append(("content", rstripped))
                in_star = True
                star_rule = False
        elif is_c and s.startswith("//"):
            if _C_SLASH_RULE_LINE.match(s):
                kinds.append(("rule", rstripped))
            elif _C_SLASH_BANNER_LINE.match(s):
                kinds.append(("banner", rstripped))
            elif _FLANKED_LABEL.match(s[2:].strip()):
                kinds.append(("single", rstripped))
            else:
                kinds.append(("content", rstripped))
        elif not is_c and s.startswith("#"):
            if _PY_RULE_LINE.match(s):
                kinds.append(("rule", rstripped))
            elif _PY_BANNER_LINE.match(s):
                kinds.append(("banner", rstripped))
            elif _FLANKED_LABEL.match(s[1:].strip()):
                kinds.append(("single", rstripped))
            else:
                kinds.append(("content", rstripped))
        else:
            kinds.append(("code", rstripped))
    return kinds


def _strip_comment_prefix(line: str, is_c: bool) -> str:
    """Strip the comment marker from a divider content line."""
    stripped = line.strip()
    if is_c:
        return stripped[2:].strip() if stripped.startswith("//") else stripped[1:].strip()
    return stripped[1:].strip()


def _check_divider_title(title: str, line_no: int) -> list[tuple[int, str, str]]:
    """Check one divider title: lowercase label form, no trailing period.

    Embedded identifiers and initialisms keep their casing (AGENTS.md
    1.7): a first token whose alphabetic content is all uppercase (two
    or more letters) or that carries an uppercase letter past its first
    position is exempt from the lowercase-start rule.
    """
    if not title:
        return []
    violations: list[tuple[int, str, str]] = []
    words = title.split()
    token = words[0] if words else ""
    letters = [c for c in token if c.isalpha()]
    exempt = (len(letters) >= 2 and all(c.isupper() for c in letters)) or any(
        c.isupper() for c in token[1:]
    )
    if not exempt and token[:1].isupper():
        violations.append((line_no, title, "divider title must start lowercase"))
    if title.endswith("."):
        violations.append((line_no, title, "divider title must not end with a period"))
    return violations


def check_dividers(text: str, is_c: bool) -> list[tuple[int, str, str]]:
    """Check the structural-divider shape of one file (AGENTS.md 1.7).

    The divider is a rule-title-rule sandwich: one divider form per file,
    uniform rule-line width, dash-only fills, lowercase noun-phrase
    titles with no trailing period. Body prose inside a banner is legal;
    the first non-empty content line is the title.
    """
    lines = elide_strings(text).split("\n")
    kinds = _classify_divider_lines(lines, is_c)
    violations: list[tuple[int, str, str]] = []
    total = len(kinds)

    first_width = -1
    forms: dict[str, int] = {}
    for idx, (kind, line) in enumerate(kinds):
        if kind != "rule":
            continue
        width = len(line)
        if first_width < 0:
            first_width = width
        elif width != first_width:
            violations.append((idx + 1, line.strip(), "divider rule width differs within file"))
        form = (
            "slash"
            if line.lstrip().startswith("//")
            else ("py" if line.lstrip().startswith("#") else "star")
        )
        if form not in forms:
            forms[form] = idx
    if len(forms) > 1:
        later = max(forms.values())
        violations.append((later + 1, kinds[later][1].strip(), "divider forms mixed within file"))

    i = 0
    while i < total:
        kind, line = kinds[i]
        if kind == "single":
            violations.append((i + 1, line.strip(), "divider must be the three-line block form"))
            i += 1
            continue
        if kind == "banner":
            violations.append((i + 1, line.strip(), "divider banner must use dashes"))
            i += 1
            continue
        if kind != "rule":
            i += 1
            continue
        prev = kinds[i - 1][0] if i else "code"
        j = i + 1
        while j < total and kinds[j][0] == "content":
            j += 1
        closes = j < total and kinds[j][0] == "rule"
        if not closes:
            if prev != "code":
                violations.append((i + 1, line.strip(), "divider rule line without a title block"))
            else:
                violations.append((i + 1, line.strip(), "divider opener without a rule closer"))
            i = j
            continue
        title_idx = next(
            (c for c in range(i + 1, j) if _strip_comment_prefix(kinds[c][1], is_c)),
            None,
        )
        if title_idx is None:
            violations.append((i + 1, line.strip(), "divider without a title"))
        else:
            violations.extend(
                _check_divider_title(
                    _strip_comment_prefix(kinds[title_idx][1], is_c),
                    title_idx + 1,
                )
            )
        i = j + 1
    return violations


# ---------------------------------------------------------------------------
# checking
# ---------------------------------------------------------------------------


def check_comment(
    comment_text: str,
    is_doc: bool,
    file_rel: str,
) -> list[tuple[str, str]]:
    """Check one comment for forbidden tokens. Returns
    [(matched_text, description), ...]."""
    violations: list[tuple[str, str]] = []

    for token in FORBIDDEN_TOKENS:
        if is_doc and token.skip_in_doc:
            continue
        for m in token.pattern.finditer(comment_text):
            matched = m.group()
            key = (file_rel, matched.lower())
            if key in TEMP_EXCEPTIONS:
                continue
            violations.append((matched, token.description))

    # Emoji and decorative characters
    for char in comment_text:
        if is_decorative(char):
            key = (file_rel, char)
            if key in TEMP_EXCEPTIONS:
                continue
            violations.append((char, f"emoji/decorative U+{ord(char):04X}"))

    # Game-specific terms (only in src/ and include/)
    if is_game_terms_file(file_rel):
        for pattern in GAME_TERMS:
            for m in pattern.finditer(comment_text):
                matched = m.group()
                key = (file_rel, matched.lower())
                if key in TEMP_EXCEPTIONS:
                    continue
                violations.append((matched, "game-specific term"))

    # Record citations (outside docs/ and the record-subject files)
    if not is_record_subject_file(file_rel):
        for m in RECORD_CITATION.finditer(comment_text):
            matched = m.group()
            key = (file_rel, matched.lower())
            if key in TEMP_EXCEPTIONS:
                continue
            violations.append((matched, "record citation"))

    return violations


def check_file(file_path: Path, root: Path) -> list[tuple[int, str, str]]:
    """Check one file. Returns [(line_no, matched_text, description), ...]."""
    try:
        file_rel = file_path.resolve().relative_to(root).as_posix()
    except ValueError:
        file_rel = file_path.as_posix()
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    comments: list[tuple[int, str, bool]] = []
    is_c = file_path.suffix in (".c", ".h")
    if is_hash_surface(file_path):
        comments = extract_hash_comments(text)
    elif is_c:
        comments = extract_c_comments(text)
    elif file_path.suffix == ".py":
        comments = extract_py_comments(text)
    violations: list[tuple[int, str, str]] = []
    if is_c or file_path.suffix == ".py":
        violations = check_dividers(text, is_c)
    for line_no, comment_text, is_doc in comments:
        for matched, description in check_comment(comment_text, is_doc, file_rel):
            violations.append((line_no, matched, description))
    return violations


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <file> [<file> ...]", file=sys.stderr)
        return 2

    # Root for relative-path computation: the current working directory
    # (pre-commit runs from the repo root).
    root = Path.cwd()

    all_violations: list[tuple[str, int, str, str]] = []

    for file_arg in argv[1:]:
        file_path = Path(file_arg)
        if not file_path.exists() or not file_path.is_file():
            print(f"error: invalid file: {file_path}", file=sys.stderr)
            return 2
        if file_path.suffix not in (".c", ".h", ".py") and not is_hash_surface(file_path):
            continue
        for line_no, matched, description in check_file(file_path, root):
            all_violations.append((file_path.as_posix(), line_no, matched, description))

    if all_violations:
        print("Comment philosophy violations detected:")
        for file_rel, line_no, matched, description in all_violations:
            print(f"  {file_rel}:{line_no}: {description}: '{matched}'")
        print(f"Total violations: {len(all_violations)}")
        return 1

    print("OK: no comment philosophy violations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

"""Self-test for tools/check_comments.py.

Drives the comment philosophy checker against fixture texts covering
the token bans and the doc-comment tag vocabulary: the at-prefixed
@note tag passes in every comment type, the bare word stays banned in
every comment type, modal verbs split on the doc/implementation
boundary, the remaining token classes hold as pinned regressions,
record citations stay scoped to docs/ and the record-subject files,
docstrings are scanned through the parse tree, and the hash-comment
surfaces (YAML, CMake) run the same battery with no divider scan.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

import tools.check_comments as checker


def _c_violations(text: str) -> list[tuple[str, str]]:
    """Run the token checks over every comment in a fixture C text."""
    out: list[tuple[str, str]] = []
    for _, comment, is_doc in checker.extract_c_comments(text):
        out.extend(checker.check_comment(comment, is_doc, "sample.c"))
    return out


def _py_violations(text: str) -> list[tuple[str, str]]:
    """Run the token checks over every comment in a fixture Python text."""
    out: list[tuple[str, str]] = []
    for _, comment, is_doc in checker.extract_py_comments(text):
        out.extend(checker.check_comment(comment, is_doc, "sample.py"))
    return out


def test_at_note_tag_passes_in_doc_comment() -> None:
    text = "/** Closes the handle. @note the lock is held across the call. */"
    assert _c_violations(text) == []


def test_at_note_tag_passes_in_implementation_comment() -> None:
    # The exemption keys on the at-prefix, not the comment type: a tag
    # mention in an implementation comment is not the noise marker.
    text = "/* mirrors the @note tag on the public contract */"
    assert _c_violations(text) == []


def test_at_notes_is_not_the_tag() -> None:
    # Word boundaries keep the exemption exact: the plural form is not
    # the tag, and the ban never matched it either.
    text = "// the @notes column holds the tag names"
    assert _c_violations(text) == []


def test_bare_note_banned_in_line_comment() -> None:
    text = "// NOTE: temporary fixture wiring"
    assert _c_violations(text) == [("NOTE", "aspirational NOTE")]


def test_bare_note_banned_in_doc_comment() -> None:
    # The tag exemption is not a doc-comment exemption: the bare word
    # stays banned where the throat-clearing opener lives too.
    text = "/** Note that the buffer is reused across calls. */"
    assert _c_violations(text) == [("Note", "aspirational NOTE")]


def test_bare_note_banned_in_python_comment() -> None:
    text = "# note to self: rework the ladder"
    assert _py_violations(text) == [("note", "aspirational NOTE")]


def test_at_note_tag_passes_in_python_comment() -> None:
    text = "# mirrors the @note tag on the C contract"
    assert _py_violations(text) == []


def test_warning_tag_passes() -> None:
    text = "/** Publishes the delta. @warning not reentrant during rebind. */"
    assert _c_violations(text) == []


def test_modal_verbs_split_on_doc_boundary() -> None:
    impl = "// the caller should revalidate before the next pass"
    assert ("should", "modal should") in _c_violations(impl)
    doc = "/** Acquires the lock. The caller should hold it across the call. */"
    assert _c_violations(doc) == []


def test_core_token_regressions_hold() -> None:
    assert ("TODO", "aspirational TODO") in _c_violations("// TODO tighten the bounds")
    assert ("we", "first-person we") in _c_violations("// we buffer one frame")
    assert ("@author", "authorship @author") in _c_violations("/* @author someone */")
    assert _c_violations("grow(pools); /* NOLINT(readability-magic-numbers) */") == []


def test_check_file_reports_line_numbers() -> None:
    source = (
        "/** Closes the handle. @note the lock is held across the call. */\n"
        "\n"
        "// NOTE: stale fixture wiring\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        sample = root / "sample.c"
        sample.write_text(source, encoding="utf-8")
        violations = checker.check_file(sample, root)
    assert len(violations) == 1
    line_no, matched, description = violations[0]
    assert line_no == 3
    assert matched == "NOTE"
    assert description == "aspirational NOTE"


# ---------------------------------------------------------------------------
# record citations
# ---------------------------------------------------------------------------


def _file_violations(rel_path: str, source: str) -> list[tuple[int, str, str]]:
    """Run the full file check over a fixture file at a repo-relative path."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        sample = root / rel_path
        sample.parent.mkdir(parents=True, exist_ok=True)
        sample.write_text(source, encoding="utf-8")
        return checker.check_file(sample, root)


def test_record_citation_flagged_in_c_comment() -> None:
    violations = _file_violations(
        "src/sample.c", "// mirrors the record contract\n// see ADR-0004\n"
    )
    assert (2, "ADR-0004", "record citation") in violations


def test_record_citation_flagged_in_py_comment() -> None:
    violations = _file_violations("python/sample.py", "# see ADR-0004\n")
    assert (1, "ADR-0004", "record citation") in violations


def test_record_citation_flagged_in_docstring() -> None:
    # The load-bearing case: docstrings reach the checker through
    # the parse tree, so a citation inside a contract docstring reaches
    # the floor.
    source = '"""The facade contract (ADR-0004)."""\n\nx = 1\n'
    violations = _file_violations("python/sample.py", source)
    assert (1, "ADR-0004", "record citation") in violations


def test_record_citation_case_insensitive() -> None:
    violations = _file_violations("python/sample.py", "# see adr-0007\n")
    assert (1, "adr-0007", "record citation") in violations


def test_record_citation_needs_four_digits() -> None:
    # Word boundaries keep the shape exact: a longer digit run is an
    # unrelated token, a shorter one is not a record id.
    text = "# ADR-12345 and ADR-123 are not citations\n"
    assert _file_violations("python/sample.py", text) == []


def test_record_citation_allowed_under_docs() -> None:
    text = "// see ADR-0004\n"
    assert _file_violations("docs/snippets/sample.c", text) == []


def test_record_citation_allowed_in_record_subject_files() -> None:
    text = "# fixture: ADR-0042 and ADR-0099\n"
    for rel in sorted(checker.RECORD_SUBJECT_FILES):
        assert _file_violations(rel, text) == []


# ---------------------------------------------------------------------------
# docstring coverage
# ---------------------------------------------------------------------------


def test_docstring_treated_as_doc_comment() -> None:
    # Modal verbs are contract prose in a docstring (is_doc=True,
    # matching the C doc-comment split) and narration in an inline
    # comment.
    source = (
        '"""Acquires the lock. The caller should hold it across the call."""\n'
        "x = 1  # the caller should revalidate\n"
    )
    violations = _file_violations("python/sample.py", source)
    assert [(line, matched) for line, matched, _ in violations] == [(2, "should")]


def test_docstring_tokens_are_scanned() -> None:
    source = '"""Closes the handle. TODO tighten the bounds."""\n'
    violations = _file_violations("python/sample.py", source)
    assert (1, "TODO", "aspirational TODO") in violations


def test_fstring_is_not_a_docstring() -> None:
    # A quoted template at the top of a function is an expression, not
    # contract prose; string literals stay outside the floor.
    source = 'def f():\n    f"""uses ADR-0004 and TODO"""\n'
    assert _file_violations("python/sample.py", source) == []


def test_syntax_error_falls_back_to_comments() -> None:
    # An unparsable file narrows the floor to the comment scan instead
    # of crashing the hook.
    source = "def broken(:\n# TODO tighten the bounds\n"
    violations = _file_violations("python/sample.py", source)
    assert (2, "TODO", "aspirational TODO") in violations


# ---------------------------------------------------------------------------
# whole-corpus scan
# ---------------------------------------------------------------------------


def test_whole_corpus_is_free_of_record_citations() -> None:
    root = Path(__file__).resolve().parents[2]
    tracked = subprocess.run(
        ["git", "ls-files", "--", "*.py", "*.c", "*.h"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert tracked, "git ls-files returned nothing; run from the checkout"
    flagged: list[str] = []
    for rel in tracked:
        if rel.startswith("tools/fixtures/"):
            continue
        for line_no, matched, description in checker.check_file(root / rel, root):
            if description == "record citation":
                flagged.append(f"{rel}:{line_no}: {matched}")
    assert flagged == []


# ---------------------------------------------------------------------------
# structural dividers
# ---------------------------------------------------------------------------


def _rule(width: int = 77, indent: str = "", marker: str = "# ") -> str:
    """A divider rule line at a total width, indent included."""
    return indent + marker + "-" * (width - len(indent) - len(marker))


def _divider_violations(rel_path: str, source: str) -> list[tuple[int, str, str]]:
    """Run the file check and keep the divider-shape findings."""
    return [v for v in _file_violations(rel_path, source) if v[2].startswith("divider")]


def test_canonical_python_divider_passes() -> None:
    rule = _rule()
    text = f"{rule}\n# public enums\n{rule}\n"
    assert _divider_violations("python/sample.py", text) == []


def test_canonical_c_star_divider_passes() -> None:
    opener = _rule(77, marker="/*")
    closer = " *" + "-" * 73 + "*/"
    text = f"{opener}\n * shared helpers\n{closer}\n"
    assert _divider_violations("src/sample.c", text) == []


def test_c_slash_divider_with_body_prose_passes() -> None:
    rule = _rule(77, marker="// ")
    text = f"{rule}\n// per-fd tracking\n// prose inside the banner\n{rule}\n"
    assert _divider_violations("src/sample.c", text) == []


def test_star_divider_tolerates_blank_comment_lines_and_prose() -> None:
    opener = _rule(77, marker="/*")
    closer = " *" + "-" * 73 + "*/"
    text = f"{opener}\n * fd lifecycle\n *\n * a deregistered fd keeps a cqe\n{closer}\n"
    assert _divider_violations("src/sample.c", text) == []


def test_python_short_label_one_liner_flagged() -> None:
    line = "    # -- lifecycle " + "-" * 40
    violations = _divider_violations("python/sample.py", line + "\n")
    assert violations == [
        (1, line.strip(), "divider must be the three-line block form"),
    ]


def test_c_star_one_liner_flagged() -> None:
    text = "/* --- params --- */\n"
    assert _divider_violations("src/sample.c", text) == [
        (1, text.rstrip(), "divider must be the three-line block form"),
    ]


def test_c_slash_one_liner_flagged() -> None:
    text = "// --- params ---\n"
    assert _divider_violations("src/sample.c", text) == [
        (1, text.rstrip(), "divider must be the three-line block form"),
    ]


def test_python_symmetric_one_liner_flagged() -> None:
    text = "# --- params ---\n"
    assert _divider_violations("python/sample.py", text) == [
        (1, text.rstrip(), "divider must be the three-line block form"),
    ]


def test_divider_title_capitalized_flagged() -> None:
    rule = _rule()
    text = f"{rule}\n# Server control\n{rule}\n"
    assert _divider_violations("python/sample.py", text) == [
        (2, "Server control", "divider title must start lowercase"),
    ]


def test_divider_title_trailing_period_flagged() -> None:
    rule = _rule()
    text = f"{rule}\n# public enums.\n{rule}\n"
    assert _divider_violations("python/sample.py", text) == [
        (2, "public enums.", "divider title must not end with a period"),
    ]


def test_identifier_led_titles_keep_casing() -> None:
    rule = _rule()
    for title in (
        "HTTP helpers",
        "EventKind values",
        "IPC (NDJSON over a unix socket)",
        "_FUNCTIONS table",
    ):
        text = f"{rule}\n# {title}\n{rule}\n"
        assert _divider_violations("python/sample.py", text) == [], title


def test_rule_width_drift_flagged() -> None:
    text = f"{_rule()}\n# enums\n{_rule()}\n\n{_rule(80)}\n# helpers\n{_rule(80)}\n"
    violations = _divider_violations("python/sample.py", text)
    assert [v[0] for v in violations] == [5, 7]
    assert [v[2] for v in violations] == ["divider rule width differs within file"] * 2


def test_mixed_c_divider_forms_flagged() -> None:
    opener = _rule(77, marker="/*")
    closer = " *" + "-" * 73 + "*/"
    slash = _rule(77, marker="// ")
    text = f"{opener}\n * one\n{closer}\n\n{slash}\n// two\n{slash}\n"
    assert _divider_violations("src/sample.c", text) == [
        (5, slash, "divider forms mixed within file"),
    ]


def test_empty_divider_flagged() -> None:
    rule = _rule()
    text = f"{rule}\n{rule}\n"
    assert _divider_violations("python/sample.py", text) == [
        (1, rule, "divider without a title"),
    ]


def test_unclosed_opener_flagged() -> None:
    rule = _rule(77, marker="// ")
    text = f"{rule}\n// title\nint x = 1;\n"
    assert _divider_violations("src/sample.c", text) == [
        (1, rule, "divider opener without a rule closer"),
    ]


def test_lone_rule_line_in_prose_run_flagged() -> None:
    rule = _rule(77, marker="// ")
    text = f"// prose above\n{rule}\n// prose below\n"
    assert _divider_violations("src/sample.c", text) == [
        (2, rule, "divider rule line without a title block"),
    ]


def test_non_dash_banner_flagged() -> None:
    line = "#" + "=" * 40
    assert _divider_violations("python/sample.py", line + "\n") == [
        (1, line, "divider banner must use dashes"),
    ]


def test_c_star_non_dash_banner_flagged() -> None:
    text = "/*================================\n*================================*/\n"
    violations = _divider_violations("src/sample.c", text)
    assert [v[2] for v in violations] == ["divider banner must use dashes"]
    assert [v[0] for v in violations] == [1]


def test_divider_shape_in_string_literal_not_flagged() -> None:
    c_text = 'const char *banner = "// --- fake ---";\n'
    py_text = 'BANNER = "# ---- fake ----"\n'
    assert _divider_violations("src/sample.c", c_text) == []
    assert _divider_violations("python/sample.py", py_text) == []


def test_digit_separators_do_not_dangle_comment_extraction() -> None:
    # The odd quote count of 1'000'000'000 keeps a naive char-literal
    # scan open across lines; the comments below must still survive extraction.
    source = "long a = 1'000'000'000; // ---- spacer\n// TODO tighten the bounds\n"
    found = checker.extract_c_comments(source)
    assert [(line_no, text, is_doc) for line_no, text, is_doc in found] == [
        (1, " ---- spacer", False),
        (2, " TODO tighten the bounds", False),
    ]


# ---------------------------------------------------------------------------
# hash-comment surfaces (YAML, CMake)
# ---------------------------------------------------------------------------


def test_hash_surface_citation_flagged_in_yaml() -> None:
    violations = _file_violations(
        ".github/workflows/sample.yml", "# jobs: see ADR-0004\nname: sample\n"
    )
    assert (1, "ADR-0004", "record citation") in violations


def test_hash_surface_trailing_comment_scanned_in_cmake() -> None:
    violations = _file_violations("cmake/sample.cmake", "set(X 1) # TODO tighten the bounds\n")
    assert (1, "TODO", "aspirational TODO") in violations


def test_hash_surface_modal_flagged_in_yaml() -> None:
    # Hash comments are implementation comments, so the doc-comment
    # modal allowance does not apply.
    violations = _file_violations(
        ".github/workflows/sample.yml", "# the lane should not be skipped\n"
    )
    assert (1, "should", "modal should") in violations


def test_hash_surface_clean_file_passes_without_divider_scan() -> None:
    # The structural divider rules are C and Python forms; a "# ---" rule
    # in a YAML file is prose, not a malformed divider.
    source = (
        "# sample workflow\n"
        "#\n"
        "# ---------------------------------------------------------\n"
        "# jobs\n"
        "# ---------------------------------------------------------\n"
        "name: sample\n"
    )
    assert _file_violations(".github/workflows/sample.yml", source) == []


def test_hash_surface_cmakelists_name_is_scanned() -> None:
    # CMakeLists.txt carries no suffix; the surface check names the file.
    violations = _file_violations("CMakeLists.txt", "# TODO tighten the bounds\n")
    assert (1, "TODO", "aspirational TODO") in violations


def test_shell_stays_review_enforced() -> None:
    # Shell heredocs and quoting make mechanical comment extraction
    # unsound, so the floor extracts nothing from a shell script.
    assert _file_violations("scripts/sample.sh", "# TODO tighten the bounds\n") == []


def test_whole_corpus_divider_scan_is_clean() -> None:
    root = Path(__file__).resolve().parents[2]
    tracked = subprocess.run(
        ["git", "ls-files", "--", "*.py", "*.c", "*.h"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert tracked, "git ls-files returned nothing; run from the checkout"
    flagged: list[str] = []
    for rel in tracked:
        if rel.startswith("tools/fixtures/"):
            continue
        for line_no, matched, description in checker.check_file(root / rel, root):
            if description.startswith("divider"):
                flagged.append(f"{rel}:{line_no}: {description}: {matched}")
    assert flagged == []

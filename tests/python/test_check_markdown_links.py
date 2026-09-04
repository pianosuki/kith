"""Markdown link checker tests.

The checker holds every repository markdown link to its target: relative
targets resolve against the linking file's directory, markdown fragments
match a heading slug, and absolute URLs stay out of scope. These tests
drive it over the real repository (green) and over synthetic trees (the
root-perspective path class, fragments, code fences, reference links,
out-of-scope URLs, usage errors).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
TOOL = REPO / "tools" / "check_markdown_links.py"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(TOOL), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_real_tree_resolves() -> None:
    result = _run()
    assert result.returncode == 0, result.stdout + result.stderr


def test_missing_target_is_reported(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("[doc](b.md)\n", encoding="utf-8")
    result = _run(f"--root={tmp_path}")
    assert result.returncode == 1
    assert "a.md:1" in result.stdout
    assert "b.md" in result.stdout


def test_target_resolves_from_the_file_directory(tmp_path: Path) -> None:
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "a.md").write_text("[doc](b.md)\n", encoding="utf-8")
    (sub / "b.md").write_text("target\n", encoding="utf-8")
    assert _run(f"--root={tmp_path}").returncode == 0


def test_root_perspective_path_is_the_finding(tmp_path: Path) -> None:
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "a.md").write_text("[doc](sub/b.md)\n", encoding="utf-8")
    (sub / "b.md").write_text("target\n", encoding="utf-8")
    result = _run(f"--root={tmp_path}")
    assert result.returncode == 1
    assert "sub/b.md" in result.stdout


def test_directory_target(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("[refs](docs/)\n[void](missing/)\n", encoding="utf-8")
    (tmp_path / "docs").mkdir()
    result = _run(f"--root={tmp_path}")
    assert result.returncode == 1
    assert "missing/" in result.stdout


def test_fragment_on_markdown_target(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "[ok](b.md#hello-world)\n[bad](b.md#hello-universe)\n", encoding="utf-8"
    )
    (tmp_path / "b.md").write_text("# Hello, World!\n", encoding="utf-8")
    result = _run(f"--root={tmp_path}")
    assert result.returncode == 1
    assert "#hello-universe" in result.stdout
    assert "#hello-world" not in result.stdout


def test_same_file_fragment(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "# The Backlog\n\n[up](#the-backlog)\n[dead](#the-icebox)\n", encoding="utf-8"
    )
    result = _run(f"--root={tmp_path}")
    assert result.returncode == 1
    assert "#the-backlog" not in result.stdout
    assert "#the-icebox" in result.stdout


def test_repeated_heading_slug_is_suffixed(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "# Notes\n\n## Edge\n\nx\n\n## Edge\n\n[second](#edge-1)\n", encoding="utf-8"
    )
    assert _run(f"--root={tmp_path}").returncode == 0


def test_code_examples_never_scan(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "```\n[not a link](missing.md)\n```\n\nInline `text (missing.md)` stays put.\n",
        encoding="utf-8",
    )
    assert _run(f"--root={tmp_path}").returncode == 0


def test_absolute_urls_are_out_of_scope(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "[site](https://example.invalid/never)\n"
        "[mail](mailto:nobody@example.invalid)\n"
        "[def]: https://example.invalid/never\n"
        "[use def][def]\n",
        encoding="utf-8",
    )
    assert _run(f"--root={tmp_path}").returncode == 0


def test_reference_links_resolve(tmp_path: Path) -> None:
    (tmp_path / "b.md").write_text("target\n", encoding="utf-8")
    (tmp_path / "a.md").write_text(
        "[ok][ref]\n[shortcut]\n[dead][void]\n\n[ref]: b.md\n[shortcut]: b.md\n[void]: absent.md\n",
        encoding="utf-8",
    )
    result = _run(f"--root={tmp_path}")
    assert result.returncode == 1
    assert "absent.md" in result.stdout
    assert "b.md" not in result.stdout


def test_bare_brackets_without_definition_are_prose(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "See [1] and [Appendix B] for the unlinked mentions.\n", encoding="utf-8"
    )
    assert _run(f"--root={tmp_path}").returncode == 0


def test_leading_slash_target_is_a_finding(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("[rooted](/docs/b.md)\n", encoding="utf-8")
    result = _run(f"--root={tmp_path}")
    assert result.returncode == 1
    assert "site root" in result.stdout


def test_missing_root_is_a_usage_error(tmp_path: Path) -> None:
    result = _run(f"--root={tmp_path / 'absent'}")
    assert result.returncode == 2
    assert "error" in result.stderr

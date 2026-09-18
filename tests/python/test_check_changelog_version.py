"""Changelog version-lockstep checker tests.

The checker holds CHANGELOG.md's newest released section heading to the
CMake project version. These tests drive it over the real repository
(green) and over synthetic trees (mismatch, unreleased-tolerated, missing
input, no release heading).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
TOOL = REPO / "tools" / "check_changelog_version.py"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(TOOL), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _write_tree(tmp_path: Path, cmake_version: str, changelog: str) -> tuple[Path, Path]:
    cmake = tmp_path / "CMakeLists.txt"
    cmake.write_text(f"project(kith VERSION {cmake_version} LANGUAGES C)\n")
    changelog_path = tmp_path / "CHANGELOG.md"
    changelog_path.write_text(changelog)
    return cmake, changelog_path


def test_real_tree_is_in_lockstep() -> None:
    result = _run()
    assert result.returncode == 0, result.stderr


def test_mismatch_names_both_versions(tmp_path: Path) -> None:
    cmake, changelog = _write_tree(tmp_path, "1.2.3", "# Changelog\n\n## [1.2.2]\n\nNotes.\n")
    result = _run(f"--changelog={changelog}", f"--cmake={cmake}")
    assert result.returncode == 1
    assert "[1.2.2]" in result.stdout
    assert "1.2.3" in result.stdout


def test_unreleased_section_is_skipped(tmp_path: Path) -> None:
    text = "# Changelog\n\n## [Unreleased]\n\n## [1.2.3]\n\nNotes.\n"
    cmake, changelog = _write_tree(tmp_path, "1.2.3", text)
    result = _run(f"--changelog={changelog}", f"--cmake={cmake}")
    assert result.returncode == 0


def test_missing_changelog_is_a_usage_error(tmp_path: Path) -> None:
    cmake, _ = _write_tree(tmp_path, "1.2.3", "")
    result = _run(f"--changelog={tmp_path / 'missing.md'}", f"--cmake={cmake}")
    assert result.returncode == 2
    assert "error" in result.stderr


def test_changelog_without_release_heading_is_a_usage_error(tmp_path: Path) -> None:
    cmake, changelog = _write_tree(tmp_path, "1.2.3", "# Changelog\n\n## [Unreleased]\n\nNotes.\n")
    result = _run(f"--changelog={changelog}", f"--cmake={cmake}")
    assert result.returncode == 2
    assert "error" in result.stderr


def test_cmake_without_project_line_is_a_usage_error(tmp_path: Path) -> None:
    cmake = tmp_path / "CMakeLists.txt"
    cmake.write_text("project(other VERSION 1.2.3 LANGUAGES C)\n")
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text("# Changelog\n\n## [1.2.3]\n\nNotes.\n")
    result = _run(f"--changelog={changelog}", f"--cmake={cmake}")
    assert result.returncode == 2
    assert "error" in result.stderr

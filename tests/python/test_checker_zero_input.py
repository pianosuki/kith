"""Zero-input refusal tests for the repository's green-producing checkers.

A checker whose success is reachable with nothing scanned is a vacuous
gate. These tests pin the loud path for every first-party checker that
collects its own input: a missing source root, a missing include or
bindings tree, and an invalid file argument all fail with a usage error
instead of exiting 0, and the scanners still pass over their real roots.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[2]
TOOLS = REPO / "tools"

ROOT_SCANNERS = [
    "check_internal_includes.py",
    "check_layout_consistency.py",
    "check_module_layers.py",
    "check_public_api_includes.py",
]


def _run(tool: str, *args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Run one checker tool and capture its streams."""
    return subprocess.run(
        [sys.executable, str(TOOLS / tool), *args],
        capture_output=True,
        text=True,
        cwd=cwd,
        check=False,
    )


@pytest.mark.parametrize("tool", ROOT_SCANNERS)
def test_missing_root_is_a_usage_error(tool: str, tmp_path: Path) -> None:
    """A requested root that does not exist is a usage error, not a pass."""
    result = _run(tool, str(tmp_path / "missing"))
    assert result.returncode == 2
    assert "error" in result.stderr


@pytest.mark.parametrize("tool", ROOT_SCANNERS)
def test_existing_roots_still_scan(tool: str) -> None:
    """The scanners stay green over the repository's real roots."""
    result = _run(tool, str(REPO / "include"), str(REPO / "src"))
    assert result.returncode == 0


def test_runtime_planes_refuses_the_missing_default_roots(
    tmp_path: Path,
) -> None:
    """Bare invocation outside a source tree is a usage error."""
    result = _run("check_runtime_planes.py", cwd=tmp_path)
    assert result.returncode == 2
    assert "error" in result.stderr


def test_public_api_refuses_a_missing_include_root(tmp_path: Path) -> None:
    """A --include-root without kith/ headers is a usage error, not a pass."""
    result = _run("check_public_api.py", "--include-root", str(tmp_path))
    assert result.returncode == 2
    assert "error" in result.stderr


def test_ctypes_drift_refuses_a_missing_include_root(tmp_path: Path) -> None:
    """A --include-root without kith/ headers is a setup error."""
    result = _run("check_ctypes_drift.py", "--include-root", str(tmp_path))
    assert result.returncode == 2
    assert "error" in result.stderr


def test_ctypes_drift_refuses_missing_bindings(tmp_path: Path) -> None:
    """The checked-in bindings are required; their absence is a setup error."""
    result = _run("check_ctypes_drift.py", "--generated", str(tmp_path / "missing"))
    assert result.returncode == 2
    assert "error" in result.stderr


def test_trivial_fixer_refuses_an_invalid_file(tmp_path: Path) -> None:
    """A nonexistent file argument is a usage error, not a silent skip."""
    result = _run("check_trivial_fixers.py", str(tmp_path / "missing.py"))
    assert result.returncode == 2
    assert "error" in result.stderr


def test_trivial_fixer_still_checks_a_valid_file() -> None:
    """The fixer stays green over a gate-clean file."""
    result = _run("check_trivial_fixers.py", str(TOOLS / "check_trivial_fixers.py"))
    assert result.returncode == 0

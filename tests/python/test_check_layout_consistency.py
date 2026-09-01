"""Self-test for tools/check_layout_consistency.py.

Drives the checker end-to-end against the real tree (green) and fixture
trees covering the public-header guard rule: the path-derived guard
name (module headers and framework-level headers alike), the guard's
first-lines position, and the `#pragma once` ban. Private headers under
src/ keep their `#pragma once` allowance.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


CHECKER = Path(__file__).resolve().parents[2] / "tools" / "check_layout_consistency.py"

REPO = CHECKER.parents[1]


def _run(*roots: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECKER), *map(str, roots)],
        capture_output=True,
        text=True,
        check=False,
    )


def _write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


GUARDED = "#ifndef KITH_UTIL_RNG_H\n#define KITH_UTIL_RNG_H\n#endif\n"
FRAMEWORK_GUARDED = "#ifndef KITH_TYPES_H\n#define KITH_TYPES_H\n#endif\n"


def test_real_tree_stays_green() -> None:
    result = _run(REPO / "include", REPO / "src")
    assert result.returncode == 0, result.stdout


def test_module_header_with_derived_guard_passes(tmp_path: Path) -> None:
    _write(tmp_path / "include", "kith/util/rng.h", GUARDED)
    result = _run(tmp_path / "include")
    assert result.returncode == 0, result.stdout


def test_framework_header_with_derived_guard_passes(tmp_path: Path) -> None:
    _write(tmp_path / "include", "kith/types.h", FRAMEWORK_GUARDED)
    result = _run(tmp_path / "include")
    assert result.returncode == 0, result.stdout


def test_wrong_guard_name_is_reported(tmp_path: Path) -> None:
    _write(
        tmp_path / "include", "kith/util/rng.h", "#ifndef KITH_RNG_H\n#define KITH_RNG_H\n#endif\n"
    )
    result = _run(tmp_path / "include")
    assert result.returncode == 1
    assert "KITH_UTIL_RNG_H" in result.stdout
    assert "KITH_RNG_H" in result.stdout


def test_guard_after_content_is_reported(tmp_path: Path) -> None:
    text = f"/* late guard */\n{GUARDED}"
    _write(tmp_path / "include", "kith/util/rng.h", text)
    result = _run(tmp_path / "include")
    assert result.returncode == 1
    assert "KITH_UTIL_RNG_H" in result.stdout


def test_pragma_once_in_public_header_is_reported(tmp_path: Path) -> None:
    text = f"{GUARDED}#pragma once\n"
    _write(tmp_path / "include", "kith/util/rng.h", text)
    result = _run(tmp_path / "include")
    assert result.returncode == 1
    assert "#pragma once in a public header" in result.stdout


def test_pragma_once_stays_allowed_under_src(tmp_path: Path) -> None:
    _write(tmp_path / "src", "net/internal/conn.h", "#pragma once\n")
    _write(tmp_path / "src", "net/conn.c", "")
    result = _run(tmp_path / "src")
    assert result.returncode == 0, result.stdout

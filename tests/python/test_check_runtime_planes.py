"""Self-test for the data-flow (writes) pass of tools/check_runtime_planes.py.

Drives the libclang AST checker against a fixture gateway/view translation
unit that calls a sim-plane mutator and asserts the call is flagged, while a
same-plane call is not. The production run over src/ is asserted clean.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import tools.check_runtime_planes as planes


_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_SRC = _REPO_ROOT / "tools" / "fixtures" / "plane_writes" / "src"
_INCLUDE = _REPO_ROOT / "include"


def test_plane_write_flagged_in_fixture() -> None:
    rules = {"gateway/view": ["sim"]}
    violations = planes.scan_forbidden_writes(_FIXTURE_SRC, rules, _INCLUDE)
    assert violations, "expected a forbidden cross-plane write violation"
    flagged = [v for v in violations if v[2] == "kith_sim_set_actor_pos"]
    assert flagged, "kith_sim_set_actor_pos call must be flagged"
    assert "gateway/view/view.c" in flagged[0][0]


def test_local_call_not_flagged() -> None:
    rules = {"gateway/view": ["sim"]}
    violations = planes.scan_forbidden_writes(_FIXTURE_SRC, rules, _INCLUDE)
    assert not any(v[2] == "fixture_view_local_helper" for v in violations)


@pytest.mark.parametrize(
    "source, diagnostic",
    [
        ("void broken(void) { undeclared_call(); }\n", "undeclared_call"),
        ('#include "missing-plane-fixture.h"\n', "missing-plane-fixture.h"),
    ],
)
def test_parse_errors_and_fatal_diagnostics_fail_closed(
    tmp_path: Path, source: str, diagnostic: str
) -> None:
    source_root = tmp_path / "src"
    view = source_root / "gateway" / "view"
    view.mkdir(parents=True)
    (view / "broken.c").write_text(source)
    with pytest.raises(RuntimeError, match=diagnostic):
        planes.scan_forbidden_writes(source_root, {"gateway/view": ["sim"]}, tmp_path / "include")

    result = subprocess.run(
        [sys.executable, "-B", str(Path(planes.__file__).resolve())],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert diagnostic in result.stderr and "OK:" not in result.stdout


def test_warning_only_translation_unit_is_not_an_error(tmp_path: Path) -> None:
    source_root = tmp_path / "src"
    view = source_root / "gateway" / "view"
    view.mkdir(parents=True)
    (view / "warning.c").write_text('#warning "fixture warning"\nvoid local(void) {}\n')
    assert planes.scan_forbidden_writes(source_root, {"gateway/view": ["sim"]}, tmp_path) == []


def test_libclang_bindings_required_for_write_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(planes, "Index", None)
    with pytest.raises(RuntimeError, match="libclang Python bindings are required"):
        planes.scan_forbidden_writes(_FIXTURE_SRC, {"gateway/view": ["sim"]}, _INCLUDE)


def test_production_src_has_no_plane_writes() -> None:
    rules = planes.load_rules(planes.RULES_PATH)
    forbidden_writes = rules.get("forbidden_writes", {})
    src_root = _REPO_ROOT / "src"
    if not src_root.is_dir():
        pytest.skip("no src/ root")
    violations = planes.scan_forbidden_writes(src_root, forbidden_writes, _INCLUDE)
    assert violations == []

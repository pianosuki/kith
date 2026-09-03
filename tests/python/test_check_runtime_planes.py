"""Self-test for the data-flow (writes) pass of tools/check_runtime_planes.py.

Drives the libclang AST checker against a fixture gateway/view translation
unit that calls a sim-plane mutator and asserts the call is flagged, while a
same-plane call is not. The production run over src/ is asserted clean.
"""

from __future__ import annotations

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


def test_production_src_has_no_plane_writes() -> None:
    rules = planes.load_rules(planes.RULES_PATH)
    forbidden_writes = rules.get("forbidden_writes", {})
    src_root = _REPO_ROOT / "src"
    if not src_root.is_dir():
        pytest.skip("no src/ root")
    violations = planes.scan_forbidden_writes(src_root, forbidden_writes, _INCLUDE)
    assert violations == []

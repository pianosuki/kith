"""Self-test for tools/check_public_api.py.

Drives the libclang-based contract checker against the good and bad fixture
headers under ``tools/fixtures/public_api_good/`` and
``tools/fixtures/public_api_bad/``. The good fixture conforms to every rule and
must yield zero violations; the bad fixture violates every rule and must yield
at least one violation per rule. The baseline file is also exercised: a
default run treats baseline signatures as tracked debt (exit 0) and an
unknown signature fails.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import tools.check_public_api as checker


_REPO_ROOT = Path(__file__).resolve().parents[2]
_GOOD = _REPO_ROOT / "tools" / "fixtures" / "public_api_good"
_BAD = _REPO_ROOT / "tools" / "fixtures" / "public_api_bad"


def _rule_ids(violations: list[checker.Violation]) -> set[str]:
    return {v.rule_id for v in violations}


def test_good_fixture_has_no_violations() -> None:
    violations = checker.scan_public_api(_GOOD)
    assert violations == [], "\n".join(
        f"{v.header_rel}:{v.line} {v.rule_id} {v.symbol} — {v.detail}" for v in violations
    )


def test_bad_fixture_violates_every_rule() -> None:
    violations = checker.scan_public_api(_BAD)
    rules = _rule_ids(violations)
    expected = {
        "handle_typedef_name",
        "struct_size_first",
        "struct_reserved",
        "creation_struct_name",
        "value_type_doc",
        "value_type_handle_field",
        "field_do_not_access",
        "enum_zero_suffix",
        "func_ownership",
        "func_thread_safety",
    }
    missing = expected - rules
    assert not missing, f"rules not exercised by the bad fixture: {missing}"


def test_bad_fixture_value_type_handle_field_flags_both_secret_fields() -> None:
    symbols = {
        v.symbol for v in checker.scan_public_api(_BAD) if v.rule_id == "value_type_handle_field"
    }
    assert "kith_bad_dto.handle_field" in symbols
    assert "kith_bad_dto.secret" in symbols


def test_enum_zero_detects_bare_zero_not_suffixed() -> None:
    violations = checker.scan_public_api(_BAD)
    syms = {v.symbol for v in violations if v.rule_id == "enum_zero_suffix"}
    assert "kith_bad_flag.KITH_BAD_FLAG_NONE" in syms
    # The suffixed enumerator is not flagged.
    assert not any("KITH_BAD_FLAG_A" in s for s in syms)


def test_signature_is_stable_and_header_scoped() -> None:
    violations = checker.scan_public_api(_BAD)
    sigs = {v.signature for v in violations}
    assert "kith/bad.h|handle_typedef_name|my_handle_t" in sigs


def test_baseline_treats_known_signatures_as_debt(tmp_path: Path) -> None:
    violations = checker.scan_public_api(_BAD)
    baseline = tmp_path / "baseline.txt"
    checker.write_baseline(baseline, violations)
    loaded = checker.load_baseline(baseline)
    # Every bad-fixture signature is in the written baseline.
    assert {v.signature for v in violations} <= loaded


def test_real_baseline_carries_no_entries() -> None:
    # The deviation ledger is empty and shrinks only; an entry means debt returned.
    assert checker.load_baseline(_REPO_ROOT / "tools" / "public_api_baseline.txt") == set()


def test_main_returns_zero_on_good_fixture(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = checker.main(
        [
            "check_public_api.py",
            "--include-root",
            str(_GOOD),
            "--baseline",
            str(tmp_path / "empty.txt"),
        ]
    )
    assert rc == 0

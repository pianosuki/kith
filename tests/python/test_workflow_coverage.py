"""Combined native coverage and diagnostic Python scope reporting in CI."""

from __future__ import annotations

import configparser
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def _scripts() -> list[str]:
    path = ROOT / ".github" / "workflows" / "coverage.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    steps: list[dict[str, str]] = document["jobs"]["coverage"]["steps"]
    return [step["run"] for step in steps if "run" in step]


def _scope_program() -> str:
    script = next(script for script in _scripts() if "coverage json" in script)
    match = re.search(r"python - <<'PY'[^\n]*\n(.*?)\nPY(?:\n|$)", script, re.DOTALL)
    assert match is not None
    return match.group(1)


def test_c_reports_merge_both_completed_test_suites() -> None:
    scripts = _scripts()
    c_tests = next(index for index, script in enumerate(scripts) if "ctest --preset" in script)
    python_tests = next(index for index, script in enumerate(scripts) if "coverage run" in script)
    merge = next(index for index, script in enumerate(scripts) if "llvm-profdata merge" in script)
    report = next(index for index, script in enumerate(scripts) if "llvm-cov report" in script)
    html = next(index for index, script in enumerate(scripts) if "llvm-cov show" in script)
    assert c_tests < python_tests < merge <= report < html


def test_python_coverage_includes_callbacks_and_children_without_metric_gates() -> None:
    scripts = _scripts()
    config = configparser.ConfigParser()
    config.read(ROOT / ".coveragerc")
    run = config["run"]
    assert run["source"].split() == ["python/kith"]
    assert run["omit"].split() == ["*/_generated/*"]
    assert run["core"] == "sysmon"
    assert run.get("dynamic_context", "none") == "none"
    assert run.getboolean("branch")
    assert run.getboolean("parallel")
    assert run.getboolean("sigterm")
    assert "subprocess" in run["patch"].split()
    commands = "\n".join(scripts)
    assert "--fail-under" not in commands
    assert not config.has_option("report", "fail_under")
    policy = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    for declared, emitted in (
        (r"C core:\s*≥\s*(\d+)%", r"Target:\s*>=\s*(\d+)%[^\n]*C core"),
        (r"Python framework:\s*≥\s*(\d+)%", r"Framework line target:\s*>=\s*(\d+)%"),
    ):
        expected = re.search(declared, policy)
        actual = re.search(emitted, commands)
        assert expected is not None and actual is not None
        assert int(actual.group(1)) == int(expected.group(1))
    assert "coverage combine --keep" in commands
    assert commands.index("coverage combine") < commands.index("coverage json")
    workflow = yaml.safe_load((ROOT / ".github/workflows/coverage.yml").read_text())
    uploads = [step for step in workflow["jobs"]["coverage"]["steps"] if "with" in step]
    evidence = next(step for step in uploads if ".coverage.*" in step["with"].get("path", ""))
    assert evidence["if"] == "always()"
    assert evidence["with"]["include-hidden-files"] is True


def test_python_scope_diagnostics_partition_without_changing_aggregate(tmp_path: Path) -> None:
    data = {
        "files": {
            "examples/checkout/python/kith/core.py": {
                "summary": {
                    "covered_lines": 5,
                    "num_statements": 10,
                    "covered_branches": 1,
                    "num_branches": 4,
                }
            },
            "python\\kith\\examples\\visual\\client.py": {
                "summary": {
                    "covered_lines": 2,
                    "num_statements": 8,
                    "covered_branches": 1,
                    "num_branches": 2,
                }
            },
            "python/kith/examples/__init__.py": {
                "summary": {"covered_lines": 0, "num_statements": 0}
            },
        },
        "totals": {"covered_lines": 7, "num_statements": 18},
    }
    path = tmp_path / "python-coverage.json"
    original = json.dumps(data)
    path.write_text(original, encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-c", _scope_program()],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "| Package aggregate | 7 | 18 | 38.89% | 33.33% |" in result.stdout
    assert "| Framework excluding examples | 5 | 10 | 50.00% | 25.00% |" in result.stdout
    assert "| Examples | 2 | 8 | 25.00% | 50.00% |" in result.stdout
    assert path.read_text(encoding="utf-8") == original


def test_python_scope_diagnostics_handle_empty_groups(tmp_path: Path) -> None:
    path = tmp_path / "python-coverage.json"
    path.write_text('{"files": {}}', encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-c", _scope_program()],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "| Framework excluding examples | 0 | 0 | N/A | N/A |" in result.stdout
    assert "| Examples | 0 | 0 | N/A | N/A |" in result.stdout

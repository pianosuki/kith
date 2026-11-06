"""Exact Python selection and artifact isolation in weekly workflow matrices."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]


def _job(workflow: str, name: str) -> dict[str, Any]:
    path = ROOT / ".github" / "workflows" / f"{workflow}.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    job: dict[str, Any] = document["jobs"][name]
    return job


def _standard_versions() -> list[str]:
    rows = _job("ci", "build")["strategy"]["matrix"]["include"]
    return sorted({row["python"] for row in rows})


@pytest.mark.parametrize(
    ("workflow", "name"),
    [
        ("sanitizers", "pytest-tsan"),
        ("sanitizers", "pytest-asan"),
        ("sanitizers", "pytest-asan-ft"),
        ("sanitizers", "gcc-build"),
        ("coverage", "coverage"),
        ("stress", "smoke-dense"),
        ("stress", "smoke-distributed"),
    ],
)
def test_weekly_python_versions_and_artifacts_are_isolated(
    workflow: str, name: str, tmp_path: Path
) -> None:
    job = _job(workflow, name)
    assert job["strategy"]["matrix"]["python"] == _standard_versions()
    assert job["env"]["KITH_PYTHON_VERSION"] == "${{ matrix.python }}"
    for step in job["steps"]:
        action = step.get("uses", "")
        if action.startswith("actions/setup-python@"):
            assert step["with"]["python-version"] == "${{ matrix.python }}"
        if action.startswith("actions/upload-artifact@"):
            assert "${{ matrix.python }}" in step["with"]["name"]
    if workflow == "sanitizers" and name in {"pytest-tsan", "pytest-asan", "pytest-asan-ft"}:
        preset = "tsan" if name == "pytest-tsan" else "asan-ubsan"
        kind = "tsan" if preset == "tsan" else "asan"
        assert any(step.get("run") == f"cmake --preset {preset}" for step in job["steps"])
        script = next(step["run"] for step in job["steps"] if "runtime=" in step.get("run", ""))
        build = f'export KITH_BUILD_DIR="$PWD/build/{preset}"'
        resolver = f'scripts/sanitizer-runtime.sh {kind} "$KITH_BUILD_DIR"'
        assert script.index(build) < script.index(resolver)
        assert script.index("not-started") < script.index('export LD_PRELOAD="$runtime"')
        assert script.index('export LD_PRELOAD="$runtime"') < script.index(
            "tools/python_runtime_info.py"
        )
        expected_ft = 1 if name.endswith("-ft") else 0
        assert (
            f'--expected-version "$KITH_PYTHON_VERSION" --expected-free-threaded {expected_ft}'
            in script
        )
        upload = next(
            step
            for step in job["steps"]
            if step.get("uses", "").startswith("actions/upload-artifact@")
        )
        assert upload["if"] == "always()"
        assert {"python-runtime.json", "sanitizer-runtime.log"} <= set(
            upload["with"]["path"].splitlines()
        )
        fallback = next(
            step
            for step in job["steps"]
            if step.get("name") == "Preserve unavailable runtime metadata"
        )
        assert fallback["if"] == "always()"
        assert job["steps"].index(fallback) < job["steps"].index(upload)
        subprocess.run(["bash", "-e", "-c", fallback["run"]], cwd=tmp_path, check=True)
        record = tmp_path / "python-runtime.json"
        assert json.loads(record.read_text())["collection_status"] == "not-started"
        retained = '{"collection_status":"partial","errors":["retained"]}\n'
        record.write_text(retained)
        subprocess.run(["bash", "-e", "-c", fallback["run"]], cwd=tmp_path, check=True)
        assert record.read_text() == retained


def test_free_threaded_sanitizer_uses_selected_interpreter_after_preload() -> None:
    job = _job("sanitizers", "pytest-asan-ft")
    script = next(step["run"] for step in job["steps"] if "runtime=" in step.get("run", ""))
    assert 'uv python find "cpython-${KITH_PYTHON_VERSION}+freethreaded-linux-x86_64-gnu"' in script
    assert 'uv venv --python "$ft_python"' in script
    assert '"$PWD/.venv-ft/bin/python" -m pytest' in script
    assert script.index('export LD_PRELOAD="$runtime"') < script.index(
        "tools/python_runtime_info.py"
    )
    assert "python3.14t" not in script
    assert "tests/lsan_python.supp" in script


def test_tsan_python_matrix_remains_standard_interpreter_only() -> None:
    job = _job("sanitizers", "pytest-tsan")
    assert any(step.get("uses", "").startswith("actions/setup-python@") for step in job["steps"])
    script = "\n".join(step.get("run", "") for step in job["steps"])
    assert "python -m pytest tests/integration" in script
    assert "--expected-free-threaded 0" in script
    assert ".venv-ft" not in script
    assert "tests/tsan_python.supp" in script


def test_main_ci_covers_both_supported_minors_with_and_without_gil() -> None:
    versions = _standard_versions()
    assert len(versions) == 2
    assert {".".join(version.split(".")[:2]) for version in versions} == {"3.14", "3.15"}
    assert all(re.fullmatch(r"3\.\d+\.\d+", version) for version in versions)
    standard = _job("ci", "build")
    setup = next(
        step
        for step in standard["steps"]
        if step.get("uses", "").startswith("actions/setup-python@")
    )
    assert setup["with"]["python-version"] == "${{ matrix.python }}"
    threaded = _job("ci", "free-threaded")
    assert threaded["strategy"]["matrix"]["python"] == versions
    for command in ("./scripts/provision-ci.sh free-threaded", "./scripts/verify.sh free-threaded"):
        step = next(step for step in threaded["steps"] if step.get("run") == command)
        assert step["env"]["KITH_PYTHON_VERSION"] == "${{ matrix.python }}"


def test_release_runtime_gil_modes_and_smokes_gate_the_draft() -> None:
    job = _job("release", "smoke-python")
    rows = job["strategy"]["matrix"]["include"]
    actual = {(row["base"], row["python"], row["free_threaded"]) for row in rows}
    expected = {
        (version, version + suffix, mode)
        for version in _standard_versions()
        for suffix, mode in (("", "0"), ("t", "1"))
    }
    assert actual == expected
    assert len(rows) == len(expected)
    script = "\n".join(step.get("run", "") for step in job["steps"])
    assert "matrix.python" in script
    check = next(step for step in job["steps"] if "sys._is_gil_enabled()" in step.get("run", ""))
    assert check["env"]["EXPECT_FREE_THREADED"] == "${{ matrix.free_threaded }}"
    assert '(not sys._is_gil_enabled()) is expected' in check["run"]
    assert 'expected = os.environ["EXPECT_FREE_THREADED"] == "1"' in check["run"]
    assert {"smoke-python", "smoke-portable"} <= set(_job("release", "draft-release")["needs"])

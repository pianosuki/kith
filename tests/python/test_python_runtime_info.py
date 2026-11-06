"""Artifact identity and partial evidence in Python runtime metadata."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import subprocess
import sys
import sysconfig
from importlib.machinery import ModuleSpec
from itertools import product
from pathlib import Path
from types import ModuleType

import pytest
import tools.python_runtime_info as runtime_info


SCRIPT = Path(runtime_info.__file__)
VERSION = ".".join(str(part) for part in sys.version_info[:3])
FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def _collect(output: Path, version: str, free_threaded: bool) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--output",
            str(output),
            "--expected-version",
            version,
            "--expected-free-threaded",
            str(int(free_threaded)),
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_runtime_metadata_matches_selected_files_and_python_mode(tmp_path: Path) -> None:
    output = tmp_path / "runtime.json"
    result = _collect(output, VERSION, FREE_THREADED)
    assert result.returncode == 0, result.stderr
    record = json.loads(output.read_text())
    assert record["collection_status"] == "ok"
    assert record["version"] == VERSION
    assert record["build_free_threaded"] == FREE_THREADED
    assert record["gil_before_ctypes_import"] == (not FREE_THREADED)
    assert record["gil_after_ctypes_import"] == (not FREE_THREADED)
    assert record["process_scope"] == "collector only"
    assert record["errors"] == []
    for file in [record["executable"], record["base_executable"], record["ctypes"]]:
        if file["status"] == "built-in":
            continue
        path = Path(file["path"])
        assert file["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert record["libffi_source_version"] is None


def test_runtime_metadata_retains_evidence_on_version_and_mode_mismatch(tmp_path: Path) -> None:
    for label, version, mode in [
        ("version", "0.0.0", FREE_THREADED),
        ("mode", VERSION, not FREE_THREADED),
    ]:
        output = tmp_path / f"{label}.json"
        result = _collect(output, version, mode)
        assert result.returncode != 0
        record = json.loads(output.read_text())
        assert record["collection_status"] == "partial"
        assert record["version"] == VERSION
        assert record["executable"]["sha256"]
        assert record["errors"]


def test_loaded_library_hash_requires_matching_device_and_inode(tmp_path: Path) -> None:
    library = tmp_path / "libffi sample\\name.so"
    library.write_bytes(b"owned fixture library\n")
    stat = library.stat()
    device = f"{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}"
    escaped = str(library).replace("\\", r"\134").replace(" ", r"\040")
    entry = f"1000-2000 r--p 00000000 {device} {stat.st_ino} {escaped}\n"
    mismatch = f"2000-3000 r-xp 00000000 {device} {stat.st_ino + 1} {escaped}\n"
    maps = tmp_path / "maps"
    maps.write_text(entry + entry + mismatch)
    errors: list[str] = []
    libraries = runtime_info.loaded_libraries(maps, errors)
    assert len(libraries) == 2
    assert libraries[0]["sha256"] == hashlib.sha256(library.read_bytes()).hexdigest()
    assert libraries[0]["path"] == str(library)
    assert libraries[1]["status"] == "unavailable"
    assert "device/inode" in str(libraries[1]["error"])
    assert len(errors) == 1


def test_runtime_mode_transitions_and_missing_ctypes_remain_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("_ctypes")
    module.__spec__ = ModuleSpec("_ctypes", loader=None, origin="built-in")
    monkeypatch.setattr(importlib, "import_module", lambda name: module)
    monkeypatch.setattr(runtime_info, "capture_file", lambda path, errors: {"path": path})
    monkeypatch.setattr(runtime_info, "loaded_libraries", lambda maps, errors: [])
    for expected, build, before, after in product((False, True), repeat=4):
        gil = iter((before, after))
        monkeypatch.setattr(sys, "_is_gil_enabled", lambda states=gil: next(states))
        monkeypatch.setattr(
            sysconfig,
            "get_config_var",
            lambda name, configured=build: int(configured) if name == "Py_GIL_DISABLED" else "abi",
        )
        result = runtime_info.collect_runtime_info(VERSION, expected)
        valid = (build, before, after) == (expected, not expected, not expected)
        assert (result["collection_status"] == "ok") == valid

    def missing_module(name: str) -> ModuleType:
        raise ImportError("fixture unavailable")

    monkeypatch.setattr(importlib, "import_module", missing_module)
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True)
    monkeypatch.setattr(sysconfig, "get_config_var", lambda name: 0)
    result = runtime_info.collect_runtime_info(VERSION, False)
    assert result["collection_status"] == "partial"
    assert result["errors"] == ["_ctypes import: fixture unavailable"]
    assert result["executable"]


def test_failed_metadata_publish_preserves_placeholder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "runtime.json"
    placeholder = '{"collection_status":"not-started"}\n'
    output.write_text(placeholder)

    def fail_replace(temporary: Path, target: Path) -> Path:
        assert json.loads(temporary.read_text())["collection_status"] == "ok"
        assert target.read_text() == placeholder
        raise OSError("fixture publish failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    monkeypatch.setattr(
        runtime_info, "collect_runtime_info", lambda version, mode: {"collection_status": "ok"}
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            "--output",
            str(output),
            "--expected-version",
            VERSION,
            "--expected-free-threaded",
            str(int(FREE_THREADED)),
        ],
    )
    with pytest.raises(OSError, match="fixture publish failure"):
        runtime_info.main()
    assert output.read_text() == placeholder
    assert list(tmp_path.iterdir()) == [output]

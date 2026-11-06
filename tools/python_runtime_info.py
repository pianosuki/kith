#!/usr/bin/env python3
"""Record Python and loaded native-library identities before integration tests."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import sys
import sysconfig
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def file_identity(path: str, expected: tuple[int, int, int] | None = None) -> dict[str, object]:
    """Hash a stable file, checking an observed mapping identity when supplied."""
    reported = Path(path)
    resolved = reported.resolve(strict=True)
    with resolved.open("rb") as source:
        before = os.fstat(source.fileno())
        identity = (os.major(before.st_dev), os.minor(before.st_dev), before.st_ino)
        if expected is not None and identity != expected:
            raise ValueError("file no longer matches the loaded device/inode")
        digest = hashlib.file_digest(source, "sha256").hexdigest()
        after = os.fstat(source.fileno())
    current = reported.stat()
    fingerprints = {
        (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns)
        for item in (before, after, current)
    }
    if len(fingerprints) != 1:
        raise ValueError("file changed during collection")
    return {
        "status": "recorded",
        "reported_path": path,
        "path": str(resolved),
        "device": f"{identity[0]:x}:{identity[1]:x}",
        "inode": identity[2],
        "size": before.st_size,
        "sha256": digest,
    }


def capture_file(
    path: str, errors: list[str], expected: tuple[int, int, int] | None = None
) -> dict[str, object]:
    """Retain unavailable identities as explicit partial evidence."""
    try:
        return file_identity(path, expected)
    except (OSError, ValueError) as exc:
        error = f"{path}: {exc}"
        errors.append(error)
        return {"status": "unavailable", "reported_path": path, "error": error}


def loaded_libraries(maps: Path, errors: list[str]) -> list[dict[str, object]]:
    """Identify relevant mapped files without retaining virtual addresses."""
    libraries: list[dict[str, object]] = []
    seen: set[tuple[int, int, int, str]] = set()
    for line in maps.read_text(encoding="utf-8").splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6 or not fields[5].startswith("/"):
            continue
        path = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[5])
        name = Path(path).name
        if not name.startswith(("libffi", "libpython", "libclang_rt.asan", "libclang_rt.tsan")):
            continue
        major, minor = (int(part, 16) for part in fields[3].split(":"))
        inode = int(fields[4])
        key = (major, minor, inode, path)
        if key in seen:
            continue
        seen.add(key)
        libraries.append(capture_file(path, errors, (major, minor, inode)))
    return libraries


def collect_runtime_info(expected_version: str, expected_ft: bool) -> dict[str, object]:
    """Capture this process's artifacts and validate the requested Python mode."""
    errors: list[str] = []
    version = ".".join(str(part) for part in sys.version_info[:3])
    gil_before = sys._is_gil_enabled()
    ctypes_info: dict[str, object]
    try:
        module = importlib.import_module("_ctypes")
        origin = module.__spec__.origin if module.__spec__ is not None else None
        path = getattr(module, "__file__", None)
        ctypes_info = {"origin": origin}
        if path is not None:
            ctypes_info.update(capture_file(str(path), errors))
        elif origin == "built-in":
            ctypes_info["status"] = "built-in"
        else:
            errors.append("_ctypes has no identifiable file or built-in origin")
            ctypes_info["status"] = "unavailable"
    except (ImportError, OSError, RuntimeError) as exc:
        errors.append(f"_ctypes import: {exc}")
        ctypes_info = {"status": "unavailable", "error": str(exc)}
    gil_after = sys._is_gil_enabled()
    build_ft = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    if version != expected_version:
        errors.append(f"expected Python {expected_version}, observed {version}")
    if build_ft != expected_ft or gil_before == expected_ft or gil_after == expected_ft:
        errors.append("Python build or actual GIL mode differs from the requested mode")
    executable = capture_file(sys.executable, errors)
    base = capture_file(str(getattr(sys, "_base_executable", sys.executable)), errors)
    try:
        libraries = loaded_libraries(Path("/proc/self/maps"), errors)
    except (OSError, ValueError) as exc:
        errors.append(f"loaded-library collection: {exc}")
        libraries = []
    return {
        "schema": 1,
        "captured_at": datetime.now(UTC).isoformat(),
        "stage": "before-integration-tests",
        "process_scope": "collector only",
        "collection_status": "partial" if errors else "ok",
        "version": version,
        "version_string": sys.version,
        "expected_version": expected_version,
        "expected_free_threaded": expected_ft,
        "build_free_threaded": build_ft,
        "gil_before_ctypes_import": gil_before,
        "gil_after_ctypes_import": gil_after,
        "soabi": sysconfig.get_config_var("SOABI"),
        "executable": executable,
        "base_executable": base,
        "ctypes": ctypes_info,
        "loaded_native_libraries": libraries,
        "libffi_source_version": None,
        "libffi_linkage": "separate mapping observed"
        if any(Path(str(item["reported_path"])).name.startswith("libffi") for item in libraries)
        else "unknown",
        "errors": errors,
    }


def main() -> int:
    """Write partial evidence even when identity or mode validation fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-free-threaded", choices=("0", "1"), required=True)
    args = parser.parse_args()
    result = collect_runtime_info(args.expected_version, args.expected_free_threaded == "1")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=args.output.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            json.dump(result, stream, indent=2)
            stream.write("\n")
        temporary.replace(args.output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    if result["collection_status"] != "ok":
        print(json.dumps(result["errors"]), file=sys.stderr)
    return 0 if result["collection_status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())

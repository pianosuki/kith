"""Self-test for tools/gen_release_sbom.py.

Builds synthetic release wheels and drives the generator against them:
bundle members are discovered from kith/_libs/ rather than assumed, the
ldd resolver is injectable so the system-dependency graph is exercisable
without real ELF files, the C runtime family never enters the document,
two runs over the same wheel produce byte-identical output, and the
output carries the OSSF naming convention.
"""

from __future__ import annotations

import json
import uuid
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import tools.gen_release_sbom as sbom


def _write_wheel(path: Path, name: str = "kith", version: str = "1.0.0") -> None:
    """Build a minimal wheel with two bundle members and no python deps."""
    metadata = f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\n"
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(f"{name}-{version}.dist-info/METADATA", metadata)
        wheel.writestr(f"{name}/__init__.py", "")
        wheel.writestr(f"{sbom.BUNDLE_DIR}/libkith_gateway.so.1", b"\x7fELF-gateway")
        wheel.writestr(f"{sbom.BUNDLE_DIR}/libkith_reactor.so.1", b"\x7fELF-reactor")


def _resolver(paths: dict[str, list[str]]) -> Callable[[Path], list[str]]:
    """Return a resolver stub keyed by the lib filename."""

    def resolve(lib: Path) -> list[str]:
        return paths.get(lib.name, [])

    return resolve


def _bom_via_pipeline(tmp_path: Path, resolver: Callable[[Path], list[str]]) -> dict[str, Any]:
    """Run the generator's stages over a synthetic wheel, return the BOM."""
    wheel = tmp_path / "kith-1.0.0-py3-none-any.whl"
    _write_wheel(wheel)
    with zipfile.ZipFile(wheel) as archive:
        metadata = sbom._dist_info_metadata(archive)
        name = sbom._metadata_field(metadata, "Name")
        version = sbom._metadata_field(metadata, "Version")
        libs = sbom._bundled_libs(archive)
        deps = sbom._system_sonames(archive, libs, resolver)
    bom = sbom.build_bom(name, version, libs, deps)
    decoded: dict[str, Any] = json.loads(json.dumps(bom))
    return decoded


def test_sbom_describes_wheel_bundled_libs_and_system_deps(tmp_path: Path) -> None:
    resolver = _resolver(
        {
            "libkith_gateway.so.1": ["liburing.so.2", "libc.so.6"],
            "libkith_reactor.so.1": [],
        }
    )
    bom = _bom_via_pipeline(tmp_path, resolver)

    assert bom["bomFormat"] == "CycloneDX"
    assert bom["specVersion"] == "1.6"
    assert bom["serialNumber"] == (
        "urn:uuid:" + str(uuid.uuid5(uuid.NAMESPACE_URL, "pkg:pypi/kith@1.0.0"))
    )
    component = bom["metadata"]["component"]
    assert component["name"] == "kith"
    assert component["purl"] == "pkg:pypi/kith@1.0.0"

    names = [c["name"] for c in bom["components"]]
    assert names == ["libkith_gateway", "liburing.so.2", "libkith_reactor"]
    bundled = {c["name"]: c["properties"][0]["value"] for c in bom["components"]}
    assert bundled["libkith_gateway"] == "true"
    assert bundled["liburing.so.2"] == "false"

    graph = {d["ref"]: d.get("dependsOn", []) for d in bom["dependencies"]}
    assert graph["pkg:pypi/kith@1.0.0"] == [
        "libkith_gateway.so.1",
        "libkith_reactor.so.1",
    ]
    assert graph["libkith_gateway.so.1"] == ["liburing.so.2"]
    assert "libkith_reactor.so.1" not in graph


def test_runtime_family_is_filtered_from_the_graph(tmp_path: Path) -> None:
    resolver = _resolver(
        {
            "libkith_gateway.so.1": [
                "libc.so.6",
                "libm.so.6",
                "ld-linux-x86-64.so.2",
                "libpq.so.5",
            ]
        }
    )
    bom = _bom_via_pipeline(tmp_path, resolver)
    names = [c["name"] for c in bom["components"]]
    assert "libc.so.6" not in names
    assert "libm.so.6" not in names
    assert "ld-linux-x86-64.so.2" not in names
    assert "libpq.so.5" in names


def test_generate_is_deterministic_and_names_output(tmp_path: Path) -> None:
    wheel = tmp_path / "kith-1.0.0-py3-none-any.whl"
    _write_wheel(wheel)
    # The default ldd resolver reads nothing from the stub members (ldd
    # fails on non-ELF bytes), so the system graph is empty and the run
    # needs no real ELF files.
    first = sbom.generate(wheel, tmp_path / "out")
    second = sbom.generate(wheel, tmp_path / "out")
    assert first.name == "kith-1.0.0.cdx.json"
    assert first.read_bytes() == second.read_bytes()
    bom = json.loads(first.read_text(encoding="utf-8"))
    assert bom["components"][0]["name"] == "libkith_gateway"
    assert bom["components"][0]["version"] == "1.0.0"

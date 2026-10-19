"""Generate an artifact-derived CycloneDX SBOM for a kith release wheel.

Every bundled ELF object, including grafted native dependencies, is hashed.
Direct ELF dependencies are read without executing the shipped objects.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import tempfile
import uuid
import zipfile
from collections.abc import Callable
from pathlib import Path


BOM_FORMAT = "CycloneDX"
SPEC_VERSION = "1.6"
BUNDLE_DIR = "kith/_libs"
GENERATOR_REF = "tool:kith-gen-release-sbom"

# Core runtime dependencies remain supplied by the platform. A graft's hash
# changes its name, so bundled objects stay visible even within this family.
_RUNTIME_SONAME = re.compile(
    r"^(ld-linux[^/]*|lib(c|m|rt|dl|pthread|gcc_s|stdc\+\+|resolv)\.so(?:\..*)?)$"
)


def _dist_info_metadata(wheel: zipfile.ZipFile) -> str:
    """Return the METADATA content of the wheel's .dist-info directory."""
    names = [n for n in wheel.namelist() if n.endswith(".dist-info/METADATA")]
    if len(names) != 1:
        raise ValueError(f"expected one .dist-info/METADATA, found {len(names)}")
    return wheel.read(names[0]).decode("utf-8")


def _metadata_field(metadata: str, field: str) -> str:
    """Return the single top-level header value for @p field."""
    values = [
        line[len(field) + 2 :] for line in metadata.splitlines() if line.startswith(f"{field}: ")
    ]
    if len(values) != 1:
        raise ValueError(f"expected one {field} header, found {len(values)}")
    return values[0]


def _bundled_libs(wheel: zipfile.ZipFile) -> list[str]:
    """Return every wheel member containing an ELF object, sorted."""
    return sorted(name for name in wheel.namelist() if wheel.read(name)[:4] == b"\x7fELF")


def _ldd_sonames(lib: Path) -> list[str]:
    """Return direct ELF dependency names, failing on unreadable artifacts."""
    result = subprocess.run(["readelf", "-d", str(lib)], capture_output=True, text=True, check=True)
    return sorted(set(re.findall(r"\(NEEDED\).*?\[([^]]+)\]", result.stdout)))


def _extract_bundle(wheel: zipfile.ZipFile, members: list[str], target: Path) -> list[Path]:
    """Extract the bundle members into @p target and return their paths."""
    paths = []
    for member in members:
        wheel.extract(member, target)
        paths.append(target / member)
    return paths


def _system_sonames(
    wheel: zipfile.ZipFile,
    libs: list[str],
    resolver: Callable[[Path], list[str]],
) -> dict[str, list[str]]:
    """Map each bundled lib to the non-runtime sonames it links against."""
    deps: dict[str, list[str]] = {}
    with tempfile.TemporaryDirectory() as tmp:
        paths = _extract_bundle(wheel, libs, Path(tmp))
        for member, path in zip(libs, paths, strict=True):
            deps[member] = [
                soname for soname in resolver(path) if not _RUNTIME_SONAME.match(soname)
            ]
    return deps


def _lib_component(name: str, version: str | None, bundled: bool) -> dict[str, object]:
    """Build one bundled or platform library component."""
    component: dict[str, object] = {"type": "library", "name": name}
    if version is not None:
        component["version"] = version
    component["properties"] = [{"name": "kith:bundled", "value": str(bundled).lower()}]
    if not bundled:
        component["scope"] = "required"
    return component


def build_bom(
    name: str,
    version: str,
    libs: list[str],
    deps: dict[str, list[str]],
    hashes: dict[str, str] | None = None,
    provenance: dict[str, dict[str, str]] | None = None,
) -> dict[str, object]:
    """Assemble the CycloneDX document for the wheel and its bundle."""
    wheel_ref = f"pkg:pypi/{name}@{version}"
    # The attest step requires the serial though CycloneDX 1.6 makes it
    # optional; derive it from the purl.
    serial = f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_URL, wheel_ref)}"
    components: list[dict[str, object]] = []
    dependencies: list[dict[str, object]] = [{"ref": wheel_ref, "dependsOn": libs}]
    by_soname = {Path(member).name: member for member in libs}
    external: set[str] = set()
    for member in libs:
        details = (provenance or {}).get(member, {})
        lib_version = (
            version if Path(member).name.startswith("libkith_") else details.get("version")
        )
        component = _lib_component(Path(member).name, lib_version, bundled=True)
        component["bom-ref"] = member
        if hashes is not None:
            component["hashes"] = [{"alg": "SHA-256", "content": hashes[member]}]
        if details:
            component["properties"] = [
                {"name": f"kith:{key}", "value": value} for key, value in sorted(details.items())
            ] + [{"name": "kith:bundled", "value": "true"}]
        components.append(component)
        targets = sorted(by_soname.get(soname, soname) for soname in deps[member])
        dependencies.append({"ref": member, "dependsOn": targets})
        external.update(soname for soname in deps[member] if soname not in by_soname)
    for soname in sorted(external):
        component = _lib_component(soname, None, bundled=False)
        component["bom-ref"] = soname
        components.append(component)
    return {
        "bomFormat": BOM_FORMAT,
        "specVersion": SPEC_VERSION,
        "serialNumber": serial,
        "version": 1,
        "metadata": {
            "component": {
                "type": "library",
                "bom-ref": wheel_ref,
                "name": name,
                "version": version,
                "purl": wheel_ref,
            },
            "tools": {
                "components": [
                    {
                        "type": "application",
                        "name": "kith gen_release_sbom",
                        "bom-ref": GENERATOR_REF,
                    }
                ]
            },
        },
        "components": components,
        "dependencies": dependencies,
    }


def generate(wheel_path: Path, output_dir: Path) -> Path:
    """Write the SBOM for @p wheel_path into @p output_dir; return its path."""
    with zipfile.ZipFile(wheel_path) as wheel:
        metadata = _dist_info_metadata(wheel)
        name = _metadata_field(metadata, "Name")
        version = _metadata_field(metadata, "Version")
        libs = _bundled_libs(wheel)
        deps = _system_sonames(wheel, libs, _ldd_sonames)
        hashes = {member: hashlib.sha256(wheel.read(member)).hexdigest() for member in libs}
        manifests = [
            member for member in wheel.namelist() if member.endswith("/native/manifest.json")
        ]
        provenance = json.loads(wheel.read(manifests[0])) if manifests else {}
    bom = build_bom(name, version, libs, deps, hashes, provenance)
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"{name}-{version}.cdx.json"
    output.write_text(json.dumps(bom, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and generate the SBOM."""
    parser = argparse.ArgumentParser(
        description="Generate the CycloneDX SBOM for a kith release wheel."
    )
    parser.add_argument("wheel", type=Path, help="the release wheel to describe")
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="directory for the .cdx.json"
    )
    args = parser.parse_args(argv)
    output = generate(args.wheel, args.output_dir)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

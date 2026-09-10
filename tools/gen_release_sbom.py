"""Generate the CycloneDX SBOM for a kith release wheel.

The release SBOM describes the composition of the shipped binary artifact:
the wheel itself, the C shared libraries bundled inside it under
kith/_libs/, and the system libraries those bundle members dynamically
link against (declared dependencies, not shipped). The generator reads
the wheel and runs ldd on its bundle members; nothing else informs the
output, so the same wheel bytes always produce the same SBOM bytes.

Output is CycloneDX 1.6 JSON named per the OSSF SBOM naming convention
(kith-fw-<version>.cdx.json) and is attached to the release draft by the
release pipeline.
"""

from __future__ import annotations

import argparse
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

# The C runtime family every Linux system provides. ldd reports these for
# every bundle member; they are environment, not composition.
_RUNTIME_SONAME = re.compile(
    r"^(ld-linux|libc\.|libm\.|librt\.|libdl\.|libpthread\.|libgcc_s|"
    r"libstdc\+\+|libresolv|libnsl|libcrypt)"
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
    """Return the SONAME-form filenames bundled under kith/_libs/, sorted."""
    prefix = f"{BUNDLE_DIR}/"
    names = [
        n[len(prefix) :] for n in wheel.namelist() if n.startswith(prefix) and not n.endswith("/")
    ]
    return sorted(names)


def _ldd_sonames(lib: Path) -> list[str]:
    """Return the sonames @p lib links against, filtered to library lines."""
    result = subprocess.run(["ldd", str(lib)], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        return []
    sonames = []
    for line in result.stdout.splitlines():
        match = re.match(r"^\s*(\S+)\s+=>", line)
        if match:
            sonames.append(match.group(1))
    return sorted(set(sonames))


def _extract_bundle(wheel: zipfile.ZipFile, members: list[str], target: Path) -> list[Path]:
    """Extract the bundle members into @p target and return their paths."""
    paths = []
    for member in members:
        wheel.extract(f"{BUNDLE_DIR}/{member}", target)
        paths.append(target / BUNDLE_DIR / member)
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
                soname
                for soname in resolver(path)
                if not _RUNTIME_SONAME.match(soname) and not soname.startswith("libkith_")
            ]
    return deps


def _lib_component(name: str, version: str | None, bundled: bool) -> dict[str, object]:
    """Build one library component; bundled members carry the wheel version."""
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
) -> dict[str, object]:
    """Assemble the CycloneDX document for the wheel and its bundle."""
    wheel_ref = f"pkg:pypi/{name}@{version}"
    # The attest step requires the serial though CycloneDX 1.6 makes it
    # optional; derive it from the purl.
    serial = f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_URL, wheel_ref)}"
    components: list[dict[str, object]] = []
    dependencies: list[dict[str, object]] = [{"ref": wheel_ref, "dependsOn": libs}]
    for member in libs:
        lib_name = member[: -len(".so.1")] if member.endswith(".so.1") else member
        components.append(_lib_component(lib_name, version, bundled=True))
        if deps[member]:
            dependencies.append({"ref": member, "dependsOn": sorted(deps[member])})
        for soname in deps[member]:
            if soname not in [c["name"] for c in components]:
                components.append(_lib_component(soname, None, bundled=False))
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
    bom = build_bom(name, version, libs, deps)
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

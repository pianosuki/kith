"""Artifact composition and reference integrity checks for the release SBOM."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
import tools.gen_release_sbom as sbom


def test_bom_tracks_grafted_components_and_direct_edges() -> None:
    framework = 'kith/_libs/libkith_state.so.1'
    hiredis = 'kith_fw.libs/libhiredis-12345678.so.1.1.0'
    bom = sbom.build_bom(
        'kith-fw',
        '1.0.1',
        [framework, hiredis],
        {framework: ['libhiredis-12345678.so.1.1.0'], hiredis: []},
        {framework: 'a' * 64, hiredis: 'b' * 64},
        {hiredis: {'version': '1.2.0-6', 'package': 'libhiredis1.1.0'}},
    )
    components = bom['components']
    assert isinstance(components, list)
    by_ref = {component['bom-ref']: component for component in components}
    assert by_ref[framework]['version'] == '1.0.1'
    assert by_ref[hiredis]['version'] == '1.2.0-6'
    assert by_ref[hiredis]['hashes'] == [{'alg': 'SHA-256', 'content': 'b' * 64}]
    graph = bom['dependencies']
    assert isinstance(graph, list)
    assert {edge['ref']: edge['dependsOn'] for edge in graph}[framework] == [hiredis]
    refs = set(by_ref) | {'pkg:pypi/kith-fw@1.0.1'}
    assert all(edge['ref'] in refs for edge in graph)
    assert all(target in refs for edge in graph for target in edge['dependsOn'])


def test_bundled_objects_are_discovered_outside_framework_directory(tmp_path: Path) -> None:
    wheel_path = tmp_path / 'sample.whl'
    with zipfile.ZipFile(wheel_path, 'w') as wheel:
        wheel.writestr('kith/_libs/libkith_state.so.1', b'\x7fELFframework')
        wheel.writestr('kith_fw.libs/libhiredis-12345678.so.1.1.0', b'\x7fELFhiredis')
        wheel.writestr('kith_fw-1.0.1.dist-info/licenses/native/copyright', b'notice')
    with zipfile.ZipFile(wheel_path) as wheel:
        assert sbom._bundled_libs(wheel) == [
            'kith/_libs/libkith_state.so.1',
            'kith_fw.libs/libhiredis-12345678.so.1.1.0',
        ]


def test_dependency_scanner_rejects_invalid_elf(tmp_path: Path) -> None:
    invalid = tmp_path / 'invalid.so'
    invalid.write_bytes(b'\x7fELFbroken')
    with pytest.raises(subprocess.CalledProcessError):
        sbom._ldd_sonames(invalid)


def test_generate_uses_artifact_hashes_and_is_deterministic(tmp_path: Path) -> None:
    wheel_path = tmp_path / 'sample.whl'
    member = 'kith/_libs/libkith_util.so.1'
    native = Path(sys.executable).read_bytes()
    with zipfile.ZipFile(wheel_path, 'w') as wheel:
        wheel.writestr('kith_fw-1.0.1.dist-info/METADATA', 'Name: kith-fw\nVersion: 1.0.1\n')
        wheel.writestr(member, native)
    first = sbom.generate(wheel_path, tmp_path / 'out').read_bytes()
    second = sbom.generate(wheel_path, tmp_path / 'out').read_bytes()
    assert first == second
    bom = json.loads(first)
    assert bom['components'][0]['hashes'][0]['content'] == hashlib.sha256(native).hexdigest()

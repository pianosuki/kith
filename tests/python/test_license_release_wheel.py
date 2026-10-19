"""Redistribution notices must cover every grafted library and wheel record."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest
import tools.license_release_wheel as notices


def _repaired_wheel(tmp_path: Path) -> Path:
    wheel_path = tmp_path / 'sample.whl'
    with zipfile.ZipFile(wheel_path, 'w') as wheel:
        wheel.writestr('kith_fw-1.0.1.dist-info/METADATA', 'Name: kith-fw\nVersion: 1.0.1\n')
        wheel.writestr('kith_fw-1.0.1.dist-info/RECORD', 'stale')
        wheel.writestr('kith/', b'')
        wheel.writestr('kith_fw.libs/libssl-12345678.so.3', b'\x7fELFsample')
    return wheel_path


def test_notices_cover_grafts_and_rebuild_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wheel_path = _repaired_wheel(tmp_path)
    original = tmp_path / 'libssl.so.3'
    original.write_bytes(b'original')
    monkeypatch.setattr(notices, '_linked_paths', lambda _: {'libssl.so.3': original})
    monkeypatch.setattr(notices, '_package_notice', lambda _: ('libssl3', '3.0.13', b'copyright'))
    notices.attach_notices(tmp_path / 'raw.whl', wheel_path, tmp_path)
    with zipfile.ZipFile(wheel_path) as archive:
        manifest_path = next(name for name in archive.namelist() if name.endswith('manifest.json'))
        manifest = json.loads(archive.read(manifest_path))
        details = manifest['kith_fw.libs/libssl-12345678.so.3']
        assert archive.read(details['copyright']) == b'copyright'
        assert details['version'] == '3.0.13'
        record = archive.read('kith_fw-1.0.1.dist-info/RECORD').decode()
        rows = list(csv.reader(io.StringIO(record)))
        assert len(rows) == len(archive.namelist())
        assert 'kith/' not in archive.namelist()
        for name, digest, size in rows:
            if name.endswith('/RECORD'):
                assert digest == size == ''
                continue
            data = archive.read(name)
            expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()
            assert digest == f'sha256={expected}'
            assert size == str(len(data))


def test_unknown_graft_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    wheel_path = _repaired_wheel(tmp_path)
    monkeypatch.setattr(notices, '_linked_paths', lambda _: {})
    before = wheel_path.read_bytes()
    with pytest.raises(ValueError, match='cannot identify original'):
        notices.attach_notices(tmp_path / 'raw.whl', wheel_path, tmp_path)
    assert wheel_path.read_bytes() == before


def test_graft_real_filename_resolves_to_original_soname(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wheel_path = tmp_path / 'versioned.whl'
    with zipfile.ZipFile(wheel_path, 'w') as wheel:
        wheel.writestr('kith_fw-1.0.1.dist-info/METADATA', 'Name: kith-fw\nVersion: 1.0.1\n')
        wheel.writestr('kith_fw.libs/liburing-12345678.so.2.15', b'\x7fELFsample')
    real_library = tmp_path / 'liburing.so.2.15'
    real_library.write_bytes(b'original')
    soname_link = tmp_path / 'liburing.so.2'
    soname_link.symlink_to(real_library)
    monkeypatch.setattr(notices, '_linked_paths', lambda _: {'liburing.so.2': soname_link})
    monkeypatch.setattr(notices, '_package_notice', lambda _: ('liburing2', '2.15', b'notice'))
    notices.attach_notices(tmp_path / 'raw.whl', wheel_path, tmp_path)
    with zipfile.ZipFile(wheel_path) as wheel:
        manifest_path = next(name for name in wheel.namelist() if name.endswith('manifest.json'))
        manifest = json.loads(wheel.read(manifest_path))
        assert manifest['kith_fw.libs/liburing-12345678.so.2.15']['soname'] == 'liburing.so.2'

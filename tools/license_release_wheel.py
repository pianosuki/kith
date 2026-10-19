"""Attach the build host's redistribution notices to a repaired release wheel."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
import re
import subprocess
import tempfile
import zipfile
from pathlib import Path


def _linked_paths(raw_wheel: Path) -> dict[str, Path]:
    """Resolve the raw wheel's complete dependency closure on the build host."""
    paths: dict[str, Path] = {}
    with tempfile.TemporaryDirectory() as temporary, zipfile.ZipFile(raw_wheel) as archive:
        archive.extractall(temporary)
        for member in archive.namelist():
            if not member.startswith('kith/_libs/') or '.so' not in member:
                continue
            result = subprocess.run(
                ['ldd', str(Path(temporary) / member)],
                check=True,
                capture_output=True,
                text=True,
            )
            for line in result.stdout.splitlines():
                if '=> not found' in line:
                    raise ValueError(f'unresolved native dependency: {line.strip()}')
                match = re.match(r'\s*(\S+) => (/\S+)', line)
                if match:
                    paths[match[1]] = Path(match[2])
    return paths


def _package_notice(path: Path) -> tuple[str, str, bytes]:
    """Find the Ubuntu package and full copyright notice owning a library."""
    candidates = [path, path.resolve()]
    for candidate in list(candidates):
        if str(candidate).startswith('/usr/lib/'):
            candidates.append(Path(str(candidate).removeprefix('/usr')))
    for candidate in candidates:
        result = subprocess.run(
            ['dpkg-query', '-S', str(candidate)], capture_output=True, text=True, check=False
        )
        if result.returncode:
            continue
        package = result.stdout.split(': ', 1)[0]
        notice = Path('/usr/share/doc') / package.split(':', 1)[0] / 'copyright'
        if not notice.is_file():
            raise ValueError(f'missing copyright notice for {package}: {notice}')
        version = subprocess.check_output(
            ['dpkg-query', '-W', '-f=${Version}', package], text=True
        ).strip()
        return package, version, notice.read_bytes()
    raise ValueError(f'no Ubuntu package owns bundled dependency {path}')


def attach_notices(raw_wheel: Path, repaired_wheel: Path, source_root: Path) -> None:
    """Embed notices and provenance for every dependency grafted by auditwheel."""
    paths = _linked_paths(raw_wheel)
    with zipfile.ZipFile(repaired_wheel) as archive:
        contents = {
            member: archive.read(member)
            for member in archive.namelist()
            if not member.endswith('/')
        }
    metadata = [name for name in contents if name.endswith('.dist-info/METADATA')]
    if len(metadata) != 1:
        raise ValueError('expected exactly one wheel metadata directory')
    dist_info = metadata[0].rsplit('/', 1)[0]
    prefix = f'{dist_info}/licenses/native'
    manifest: dict[str, object] = {}
    grafts = [name for name in contents if '.libs/' in name and '.so' in Path(name).name]
    if not grafts:
        raise ValueError('repaired wheel contains no grafted dependencies')
    for member in sorted(grafts):
        filename = re.sub(r'-[0-9a-f]{8}(?=\.so)', '', Path(member).name)
        matched = next(
            (
                (soname, path)
                for soname, path in paths.items()
                if soname == filename or path.resolve().name == filename
            ),
            None,
        )
        if matched is None:
            raise ValueError(f'cannot identify original dependency for {member}')
        soname, original = matched
        allowed = {
            'libpq.so.5',
            'liburing.so.2',
            'libhiredis.so.1.5.0',
            'libssl.so.3',
            'libcrypto.so.3',
        }
        if soname not in allowed:
            raise ValueError(f'unreviewed release dependency: {soname}')
        if soname in {'libpq.so.5', 'libhiredis.so.1.5.0'}:
            is_postgres = soname == 'libpq.so.5'
            prefix_variable = (
                'KITH_RELEASE_LIBPQ_PREFIX' if is_postgres else 'KITH_RELEASE_HIREDIS_PREFIX'
            )
            native_prefix = Path(os.environ[prefix_variable]).resolve()
            if not original.resolve().is_relative_to(native_prefix):
                raise ValueError(f'{soname} does not come from the release prefix: {original}')
            directory = 'kith-libpq' if is_postgres else 'kith-hiredis'
            provenance = native_prefix / 'share' / directory
            package = 'postgresql-source' if is_postgres else 'hiredis-source'
            version = (provenance / 'VERSION').read_text().strip()
            notice = (provenance / ('COPYRIGHT' if is_postgres else 'COPYING')).read_bytes()
        elif soname == 'liburing.so.2' and str(original).startswith('/usr/local/'):
            package = 'liburing-source'
            version = subprocess.check_output(
                ['pkg-config', '--modversion', 'liburing'], text=True
            ).strip()
            notice = (source_root / 'docs/third-party/MIT.txt').read_bytes()
        else:
            package, version, notice = _package_notice(original)
        notice_path = f'{prefix}/{package.replace(":", "_")}/copyright'
        contents[notice_path] = notice
        manifest[member] = {
            'soname': soname,
            'package': package,
            'version': version,
            'copyright': notice_path,
            'source_sha256': hashlib.sha256(original.read_bytes()).hexdigest(),
        }
        if soname in {'libpq.so.5', 'libhiredis.so.1.5.0'}:
            details = manifest[member]
            assert isinstance(details, dict)
            details['source_url'] = (provenance / 'SOURCE_URL').read_text().strip()
            details['source_archive_sha256'] = (provenance / 'SOURCE_SHA256').read_text().strip()
    # Debian copyright files can incorporate a common license by reference.
    common_directory = Path('/usr/share/common-licenses')
    if common_directory.is_dir():
        for common in sorted(common_directory.iterdir()):
            if common.is_file():
                contents[f'{prefix}/common-licenses/{common.name}'] = common.read_bytes()
    contents[f'{prefix}/manifest.json'] = (
        json.dumps(manifest, indent=2, sort_keys=True) + '\n'
    ).encode()
    record = f'{dist_info}/RECORD'
    contents.pop(record, None)
    rows = io.StringIO(newline='')
    writer = csv.writer(rows, lineterminator='\n')
    for member, data in sorted(contents.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()
        writer.writerow([member, f'sha256={digest}', str(len(data))])
    writer.writerow([record, '', ''])
    contents[record] = rows.getvalue().encode()
    with zipfile.ZipFile(repaired_wheel, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for member, data in sorted(contents.items()):
            archive.writestr(member, data)


def main() -> None:
    """Collect the build host's notices into the repaired wheel."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('raw_wheel', type=Path)
    parser.add_argument('repaired_wheel', type=Path)
    args = parser.parse_args()
    attach_notices(args.raw_wheel, args.repaired_wheel, Path(__file__).resolve().parents[1])


if __name__ == '__main__':
    main()

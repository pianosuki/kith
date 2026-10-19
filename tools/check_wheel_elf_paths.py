"""Reject native wheel search paths that escape the package or search CWD."""

from __future__ import annotations

import argparse
import posixpath
import re
import subprocess
import tempfile
import zipfile
from pathlib import Path


def validate_paths(member: str, dynamic_section: str) -> None:
    """Validate a wheel object's loader paths and direct dependency names."""
    for search_path in re.findall(r'\((?:RUNPATH|RPATH)\).*?\[([^]]*)\]', dynamic_section):
        for component in search_path.split(':'):
            if component != '$ORIGIN' and not component.startswith('$ORIGIN/'):
                raise ValueError(f'{member}: unsafe loader search component {component!r}')
            suffix = component.removeprefix('$ORIGIN').lstrip('/')
            destination = posixpath.normpath(
                posixpath.join('/wheel', posixpath.dirname(member), suffix)
            )
            if destination != '/wheel' and not destination.startswith('/wheel/'):
                raise ValueError(f'{member}: loader path escapes the wheel: {component}')
    for dependency in re.findall(r'\(NEEDED\).*?\[([^]]*)\]', dynamic_section):
        if '/' in dependency:
            raise ValueError(f'{member}: absolute or relative dependency path: {dependency}')


def check(wheel_path: Path) -> int:
    """Inspect every ELF object's actual dynamic section inside a wheel."""
    count = 0
    with zipfile.ZipFile(wheel_path) as archive, tempfile.TemporaryDirectory() as temporary:
        for member in archive.namelist():
            if archive.read(member)[:4] != b'\x7fELF':
                continue
            path = Path(archive.extract(member, temporary))
            dynamic_section = subprocess.check_output(['readelf', '-d', str(path)], text=True)
            validate_paths(member, dynamic_section)
            count += 1
    if not count:
        raise ValueError(f'{wheel_path}: no ELF objects found')
    return count


def main() -> None:
    """Check the native loader paths of a built or repaired wheel."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('wheel', type=Path)
    args = parser.parse_args()
    count = check(args.wheel)
    print(f'wheel ELF paths: {count} objects, package-relative search only')


if __name__ == '__main__':
    main()

"""Reject unsafe loader paths in actual ELF dynamic-section text."""

from __future__ import annotations

import pytest
import tools.check_wheel_elf_paths as checker


@pytest.mark.parametrize('value', ['', ':', '$ORIGIN:', ':$ORIGIN', '/usr/local/lib', '.', '$LIB'])
def test_current_directory_and_host_search_paths_are_rejected(value: str) -> None:
    with pytest.raises(ValueError, match='unsafe loader search'):
        checker.validate_paths(
            'kith/_libs/libkith_util.so.1', f'(RUNPATH) Library runpath: [{value}]'
        )


def test_private_graft_relative_path_is_allowed() -> None:
    checker.validate_paths(
        'kith/_libs/libkith_state.so.1',
        '(RPATH) Library rpath: [$ORIGIN:$ORIGIN/../../kith_fw.libs]',
    )


def test_origin_path_cannot_escape_wheel() -> None:
    with pytest.raises(ValueError, match='escapes the wheel'):
        checker.validate_paths(
            'kith/_libs/libkith_util.so.1', '(RUNPATH) [$ORIGIN/../../../outside]'
        )


def test_needed_entries_cannot_name_host_files() -> None:
    with pytest.raises(ValueError, match='dependency path'):
        checker.validate_paths(
            'kith/_libs/libkith_state.so.1', '(NEEDED) [/usr/local/lib/libhiredis.so]'
        )

"""Shared build-availability gate for the tests/python modules.

Importable from the test files via the ``pythonpath`` and ``mypy_path``
entries that include ``tests/python``. The module name is not
``_helpers``: both pythonpath roots would then expose the same top-level
name and the first import would win ``sys.modules``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[2]
_BUILD_DEBUG = Path(os.environ.get("KITH_BUILD_DIR", str(_REPO_ROOT / "build" / "debug"))).resolve()


def _debug_libs_present() -> bool:
    return (_BUILD_DEBUG / "libkith_server.so").is_file()


needs_build = pytest.mark.skipif(
    not _debug_libs_present(),
    reason="debug build libraries not present; run 'cmake --build build/debug'",
)

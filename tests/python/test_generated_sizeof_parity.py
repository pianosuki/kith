"""Exhaustive ctypes-to-C sizeof parity over the size-versioned structs.

The drift checker proves the checked-in bindings are exactly what the
generator produces from the current headers — bindings equal headers.
What nothing else pins is ctypes layout equal to the C compiler's
layout: a builtin-map mistake or a padding assumption would be
self-consistent under the regeneration diff and surface only when the
C core reads a caller-filled buffer at its own offsets.

This module runs the ``sizeof_probe`` instrument (built from the public
headers alone, no library linkage) and asserts, for every size-versioned
struct the bindings carry, that ``ctypes.sizeof`` matches the C
compiler's ``sizeof``. The struct set is discovered from ``_generated``
— every structure whose fields open with the size/version pair — so a
new size-versioned struct joins the assertions automatically, and fails
them until the probe learns its name: the probe's set must equal the
bindings' set in both directions.
"""

from __future__ import annotations

import ctypes
import importlib
import os
import pkgutil
import subprocess
from pathlib import Path

import pytest

import kith._generated as gen_pkg


_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROBE_RELATIVE = ("tools/sizeof_probe", "sizeof_probe")


def _build_roots() -> list[Path]:
    """Return the build trees the probe may live in, most explicit first."""
    roots: list[Path] = []
    override = os.environ.get("KITH_BUILD_DIR")
    if override:
        roots.append(Path(override).resolve())
    lib = os.environ.get("KITH_LIB")
    if lib:
        lib_path = Path(lib).resolve()
        roots += [lib_path, lib_path.parent]
    roots += [
        (_REPO_ROOT / "build" / "debug").resolve(),
        (_REPO_ROOT / "build" / "release").resolve(),
    ]
    unique: list[Path] = []
    for root in roots:
        if root not in unique:
            unique.append(root)
    return unique


def _probe_path() -> Path | None:
    """Locate the built ``sizeof_probe`` instrument, or None."""
    for root in _build_roots():
        for relative in _PROBE_RELATIVE:
            candidate = root / relative
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return candidate
    return None


def _is_size_versioned(struct: type[ctypes.Structure]) -> bool:
    """Return True when the structure opens with the size/version pair."""
    fields = getattr(struct, "_fields_", None)
    return (
        fields is not None
        and len(fields) >= 2
        and fields[0][0] == "size"
        and fields[0][1] is ctypes.c_uint32
        and fields[1][0] == "abi_version"
        and fields[1][1] is ctypes.c_uint32
    )


def _size_versioned_structs() -> dict[str, type[ctypes.Structure]]:
    """Discover the size-versioned structs across the generated bindings.

    Keyed by the C typedef name: the generator names each structure class
    after its struct tag and aliases ``<tag>_t`` to it, so the class's
    own binding (the one whose name equals the class name) maps to the
    typedef the probe reports by appending ``_t``.
    """
    found: dict[str, type[ctypes.Structure]] = {}
    for info in pkgutil.iter_modules(gen_pkg.__path__):
        module = importlib.import_module(f"kith._generated.{info.name}")
        for name, obj in vars(module).items():
            if (
                isinstance(obj, type)
                and issubclass(obj, ctypes.Structure)
                and obj.__module__ == module.__name__
                and obj.__name__ == name
                and _is_size_versioned(obj)
            ):
                found[f"{name}_t"] = obj
    return found


_BINDINGS = _size_versioned_structs()
_PROBE = _probe_path()

needs_probe = pytest.mark.skipif(
    _PROBE is None,
    reason="sizeof_probe not built; run 'cmake --build build/debug'",
)


@pytest.fixture(scope="module")
def c_sizes() -> dict[str, int]:
    """The C compiler's sizeof for every struct the probe reports."""
    assert _PROBE is not None
    output = subprocess.run(
        [_PROBE],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout
    sizes: dict[str, int] = {}
    for line in output.splitlines():
        name, _, raw = line.partition(" ")
        sizes[name] = int(raw)
    return sizes


class TestSizeofParity:
    @needs_probe
    def test_probe_covers_the_binding_set(self, c_sizes: dict[str, int]) -> None:
        # The probe's list and the bindings' discovered set must match in
        # both directions: a struct the bindings carry but the probe does
        # not print silently leaves that struct unasserted, and a probe
        # name absent from the bindings is a stale instrument.
        probe_names = set(c_sizes)
        binding_names = set(_BINDINGS)
        assert probe_names == binding_names, (
            "size-versioned struct sets diverged:"
            f" probe-only {sorted(probe_names - binding_names)},"
            f" binding-only {sorted(binding_names - probe_names)}"
        )

    @needs_probe
    @pytest.mark.parametrize("c_name", sorted(_BINDINGS))
    def test_ctypes_sizeof_matches_c(self, c_sizes: dict[str, int], c_name: str) -> None:
        struct = _BINDINGS[c_name]
        assert ctypes.sizeof(struct) == c_sizes[c_name], (
            f"{c_name}: ctypes layout {ctypes.sizeof(struct)} bytes !="
            f" C compiler layout {c_sizes[c_name]} bytes"
        )

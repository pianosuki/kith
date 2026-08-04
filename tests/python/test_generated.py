"""Unit tests for the generated ctypes bindings.

The generated package is checked against the public headers it mirrors: every
``KITH_API`` function in a header has an entry in the module's ``_FUNCTIONS``
table; the ABI version constant is present; and ``configure`` attaches
``argtypes``/``restype`` to the loaded libraries.
"""

from __future__ import annotations

import ctypes
import importlib
import re
import tempfile
from pathlib import Path
from types import ModuleType

import pytest
import tools.gen_ctypes as gen_ctypes
from _build_gate import _BUILD_DEBUG, _REPO_ROOT, needs_build

from kith._generated import configure as configure_all
from kith._generated import version as gen_version


_INCLUDE_ROOT = _REPO_ROOT / "include"


# ---------------------------------------------------------------------------
# header scanning (independent of the generator's libclang walk)
# ---------------------------------------------------------------------------


def _strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    text = re.sub(r"//[^\n]*", "", text)
    return text


def _count_kith_api(header: Path) -> int:
    """Count ``KITH_API`` function declarations in a header by text scan.

    Independent of the generator's libclang traversal so a generator defect
    that drops a function is caught here rather than mirrored in the count.
    """
    text = _strip_comments(header.read_text(encoding="utf-8"))
    count = 0
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#define") and "KITH_API" in stripped:
            continue
        count += len(re.findall(r"\bKITH_API\b", line))
    return count


def _headers_for_module(module: str) -> list[Path]:
    nested = sorted((_INCLUDE_ROOT / "kith" / module).rglob("*.h"))
    if nested:
        return nested
    top = _INCLUDE_ROOT / "kith" / f"{module}.h"
    return [top] if top.is_file() else []


def _function_names_in_module(mod: ModuleType) -> list[str]:
    table = getattr(mod, "_FUNCTIONS", None)
    if table is None:
        return []
    return [entry[0] for entry in table]


_MODULES_WITH_HEADERS = [m for m in gen_ctypes._MODULE_ORDER if _headers_for_module(m)]


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


class TestAbiVersion:
    def test_abi_version_is_one(self) -> None:
        assert gen_version.KITH_ABI_VERSION == 1

    def test_abi_version_matches_bridge_expected(self) -> None:
        from kith._bridge import EXPECTED_ABI_VERSION

        assert EXPECTED_ABI_VERSION == gen_version.KITH_ABI_VERSION


class TestFunctionCoverage:
    @pytest.mark.parametrize("module", _MODULES_WITH_HEADERS)
    def test_every_api_function_has_a_table_entry(self, module: str) -> None:
        header_count = sum(_count_kith_api(header) for header in _headers_for_module(module))
        gen_mod = importlib.import_module(f"kith._generated.{module}")
        gen_names = _function_names_in_module(gen_mod)
        assert len(gen_names) == header_count, (
            f"{module}: header declares {header_count} KITH_API functions but "
            f"_FUNCTIONS has {len(gen_names)} entries"
        )

    def test_decayed_array_parameter_maps_to_buffer_pointer(self) -> None:
        from kith._generated import proto as gen_proto

        # ``char buf[17]`` decays to ``char *`` at the ABI: the generated
        # argtype is the buffer pointer, never the single-byte element type.
        entry = next(e for e in gen_proto._FUNCTIONS if e[0] == "kith_proto_correlation_hex")
        assert entry[2] == [ctypes.c_uint64, ctypes.c_char_p]

    def test_no_single_byte_argtypes_in_any_module(self) -> None:
        # The public surface has no scalar char parameters — char appears
        # only as pointers and decayed arrays, both mapping to c_char_p —
        # so a bare c_char argtype is a decayed-array mapping defect.
        for module in _MODULES_WITH_HEADERS:
            mod = importlib.import_module(f"kith._generated.{module}")
            for name, _restype, argtypes in getattr(mod, "_FUNCTIONS", []):
                assert ctypes.c_char not in argtypes, f"{module}.{name} declares c_char"


@needs_build
class TestConfigure:
    @pytest.fixture(autouse=True)
    def _isolate_bridge(self, monkeypatch: pytest.MonkeyPatch) -> object:
        monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
        from kith._bridge import reset

        reset()
        yield
        reset()

    def test_configure_attaches_version_symbols(self) -> None:
        from kith._bridge import load

        bridge = load()
        configure_all(bridge)
        util = bridge.lib("util")
        assert util.kith_version_abi.argtypes == []
        assert util.kith_version_abi.restype == ctypes.c_uint
        assert util.kith_version_string.restype == ctypes.c_char_p


class TestGeneratorRoundTrip:
    def test_regenerate_matches_checked_in(self) -> None:
        # Regenerating into a fresh scratch dir must produce byte-identical output
        # to the checked-in bindings.
        checked_in = _REPO_ROOT / "python" / "kith" / "_generated"
        with tempfile.TemporaryDirectory(prefix="kith-gen-roundtrip-") as tmp:
            tmp_out = Path(tmp) / "_generated"
            gen_ctypes.generate(_INCLUDE_ROOT, tmp_out)
            checked_files = {p.relative_to(checked_in) for p in checked_in.rglob("*.py")}
            tmp_files = {p.relative_to(tmp_out) for p in tmp_out.rglob("*.py")}
            assert checked_files == tmp_files
            for rel in sorted(checked_files):
                a = (checked_in / rel).read_bytes()
                b = (tmp_out / rel).read_bytes()
                assert a == b, f"{rel} differs after regeneration"

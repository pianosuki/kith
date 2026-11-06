"""Unit tests for the generated ctypes bindings.

The generated package is checked against the public headers it mirrors: every
``KITH_API`` function in a header has an entry in the module's ``_FUNCTIONS``
table; a representative exposed-layout struct's ``_fields_`` matches the
header's field count; the ABI version constant is present; and ``configure``
attaches ``argtypes``/``restype`` to the loaded libraries. The drift checker
is also self-tested: it returns 0 against the checked-in bindings and would
return 1 against a tampered set.
"""

from __future__ import annotations

import ctypes
import importlib
import re
import tempfile
from pathlib import Path
from types import ModuleType

import pytest
import tools.check_ctypes_drift as drift
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


def _api_function_names(header: Path) -> set[str]:
    """Read public function names independently of the generator's AST walk.

    Independent of the generator's libclang traversal so a generator defect
    that substitutes or drops a function cannot pass by retaining the count.
    """
    text = _strip_comments(header.read_text(encoding="utf-8"))
    lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#define") and "KITH_API" in stripped:
            continue
        lines.append(line)
    return set(re.findall(r"\bKITH_API\b[^;]*?\b(kith_\w+)\s*\(", "\n".join(lines)))


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


@pytest.fixture
def small_bindings(tmp_path: Path) -> tuple[Path, Path]:
    """Generate one public declaration for drift-checker contract tests."""
    include = tmp_path / "include"
    headers = include / "kith" / "proto"
    headers.mkdir(parents=True)
    (headers / "proto.h").write_text(
        '#define KITH_API __attribute__((visibility("default")))\n'
        "KITH_API int kith_fixture_probe(void);\n",
        encoding="utf-8",
    )
    generated = tmp_path / "_generated"
    gen_ctypes.generate(include, generated)
    return include, generated


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
    def test_every_module_in_order_has_headers(self) -> None:
        """A module with no header directory drops out of the
        coverage parametrization and 0 == 0 passes silently; the generator's
        module order and the header tree must agree."""
        missing = [m for m in gen_ctypes._MODULE_ORDER if not _headers_for_module(m)]
        assert not missing, f"modules without headers under {_INCLUDE_ROOT}: {missing}"

    @pytest.mark.parametrize("module", _MODULES_WITH_HEADERS)
    def test_every_api_function_has_a_table_entry(self, module: str) -> None:
        header_names = {
            name for header in _headers_for_module(module) for name in _api_function_names(header)
        }
        gen_mod = importlib.import_module(f"kith._generated.{module}")
        gen_names = _function_names_in_module(gen_mod)
        assert set(gen_names) == header_names, (
            f"{module}: missing={header_names - set(gen_names)}, "
            f"unexpected={set(gen_names) - header_names}"
        )
        assert len(gen_names) == len(header_names), f"{module}: duplicate function entries"

    def test_server_function_names_match_header(self) -> None:
        from kith._generated import server as gen_server

        expected = {
            "kith_server_create",
            "kith_server_destroy",
            "kith_server_run",
            "kith_server_shutdown",
            "kith_server_status",
            "kith_server_listen_port",
            "kith_server_control_port",
            "kith_server_register_tick_handler",
            "kith_server_unregister_tick_handler",
            "kith_server_register_poll_observer",
            "kith_server_tick_drops",
            "kith_server_gateway",
            "kith_server_sim",
            "kith_server_fabric",
            "kith_server_proto",
            "kith_server_control",
            "kith_server_db",
        }
        assert set(_function_names_in_module(gen_server)) == expected

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


class TestStructLayout:
    def test_server_params_has_twenty_two_fields(self) -> None:
        from kith._generated import server as gen_server

        fields = gen_server.kith_server_params._fields_
        # size + abi_version + config + topology + instance_id + listen_host +
        # listen_port + tick_hz + python_worker_count + handler_table_size +
        # replication_type_id + replication_batch_type_id + delivery_strategy +
        # delivery_config + delivery_worker_count + delivery_wait_budget_us +
        # self_echo_disabled + control_write_buffer_cap + view_max_subjects +
        # view_refresh_interval_ms + cache_refresh_interval_ms + reserved = 22
        assert len(fields) == 22
        names = [f[0] for f in fields]
        assert names[0] == "size"
        assert names[1] == "abi_version"
        assert names[-7:] == [
            "delivery_wait_budget_us",
            "self_echo_disabled",
            "control_write_buffer_cap",
            "view_max_subjects",
            "view_refresh_interval_ms",
            "cache_refresh_interval_ms",
            "reserved",
        ]

    def test_client_params_field_count_matches_header(self) -> None:
        from kith._generated import client as gen_client

        fields = gen_client.kith_client_params._fields_
        # size + abi_version + 8 tunables + reserved = 11
        assert len(fields) == 11

    def test_opaque_struct_has_no_fields(self) -> None:
        from kith._generated import server as gen_server

        # kith_server is an opaque handle: no _fields_ assigned.
        assert not hasattr(gen_server.kith_server, "_fields_") or not getattr(
            gen_server.kith_server, "_fields_", None
        )


class TestCrossModuleImports:
    def test_client_imports_logger_handle(self) -> None:
        from kith._generated import client as gen_client

        # kith_logger_t is owned by the logger module; client references it
        # in kith_client_create's argtypes and imports it rather than
        # redeclaring a duplicate class.
        client_logger = vars(gen_client)["kith_logger_t"]
        logger_logger = vars(importlib.import_module("kith._generated.logger"))["kith_logger_t"]
        assert client_logger is logger_logger

    def test_no_duplicate_struct_class_across_modules(self) -> None:
        # A struct class declared in its owning module must not be
        # redeclared (as a distinct class object) in a module that only
        # forward-declares the opaque handle.
        for mod_name in ("client", "control"):
            mod = importlib.import_module(f"kith._generated.{mod_name}")
            # The module must not define its own kith_logger class; it imports
            # kith_logger_t from logger instead.
            assert "kith_logger" not in vars(mod), (
                f"{mod_name} redeclares kith_logger instead of importing it"
            )


@needs_build
class TestConfigure:
    @pytest.fixture(autouse=True)
    def _isolate_bridge(self, monkeypatch: pytest.MonkeyPatch) -> object:
        monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
        from kith._bridge import reset

        reset()
        yield
        reset()

    def test_configure_attaches_argtypes_and_restype(self) -> None:
        from kith._bridge import load

        bridge = load()
        configure_all(bridge)
        server = bridge.server()
        assert server.kith_server_create.argtypes is not None
        assert len(server.kith_server_create.argtypes) == 3
        assert server.kith_server_create.restype == ctypes.c_int
        assert server.kith_server_destroy.restype is None
        assert server.kith_server_status.restype == ctypes.c_uint32

    def test_configure_attaches_version_symbols(self) -> None:
        from kith._bridge import load

        bridge = load()
        configure_all(bridge)
        util = bridge.lib("util")
        assert util.kith_version_abi.argtypes == []
        assert util.kith_version_abi.restype == ctypes.c_uint
        assert util.kith_version_string.restype == ctypes.c_char_p

    def test_configure_idempotent(self) -> None:
        from kith._bridge import load

        bridge = load()
        configure_all(bridge)
        first = bridge.server().kith_server_run.argtypes
        configure_all(bridge)
        second = bridge.server().kith_server_run.argtypes
        assert first == second


class TestDriftChecker:
    def test_check_drift_returns_zero_when_current(self, small_bindings: tuple[Path, Path]) -> None:
        include, generated = small_bindings
        assert drift.check_drift(include, generated) == 0

    def test_check_drift_returns_one_on_tampered_file(
        self, small_bindings: tuple[Path, Path]
    ) -> None:
        include, generated = small_bindings
        module = generated / "proto.py"
        original = module.read_text(encoding="utf-8")
        tampered = original.replace("kith_fixture_probe", "kith_fixture_changed")
        assert tampered != original
        module.write_text(tampered, encoding="utf-8")
        assert drift.check_drift(include, generated) == 1

    def test_check_drift_refuses_a_missing_generated_dir(self, tmp_path: Path) -> None:
        # No generated bindings directory at all: a setup error, never a
        # silent pass — the checked-in bindings are required artifacts.
        assert drift.check_drift(_INCLUDE_ROOT, tmp_path / "missing") == 2


class TestGeneratorRoundTrip:
    def test_regenerate_matches_checked_in(self) -> None:
        # Regenerating into a fresh scratch dir must produce byte-identical output
        # to the checked-in bindings (the drift checker's guarantee, tested
        # directly against the generator entry point).
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


@pytest.mark.parametrize(
    ("bad_header", "diagnostic"),
    [
        ("int broken = ;\n", "expected expression"),
        ('#include "absent_fixture_header.h"\n', "file not found"),
    ],
)
def test_generator_rejects_parse_errors_before_writing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], bad_header: str, diagnostic: str
) -> None:
    include = tmp_path / "include"
    headers = include / "kith" / "proto"
    headers.mkdir(parents=True)
    (headers / "proto.h").write_text(
        '#define KITH_API __attribute__((visibility("default")))\n'
        "KITH_API int kith_fixture_probe(void);\n" + bad_header,
        encoding="utf-8",
    )
    generated = tmp_path / "generated"
    generated.mkdir()
    sentinel = generated / "proto.py"
    sentinel.write_text("existing bindings\n", encoding="utf-8")
    assert (
        gen_ctypes.main(["gen_ctypes.py", "--include-root", str(include), "--out", str(generated)])
        == 2
    )
    assert diagnostic in capsys.readouterr().err
    assert sentinel.read_text(encoding="utf-8") == "existing bindings\n"
    assert drift.check_drift(include, generated) == 2
    assert diagnostic in capsys.readouterr().err
    assert sorted(p.name for p in generated.iterdir()) == ["proto.py"]

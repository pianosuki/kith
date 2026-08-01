"""Unit tests for the ctypes bridge loader.

The tests point KITH_LIB at the debug build directory so discovery is
deterministic regardless of the pytest working directory, then reset the
bridge singleton around each test so cached state from a prior test cannot
leak in.
"""

from __future__ import annotations

import ctypes
import sys
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from _build_gate import _BUILD_DEBUG, _REPO_ROOT, needs_build

import kith
from kith import _bridge
from kith._bridge import (
    EXPECTED_ABI_VERSION,
    Bridge,
    load,
    reset,
)
from kith.exceptions import (
    ABIVersionError,
    BridgeError,
    BridgeLoadError,
)


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test."""
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


@needs_build
class TestDiscovery:
    def test_load_returns_bridge_with_individual_mode(self) -> None:
        bridge = load()
        assert isinstance(bridge, Bridge)
        assert bridge.mode == "individual"
        assert bridge.root == _BUILD_DEBUG

    def test_load_abi_version_matches_expected(self) -> None:
        bridge = load()
        assert bridge.abi_version == EXPECTED_ABI_VERSION
        assert bridge.abi_version >= 1

    def test_load_version_string_is_dotted_form(self) -> None:
        bridge = load()
        assert bridge.version_string.count(".") >= 2
        assert all(part.isdigit() for part in bridge.version_string.split("."))

    def test_kith_lib_env_overrides_build_dirs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sub = _BUILD_DEBUG
        monkeypatch.setenv("KITH_LIB", str(sub))
        reset()
        bridge = load()
        assert bridge.root == sub

    def test_kith_lib_env_relative_path_resolves(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(_REPO_ROOT)
        monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG.relative_to(_REPO_ROOT)))
        reset()
        bridge = load()
        assert bridge.root == _BUILD_DEBUG

    def test_kith_build_dir_env_is_first_candidate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KITH_LIB", raising=False)
        monkeypatch.setenv("KITH_BUILD_DIR", str(_BUILD_DEBUG))
        reset()
        bridge = load()
        assert bridge.root == _BUILD_DEBUG

    def test_kith_lib_invalid_directory_raises_load_error(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        empty = tmp_path / "empty"
        empty.mkdir()
        monkeypatch.setenv("KITH_LIB", str(empty))
        reset()
        with pytest.raises(BridgeLoadError, match="contains no kith"):
            load()

    def test_kith_lib_pointing_at_a_file_raises_load_error(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        marker = tmp_path / "not_a_dir"
        marker.write_text("x")
        monkeypatch.setenv("KITH_LIB", str(marker))
        reset()
        with pytest.raises(BridgeLoadError, match="not a directory"):
            load()


@needs_build
class TestHandles:
    def test_lib_returns_cdll_per_required_module(self) -> None:
        bridge = load()
        for name in ("util", "config", "server", "sim", "gateway", "client"):
            handle = bridge.lib(name)
            assert isinstance(handle, ctypes.CDLL)

    def test_server_handle_exposes_create_symbol(self) -> None:
        bridge = load()
        assert hasattr(bridge.server(), "kith_server_create")

    def test_util_handle_exposes_version_abi(self) -> None:
        bridge = load()
        util = bridge.lib("util")
        assert hasattr(util, "kith_version_abi")
        util.kith_version_abi.restype = ctypes.c_uint
        assert int(util.kith_version_abi()) == EXPECTED_ABI_VERSION

    def test_lib_unknown_name_raises_load_error(self) -> None:
        bridge = load()
        with pytest.raises(BridgeLoadError):
            bridge.lib("nonexistent")

    def test_lib_names_includes_all_required(self) -> None:
        bridge = load()
        names = set(bridge.lib_names())
        assert {"util", "server", "sim", "gateway", "client"}.issubset(names)

    def test_control_is_optional_and_present_in_debug_build(self) -> None:
        bridge = load()
        names = set(bridge.lib_names())
        assert "control" in names


@needs_build
class TestSingleton:
    def test_load_is_cached_singleton(self) -> None:
        first = load()
        second = load()
        assert first is second
        # The cache slot itself holds the first bridge: a second load
        # hits the cache rather than rebuilding and replacing it.
        assert _bridge._bridge is first

    def test_force_reloads_new_handle(self) -> None:
        first = load()
        second = load(force=True)
        assert first is not second
        assert second.abi_version == EXPECTED_ABI_VERSION

    def test_reset_clears_cache(self) -> None:
        first = load()
        reset()
        second = load()
        assert first is not second


@needs_build
class TestFreeThreadedSafety:
    """Concurrent bridge use under the free-threaded interpreter.

    Both tests run under the standard and free-threaded builds. Under the GIL
    they pass trivially because thread switching is cooperative and no two
    bodies overlap; under free-threaded Python (``python3.14t``) they exercise
    real cross-thread contention on the singleton's double-checked lock and
    concurrent ctypes dispatch into the C libraries. This guards the threading
    contract that the loader must stay safe without the GIL.
    """

    @staticmethod
    def _capture(
        barrier: threading.Barrier,
        fn: Callable[[], object],
        out: list[object],
        errs: list[BaseException | None],
        i: int,
    ) -> None:
        try:
            barrier.wait()
            out[i] = fn()
        except BaseException as exc:
            errs[i] = exc

    def test_concurrent_load_returns_one_singleton(self) -> None:
        reset()
        n = 16
        barrier = threading.Barrier(n)
        results: list[object] = [None] * n
        errs: list[BaseException | None] = [None] * n
        threads = [
            threading.Thread(
                target=self._capture,
                args=(barrier, load, results, errs, i),
                name=f"ft-load-{i}",
            )
            for i in range(n)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for exc in errs:
            assert exc is None, f"concurrent load raised: {exc!r}"
        first = results[0]
        assert isinstance(first, Bridge)
        for r in results:
            assert r is first, "concurrent load() returned distinct bridge objects"

    def test_concurrent_c_entry_point_calls_are_safe(self) -> None:
        bridge = load()
        util = bridge.lib("util")
        n = 16
        calls_per_thread = 200
        barrier = threading.Barrier(n)
        totals: list[int] = [0] * n
        errs: list[BaseException | None] = [None] * n

        def worker(i: int) -> None:
            try:
                barrier.wait()
                total = 0
                for _ in range(calls_per_thread):
                    total += int(util.kith_version_abi())
                totals[i] = total
            except BaseException as exc:
                errs[i] = exc

        threads = [
            threading.Thread(target=worker, args=(i,), name=f"ft-abicall-{i}") for i in range(n)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected = EXPECTED_ABI_VERSION * calls_per_thread
        for exc in errs:
            assert exc is None, f"concurrent C call raised: {exc!r}"
        for total in totals:
            assert total == expected, "concurrent kith_version_abi() returned a torn value"


class TestErrorHierarchy:
    def test_load_error_is_bridge_error_and_oserror(self) -> None:
        assert issubclass(BridgeLoadError, BridgeError)
        assert issubclass(BridgeLoadError, OSError)

    def test_abiversion_error_is_bridge_error(self) -> None:
        assert issubclass(ABIVersionError, BridgeError)
        assert not issubclass(ABIVersionError, OSError)

    def test_bridge_error_is_runtime_error(self) -> None:
        assert issubclass(BridgeError, RuntimeError)

    def test_bridge_family_is_public(self) -> None:
        assert BridgeError.__module__ == "kith.exceptions"
        assert kith.BridgeError is BridgeError


@needs_build
class TestABICheck:
    def test_abi_mismatch_raises_abiversion_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_bridge, "EXPECTED_ABI_VERSION", EXPECTED_ABI_VERSION + 1000)
        reset()
        with pytest.raises(ABIVersionError):
            load()


class TestPlatformSuffix:
    def test_lib_suffix_matches_platform(self) -> None:
        suffix = _bridge._lib_suffix()
        if sys.platform == "darwin":
            assert suffix == ".dylib"
        else:
            assert suffix == ".so"

    def test_soname_uses_versioned_form_on_linux(self) -> None:
        soname = _bridge._soname("util")
        if sys.platform == "darwin":
            assert soname == "libkith_util.dylib"
        else:
            assert soname == "libkith_util.so.1"


class TestPlatformGuard:
    def test_load_fails_clearly_off_linux(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "platform", "darwin")
        reset()
        with pytest.raises(BridgeLoadError) as excinfo:
            load()
        assert "Linux" in str(excinfo.value)
        assert "darwin" in str(excinfo.value)
        reset()

    def test_guard_precedes_discovery_off_linux(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setenv("KITH_LIB", str(tmp_path))
        reset()
        with pytest.raises(BridgeLoadError) as excinfo:
            load()
        assert "sys.platform" in str(excinfo.value)
        reset()


@needs_build
class TestEnvironment:
    def test_no_kith_lib_falls_back_to_build_dirs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("KITH_LIB", raising=False)
        reset()
        bridge = load()
        assert bridge.root == _BUILD_DEBUG

    def test_kith_lib_empty_string_ignored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KITH_LIB", "")
        reset()
        bridge = load()
        assert bridge.root == _BUILD_DEBUG


def test_module_does_not_load_on_import() -> None:
    """Importing the bridge module never triggers dlopen (deferred to load())."""
    reset()
    import importlib

    mod = importlib.import_module("kith._bridge")
    assert mod._bridge is None
    reset()

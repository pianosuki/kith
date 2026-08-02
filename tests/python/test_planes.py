"""Unit tests for the Config wrapper.

The tests point ``KITH_LIB`` at the debug build directory so the ctypes bridge
loads the freshly built shared libraries, then reset the bridge singleton
around each test so cached state from a prior test cannot leak in.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from _build_gate import _BUILD_DEBUG, needs_build

from kith import (
    Config,
    KithConfigError,
    KithError,
    KithNotFoundError,
    KithProtocolError,
    KithStateError,
)
from kith._bridge import reset
from kith._generated import types as gen_types


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


# ---------------------------------------------------------------------------
# exceptions
# ---------------------------------------------------------------------------


@needs_build
class TestExceptions:
    def test_hierarchy_families(self) -> None:
        assert issubclass(KithConfigError, KithError)
        assert issubclass(KithProtocolError, KithError)
        assert issubclass(KithStateError, KithError)
        assert issubclass(KithNotFoundError, KithError)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


@needs_build
class TestConfig:
    def test_builds_from_env_file(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("tick_hz = 30\nname = alpha\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg:
            assert cfg.has("tick_hz")
            assert cfg.get_u32("tick_hz", default=20) == 30
            assert cfg.get_string("name") == "alpha"

    def test_missing_key_returns_default(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("present = 1\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg:
            assert cfg.get_string("absent", default="fallback") == "fallback"
            assert cfg.get_bool("absent") is False
            assert cfg.get_u16("absent", default=7) == 7

    def test_bool_spellings(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("a = yes\nb = 0\nc = true\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg:
            assert cfg.get_bool("a") is True
            assert cfg.get_bool("b") is False
            assert cfg.get_bool("c") is True

    def test_range_violation_raises_config_error(self, tmp_path: Path) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("port = 99999\n", encoding="utf-8")
        with Config(file_path=env_file) as cfg:
            with pytest.raises(KithConfigError) as exc_info:
                cfg.get_u16("port", maximum=65535)
            assert exc_info.value.code == gen_types.kith_error.KITH_ERANGE

    def test_unreadable_file_raises_config_error(self, tmp_path: Path) -> None:
        with pytest.raises(KithConfigError) as exc_info:
            Config(file_path=tmp_path)  # a directory is not a readable file
        assert exc_info.value.code == gen_types.kith_error.KITH_EIO

    def test_env_prefix_overrides_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        env_file = tmp_path / "game.env"
        env_file.write_text("key = from_file\n", encoding="utf-8")
        monkeypatch.setenv("KITHTEST_key", "from_env")
        with Config(file_path=env_file, env_prefix="KITHTEST_") as cfg:
            assert cfg.get_string("key") == "from_env"

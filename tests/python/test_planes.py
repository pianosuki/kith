"""Unit tests for the per-plane Python submodules and the Config wrapper.

The tests point ``KITH_LIB`` at the debug build directory so the ctypes bridge
loads the freshly built shared libraries, then reset the bridge singleton
around each test so cached state from a prior test cannot leak in. Each plane
module wraps its generated binding's C surface into Python types; the tests
exercise the lifecycle, the wrapped operations, and the error translation at
the boundary (the exception hierarchy in :mod:`kith.exceptions`).
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
from kith.proto import MsgFlag, Proto


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

    def test_not_found_raised_for_enoent(self) -> None:
        with Proto() as proto:
            with pytest.raises(KithNotFoundError) as exc_info:
                proto.lookup_type("absent")
            assert exc_info.value.code == gen_types.kith_error.KITH_ENOENT
            assert isinstance(exc_info.value, KithError)

    def test_protocol_error_for_unknown_type_decode(self) -> None:
        with Proto() as proto, pytest.raises(KithProtocolError):
            # Encoding an unregistered type id succeeds (the codec does not
            # consult the registry on encode), but decode rejects frames
            # whose type_id is not registered.
            frame = proto.encode(9999, b"x")
            proto.decode(frame)


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


# ---------------------------------------------------------------------------
# proto
# ---------------------------------------------------------------------------


@needs_build
class TestProto:
    def test_register_lookup_roundtrip(self) -> None:
        with Proto() as proto:
            proto.register_type("move", 1100)
            assert proto.lookup_type("move") == 1100
            assert proto.type_name(1100) == "move"
            assert proto.type_name(9999) is None

    def test_encode_decode_roundtrip(self) -> None:
        with Proto() as proto:
            proto.register_type("move", 1100)
            frame = proto.encode(1100, b"hello")
            decoded = proto.decode(frame)
            assert decoded.type_id == 1100
            assert decoded.payload == b"hello"
            assert decoded.has_correlation is False

    def test_encode_decode_with_correlation(self) -> None:
        with Proto() as proto:
            proto.register_type("move", 1100)
            frame = proto.encode(
                1100, b"payload", flags=MsgFlag.CORRELATION, correlation_id=0xDEADBEEF
            )
            decoded = proto.decode(frame)
            assert decoded.has_correlation is True
            assert decoded.correlation_id == 0xDEADBEEF

    def test_correlation_hex_format(self) -> None:
        with Proto() as proto:
            assert proto.correlation_hex(0xDEADBEEF) == "00000000deadbeef"
            assert proto.correlation_hex(0) == "0000000000000000"

    def test_incomplete_frame_raises_protocol_error(self) -> None:
        with Proto() as proto, pytest.raises(KithProtocolError):
            proto.decode(b"\x00")  # far too short for a header

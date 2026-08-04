"""Tests for the proto codec's flag-bit and version posture.

The wire contract is two-tiered: the version byte is the dialect
boundary (an unimplemented version hard-rejects with
``KithProtocolError``), and within an implemented version unknown flag
bits are ignored on decode and passed through to the frame view
unchanged — they never alter the meaning of a known bit, the header
layout, or the length semantics. The correlation bit is the only defined
v1 bit; these tests hand-build frames so the unknown-bit behavior is
exercised without encode-side help.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator

import pytest
from _build_gate import _BUILD_DEBUG, needs_build

from kith import KithProtocolError
from kith._bridge import reset
from kith._generated import proto as gen_proto
from kith.proto import Proto


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


_CORR = int(gen_proto.kith_proto_flag.KITH_PROTO_FLAG_CORRELATION)
_UNKNOWN_BIT = 0x40
_TYPE_ID = 1000


def _frame(flags: int, payload: bytes, version: int | None = None) -> bytes:
    header = struct.pack(
        ">BBBBHI",
        int(gen_proto.kith_proto_format.KITH_PROTO_MAGIC0),
        int(gen_proto.kith_proto_format.KITH_PROTO_MAGIC1),
        int(gen_proto.kith_proto_format.KITH_PROTO_VERSION) if version is None else version,
        flags,
        _TYPE_ID,
        len(payload),
    )
    return header + payload


@needs_build
class TestUnknownFlagBits:
    def test_unknown_bit_passes_through_alongside_correlation(self) -> None:
        with Proto() as proto:
            proto.register_type("probe", _TYPE_ID)
            # The correlation trailer is part of the declared payload
            # region: payload_len covers payload bytes plus the trailer.
            payload_region = b"hello" + struct.pack(">Q", 0xDEADBEEF)
            frame = proto.decode(_frame(_CORR | _UNKNOWN_BIT, payload_region))
        assert frame.flags == (_CORR | _UNKNOWN_BIT)
        assert frame.has_correlation is True
        assert frame.correlation_id == 0xDEADBEEF
        assert frame.payload == b"hello"

    def test_unknown_bit_alone_carries_no_trailer(self) -> None:
        with Proto() as proto:
            proto.register_type("probe", _TYPE_ID)
            frame = proto.decode(_frame(_UNKNOWN_BIT, b"hello"))
        assert frame.flags == _UNKNOWN_BIT
        assert frame.has_correlation is False
        assert frame.payload == b"hello"


@needs_build
class TestVersionBoundary:
    def test_unimplemented_version_rejects(self) -> None:
        wire = _frame(0, b"hello", version=int(gen_proto.kith_proto_format.KITH_PROTO_VERSION) + 1)
        with Proto() as proto:
            proto.register_type("probe", _TYPE_ID)
            with pytest.raises(KithProtocolError):
                proto.decode(wire)

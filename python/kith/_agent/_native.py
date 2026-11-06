"""Client ctypes layouts, library signatures and bootstrap trampolines."""

from __future__ import annotations

import ctypes
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ._errors import KithClientError
from ._types import BootstrapStep, DecodedFrame


KITH_ABI_VERSION: int = 1


KITH_PROTO_FLAG_CORRELATION: int = 0x02


KITH_PROTO_HDR_SIZE: int = 10


KITH_CLIENT_EVENT_PAYLOAD_MAX: int = 1024


KITH_EAGAIN: int = 7


KITH_EOVERFLOW: int = 10


PROTO_LIB_BASENAME = "libkith_proto.so.1"


CLIENT_LIB_BASENAME = "libkith_client.so.1"


class ProtoParams(ctypes.Structure):
    """kith_proto_params_t."""

    _fields_ = [
        ("size", ctypes.c_uint32),
        ("abi_version", ctypes.c_uint32),
        ("max_payload", ctypes.c_uint32),
        ("reserved", ctypes.c_void_p * 8),
    ]


class ProtoFrame(ctypes.Structure):
    """kith_proto_frame_t (exposed-layout decoded view)."""

    _fields_ = [
        ("type_id", ctypes.c_uint16),
        ("flags", ctypes.c_uint8),
        ("has_correlation", ctypes.c_bool),
        ("correlation_id", ctypes.c_uint64),
        ("payload", ctypes.c_void_p),
        ("payload_len", ctypes.c_uint32),
    ]


class ClientParams(ctypes.Structure):
    """kith_client_params_t."""

    _fields_ = [
        ("size", ctypes.c_uint32),
        ("abi_version", ctypes.c_uint32),
        ("outbound_cap", ctypes.c_uint32),
        ("event_cap", ctypes.c_uint32),
        ("ping_type_id", ctypes.c_uint16),
        ("pong_type_id", ctypes.c_uint16),
        ("ping_interval_ms", ctypes.c_uint32),
        ("reconnect_base_ms", ctypes.c_uint32),
        ("reconnect_max_ms", ctypes.c_uint32),
        ("reconnect_max_attempts", ctypes.c_uint32),
        ("reserved", ctypes.c_void_p * 8),
    ]


class ClientCommand(ctypes.Structure):
    """kith_client_command_t."""

    _fields_ = [
        ("type_id", ctypes.c_uint16),
        ("flags", ctypes.c_uint8),
        ("correlation_id", ctypes.c_uint64),
        ("payload", ctypes.c_void_p),
        ("payload_len", ctypes.c_uint32),
    ]


class ClientBootstrapStep(ctypes.Structure):
    """kith_client_bootstrap_step_t."""

    _fields_ = [
        ("await_type_id", ctypes.c_uint16),
        ("on_enter", ctypes.c_void_p),
        ("on_reply", ctypes.c_void_p),
        ("ctx", ctypes.c_void_p),
    ]


class ClientBootstrapStatus(ctypes.Structure):
    """kith_client_bootstrap_status_t."""

    _fields_ = [
        ("size", ctypes.c_uint32),
        ("abi_version", ctypes.c_uint32),
        ("state", ctypes.c_uint32),
        ("current_step", ctypes.c_uint32),
        ("step_count", ctypes.c_uint32),
        ("reserved", ctypes.c_void_p * 8),
    ]


class ClientRuntimeStatus(ctypes.Structure):
    """kith_client_runtime_status_t."""

    _fields_ = [
        ("size", ctypes.c_uint32),
        ("abi_version", ctypes.c_uint32),
        ("connected", ctypes.c_uint8),
        ("awaiting_pong", ctypes.c_uint8),
        ("reconnect_due", ctypes.c_uint8),
        ("reserved_u8", ctypes.c_uint8),
        ("reconnect_attempts", ctypes.c_uint32),
        ("rtt_last_ms", ctypes.c_uint32),
        ("rtt_min_ms", ctypes.c_uint32),
        ("rtt_max_ms", ctypes.c_uint32),
        ("rtt_sum_ms", ctypes.c_uint64),
        ("rtt_samples", ctypes.c_uint32),
        ("reserved_u32", ctypes.c_uint32),
        ("last_ping_sent_ms", ctypes.c_uint64),
        ("next_ping_due_ms", ctypes.c_uint64),
        ("next_reconnect_due_ms", ctypes.c_uint64),
        ("reserved", ctypes.c_void_p * 8),
    ]


U8 = ctypes.c_uint8


U16Ptr = ctypes.POINTER(ctypes.c_uint16)


U8Ptr = ctypes.POINTER(ctypes.c_uint8)


U32Ptr = ctypes.POINTER(ctypes.c_uint32)


FramePtr = ctypes.POINTER(ProtoFrame)


EnterFn = ctypes.CFUNCTYPE(
    ctypes.c_int,
    ctypes.c_void_p,
    ctypes.c_void_p,
    U16Ptr,
    U8Ptr,
    ctypes.c_uint32,
    U32Ptr,
)


ReplyFn = ctypes.CFUNCTYPE(
    ctypes.c_int,
    ctypes.c_void_p,
    ctypes.c_void_p,
    FramePtr,
    U32Ptr,
)


FrameFn = ctypes.CFUNCTYPE(
    None,
    ctypes.c_void_p,
    ctypes.c_void_p,
    FramePtr,
)


def find_lib(basename: str, env_var: str, explicit: str | None) -> Path:
    """Locate a kith shared library by explicit path, env var, wheel bundle,
    or build walk."""
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise KithClientError(1, f"library not found: {explicit}")
        return path

    env = os.environ.get(env_var)
    if env:
        path = Path(env)
        if not path.is_file():
            raise KithClientError(1, f"library not found: {env}")
        return path

    pkg_dir = Path(__file__).resolve().parent.parent
    for cand in (
        pkg_dir / basename,
        pkg_dir / "lib" / basename,
        pkg_dir / "_libs" / basename,
    ):
        if cand.is_file():
            return cand

    repo_root = pkg_dir.parents[1]
    for cand in (
        repo_root / "build" / "debug" / basename,
        repo_root / "build" / "release" / basename,
        repo_root / "cmake-build-debug" / basename,
        repo_root / "cmake-build-release" / basename,
    ):
        if cand.is_file():
            return cand

    try:
        ctypes.CDLL(basename)
    except OSError as exc:
        raise KithClientError(1, f"could not locate {basename}: {exc}") from exc
    return Path(basename)


def load_proto(path: Path) -> ctypes.CDLL:
    """Load libkith_proto and declare argtypes/restype for the called symbols."""
    lib = ctypes.CDLL(str(path))

    lib.kith_proto_create.argtypes = [
        ctypes.POINTER(ProtoParams),
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.kith_proto_create.restype = ctypes.c_int

    lib.kith_proto_destroy.argtypes = [ctypes.c_void_p]
    lib.kith_proto_destroy.restype = None

    lib.kith_proto_register_type_id.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.c_uint16,
    ]
    lib.kith_proto_register_type_id.restype = ctypes.c_int

    lib.kith_proto_decode.argtypes = [
        ctypes.c_void_p,
        U8Ptr,
        ctypes.c_size_t,
        ctypes.POINTER(ProtoFrame),
        ctypes.POINTER(ctypes.c_size_t),
    ]
    lib.kith_proto_decode.restype = ctypes.c_int

    return lib


def load_client(path: Path) -> ctypes.CDLL:
    """Load libkith_client and declare argtypes/restype for the called symbols."""
    lib = ctypes.CDLL(str(path))

    lib.kith_client_create.argtypes = [
        ctypes.POINTER(ClientParams),
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.kith_client_create.restype = ctypes.c_int

    lib.kith_client_destroy.argtypes = [ctypes.c_void_p]
    lib.kith_client_destroy.restype = None

    lib.kith_client_configure_bootstrap.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ClientBootstrapStep),
        ctypes.c_uint32,
    ]
    lib.kith_client_configure_bootstrap.restype = ctypes.c_int

    lib.kith_client_set_frame_handler.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    lib.kith_client_set_frame_handler.restype = None

    lib.kith_client_on_connected.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.kith_client_on_connected.restype = ctypes.c_int

    lib.kith_client_on_disconnected.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.kith_client_on_disconnected.restype = ctypes.c_int

    lib.kith_client_on_connect_failed.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.kith_client_on_connect_failed.restype = ctypes.c_int

    lib.kith_client_feed_frame.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ProtoFrame),
        ctypes.c_uint64,
    ]
    lib.kith_client_feed_frame.restype = ctypes.c_int

    lib.kith_client_pop_outbound.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    lib.kith_client_pop_outbound.restype = ctypes.c_int

    lib.kith_client_submit_interactive.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ClientCommand),
        ctypes.POINTER(ctypes.c_uint64),
    ]
    lib.kith_client_submit_interactive.restype = ctypes.c_int

    lib.kith_client_tick.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.kith_client_tick.restype = ctypes.c_int

    lib.kith_client_bootstrap_status.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ClientBootstrapStatus),
    ]
    lib.kith_client_bootstrap_status.restype = ctypes.c_int

    lib.kith_client_runtime_status.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ClientRuntimeStatus),
    ]
    lib.kith_client_runtime_status.restype = ctypes.c_int

    return lib


def make_enter(step: BootstrapStep) -> Callable[..., int]:
    # Pointer params are Any: ctypes passes opaque pointer objects whose
    # only stub type is private; indexing and memmove accept them.
    def enter(
        _client: int,
        _ctx: int,
        out_type_id: Any,
        out_payload: Any,
        payload_cap: int,
        out_payload_len: Any,
    ) -> int:
        assert step.on_enter is not None
        result = step.on_enter()
        if result is None:
            out_payload_len[0] = 0
            return 0
        type_id, payload = result
        out_type_id[0] = type_id & 0xFFFF
        n = min(len(payload), int(payload_cap))
        if n:
            ctypes.memmove(out_payload, payload, n)
        out_payload_len[0] = n
        return 0

    return enter


def make_reply(step: BootstrapStep) -> Callable[..., int]:
    def reply(
        _client: int,
        _ctx: int,
        frame_ptr: Any,
        out_next_step: Any,
    ) -> int:
        assert step.on_reply is not None
        frame = frame_ptr.contents
        addr = frame.payload
        payload = ctypes.string_at(addr, int(frame.payload_len)) if addr else b""
        view = DecodedFrame(
            type_id=int(frame.type_id),
            flags=int(frame.flags),
            has_correlation=bool(frame.has_correlation),
            correlation_id=int(frame.correlation_id),
            payload=payload,
        )
        out_next_step[0] = step.on_reply(view) & 0xFFFFFFFF
        return 0

    return reply

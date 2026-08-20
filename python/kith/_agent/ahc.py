#!/usr/bin/env python3
"""Agentic headless client (AHC).

Wraps libkith_client and libkith_proto via ctypes and drives a headless
client connection over asyncio. The client engine is transport-agnostic:
this harness owns the TCP socket, decodes inbound bytes via the proto codec
into frame views (kith_client_feed_frame), pops encoded outbound frames
(kith_client_pop_outbound) onto the socket, and drives the keepalive tick
(kith_client_tick). A Unix-socket NDJSON IPC server and a minimal HTTP/1.1
introspection server run on the same loop.

The harness is type-id agnostic: message types are registered by name and
numeric id, and interactive commands carry a caller-chosen type id and
opaque payload bytes. No game-specific vocabulary (sessions, entities,
chat, movement) lives here; scenarios attach meaning to type ids.

Events are derived in the Python frame handler rather than the C event ring
so that correlation IDs (present on the decoded frame but not on the ring
record) are preserved in the event history.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import contextlib
import ctypes
import itertools
import json
import logging
import os
import struct
import threading
import time
from collections import deque
from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


KITH_ABI_VERSION: int = 1

KITH_PROTO_FLAG_CORRELATION: int = 0x02
KITH_PROTO_HDR_SIZE: int = 10
KITH_CLIENT_EVENT_PAYLOAD_MAX: int = 1024

KITH_EAGAIN: int = 7

_PROTO_LIB_BASENAME = "libkith_proto.so.1"
_CLIENT_LIB_BASENAME = "libkith_client.so.1"

_ERROR_NAMES: Mapping[int, str] = {
    1: "EINVAL",
    2: "ENOMEM",
    3: "ENOSYS",
    4: "ENOENT",
    5: "EEXIST",
    6: "EBUSY",
    7: "EAGAIN",
    8: "EPERM",
    9: "ERANGE",
    10: "EOVERFLOW",
    11: "EFAULT",
    12: "EIO",
    13: "ETIMEDOUT",
    14: "ECONNRESET",
    15: "ESHUTDOWN",
    16: "ESTATE",
    17: "EPROTO",
    18: "ESIZE",
    19: "EABIVER",
}

_log = logging.getLogger("kith.ahc")


class KithClientError(Exception):
    """Raised when a kith client or proto C call returns a negative code."""

    def __init__(self, code: int, context: str = "") -> None:
        self.code = code
        self.context = context
        name = _ERROR_NAMES.get(code, f"E{code}")
        msg = f"kith {name} ({code})"
        if context:
            msg = f"{msg}: {context}"
        super().__init__(msg)


def _check(code: int, context: str = "") -> int:
    """Translate a C return: success passes through, negative raises."""
    if code < 0:
        raise KithClientError(-code, context)
    return code


def _to_int(value: object) -> int:
    """Coerce a JSON-decoded value to int (str/int/float all accepted)."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return int(value)
    return int(str(value))


def _event_list(value: object) -> list[object]:
    """Extract a list from a JSON-decoded events field, or empty."""
    if isinstance(value, list):
        return list(value)
    return []


def _now_ms() -> int:
    """Monotonic time in milliseconds (the engine's clock domain)."""
    return int(time.monotonic_ns() // 1_000_000)


# ---------------------------------------------------------------------------
# ctypes struct mirrors (size-versioned public ABI)
# ---------------------------------------------------------------------------


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


# Pointer type aliases used in callback annotations.
_U8 = ctypes.c_uint8
_U16Ptr = ctypes.POINTER(ctypes.c_uint16)
_U8Ptr = ctypes.POINTER(ctypes.c_uint8)
_U32Ptr = ctypes.POINTER(ctypes.c_uint32)
_FramePtr = ctypes.POINTER(ProtoFrame)

# C callback function-pointer types. Instances must be kept alive for the
# client handle's lifetime (stored on the AgenticHeadlessClient instance).
_EnterFn = ctypes.CFUNCTYPE(
    ctypes.c_int,
    ctypes.c_void_p,
    ctypes.c_void_p,
    _U16Ptr,
    _U8Ptr,
    ctypes.c_uint32,
    _U32Ptr,
)
_ReplyFn = ctypes.CFUNCTYPE(
    ctypes.c_int,
    ctypes.c_void_p,
    ctypes.c_void_p,
    _FramePtr,
    _U32Ptr,
)
_FrameFn = ctypes.CFUNCTYPE(
    None,
    ctypes.c_void_p,
    ctypes.c_void_p,
    _FramePtr,
)


# ---------------------------------------------------------------------------
# public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DecodedFrame:
    """A decoded proto frame, copied for Python-side ownership."""

    type_id: int
    flags: int
    has_correlation: bool
    correlation_id: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class ClientEvent:
    """An event derived from an inbound runtime frame."""

    ts_mono_ns: int
    type_id: int
    correlation_id: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class BootstrapStep:
    """A Python spec for one bootstrap FSM step.

    on_enter returns (type_id, payload_bytes) for the outbound frame, or
    None to wait passively. on_reply receives the decoded inbound frame and
    returns the next step index (== step_count means READY, > step_count
    means FAILED, == current step retries). A None callback auto-advances.
    """

    await_type_id: int
    on_enter: Callable[[], tuple[int, bytes] | None] | None = None
    on_reply: Callable[[DecodedFrame], int] | None = None


@dataclass(frozen=True, slots=True)
class ClientStatus:
    """A snapshot of connection and bootstrap state."""

    connected: bool
    bootstrap_state: int
    bootstrap_step: int
    bootstrap_step_count: int
    rtt_last_ms: int
    reconnect_attempts: int


# ---------------------------------------------------------------------------
# library loading (mirrors tools/replay.py)
# ---------------------------------------------------------------------------


def _find_lib(basename: str, env_var: str, explicit: str | None) -> Path:
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


def _load_proto(path: Path) -> ctypes.CDLL:
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
        _U8Ptr,
        ctypes.c_size_t,
        ctypes.POINTER(ProtoFrame),
        ctypes.POINTER(ctypes.c_size_t),
    ]
    lib.kith_proto_decode.restype = ctypes.c_int

    return lib


def _load_client(path: Path) -> ctypes.CDLL:
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


# ---------------------------------------------------------------------------
# the harness
# ---------------------------------------------------------------------------


_IPC_BUFFER_SIZE = 65536
_HTTP_MAX_BODY = 1 << 20
_MAX_OUTBOUND_FRAME = KITH_PROTO_HDR_SIZE + (1 << 20)

_OUTBOUND_IDLE_PROBE_S = 1.0

# The inline drain stops popping once this many bytes sit unwritten in
# the transport: further residue waits engine-side for the writable
# callback instead of migrating queue bulk into the transport buffer.
_OUTBOUND_HIGH_WATER = 256 * 1024

# Scratch buffer for the stalled-outbound probe: the asyncio loop runs
# the probe synchronously, so one buffer serves every connection.
_PROBE_SCRATCH = (_U8 * _MAX_OUTBOUND_FRAME)()

_PROTO_DEFAULT_MAX_PAYLOAD = 1 << 20

_PROTO_MAGIC0 = 0x4B
_PROTO_MAGIC1 = 0x54
_PROTO_VERSION = 1

# Bootstrap FSM state KITH_CLIENT_BOOTSTRAP_READY (include/kith/client/client.h).
_BOOTSTRAP_READY: int = 2

# Wire header layout: magic(2) version(1) flags(1) type_id(u16 BE)
# wire_len(u32 BE). wire_len counts payload plus the correlation trailer
# when KITH_PROTO_FLAG_CORRELATION is set.
_HEADER = struct.Struct(">4BHI")
_CORR_TRAILER_SIZE = 8

# Summary retention keeps a replication frame's payload only when this much
# time passed since the last kept frame; the rest advance the framing
# position and the replication counter only. The bound keeps the retained
# payload recent for post-run consumers regardless of frame rate.
_SUMMARY_MIN_INTERVAL_S = 1.0

# The movement-window view read stops admitting ids once its set holds
# this many: the breadth bar asks whether a client saw at least a small
# multiple fewer distinct neighbors than this cap, so a saturated set
# answers every question the probe asks and the read skips extraction
# for the remaining frames from there on. The frame timestamp list is
# not capped — the stall guard scans every in-window arrival.
_VIEW_WINDOW_ID_CAP = 256


@dataclass(frozen=True, slots=True)
class WindowEvidence:
    """One client's frozen movement-window view evidence.

    ``distinct_ids`` holds every non-membership actor id received while
    the window was open (saturated at ``_VIEW_WINDOW_ID_CAP``);
    ``frame_ts_ns`` holds each in-window frame's arrival timestamp in
    ascending order for the breadth probe's stall scan. ``incomplete``
    marks a basis that cannot prove completeness: the evidence deque
    evicted frames while the window was open, or no byte budget was
    configured, so the counts are a floor — the breadth probe reads
    this and reports the client as unfounded rather than trusting the
    counts. ``deque_distinct_ids`` runs the same saturation-capped
    extraction over the whole retained deque with the window bounds
    ignored, so a sparse window read splits into a window younger than
    the retained roster versus a roster the retained stream never
    carried; the self filter applies at read time exactly as it does
    for ``distinct_ids``.
    """

    distinct_ids: frozenset[int]
    frame_ts_ns: tuple[int, ...]
    incomplete: bool = False
    deque_distinct_ids: frozenset[int] = frozenset()


# Exception groups caught in multiple sites. Defined once as tuples so
# ruff format does not strip the grouping (it removes parentheses from
# `except (A, B):` because Python 3.14 accepts the bare-comma tuple form,
# but mypy 2.3 flags that form as a syntax error; a single name avoids
# the conflict).
_JSON_DECODE_ERRORS = (json.JSONDecodeError, UnicodeDecodeError)
_CONN_RESET_ERRORS = (ConnectionResetError, BrokenPipeError)
_HTTP_READ_ERRORS = (asyncio.IncompleteReadError, ConnectionError, OSError)
_INTERRUPT_ERRORS = (KeyboardInterrupt, asyncio.CancelledError)


class _ClientProtocol(asyncio.Protocol):
    """Socket-side protocol driving one AgenticHeadlessClient.

    Inbound bytes go straight into the client's synchronous consumer and
    a writable socket re-triggers the inline drain, so neither direction
    owns an asyncio task.
    """

    def __init__(self, client: AgenticHeadlessClient) -> None:
        self._client = client

    def data_received(self, data: bytes) -> None:
        self._client._on_socket_data(data)

    def eof_received(self) -> bool:
        # Returning False closes the transport, tearing the write half
        # down with the read half on inbound EOF.
        return False

    def resume_writing(self) -> None:
        # pause_writing needs no override: the inline drain checks the
        # transport buffer size after every write and stops popping on
        # its own, so backpressure is already observed there.
        self._client._on_write_resumed()

    def connection_lost(self, exc: Exception | None) -> None:
        self._client._on_connection_lost(exc)


class AgenticHeadlessClient:
    """A headless client that drives libkith_client over asyncio.

    Owns one TCP connection and, when enabled, one Unix-socket NDJSON IPC
    server and one HTTP/1.1 introspection server, all on a single asyncio
    loop. The C client engine (bootstrap FSM, keepalive, outbound queue)
    is driven from the protocol callbacks plus a shared ticker task;
    inbound frames decode inside ``data_received`` and outbound frames
    drain inline on enqueue triggers, so the connection itself costs no
    per-batch task wakes.
    """

    def __init__(
        self,
        instance_id: str,
        host: str = "127.0.0.1",
        port: int = 7777,
        *,
        proto_lib: str | None = None,
        client_lib: str | None = None,
        max_payload: int = 0,
        outbound_cap: int = 0,
        event_cap: int = 0,
        ping_type_id: int = 0,
        pong_type_id: int = 0,
        ping_interval_ms: int = 0,
        pong_timeout_ms: int = 0,
        reconnect_base_ms: int = 0,
        reconnect_max_ms: int = 0,
        reconnect_max_attempts: int = 0,
        message_types: Mapping[str, int] | None = None,
        bootstrap_steps: Sequence[BootstrapStep] | None = None,
        ipc_socket_path: str | None = None,
        ipc_enabled: bool = True,
        http_host: str = "127.0.0.1",
        http_port: int = 0,
        http_enabled: bool = True,
        event_history_size: int = 256,
        auto_connect: bool = True,
        reconnect_enabled: bool = True,
        tick_interval_s: float = 0.02,
        direct_type_ids: frozenset[int] = frozenset(),
        summary_mode: bool = False,
        principal_id: int = 0,
        evidence_budget_bytes: int = 0,
        window_id_extractor: Callable[[int, bytes], Iterable[int]] | None = None,
    ) -> None:
        self.instance_id = instance_id
        self.host = host
        self.port = port
        self.principal_id = principal_id
        self.auto_connect = auto_connect
        # The engine is single-threaded by contract (include/kith/client/
        # client.h): every engine entry funnels to the event-loop thread.
        # Captured in start(); submit() routes foreign threads through it.
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id = 0

        proto_path = _find_lib(_PROTO_LIB_BASENAME, "KITH_PROTO_LIB", proto_lib)
        client_path = _find_lib(_CLIENT_LIB_BASENAME, "KITH_CLIENT_LIB", client_lib)
        self._proto_lib = _load_proto(proto_path)
        self._client_lib = _load_client(client_path)

        self._proto = self._create_proto(max_payload)
        self._client = self._create_client(
            outbound_cap=outbound_cap,
            event_cap=event_cap,
            ping_type_id=ping_type_id,
            pong_type_id=pong_type_id,
            ping_interval_ms=ping_interval_ms,
            reconnect_base_ms=reconnect_base_ms,
            reconnect_max_ms=reconnect_max_ms,
            reconnect_max_attempts=reconnect_max_attempts,
        )

        if message_types:
            for name, type_id in message_types.items():
                self.register_type(name, type_id)

        self._enter_cbs: list[object] = []
        self._reply_cbs: list[object] = []
        if bootstrap_steps:
            self._configure_bootstrap(list(bootstrap_steps))

        self._frame_cb = _FrameFn(self._on_frame_c)
        self._client_lib.kith_client_set_frame_handler(self._client, self._frame_cb, None)

        self.ipc_socket_path = ipc_socket_path or os.path.join(
            "/tmp/kith-ahc", f"{instance_id}.sock"
        )
        self.http_host = http_host
        self.http_port = http_port
        self._http_enabled = http_enabled
        self._ipc_enabled = ipc_enabled
        self._reconnect_enabled = reconnect_enabled
        self._tick_interval_s = tick_interval_s
        # Pong staleness: with keepalive pings enabled, a peer that stops
        # replying without ever closing (hung mid-shutdown, silent
        # partition) otherwise holds the client in a phantom-live
        # state forever — RST and FIN are the only shapes the transport
        # notices. 0 selects 3x the ping interval; with pings disabled
        # (ping_interval_ms=0) no liveness probe exists and detection is
        # off.
        self._ping_interval_ms = ping_interval_ms
        self._pong_timeout_ms = pong_timeout_ms or (3 * ping_interval_ms)
        self._direct_type_ids = direct_type_ids
        self._direct_wire_limit = max_payload if max_payload else _PROTO_DEFAULT_MAX_PAYLOAD
        # Direct-type frames bypass the engine until the bootstrap FSM is
        # READY: the login handshake awaits a replication frame, so early
        # arrivals of a direct type must still reach feed_frame. Re-armed
        # on every (re)connect.
        self._direct_ready = False
        # Summary mode drops per-frame event construction for clients whose
        # event streams no metric consumes: replication frames advance the
        # counter and refresh one retained slot — the first frame, then any
        # frame at least _SUMMARY_MIN_INTERVAL_S after the last kept one —
        # that recent_events() synthesizes from. The counter runs in both
        # modes so delivered-frame counts stay available.
        self._summary_mode = summary_mode
        self._summary_slot: ClientEvent | None = None
        self._summary_last_keep_ns: int | None = None
        # Breadth-evidence window: summary retention keeps one periodic
        # frame, which under change-suppression delivery is usually a
        # self-only view. When evidence_budget_bytes is positive, every
        # direct frame's payload is also retained here (raw, no decode)
        # in a recency deque bounded by that byte budget. The deque is a
        # forensic record: O(1) span reads render its retention margin,
        # and census-class post-mortem tooling decodes its payloads after
        # a run to reconstruct what a client's stream actually carried.
        # The breadth probe's counting basis is the movement-window
        # accumulator below, not this deque — the deque evicts
        # oldest-first under its byte budget, so at probe time its span
        # stops short of the movement window's start.
        self._evidence_budget_bytes = evidence_budget_bytes
        self._evidence: deque[tuple[int, int, bytes]] = deque()
        self._evidence_bytes = 0
        self._evidence_evictions = 0
        # Movement-window view evidence: while the driver's breadth window
        # is open, the counting basis is this client's evidence deque —
        # every direct replication frame is retained there (arrival
        # timestamp, type, payload) under the byte budget, so the first
        # post-seal read can walk the in-window frames and extract the
        # non-membership actor ids without any per-frame work on the
        # ingest path. The ingest loop is the load driver's single
        # contended event loop, and even a small per-frame hook there
        # perturbs the driver/server cadence enough to shift the round
        # pace, so the window keeps zero ingest cost. The read applies
        # the window bounds (arrival timestamp within [window start,
        # seal deadline]) and reports whether the basis is provably
        # complete: any deque eviction while the window was open means
        # the oldest in-window frames may be gone, and the evidence
        # says so instead of undercounting silently. The driver opens
        # the window at movement start and seals it after the movement
        # end stamp, so the set answers "what did this client receive
        # during the movement window" deterministically. The window
        # survives reconnection (unlike a probe-moment read): the
        # connection arm leaves the deque in place while the window is
        # open, so frames delivered before a reconnect still count.
        self._window_id_extractor = window_id_extractor
        self._window_start_ns: int | None = None
        self._window_open = False
        self._window_deadline_ns: int | None = None
        self._window_evidence: WindowEvidence | None = None
        self._window_evicted_in_window = False
        self._last_frame_ts_ns: int | None = None
        self._replication_count = 0
        # True while the write loop owns the drain: the stalled-outbound
        # probe must not steal frames mid-drain just because the flush
        # event was already cleared.
        self._draining = False
        # Reused decode out-params: the drain loop is the only consumer,
        # and one allocation per connection beats one per read batch.
        self._decode_frame = ProtoFrame()
        self._decode_consumed = ctypes.c_size_t(0)

        self._event_history: deque[ClientEvent] = deque(maxlen=event_history_size)
        self._pending_push: deque[ClientEvent] = deque()
        self._subscriptions: set[int] = set()
        self._subscribe_all = False

        self._connected = False
        self._running = False
        self._transport: asyncio.Transport | None = None
        self._protocol: _ClientProtocol | None = None
        self._closed_future: asyncio.Future[None] | None = None
        # Inbound bytes accumulate across data_received calls; complete
        # frames are consumed from the front after every batch.
        self._inbuf = bytearray()
        # Reused outbound pop buffer: one allocation per connection beats
        # one per flush or per frame.
        self._out_buf = (_U8 * _MAX_OUTBOUND_FRAME)()
        self._out_len = ctypes.c_uint32(0)

        self._ipc_server: asyncio.AbstractServer | None = None
        self._ipc_peers: set[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = set()
        self._http_server: asyncio.AbstractServer | None = None
        self._sse_writers: set[asyncio.StreamWriter] = set()

        self._connection_task: asyncio.Task[None] | None = None

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def _create_proto(self, max_payload: int) -> ctypes.c_void_p:
        params = ProtoParams(
            size=ctypes.sizeof(ProtoParams),
            abi_version=KITH_ABI_VERSION,
            max_payload=max_payload,
        )
        handle = ctypes.c_void_p()
        _check(
            int(
                self._proto_lib.kith_proto_create(ctypes.byref(params), None, ctypes.byref(handle))
            ),
            "kith_proto_create",
        )
        return handle

    def _create_client(
        self,
        *,
        outbound_cap: int,
        event_cap: int,
        ping_type_id: int,
        pong_type_id: int,
        ping_interval_ms: int,
        reconnect_base_ms: int,
        reconnect_max_ms: int,
        reconnect_max_attempts: int,
    ) -> ctypes.c_void_p:
        params = ClientParams(
            size=ctypes.sizeof(ClientParams),
            abi_version=KITH_ABI_VERSION,
            outbound_cap=outbound_cap,
            event_cap=event_cap,
            ping_type_id=ping_type_id,
            pong_type_id=pong_type_id,
            ping_interval_ms=ping_interval_ms,
            reconnect_base_ms=reconnect_base_ms,
            reconnect_max_ms=reconnect_max_ms,
            reconnect_max_attempts=reconnect_max_attempts,
        )
        handle = ctypes.c_void_p()
        _check(
            int(
                self._client_lib.kith_client_create(
                    ctypes.byref(params), self._proto, None, None, ctypes.byref(handle)
                )
            ),
            "kith_client_create",
        )
        return handle

    def _configure_bootstrap(self, steps: list[BootstrapStep]) -> None:
        c_steps = (ClientBootstrapStep * len(steps))()
        for i, step in enumerate(steps):
            enter_fn: Any = None
            reply_fn: Any = None
            if step.on_enter is not None:
                enter_fn = _EnterFn(_make_enter(step))
                self._enter_cbs.append(enter_fn)
            if step.on_reply is not None:
                reply_fn = _ReplyFn(_make_reply(step))
                self._reply_cbs.append(reply_fn)
            c_steps[i] = ClientBootstrapStep(
                await_type_id=step.await_type_id,
                on_enter=ctypes.cast(enter_fn, ctypes.c_void_p) if enter_fn else None,
                on_reply=ctypes.cast(reply_fn, ctypes.c_void_p) if reply_fn else None,
                ctx=None,
            )
        _check(
            int(
                self._client_lib.kith_client_configure_bootstrap(self._client, c_steps, len(steps))
            ),
            "kith_client_configure_bootstrap",
        )

    def register_type(self, name: str, type_id: int) -> None:
        """Register a message type by name and numeric id with the proto codec."""
        _check(
            int(
                self._proto_lib.kith_proto_register_type_id(
                    self._proto, name.encode("utf-8"), type_id & 0xFFFF
                )
            ),
            f"register_type {name}",
        )

    def submit(
        self,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int | concurrent.futures.Future[int]:
        """Submit an interactive command; returns the assigned command id.

        The engine is single-threaded by contract (include/kith/client/
        client.h marks every function unsafe to call concurrently, and the
        submit path mutates the command counter and the outbound queue
        non-atomically). Callers on the event-loop thread submit inline
        and get the command id synchronously; foreign-thread callers get
        a Future resolved by the loop thread, which also drains outbound —
        the transport is loop-affine and concurrent pops race the queue.
        Before start() there is no loop yet: submits queue into the engine
        from the calling thread and drain once the transport attaches.
        """
        if self._loop is not None and threading.get_ident() != self._loop_thread_id:
            fut: concurrent.futures.Future[int] = concurrent.futures.Future()
            self._loop.call_soon_threadsafe(
                self._submit_foreign, type_id, payload, flags, correlation_id, fut
            )
            return fut
        return self._submit_on_loop(type_id, payload, flags=flags, correlation_id=correlation_id)

    def _submit_on_loop(
        self,
        type_id: int,
        payload: bytes,
        *,
        flags: int,
        correlation_id: int,
    ) -> int:
        buf = (ctypes.c_uint8 * len(payload)).from_buffer_copy(payload)
        cmd = ClientCommand(
            type_id=type_id & 0xFFFF,
            flags=flags & 0xFF,
            correlation_id=correlation_id & 0xFFFFFFFFFFFFFFFF,
            payload=ctypes.cast(buf, ctypes.c_void_p),
            payload_len=len(payload),
        )
        cmd_id = ctypes.c_uint64(0)
        _check(
            int(
                self._client_lib.kith_client_submit_interactive(
                    self._client, ctypes.byref(cmd), ctypes.byref(cmd_id)
                )
            ),
            "submit_interactive",
        )
        self._request_flush()
        return int(cmd_id.value)

    def _submit_foreign(
        self,
        type_id: int,
        payload: bytes,
        flags: int,
        correlation_id: int,
        fut: concurrent.futures.Future[int],
    ) -> None:
        try:
            fut.set_result(
                self._submit_on_loop(type_id, payload, flags=flags, correlation_id=correlation_id)
            )
        except Exception as exc:  # the Future is the caller's error channel
            fut.set_exception(exc)

    def query_status(self) -> ClientStatus:
        """Snapshot connection and bootstrap state."""
        bs = ClientBootstrapStatus(
            size=ctypes.sizeof(ClientBootstrapStatus),
            abi_version=KITH_ABI_VERSION,
        )
        _check(
            int(self._client_lib.kith_client_bootstrap_status(self._client, ctypes.byref(bs))),
            "bootstrap_status",
        )
        rs = ClientRuntimeStatus(
            size=ctypes.sizeof(ClientRuntimeStatus),
            abi_version=KITH_ABI_VERSION,
        )
        _check(
            int(self._client_lib.kith_client_runtime_status(self._client, ctypes.byref(rs))),
            "runtime_status",
        )
        return ClientStatus(
            connected=bool(rs.connected),
            bootstrap_state=int(bs.state),
            bootstrap_step=int(bs.current_step),
            bootstrap_step_count=int(bs.step_count),
            rtt_last_ms=int(rs.rtt_last_ms),
            reconnect_attempts=int(rs.reconnect_attempts),
        )

    def recent_events(self, count: int = 64) -> list[ClientEvent]:
        """Return up to the last ``count`` events (newest last).

        In summary mode the history is not retained; the call synthesizes
        its result from the periodic retention slot instead, so consumers
        that need one recent replication payload (the breadth health
        check) see the same shape a full-retention client produces.
        """
        if self._summary_mode:
            return [] if self._summary_slot is None else [self._summary_slot]
        items = list(self._event_history)
        return items[-count:] if count < len(items) else items

    def evidence_frames(self) -> list[ClientEvent]:
        """Return the breadth-evidence window, oldest first.

        The deque is the forensic payload record AND the movement-window
        view accumulator's counting basis: every direct frame retained
        under its byte budget is available to the window read, which
        applies the window bounds and the id extraction at
        :meth:`view_window` time. Oldest-first eviction under the byte
        budget is the one threat to that basis, and the window read
        answers it with the eviction counter rather than silently
        trusting a shrunken deque.
        """
        return [
            ClientEvent(ts_mono_ns=ts, type_id=type_id, correlation_id=0, payload=payload)
            for ts, type_id, payload in self._evidence
        ]

    def evidence_span_ns(self) -> int | None:
        """Return the evidence deque's oldest-to-newest span, or None when empty."""
        if not self._evidence:
            return None
        return self._evidence[-1][0] - self._evidence[0][0]

    def begin_view_window(self) -> None:
        """Open the movement-window view accumulator, discarding prior evidence."""
        self._window_start_ns = time.monotonic_ns()
        self._window_open = True
        self._window_deadline_ns = None
        self._window_evidence = None
        self._window_evicted_in_window = False

    def seal_view_window(self, deadline_ns: int) -> None:
        """Stop the accumulator at ``deadline_ns``; frames arriving after the
        deadline stop counting.

        Frames already received with a timestamp at or before the
        deadline keep their contributions, so a frame in flight at
        window end counts while the post-drain backstop waves do not.
        """
        self._window_deadline_ns = deadline_ns

    def view_window(self) -> WindowEvidence | None:
        """Return the movement-window view evidence, or None when never opened.

        The first call after the seal walks the evidence deque once and
        caches the result; the deadline has passed by then, so no frame
        can join and the cache is final. Before the seal the call
        walks a live snapshot without caching, keeping the growing
        window readable. The counting basis is the deque itself, so the
        ingest path carries no per-frame window work.
        """
        if not self._window_open:
            return None
        if self._window_evidence is not None:
            return self._window_evidence
        if self._window_deadline_ns is None:
            return self._build_window_evidence(cache=False)
        self._window_evidence = self._build_window_evidence(cache=True)
        return self._window_evidence

    def _build_window_evidence(self, cache: bool) -> WindowEvidence:
        """Walk the evidence deque within the window bounds into the frozen shape.

        Frames the window must count are the retained direct frames
        whose arrival timestamp falls in ``[window start, seal
        deadline]``.         Ids join the set in arrival order under the
        saturation cap, so the result matches what per-frame extraction
        would have produced. The same extraction runs unbounded by the
        window over every retained frame into ``deque_distinct_ids``.
        ``incomplete`` is conservative in the loud
        direction: any frame evicted from the deque while the window was
        open whose arrival fell inside the window, or no byte budget at
        all, marks the basis unable to prove completeness — the counts
        are then a floor the breadth probe reports as unfounded rather
        than trusting.
        """
        start_ns = self._window_start_ns
        deadline_ns = self._window_deadline_ns
        ids: set[int] = set()
        deque_ids: set[int] = set()
        frame_ts: list[int] = []
        extractor = self._window_id_extractor
        for ts_ns, type_id, payload in self._evidence:
            in_window = (start_ns is None or ts_ns >= start_ns) and (
                deadline_ns is None or ts_ns <= deadline_ns
            )
            if in_window:
                frame_ts.append(ts_ns)
            if extractor is None:
                continue
            want_deque = len(deque_ids) < _VIEW_WINDOW_ID_CAP
            want_window = in_window and len(ids) < _VIEW_WINDOW_ID_CAP
            if not want_deque and not want_window:
                continue
            extracted = list(extractor(type_id, payload))
            if want_deque:
                deque_ids.update(itertools.islice(extracted, _VIEW_WINDOW_ID_CAP - len(deque_ids)))
            if want_window:
                ids.update(itertools.islice(extracted, _VIEW_WINDOW_ID_CAP - len(ids)))
        incomplete = self._window_evicted_in_window or self._evidence_budget_bytes == 0
        evidence = WindowEvidence(frozenset(ids), tuple(frame_ts), incomplete, frozenset(deque_ids))
        if cache:
            self._window_evidence = evidence
        return evidence

    def _append_evidence(
        self, ts_ns: int, type_id: int, buf: bytearray, start: int, end: int
    ) -> None:
        """Retain one raw frame payload in the breadth-evidence window.

        Direct frames are replication frames by construction (the direct
        catalog carries the replication types), so no per-type gate
        applies here. The running byte counter keeps eviction O(1) per
        frame; staleness is judged at read time, and a single frame
        larger than the whole budget leaves an empty window rather than
        an over-budget one.
        """
        payload = bytes(buf[start:end])
        self._evidence.append((ts_ns, type_id, payload))
        self._evidence_bytes += len(payload)
        while self._evidence_bytes > self._evidence_budget_bytes and self._evidence:
            dropped_ts, _, dropped = self._evidence.popleft()
            self._evidence_bytes -= len(dropped)
            self._evidence_evictions += 1
            if (
                self._window_open
                and self._window_start_ns is not None
                and dropped_ts >= self._window_start_ns
            ):
                self._window_evicted_in_window = True

    def last_frame_age_ns(self) -> int | None:
        """Return ns since the last replication frame arrived, or None.

        The driver's inbound-backlog probe reads this: a large age means
        the client's socket or the driver's ingest loop is behind the
        server's delivery cadence, which biases every client-observed
        fidelity metric.
        """
        ts = self._last_frame_ts_ns
        return None if ts is None else time.monotonic_ns() - ts

    def replication_count(self) -> int:
        """Return how many replication frames this connection dispatched.

        Counts direct-type frames dispatched after bootstrap reached
        READY, across reconnects. Frames that rode the engine path before
        READY are absent, so the count trails a full history scan by at
        most the handshake frames.
        """
        return self._replication_count

    # -----------------------------------------------------------------------
    # asyncio entry
    # -----------------------------------------------------------------------

    async def start(self) -> None:
        """Start the IPC and HTTP servers (when enabled) and the connection driver."""
        if self._running:
            return
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()

        if self._ipc_enabled:
            ipc_dir = os.path.dirname(self.ipc_socket_path)
            if ipc_dir:
                os.makedirs(ipc_dir, exist_ok=True)
            if os.path.exists(self.ipc_socket_path):
                os.unlink(self.ipc_socket_path)
            self._ipc_server = await asyncio.start_unix_server(
                self._ipc_handle_peer, path=self.ipc_socket_path
            )

        if self._http_enabled:
            self._http_server = await asyncio.start_server(
                self._http_handle_peer, host=self.http_host, port=self.http_port
            )
            if self.http_port == 0:
                sockets = self._http_server.sockets
                if sockets:
                    self.http_port = sockets[0].getsockname()[1]

        self._connection_task = asyncio.create_task(
            self._run_connection(), name=f"ahc-{self.instance_id}-conn"
        )

    async def stop(self) -> None:
        """Stop all servers, cancel the connection, and release the C engine.

        Every teardown step runs even when an earlier one raises; the
        failures surface together in one group at the end, after the C
        engine is released.
        """
        self._running = False
        errors: list[Exception] = []
        if self._connection_task:
            self._connection_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, KithClientError):
                await self._connection_task
            self._connection_task = None

        if self._ipc_server:
            try:
                self._ipc_server.close()
                await self._ipc_server.wait_closed()
            except Exception as exc:
                errors.append(exc)
            finally:
                self._ipc_server = None
        try:
            if os.path.exists(self.ipc_socket_path):
                os.unlink(self.ipc_socket_path)
        except OSError as exc:
            errors.append(exc)

        if self._http_server:
            try:
                self._http_server.close()
                await self._http_server.wait_closed()
            except Exception as exc:
                errors.append(exc)
            finally:
                self._http_server = None

        for writer in list(self._sse_writers):
            writer.close()
        self._sse_writers.clear()

        if self._transport is not None:
            self._transport.close()
            self._transport = None
            self._protocol = None
        self._inbuf.clear()

        self._destroy_engine()
        if errors:
            raise ExceptionGroup("client teardown left failures", errors)

    def _destroy_engine(self) -> None:
        if self._client:
            self._client_lib.kith_client_destroy(self._client)
            self._client = ctypes.c_void_p()
        if self._proto:
            self._proto_lib.kith_proto_destroy(self._proto)
            self._proto = ctypes.c_void_p()

    # -----------------------------------------------------------------------
    # connection driver
    # -----------------------------------------------------------------------

    async def _run_connection(self) -> None:
        """Connect, pump read/write/tick, and reconnect on loss."""
        while self._running:
            if not self.auto_connect:
                await asyncio.sleep(self._tick_interval_s)
                continue
            loop = asyncio.get_running_loop()
            closed = loop.create_future()
            self._closed_future = closed
            try:
                transport, protocol = await loop.create_connection(
                    lambda: _ClientProtocol(self), self.host, self.port
                )
            except (ConnectionError, OSError) as exc:
                self._closed_future = None
                _log.warning("ahc %s connect failed: %s", self.instance_id, exc)
                if not self._reconnect_enabled:
                    break
                try:
                    _check(
                        int(
                            self._client_lib.kith_client_on_connect_failed(self._client, _now_ms())
                        ),
                        "on_connect_failed",
                    )
                except KithClientError as schedule_exc:
                    _log.warning("ahc %s on_connect_failed: %s", self.instance_id, schedule_exc)
                await self._reconnect_wait()
                continue
            self._transport = transport
            self._protocol = protocol

            _check(
                int(self._client_lib.kith_client_on_connected(self._client, _now_ms())),
                "on_connected",
            )
            self._connected = True
            self._arm_connection()
            self._request_flush()
            _log.info("ahc %s connected to %s:%d", self.instance_id, self.host, self.port)

            try:
                await self._pump()
            finally:
                self._connected = False
                if self._transport is not None:
                    self._transport.close()
                if not closed.done():
                    # Cancellation or a teardown race can end the pump
                    # before asyncio delivered connection_lost; resolve
                    # the future so nothing waits on it again.
                    closed.set_result(None)
                self._transport = None
                self._protocol = None
                self._inbuf.clear()
                try:
                    _check(
                        int(self._client_lib.kith_client_on_disconnected(self._client, _now_ms())),
                        "on_disconnected",
                    )
                except KithClientError as exc:
                    _log.warning("ahc %s on_disconnected: %s", self.instance_id, exc)

            if not self._reconnect_enabled:
                break
            await self._reconnect_wait()

    async def _reconnect_wait(self) -> None:
        """Back off before a reconnect attempt, honoring the engine's schedule."""
        if not self._running:
            return
        rs = ClientRuntimeStatus(
            size=ctypes.sizeof(ClientRuntimeStatus), abi_version=KITH_ABI_VERSION
        )
        rc = int(self._client_lib.kith_client_runtime_status(self._client, ctypes.byref(rs)))
        if rc < 0:
            await asyncio.sleep(0.25)
            return
        deadline_ms = int(rs.next_reconnect_due_ms)
        now_ms = _now_ms()
        delay_s = max(0.0, (deadline_ms - now_ms) / 1000.0) if deadline_ms else 0.25
        await asyncio.sleep(delay_s)

    async def _pump(self) -> None:
        """Run ticks and sweeps until the connection ends.

        There is no per-connection read or write task to await: inbound
        bytes arrive on the protocol callback and outbound frames drain
        inline, so the pump parks on the future that connection_lost
        resolves.
        """
        assert self._protocol is not None
        _ENGINE_TICKER.register(self)
        _OUTBOUND_SWEEPER.register(self)
        try:
            assert self._closed_future is not None
            await self._closed_future
        finally:
            _ENGINE_TICKER.unregister(self)
            _OUTBOUND_SWEEPER.unregister(self)

    def _on_connection_lost(self, exc: Exception | None) -> None:
        """Log a fatal connection error and release the parked pump."""
        if exc is not None:
            _log.warning("ahc %s connection error: %s", self.instance_id, exc)
        fut = self._closed_future
        if fut is not None and not fut.done():
            fut.set_result(None)

    def _request_flush(self) -> None:
        """Drain queued outbound frames toward the socket."""
        self._flush_outbound()

    def _flush_outbound(self) -> None:
        """Pop encoded outbound frames and write them without suspending.

        Runs on the event-loop thread only: submit routes foreign threads
        here via call_soon_threadsafe, and tick, inbound feed, connect,
        and resume_writing already execute on the loop. Inbound decoding
        never waits on this drain: writes are synchronous, and once the
        transport crosses the high-water mark the residue stays
        engine-side until the writable callback or the stalled-outbound
        probe drains it.
        """
        if self._draining or not self._running:
            return
        transport = self._transport
        if transport is None or transport.is_closing():
            return
        out = self._out_buf
        out_len = self._out_len
        self._draining = True
        try:
            while True:
                rc = int(
                    self._client_lib.kith_client_pop_outbound(
                        self._client, out, ctypes.sizeof(out), ctypes.byref(out_len)
                    )
                )
                if rc == -KITH_EAGAIN:
                    return
                _check(rc, "pop_outbound")
                transport.write(bytes(out[: int(out_len.value)]))
                if transport.get_write_buffer_size() >= _OUTBOUND_HIGH_WATER:
                    return
        finally:
            self._draining = False

    def _on_write_resumed(self) -> None:
        """Restart the inline drain below the transport's low-water mark."""
        self._request_flush()

    def _arm_connection(self) -> None:
        """Reset per-connection dispatch state for a fresh connect.

        The direct path stays off until the bootstrap FSM reports READY,
        and summary retention drops the previous connection's payload and
        keep timestamp so the first post-reconnect frame is retained and
        post-reconnect queries reflect the live stream only. The evidence
        deque is left in place while the breadth window is open: the
        window's counting basis is the deque, and frames delivered before
        the reconnect are exactly the frames the window must count —
        wiping them would make the read report the client unfounded even
        when the stream re-delivered a full view set.
        """
        self._direct_ready = False
        self._summary_slot = None
        self._summary_last_keep_ns = None
        if not self._window_open:
            self._evidence.clear()
            self._evidence_bytes = 0

    def _on_socket_data(self, data: bytes) -> None:
        """Consume one socket batch: append the bytes, dispatch frames."""
        self._inbuf.extend(data)
        self._consume_inbound()
        self._request_flush()

    def _consume_inbound(self) -> None:
        """Decode and dispatch all complete frames at the front of the buffer.

        Frames whose type is in ``direct_type_ids`` are dispatched straight
        from the buffer: their header is parsed with :mod:`struct` (no
        ctypes export), skipping the per-frame C decoder call and engine
        callback. In full mode each dispatch builds an event; in summary
        mode the counter advances and a frame's payload is retained at
        most once per interval. Every other frame — engine-reserved
        types, unregistered types, anything anomalous — goes through the
        original decode-and-feed path, which keeps validation authority.
        Either path stops at an incomplete tail; the consumed prefix is
        removed once per batch, after every view export is released,
        including when a frame raises. The method never suspends: it runs
        to completion inside the protocol callback.
        """
        buf = self._inbuf
        pos = 0
        total = len(buf)
        frame = self._decode_frame
        consumed = self._decode_consumed
        if self._direct_type_ids and not self._direct_ready:
            self._poll_direct_ready()
        try:
            while self._running and pos < total:
                matched = self._match_direct_frame(buf, pos)
                if matched is not None:
                    type_id, correlation_id, payload_end, end = matched
                    event_ts = time.monotonic_ns()
                    self._last_frame_ts_ns = event_ts
                    payload_start = pos + _HEADER.size
                    payload_len = payload_end - payload_start
                    if self._evidence_budget_bytes and payload_len:
                        self._append_evidence(event_ts, type_id, buf, payload_start, payload_end)
                    keep = not self._summary_mode or (
                        self._summary_last_keep_ns is None
                        or event_ts - self._summary_last_keep_ns
                        >= int(_SUMMARY_MIN_INTERVAL_S * 1e9)
                    )
                    if keep:
                        event = ClientEvent(
                            ts_mono_ns=event_ts,
                            type_id=type_id,
                            correlation_id=correlation_id,
                            payload=bytes(buf[payload_start:payload_end]),
                        )
                        if self._summary_mode:
                            self._summary_slot = event
                            self._summary_last_keep_ns = event_ts
                        else:
                            self._event_history.append(event)
                            self._pending_push.append(event)
                    self._replication_count += 1
                    pos = end
                    continue
                # The ctypes view exports ``buf`` to the C decoder. A
                # bytearray with a live export refuses to resize, which
                # under free-threaded Python aborts the read loop, so the
                # view dies in this block before any consumption below.
                eagain = False
                remaining = total - pos
                view = (_U8 * remaining).from_buffer(buf, pos)
                try:
                    rc = int(
                        self._proto_lib.kith_proto_decode(
                            self._proto,
                            view,
                            remaining,
                            ctypes.byref(frame),
                            ctypes.byref(consumed),
                        )
                    )
                    if rc == -KITH_EAGAIN:
                        eagain = True
                    else:
                        _check(rc, "proto_decode")
                        _check(
                            int(
                                self._client_lib.kith_client_feed_frame(
                                    self._client,
                                    ctypes.byref(frame),
                                    _now_ms(),
                                )
                            ),
                            "feed_frame",
                        )
                        pos += int(consumed.value)
                finally:
                    del view
                if eagain:
                    break
        except BaseException:
            # Mirror the original loop's incremental consumption: frames
            # dispatched before a failing one leave the buffer and reach
            # subscribers, the rest stays for the read-loop teardown.
            del buf[:pos]
            self._flush_pushes()
            raise
        del buf[:pos]
        self._flush_pushes()

    def _poll_direct_ready(self) -> None:
        """Flip the direct path on once the bootstrap FSM reports READY."""
        bs = ClientBootstrapStatus(
            size=ctypes.sizeof(ClientBootstrapStatus), abi_version=KITH_ABI_VERSION
        )
        rc = int(self._client_lib.kith_client_bootstrap_status(self._client, ctypes.byref(bs)))
        if rc >= 0 and int(bs.state) == _BOOTSTRAP_READY:
            self._direct_ready = True

    def _match_direct_frame(self, buf: bytearray, pos: int) -> tuple[int, int, int, int] | None:
        """Match a direct-type frame at ``pos``; return its dispatch bounds.

        Returns ``(type_id, correlation_id, payload_end, end)`` whenever
        the frame is exactly what the fast path accepts. Returns None
        whenever anything about the frame is not — bootstrap still in
        progress, unknown type, bad magic or version, wire length over
        the configured payload bound, or an incomplete tail — deferring
        the frame to the C decoder. The C path never rejects a frame this
        function accepts: type membership in the direct set implies
        registration, and the bound mirrors the decoder's own payload
        limit.
        """
        if not self._direct_ready or not self._direct_type_ids:
            return None
        magic_end = pos + _HEADER.size
        if magic_end > len(buf):
            return None
        m0, m1, version, flags, type_id, wire_len = _HEADER.unpack_from(buf, pos)
        if m0 != _PROTO_MAGIC0 or m1 != _PROTO_MAGIC1 or version != _PROTO_VERSION:
            return None
        if type_id not in self._direct_type_ids:
            return None
        if wire_len > self._direct_wire_limit:
            return None
        end = pos + _HEADER.size + wire_len
        if end > len(buf):
            return None
        trailer = _CORR_TRAILER_SIZE if flags & KITH_PROTO_FLAG_CORRELATION else 0
        payload_end = end - trailer
        correlation_id = int.from_bytes(buf[payload_end:end], "big") if trailer else 0
        return type_id, correlation_id, payload_end, end

    def probe_stalled_outbound(self) -> None:
        """Warn and drain when outbound sat queued without a trigger.

        Inline draining covers every enqueue point that requests a
        flush, so frames sitting queued with no drain in progress mean
        an enqueue site skipped its request. The shared sweeper calls
        this each interval: pop one frame as a probe — EAGAIN stays
        quiet, anything else logs the miss, sends the popped frame, and
        finishes the drain. Contained failures degrade to a logged
        warning so the sweep never kills its task.
        """
        if not self._running or self._draining:
            return
        transport = self._transport
        if transport is None or transport.is_closing():
            return
        out_len = ctypes.c_uint32(0)
        try:
            rc = int(
                self._client_lib.kith_client_pop_outbound(
                    self._client,
                    _PROBE_SCRATCH,
                    ctypes.sizeof(_PROBE_SCRATCH),
                    ctypes.byref(out_len),
                )
            )
            if rc == -KITH_EAGAIN:
                return
            _check(rc, "pop_outbound")
        except (KithClientError, ConnectionError, OSError) as exc:
            _log.warning("ahc %s outbound probe failed: %s", self.instance_id, exc)
            return
        _log.warning("ahc %s outbound queued without a flush trigger", self.instance_id)
        try:
            transport.write(bytes(_PROBE_SCRATCH[: int(out_len.value)]))
        except (ConnectionError, OSError) as exc:
            _log.warning("ahc %s outbound probe write: %s", self.instance_id, exc)
            return
        self._request_flush()

    def tick_engine(self) -> None:
        """Advance engine timers once and flush anything they queue.

        Called by the shared ticker at the configured cadence; never raises
        (a dead or released engine logs and returns, matching the previous
        per-client tick loop's containment).
        """
        if not self._running:
            return
        try:
            _check(
                int(self._client_lib.kith_client_tick(self._client, _now_ms())),
                "tick",
            )
        except KithClientError as exc:
            _log.warning("ahc %s tick: %s", self.instance_id, exc)
        self._check_pong_staleness()
        self._request_flush()

    def _check_pong_staleness(self) -> None:
        """Tear the transport when the peer stopped answering pings.

        RST and FIN reach the transport; a peer that hangs without
        closing never produces either, and without this check the client
        would sit phantom-live on it forever (submits refuse on the full
        outbound queue while the status snapshot reports connected).
        Closing here fires connection_lost, and the canonical unwind
        takes over: pump exit, on_disconnected, reconnect schedule.
        """
        if self._pong_timeout_ms <= 0:
            return
        rs = ClientRuntimeStatus(
            size=ctypes.sizeof(ClientRuntimeStatus), abi_version=KITH_ABI_VERSION
        )
        if int(self._client_lib.kith_client_runtime_status(self._client, ctypes.byref(rs))) < 0:
            return
        if not rs.awaiting_pong or rs.last_ping_sent_ms == 0:
            return
        elapsed = _now_ms() - int(rs.last_ping_sent_ms)
        if elapsed <= self._pong_timeout_ms:
            return
        _log.warning(
            "ahc %s pong timeout — link presumed dead (sent %d ms ago, timeout %d ms)",
            self.instance_id,
            elapsed,
            self._pong_timeout_ms,
        )
        transport = self._transport
        if transport is not None and not transport.is_closing():
            transport.close()

    # -----------------------------------------------------------------------
    # events
    # -----------------------------------------------------------------------

    def _on_frame_c(self, _client: int, _ctx: int, frame_ptr: Any) -> None:
        """C trampoline for the runtime frame handler.

        Pointer params are typed Any because ctypes passes opaque pointer
        objects that have no precise stub type; only their attributes are used.
        """
        frame = frame_ptr.contents
        addr = frame.payload
        payload = ctypes.string_at(addr, int(frame.payload_len)) if addr else b""
        event = ClientEvent(
            ts_mono_ns=time.monotonic_ns(),
            type_id=int(frame.type_id),
            correlation_id=int(frame.correlation_id) if frame.has_correlation else 0,
            payload=payload,
        )
        self._last_frame_ts_ns = event.ts_mono_ns
        self._event_history.append(event)
        self._pending_push.append(event)

    def _flush_pushes(self) -> None:
        """Push pending events to subscribed SSE and IPC peers (best effort)."""
        if not self._pending_push:
            return
        events = list(self._pending_push)
        self._pending_push.clear()
        for event in events:
            if not self._matches_subscription(event.type_id):
                continue
            line = json.dumps(_event_to_dict(event), separators=(",", ":")) + "\n"
            data = line.encode("utf-8")
            for writer in list(self._sse_writers):
                _safe_write(writer, b"data: " + data.rstrip() + b"\n\n")
            for _r, w in list(self._ipc_peers):
                _safe_write(w, data)

    def _matches_subscription(self, type_id: int) -> bool:
        if self._subscribe_all:
            return True
        return type_id in self._subscriptions

    # -----------------------------------------------------------------------
    # IPC (NDJSON over Unix socket)
    # -----------------------------------------------------------------------

    async def _ipc_handle_peer(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._ipc_peers.add((reader, writer))
        buf = b""
        try:
            while self._running:
                data = await reader.read(_IPC_BUFFER_SIZE)
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        req = json.loads(line.decode("utf-8"))
                    except _JSON_DECODE_ERRORS:
                        continue
                    resp = await self._dispatch_ipc(req)
                    if resp is not None:
                        out = json.dumps(resp, separators=(",", ":")) + "\n"
                        writer.write(out.encode("utf-8"))
                        await writer.drain()
        except _CONN_RESET_ERRORS:
            pass
        finally:
            self._ipc_peers.discard((reader, writer))
            writer.close()

    async def _dispatch_ipc(self, req: Mapping[str, object]) -> dict[str, object] | None:
        req_id = req.get("id")
        cmd = str(req.get("cmd", ""))
        try:
            handler = _IPC_COMMANDS.get(cmd)
            if handler is None:
                return _ack(req_id, "error", error=f"unknown command: {cmd}")
            return await handler(self, req_id, req)
        except KithClientError as exc:
            return _ack(req_id, "error", error=str(exc))
        except (KeyError, ValueError, TypeError) as exc:
            return _ack(req_id, "error", error=f"{type(exc).__name__}: {exc}")

    # -----------------------------------------------------------------------
    # HTTP/1.1 introspection
    # -----------------------------------------------------------------------

    async def _http_handle_peer(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            parts = request_line.decode("iso-8859-1").split()
            if len(parts) < 2:
                _http_respond(writer, 400, b"bad request\n")
                return
            method, path = parts[0], parts[1]
            headers: dict[str, str] = {}
            while True:
                hline = await reader.readline()
                if hline in (b"\r\n", b"\n", b""):
                    break
                key, _, val = hline.decode("iso-8859-1").partition(":")
                headers[key.strip().lower()] = val.strip()

            body = b""
            cl = headers.get("content-length")
            if cl:
                body = await reader.readexactly(min(int(cl), _HTTP_MAX_BODY))

            await self._http_route(method, path, headers, body, writer)
        except _HTTP_READ_ERRORS:
            pass
        finally:
            if writer not in self._sse_writers:
                writer.close()

    async def _http_route(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        writer: asyncio.StreamWriter,
    ) -> None:
        del headers
        if method == "GET" and path.startswith("/health"):
            _http_respond(writer, 200, _json_bytes({"status": "ok", "id": self.instance_id}))
            return
        if method == "GET" and path.startswith("/state"):
            _http_respond(writer, 200, _json_bytes(_status_dict(self.query_status())))
            return
        if method == "GET" and path.startswith("/events"):
            qs = path.partition("?")[2]
            count = 64
            for pair in qs.split("&"):
                if pair.startswith("count="):
                    count = int(pair[7:])
            _http_respond(
                writer, 200, _json_bytes([_event_to_dict(e) for e in self.recent_events(count)])
            )
            return
        if method == "GET" and path.startswith("/stream"):
            self._start_sse(writer)
            return
        if method == "POST" and path.startswith("/cmd"):
            try:
                req = json.loads(body.decode("utf-8")) if body else {}
            except _JSON_DECODE_ERRORS:
                _http_respond(writer, 400, b"bad json\n")
                return
            resp = await self._dispatch_ipc(req)
            _http_respond(writer, 200, _json_bytes(resp or {}))
            return
        _http_respond(writer, 404, b"not found\n")

    def _start_sse(self, writer: asyncio.StreamWriter) -> None:
        head = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: keep-alive\r\n"
            b"\r\n"
        )
        writer.write(head)
        self._sse_writers.add(writer)


# ---------------------------------------------------------------------------
# IPC command handlers
# ---------------------------------------------------------------------------


async def _cmd_connect(
    ahc: AgenticHeadlessClient, req_id: object, _req: Mapping[str, object]
) -> dict[str, object]:
    if ahc._connected:
        return _ack(req_id, "ok")
    ahc.auto_connect = True
    return _ack(req_id, "ok")


async def _cmd_disconnect(
    ahc: AgenticHeadlessClient, req_id: object, _req: Mapping[str, object]
) -> dict[str, object]:
    ahc.auto_connect = False
    if ahc._transport is not None:
        ahc._transport.close()
    return _ack(req_id, "ok")


async def _cmd_submit(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    type_id = _to_int(req["type_id"])
    flags = _to_int(req.get("flags", 0))
    corr = _to_int(req.get("correlation_id", 0))
    payload_hex = req.get("payload_hex")
    payload = bytes.fromhex(str(payload_hex)) if payload_hex else b""
    cmd_id = ahc.submit(type_id, payload, flags=flags, correlation_id=corr)
    return _ack(req_id, "ok", cmd_id=cmd_id)


async def _cmd_register_type(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    name = str(req["name"])
    type_id = _to_int(req["type_id"])
    ahc.register_type(name, type_id)
    return _ack(req_id, "ok")


async def _cmd_query_state(
    ahc: AgenticHeadlessClient, req_id: object, _req: Mapping[str, object]
) -> dict[str, object]:
    result = _status_dict(ahc.query_status())
    result["id"] = req_id
    result["type"] = "state"
    return result


async def _cmd_recent_events(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    count = _to_int(req.get("count", 64))
    return {
        "id": req_id,
        "type": "events",
        "events": [_event_to_dict(e) for e in ahc.recent_events(count)],
    }


async def _cmd_subscribe(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    events = _event_list(req.get("events", []))
    if events == ["*"]:
        ahc._subscribe_all = True
    else:
        for ev in events:
            ahc._subscriptions.add(_to_int(ev))
    return _ack(req_id, "ok", subscriptions=_subscription_state(ahc))


async def _cmd_unsubscribe(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    events = _event_list(req.get("events", []))
    if events == ["*"]:
        ahc._subscribe_all = False
    else:
        for ev in events:
            ahc._subscriptions.discard(_to_int(ev))
    return _ack(req_id, "ok", subscriptions=_subscription_state(ahc))


async def _cmd_shutdown(
    ahc: AgenticHeadlessClient, req_id: object, _req: Mapping[str, object]
) -> dict[str, object]:
    ahc.auto_connect = False
    ahc._running = False
    return _ack(req_id, "ok")


_IpcHandler = Callable[
    [AgenticHeadlessClient, object, Mapping[str, object]],
    Coroutine[None, None, dict[str, object] | None],
]

_IPC_COMMANDS: Mapping[str, _IpcHandler] = {
    "connect": _cmd_connect,
    "disconnect": _cmd_disconnect,
    "submit": _cmd_submit,
    "register_type": _cmd_register_type,
    "query_state": _cmd_query_state,
    "recent_events": _cmd_recent_events,
    "subscribe": _cmd_subscribe,
    "unsubscribe": _cmd_unsubscribe,
    "shutdown": _cmd_shutdown,
}


# ---------------------------------------------------------------------------
# shared engine ticker
# ---------------------------------------------------------------------------


class _TickClient(Protocol):
    """The slice of ``AgenticHeadlessClient`` the shared ticker drives.

    Defined as a protocol so tests substitute fakes without loading the
    C shared libraries; the real client satisfies it structurally.
    """

    _tick_interval_s: float

    def tick_engine(self) -> None: ...


class _ClientTicker:
    """Drive registered clients' engine ticks from one task per cadence.

    Replaces one asyncio sleep loop per client with a single shared task
    per distinct tick interval: each fire calls ``tick_engine`` on every
    registered client, so N clients cost one timer wake per interval
    instead of N. Registry mutations take a lock (set membership is not
    atomic under free-threaded Python); each fire iterates a snapshot and
    rotates its start index so no client holds a systematic first-in-batch
    phase advantage. Cadence is relative-from-yield like the per-client
    sleeps it replaces: sustained overload slips the nominal interval for
    the whole bucket equally.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._buckets: dict[float, list[_TickClient]] = {}
        self._tasks: dict[float, asyncio.Task[None]] = {}
        self._cursors: dict[float, int] = {}

    def register(self, client: _TickClient) -> None:
        """Add ``client`` to the bucket for its configured cadence."""
        with self._lock:
            bucket = self._buckets.setdefault(client._tick_interval_s, [])
            bucket.append(client)
            task = self._tasks.get(client._tick_interval_s)
            if task is None or task.done():
                loop = asyncio.get_running_loop()
                self._tasks[client._tick_interval_s] = loop.create_task(
                    self._run(client._tick_interval_s),
                    name=f"ahc-ticker-{client._tick_interval_s}",
                )

    def unregister(self, client: _TickClient) -> None:
        """Remove ``client`` from its bucket; the last removal keeps the
        task alive so re-registration never pays a restart."""
        with self._lock:
            bucket = self._buckets.get(client._tick_interval_s)
            if bucket is not None:
                with contextlib.suppress(ValueError):
                    bucket.remove(client)

    async def _run(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            with self._lock:
                bucket = list(self._buckets.get(interval, ()))
                cursor = self._cursors.get(interval, 0)
                self._cursors[interval] = cursor + 1
            if not bucket:
                continue
            start = cursor % len(bucket)
            for client in bucket[start:] + bucket[:start]:
                client.tick_engine()


_ENGINE_TICKER = _ClientTicker()


class _ProbeClient(Protocol):
    """The slice of ``AgenticHeadlessClient`` the outbound sweeper drives."""

    def probe_stalled_outbound(self) -> None: ...


class _OutboundSweeper:
    """Probe pumping connections for outbound stranded without a trigger.

    Inline draining runs synchronously at every enqueue point, so a
    queued frame with no drain in progress can only come from an
    enqueue site that skipped its flush request. One shared task sweeps
    the registered connections each interval and lets each one detect —
    and warn about — such a miss. Registry mutations take a lock; like
    the engine ticker, the task persists after the last removal so
    re-registration never pays a restart.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients: set[_ProbeClient] = set()
        self._task: asyncio.Task[None] | None = None

    def register(self, client: _ProbeClient) -> None:
        """Add ``client`` to the sweep set, starting the shared task once."""
        with self._lock:
            self._clients.add(client)
            if self._task is None or self._task.done():
                loop = asyncio.get_running_loop()
                self._task = loop.create_task(self._run(), name="ahc-outbound-sweeper")

    def unregister(self, client: _ProbeClient) -> None:
        """Remove ``client`` from the sweep set."""
        with self._lock:
            self._clients.discard(client)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(_OUTBOUND_IDLE_PROBE_S)
            with self._lock:
                clients = list(self._clients)
            for client in clients:
                client.probe_stalled_outbound()


_OUTBOUND_SWEEPER = _OutboundSweeper()


# ---------------------------------------------------------------------------
# bootstrap trampoline factories
# ---------------------------------------------------------------------------


def _make_enter(step: BootstrapStep) -> Callable[..., int]:
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


def _make_reply(step: BootstrapStep) -> Callable[..., int]:
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


# ---------------------------------------------------------------------------
# serialization helpers
# ---------------------------------------------------------------------------


def _ack(req_id: object, status: str, **fields: object) -> dict[str, object]:
    result: dict[str, object] = {"id": req_id, "type": "ack", "status": status}
    result.update(fields)
    return result


def _subscription_state(ahc: AgenticHeadlessClient) -> dict[str, object]:
    return {
        "all": ahc._subscribe_all,
        "type_ids": sorted(ahc._subscriptions),
    }


def _status_dict(status: ClientStatus) -> dict[str, object]:
    return {
        "connected": status.connected,
        "bootstrap_state": status.bootstrap_state,
        "bootstrap_step": status.bootstrap_step,
        "bootstrap_step_count": status.bootstrap_step_count,
        "rtt_last_ms": status.rtt_last_ms,
        "reconnect_attempts": status.reconnect_attempts,
    }


def _event_to_dict(event: ClientEvent) -> dict[str, object]:
    return {
        "ts_mono_ns": event.ts_mono_ns,
        "type_id": event.type_id,
        "correlation_id": event.correlation_id,
        "payload_hex": event.payload.hex(),
    }


def _json_bytes(obj: object) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def _http_respond(writer: asyncio.StreamWriter, code: int, body: bytes) -> None:
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found"}.get(code, "OK")
    head = (
        f"HTTP/1.1 {code} {reason}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode("iso-8859-1")
    writer.write(head + body)


def _safe_write(writer: asyncio.StreamWriter, data: bytes) -> None:
    with contextlib.suppress(ConnectionError, OSError):
        writer.write(data)


async def main() -> int:
    """CLI entry: run a single AHC until interrupted.

    Connects to ``KITH_AHC_HOST``:``KITH_AHC_PORT`` (default 127.0.0.1:7777),
    exposes IPC at ``KITH_AHC_IPC`` and HTTP at ``KITH_AHC_HTTP_PORT``.
    """
    parser = argparse.ArgumentParser(description="kith agentic headless client")
    parser.add_argument("--id", default="ahc-0", help="instance id")
    parser.add_argument("--host", default=os.environ.get("KITH_AHC_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("KITH_AHC_PORT", "7777")))
    parser.add_argument("--ipc", default=os.environ.get("KITH_AHC_IPC"))
    parser.add_argument(
        "--http-port", type=int, default=int(os.environ.get("KITH_AHC_HTTP_PORT", "0"))
    )
    args = parser.parse_args()

    ahc = AgenticHeadlessClient(
        instance_id=args.id,
        host=args.host,
        port=args.port,
        ipc_socket_path=args.ipc,
        http_port=args.http_port,
    )
    await ahc.start()
    print(f"ahc {args.id}: ipc={ahc.ipc_socket_path} http=127.0.0.1:{ahc.http_port}", flush=True)
    try:
        while ahc._running:
            await asyncio.sleep(3600)
    except _INTERRUPT_ERRORS:
        pass
    await ahc.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

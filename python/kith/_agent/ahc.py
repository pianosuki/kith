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

import asyncio
import concurrent.futures
import contextlib
import ctypes
import json
import logging
import os
import struct
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from functools import partial
from typing import Any

from kith._agent._control import CONN_RESET_ERRORS as CONN_RESET_ERRORS
from kith._agent._control import HTTP_MAX_BODY as HTTP_MAX_BODY
from kith._agent._control import HTTP_READ_ERRORS as HTTP_READ_ERRORS
from kith._agent._control import IPC_BUFFER_SIZE as IPC_BUFFER_SIZE
from kith._agent._control import IPC_COMMANDS as IPC_COMMANDS
from kith._agent._control import JSON_DECODE_ERRORS as JSON_DECODE_ERRORS
from kith._agent._control import IpcHandler as IpcHandler
from kith._agent._control import ack as ack
from kith._agent._control import cmd_connect as cmd_connect
from kith._agent._control import cmd_disconnect as cmd_disconnect
from kith._agent._control import cmd_query_state as cmd_query_state
from kith._agent._control import cmd_recent_events as cmd_recent_events
from kith._agent._control import cmd_register_type as cmd_register_type
from kith._agent._control import cmd_shutdown as cmd_shutdown
from kith._agent._control import cmd_submit as cmd_submit
from kith._agent._control import cmd_subscribe as cmd_subscribe
from kith._agent._control import cmd_unsubscribe as cmd_unsubscribe
from kith._agent._control import event_list as event_list
from kith._agent._control import event_to_dict as event_to_dict
from kith._agent._control import http_handle_peer as http_handle_peer
from kith._agent._control import http_respond as http_respond
from kith._agent._control import ipc_handle_peer as ipc_handle_peer
from kith._agent._control import json_bytes as json_bytes
from kith._agent._control import safe_write as safe_write
from kith._agent._control import status_dict as status_dict
from kith._agent._control import subscription_state as subscription_state
from kith._agent._control import to_int as to_int
from kith._agent._errors import ERROR_NAMES as ERROR_NAMES
from kith._agent._errors import check as check
from kith._agent._evidence import VIEW_WINDOW_ID_CAP as VIEW_WINDOW_ID_CAP
from kith._agent._evidence import EvidenceFrames as EvidenceFrames
from kith._agent._native import CLIENT_LIB_BASENAME as CLIENT_LIB_BASENAME
from kith._agent._native import PROTO_LIB_BASENAME as PROTO_LIB_BASENAME
from kith._agent._native import U8 as U8
from kith._agent._native import EnterFn as EnterFn
from kith._agent._native import FrameFn as FrameFn
from kith._agent._native import FramePtr as FramePtr
from kith._agent._native import ReplyFn as ReplyFn
from kith._agent._native import U8Ptr as U8Ptr
from kith._agent._native import U16Ptr as U16Ptr
from kith._agent._native import U32Ptr as U32Ptr
from kith._agent._native import find_lib as find_lib
from kith._agent._native import load_client as load_client
from kith._agent._native import load_proto as load_proto
from kith._agent._native import make_enter as make_enter
from kith._agent._native import make_reply as make_reply
from kith._agent._scheduling import ENGINE_TICKER as ENGINE_TICKER
from kith._agent._scheduling import OUTBOUND_IDLE_PROBE_S as OUTBOUND_IDLE_PROBE_S
from kith._agent._scheduling import OUTBOUND_SWEEPER as OUTBOUND_SWEEPER
from kith._agent._scheduling import ClientTicker as ClientTicker
from kith._agent._scheduling import OutboundSweeper as OutboundSweeper
from kith._agent._scheduling import ProbeClient as ProbeClient
from kith._agent._scheduling import TickClient as TickClient

from ._cli import run_client
from ._errors import KithClientError as KithClientError
from ._evidence import WindowEvidence as WindowEvidence
from ._evidence import WindowSnapshot as WindowSnapshot
from ._native import KITH_ABI_VERSION as KITH_ABI_VERSION
from ._native import KITH_CLIENT_EVENT_PAYLOAD_MAX as KITH_CLIENT_EVENT_PAYLOAD_MAX
from ._native import KITH_EAGAIN as KITH_EAGAIN
from ._native import KITH_EOVERFLOW as KITH_EOVERFLOW
from ._native import KITH_PROTO_FLAG_CORRELATION as KITH_PROTO_FLAG_CORRELATION
from ._native import KITH_PROTO_HDR_SIZE as KITH_PROTO_HDR_SIZE
from ._native import ClientBootstrapStatus as ClientBootstrapStatus
from ._native import ClientBootstrapStep as ClientBootstrapStep
from ._native import ClientCommand as ClientCommand
from ._native import ClientParams as ClientParams
from ._native import ClientRuntimeStatus as ClientRuntimeStatus
from ._native import ProtoFrame as ProtoFrame
from ._native import ProtoParams as ProtoParams
from ._types import BootstrapStep as BootstrapStep
from ._types import ClientEvent as ClientEvent
from ._types import ClientStatus as ClientStatus
from ._types import DecodedFrame as DecodedFrame


_CLIENT_LIB_BASENAME = CLIENT_LIB_BASENAME
_CONN_RESET_ERRORS = CONN_RESET_ERRORS
_ClientTicker = ClientTicker
_ENGINE_TICKER = ENGINE_TICKER
_ERROR_NAMES = ERROR_NAMES
_EnterFn = EnterFn
_EvidenceFrames = EvidenceFrames
_FrameFn = FrameFn
_FramePtr = FramePtr
_HTTP_MAX_BODY = HTTP_MAX_BODY
_HTTP_READ_ERRORS = HTTP_READ_ERRORS
_IPC_BUFFER_SIZE = IPC_BUFFER_SIZE
_IPC_COMMANDS = IPC_COMMANDS
_IpcHandler = IpcHandler
_JSON_DECODE_ERRORS = JSON_DECODE_ERRORS
_OUTBOUND_IDLE_PROBE_S = OUTBOUND_IDLE_PROBE_S
_OUTBOUND_SWEEPER = OUTBOUND_SWEEPER
_OutboundSweeper = OutboundSweeper
_PROTO_LIB_BASENAME = PROTO_LIB_BASENAME
_ProbeClient = ProbeClient
_ReplyFn = ReplyFn
_TickClient = TickClient
_U16Ptr = U16Ptr
_U32Ptr = U32Ptr
_U8 = U8
_U8Ptr = U8Ptr
_VIEW_WINDOW_ID_CAP = VIEW_WINDOW_ID_CAP
_ack = ack
_check = check
_cmd_connect = cmd_connect
_cmd_disconnect = cmd_disconnect
_cmd_query_state = cmd_query_state
_cmd_recent_events = cmd_recent_events
_cmd_register_type = cmd_register_type
_cmd_shutdown = cmd_shutdown
_cmd_submit = cmd_submit
_cmd_subscribe = cmd_subscribe
_cmd_unsubscribe = cmd_unsubscribe
_event_list = event_list
_event_to_dict = event_to_dict
_find_lib = find_lib
_http_handle_peer = http_handle_peer
_http_respond = http_respond
_ipc_handle_peer = ipc_handle_peer
_json_bytes = json_bytes
_load_client = load_client
_load_proto = load_proto
_make_enter = make_enter
_make_reply = make_reply
_safe_write = safe_write
_status_dict = status_dict
_subscription_state = subscription_state
_to_int = to_int


_log = logging.getLogger("kith.ahc")


def _now_ms() -> int:
    """Monotonic time in milliseconds (the engine's clock domain)."""
    return int(time.monotonic_ns() // 1_000_000)


# ---------------------------------------------------------------------------
# ctypes struct mirrors (size-versioned public ABI)
# ---------------------------------------------------------------------------


# Pointer type aliases used in callback annotations.

# C callback function-pointer types. Instances must be kept alive for the
# client handle's lifetime (stored on the AgenticHeadlessClient instance).


# ---------------------------------------------------------------------------
# the harness
# ---------------------------------------------------------------------------


_OUTBOUND_INITIAL_BUFFER_SIZE = 4096
_UINT32_MAX = (1 << 32) - 1


# The inline drain stops popping once this many bytes sit unwritten in
# the transport: further residue waits engine-side for the writable
# callback instead of migrating queue bulk into the transport buffer.
_OUTBOUND_HIGH_WATER = 256 * 1024

_PROTO_DEFAULT_MAX_PAYLOAD = 1 << 20
_CLIENT_DEFAULT_PING_INTERVAL_MS = 1000

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


# Exception groups caught in multiple sites. Defined once as tuples so
# ruff format does not strip the grouping (it removes parentheses from
# `except (A, B):` because Python 3.14 accepts the bare-comma tuple form,
# but mypy 2.3 flags that form as a syntax error; a single name avoids
# the conflict).
_INTERRUPT_ERRORS = (KeyboardInterrupt, asyncio.CancelledError)


for _record_type in (
    DecodedFrame,
    ClientEvent,
    BootstrapStep,
    ClientStatus,
    WindowEvidence,
    WindowSnapshot,
    KithClientError,
):
    _record_type.__module__ = __name__


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

    ``outbound_buffer_initial_bytes`` sets the starting scratch allocation.
    Storage grows with queued frames, bounded by the configured frame size.
    A zero ping type disables keepalive; a zero interval selects 1000 ms.
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
        outbound_buffer_initial_bytes: int = _OUTBOUND_INITIAL_BUFFER_SIZE,
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
        if not 0 <= max_payload <= _UINT32_MAX:
            raise ValueError("max_payload must fit uint32_t")
        if (
            not isinstance(outbound_buffer_initial_bytes, int)
            or not 0 < outbound_buffer_initial_bytes <= _UINT32_MAX
        ):
            raise ValueError("outbound_buffer_initial_bytes must be a positive uint32_t")
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

        # Allocate scratch before native handles acquire ownership.
        self._direct_wire_limit = max_payload if max_payload else _PROTO_DEFAULT_MAX_PAYLOAD
        self._out_limit = min(KITH_PROTO_HDR_SIZE + self._direct_wire_limit, _UINT32_MAX)
        self._out_buf = (_U8 * min(outbound_buffer_initial_bytes, self._out_limit))()
        self._out_len = ctypes.c_uint32(0)
        # Only an EAGAIN pop proves the native queue empty.
        self._outbound_empty = False
        effective_ping_interval_ms = (
            ctypes.c_uint32(ping_interval_ms).value or _CLIENT_DEFAULT_PING_INTERVAL_MS
        )

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
            ping_interval_ms=effective_ping_interval_ms,
            reconnect_base_ms=reconnect_base_ms,
            reconnect_max_ms=reconnect_max_ms,
            reconnect_max_attempts=reconnect_max_attempts,
        )

        self._registered_type_ids: set[int] = set()
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
        # notices. A zero timeout selects 3x the effective ping interval;
        # a zero ping type leaves the default probe disabled.
        self._ping_interval_ms = (
            effective_ping_interval_ms if ctypes.c_uint16(ping_type_id).value else 0
        )
        self._pong_timeout_ms = pong_timeout_ms or (3 * self._ping_interval_ms)
        self._direct_type_ids = direct_type_ids
        self._pong_type_id = ctypes.c_uint16(pong_type_id).value
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
        # The byte budget bounds raw direct payloads shared with retained events.
        # Snapshot references can outlive oldest-first live eviction.
        self._evidence_budget_bytes = evidence_budget_bytes
        self._evidence = _EvidenceFrames()
        self._evidence_bytes = 0
        self._evidence_evictions = 0
        self._evidence_retired = False
        # Reads apply frozen time bounds and decode retained payloads off ingest.
        # In-window eviction marks the basis incomplete; generations guard caching.
        self._window_id_extractor = window_id_extractor
        self._window_start_ns: int | None = None
        self._window_open = False
        self._window_deadline_ns: int | None = None
        self._window_evidence: WindowEvidence | None = None
        self._window_generation = 0
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
        wire_type_id = type_id & 0xFFFF
        _check(
            int(
                self._proto_lib.kith_proto_register_type_id(
                    self._proto, name.encode("utf-8"), wire_type_id
                )
            ),
            f"register_type {name}",
        )
        self._registered_type_ids.add(wire_type_id)

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
            # The encoder borrows this local buffer only for the synchronous call.
            payload=ctypes.addressof(buf),
            payload_len=len(payload),
        )
        cmd_id = ctypes.c_uint64(0)
        self._outbound_empty = False
        try:
            _check(
                int(
                    self._client_lib.kith_client_submit_interactive(
                        self._client, ctypes.byref(cmd), ctypes.byref(cmd_id)
                    )
                ),
                "submit_interactive",
            )
        finally:
            self._outbound_empty = False
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
        return self._evidence.span_ns()

    def begin_view_window(self) -> None:
        """Open a movement window and resume raw evidence retention."""
        self._window_generation += 1
        self._window_start_ns = time.monotonic_ns()
        self._window_open = True
        self._evidence_retired = False
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
        self._window_generation += 1
        self._window_evidence = None
        self._window_deadline_ns = deadline_ns

    def snapshot_view_window(self) -> WindowSnapshot | None:
        """Capture retained frames, bounds and completeness without decoding.

        Thread safety:
            Runs on the client event-loop thread, alongside frame ingestion.
        """
        if not self._window_open:
            return None
        return WindowSnapshot(
            tuple(self._evidence),
            self._window_start_ns,
            self._window_deadline_ns,
            self._window_evicted_in_window or self._evidence_budget_bytes == 0,
            self._window_id_extractor,
        )

    def retire_view_window(self) -> None:
        """Release live raw evidence and pause it until a new window begins.

        Captured snapshots retain their payloads. Event history, summary
        retention and replication counters continue independently.

        Thread safety:
            Runs on the client event loop, alongside frame ingestion.
        """
        if self._evidence_retired:
            return
        self._window_generation += 1
        self._window_open = False
        self._window_evidence = None
        self._evidence_retired = True
        self._evidence.clear()
        self._evidence_bytes = 0

    def view_window(self) -> WindowEvidence | None:
        """Return window evidence, caching only captures after the sealed deadline.

        Reads before the deadline remain provisional and admit subsequent frames.
        """
        if self._window_evidence is not None:
            return self._window_evidence
        generation = self._window_generation
        captured_ns = time.monotonic_ns()
        snapshot = self.snapshot_view_window()
        if snapshot is None:
            return None
        final = snapshot.deadline_ns is not None and captured_ns >= snapshot.deadline_ns
        evidence = snapshot.evaluate()
        self._cache_window_evidence(generation, final, evidence)
        return evidence

    async def view_window_async(self) -> WindowEvidence | None:
        """Return window evidence with cooperative decoding on the client loop.

        Captures before the sealed deadline remain provisional. Opening or
        resealing a window while decoding prevents the result entering its cache.
        """
        if self._window_evidence is not None:
            return self._window_evidence
        generation = self._window_generation
        captured_ns = time.monotonic_ns()
        snapshot = self.snapshot_view_window()
        if snapshot is None:
            return None
        final = snapshot.deadline_ns is not None and captured_ns >= snapshot.deadline_ns
        evidence = await snapshot.evaluate_async()
        self._cache_window_evidence(generation, final, evidence)
        return evidence

    def _cache_window_evidence(
        self, generation: int, final: bool, evidence: WindowEvidence
    ) -> None:
        if final and generation == self._window_generation and self._window_evidence is None:
            self._window_evidence = evidence

    def _append_evidence(self, ts_ns: int, type_id: int, payload: bytes) -> None:
        """Retain one raw frame payload in the breadth-evidence window.

        Direct frames are replication frames by construction (the direct
        catalog carries the replication types), so no per-type gate
        applies here. The running byte counter keeps eviction O(1) per
        frame; staleness is judged at read time, and a single frame
        larger than the whole budget leaves an empty window rather than
        an over-budget one.
        """
        # Direct deque writes avoid another Python call for every retained frame.
        metadata, payloads = self._evidence._metadata, self._evidence._payloads
        metadata_length, payload_length = len(metadata), len(payloads)
        try:
            metadata.append(ts_ns)
            metadata.append(type_id)
            payloads.append(payload)
        except BaseException:
            while len(payloads) > payload_length:
                payloads.pop()
            while len(metadata) > metadata_length:
                metadata.pop()
            raise
        self._evidence_bytes += len(payload)
        while self._evidence_bytes > self._evidence_budget_bytes and payloads:
            dropped_in_window = (
                self._window_open
                and self._window_start_ns is not None
                and not self._window_evicted_in_window
                and metadata[0] >= self._window_start_ns
            )
            metadata.popleft()
            metadata.popleft()
            dropped = payloads.popleft()
            self._evidence_bytes -= len(dropped)
            self._evidence_evictions += 1
            if dropped_in_window:
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
                partial(_ipc_handle_peer, self), path=self.ipc_socket_path
            )

        if self._http_enabled:
            self._http_server = await asyncio.start_server(
                partial(_http_handle_peer, self), host=self.http_host, port=self.http_port
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

            self._outbound_empty = False
            try:
                _check(
                    int(self._client_lib.kith_client_on_connected(self._client, _now_ms())),
                    "on_connected",
                )
            finally:
                self._outbound_empty = False
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
        self._outbound_empty = False
        self._flush_outbound()

    def _pop_outbound(self) -> bytes | None:
        """Copy the next frame, growing scratch while the queue retains ownership."""
        self._outbound_empty = False
        while True:
            out = self._out_buf
            capacity = ctypes.sizeof(out)
            rc = int(
                self._client_lib.kith_client_pop_outbound(
                    self._client, out, capacity, ctypes.byref(self._out_len)
                )
            )
            if rc == -KITH_EAGAIN:
                self._outbound_empty = True
                return None
            if rc == -KITH_EOVERFLOW and capacity < self._out_limit:
                self._out_buf = (_U8 * min(capacity * 2, self._out_limit))()
                continue
            _check(rc, "pop_outbound")
            return ctypes.string_at(out, int(self._out_len.value))

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
        self._draining = True
        try:
            while True:
                frame = self._pop_outbound()
                if frame is None:
                    return
                transport.write(frame)
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
        if not self._outbound_empty:
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
                    retain_evidence = bool(
                        self._evidence_budget_bytes and payload_len and not self._evidence_retired
                    )
                    keep = not self._summary_mode or (
                        self._summary_last_keep_ns is None
                        or event_ts - self._summary_last_keep_ns
                        >= int(_SUMMARY_MIN_INTERVAL_S * 1e9)
                    )
                    if retain_evidence or keep:
                        # Retention shares immutable bytes after receive-buffer compaction.
                        with (
                            memoryview(buf) as inbound,
                            inbound[payload_start:payload_end] as payload_view,
                        ):
                            payload = bytes(payload_view)
                        if retain_evidence:
                            self._append_evidence(event_ts, type_id, payload)
                        if keep:
                            event = ClientEvent(
                                ts_mono_ns=event_ts,
                                type_id=type_id,
                                correlation_id=correlation_id,
                                payload=payload,
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
                        self._outbound_empty = False
                        try:
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
                        finally:
                            # A bootstrap callback can drain before C queues its next step.
                            self._outbound_empty = False
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
        the frame to the C decoder. Accepted frames use registered types and
        satisfy the decoder's payload and correlation-trailer bounds. Pong
        frames always pass through the engine's keepalive handler.
        """
        if not self._direct_ready or not self._direct_type_ids:
            return None
        magic_end = pos + _HEADER.size
        if magic_end > len(buf):
            return None
        m0, m1, version, flags, type_id, wire_len = _HEADER.unpack_from(buf, pos)
        if m0 != _PROTO_MAGIC0 or m1 != _PROTO_MAGIC1 or version != _PROTO_VERSION:
            return None
        if type_id not in self._direct_type_ids or type_id not in self._registered_type_ids:
            return None
        if self._pong_type_id and type_id == self._pong_type_id:
            return None
        if wire_len > self._direct_wire_limit:
            return None
        end = pos + _HEADER.size + wire_len
        if end > len(buf):
            return None
        trailer = _CORR_TRAILER_SIZE if flags & KITH_PROTO_FLAG_CORRELATION else 0
        if wire_len < trailer:
            return None
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
        try:
            frame = self._pop_outbound()
            if frame is None:
                return
            _log.warning("ahc %s outbound queued without a flush trigger", self.instance_id)
            transport.write(frame)
            self._request_flush()
        except (KithClientError, ConnectionError, OSError, MemoryError) as exc:
            _log.warning("ahc %s outbound probe failed: %s", self.instance_id, exc)

    def tick_engine(self) -> None:
        """Advance engine timers once and flush anything they queue.

        Called by the shared ticker at the configured cadence; never raises
        (a dead or released engine logs and returns, matching the previous
        per-client tick loop's containment).
        """
        if not self._running:
            return
        self._outbound_empty = False
        try:
            _check(
                int(self._client_lib.kith_client_tick(self._client, _now_ms())),
                "tick",
            )
        except KithClientError as exc:
            _log.warning("ahc %s tick: %s", self.instance_id, exc)
        finally:
            self._outbound_empty = False
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

    # -----------------------------------------------------------------------
    # HTTP/1.1 introspection
    # -----------------------------------------------------------------------


# ---------------------------------------------------------------------------
# serialization helpers
# ---------------------------------------------------------------------------


async def main() -> int:
    """Run one client with command-line configuration."""
    return await run_client(AgenticHeadlessClient)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

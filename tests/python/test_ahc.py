"""Unit tests for AgenticHeadlessClient server-gating and lifecycle flags."""

from __future__ import annotations

import asyncio
import ctypes
import gc
import inspect
import itertools
import logging
import socket
import sys
import time
import weakref
from collections import deque
from collections.abc import Callable
from typing import Any, Never, SupportsIndex, overload

import pytest

import kith._agent.ahc as ahc
from kith._agent.ahc import (
    _CLIENT_LIB_BASENAME,
    _PROTO_LIB_BASENAME,
    _VIEW_WINDOW_ID_CAP,
    AgenticHeadlessClient,
    KithClientError,
    _find_lib,
)


def _libs_available() -> bool:
    """Return True if the AHC's C libraries resolve via its own lookup."""
    try:
        _find_lib(_PROTO_LIB_BASENAME, "KITH_PROTO_LIB", None)
        _find_lib(_CLIENT_LIB_BASENAME, "KITH_CLIENT_LIB", None)
    except KithClientError:
        return False
    return True


def _refused_endpoint() -> tuple[int, socket.socket]:
    """Bind (without listening) so connects are refused, race-free.

    The returned socket stays bound for the caller's scope; closing it in
    the test's teardown frees the port. A bound, non-listening socket
    makes the kernel reject connects with ECONNREFUSED rather than letting
    another process grab a closed port in between.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    return sock.getsockname()[1], sock


_SKIP_AHC_LIBS = "ahc C libraries not available; build them with 'cmake --build build/debug' (or point KITH_PROTO_LIB/KITH_CLIENT_LIB at an existing build)"

pytestmark = [
    pytest.mark.skipif(not _libs_available(), reason=_SKIP_AHC_LIBS),
]


@pytest.fixture
def build_ahc() -> Callable[..., AgenticHeadlessClient]:
    """Return a factory that builds an AHC, skipping if C libs are missing.

    The factory defaults to ``auto_connect=False`` so the connection driver
    idles and the tests do not need a live server; callers that need a real
    connect attempt override it explicitly. Each instance gets a unique id
    so the Unix-socket IPC path never collides across tests in a run.
    """
    counter = itertools.count()

    def _build(
        *,
        host: str = "127.0.0.1",
        port: int = 7777,
        http_enabled: bool = True,
        ipc_enabled: bool = True,
        auto_connect: bool = False,
        reconnect_enabled: bool = True,
        tick_interval_s: float = 0.02,
        max_payload: int = 0,
        outbound_buffer_initial_bytes: int = 4096,
        direct_type_ids: frozenset[int] = frozenset(),
        summary_mode: bool = False,
        evidence_budget_bytes: int = 0,
        window_id_extractor: Callable[[int, bytes], Any] | None = None,
    ) -> AgenticHeadlessClient:
        if not _libs_available():
            pytest.skip(_SKIP_AHC_LIBS)
        instance_id = f"test-ahc-{next(counter)}"
        return AgenticHeadlessClient(
            instance_id,
            host=host,
            port=port,
            http_enabled=http_enabled,
            ipc_enabled=ipc_enabled,
            auto_connect=auto_connect,
            reconnect_enabled=reconnect_enabled,
            tick_interval_s=tick_interval_s,
            max_payload=max_payload,
            outbound_buffer_initial_bytes=outbound_buffer_initial_bytes,
            direct_type_ids=direct_type_ids,
            summary_mode=summary_mode,
            evidence_budget_bytes=evidence_budget_bytes,
            window_id_extractor=window_id_extractor,
        )

    return _build


def test_http_disabled_leaves_no_http_server(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """http_enabled=False skips HTTP server creation in start()."""
    ahc = build_ahc(http_enabled=False, ipc_enabled=False)

    async def run() -> None:
        await ahc.start()
        assert ahc._http_server is None
        await ahc.stop()

    asyncio.run(run())


def test_ipc_disabled_leaves_no_ipc_server(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """ipc_enabled=False skips IPC server creation in start()."""
    ahc = build_ahc(http_enabled=False, ipc_enabled=False)

    async def run() -> None:
        await ahc.start()
        assert ahc._ipc_server is None
        await ahc.stop()

    asyncio.run(run())


def test_defaults_start_both_servers(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """The default flags start both the IPC and HTTP servers."""
    ahc = build_ahc()

    async def run() -> None:
        await ahc.start()
        assert ahc._http_server is not None
        assert ahc._ipc_server is not None
        await ahc.stop()

    asyncio.run(run())


def test_reconnect_disabled_exits_after_one_attempt(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """reconnect_enabled=False makes the connection task exit, not loop.

    Pointing at a bound-but-not-listening port yields ECONNREFUSED on the
    first connect; with reconnect disabled the driver breaks out of its
    loop instead of backing off and retrying, so the task completes well
    inside the timeout.
    """
    port, keep_bound = _refused_endpoint()
    try:
        ahc = build_ahc(
            auto_connect=True,
            reconnect_enabled=False,
            host="127.0.0.1",
            port=port,
            http_enabled=False,
            ipc_enabled=False,
        )

        async def run() -> None:
            await ahc.start()
            assert ahc._connection_task is not None
            await asyncio.wait_for(ahc._connection_task, timeout=2.0)
            await ahc.stop()

        asyncio.run(run())
    finally:
        keep_bound.close()


def test_tick_interval_governs_tick_cadence(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """tick_interval_s sets the engine tick cadence via the shared ticker.

    A 0.1s interval over a 0.4s window yields a handful of ticks; the 0.02s
    default would produce an order of magnitude more, so a tight upper
    bound distinguishes the configured cadence from the default.
    """
    ahc_client = build_ahc(auto_connect=False, tick_interval_s=0.1)
    ticks: list[int] = []

    def fake_tick(_client: object, now: int) -> int:
        ticks.append(now)
        return 0

    monkeypatch.setattr(ahc_client._client_lib, "kith_client_tick", fake_tick)

    async def run() -> None:
        ahc_client._running = True
        ticker = ahc._ClientTicker()
        ticker.register(ahc_client)
        await asyncio.sleep(0.4)
        ticker.unregister(ahc_client)
        await asyncio.sleep(0.1)
        ahc_client._running = False

    asyncio.run(run())
    assert 1 <= len(ticks) <= 7


class _FakeTickerClient:
    """Minimal client stand-in for shared-ticker scheduling tests."""

    def __init__(self, name: str, interval: float) -> None:
        self.name = name
        self._tick_interval_s = interval
        self.fires: list[int] = []

    def tick_engine(self) -> None:
        self.fires.append(len(self.fires))


def test_shared_ticker_fires_every_registered_client() -> None:
    """One fire per interval reaches every client in the bucket."""
    ticker = ahc._ClientTicker()
    clients = [_FakeTickerClient(f"c{i}", 0.05) for i in range(3)]

    async def run() -> None:
        for client in clients:
            ticker.register(client)
        await asyncio.sleep(0.22)

    asyncio.run(run())
    assert all(len(client.fires) >= 3 for client in clients)


def test_shared_ticker_rotates_fire_order() -> None:
    """The iteration start rotates so no client is always first in batch."""
    ticker = ahc._ClientTicker()
    order: list[str] = []
    clients = [_FakeTickerClient(name, 0.02) for name in ("a", "b")]
    for client in clients:
        client.tick_engine = lambda name=client.name: order.append(name)  # type: ignore[method-assign,misc]

    async def run() -> None:
        for client in clients:
            ticker.register(client)
        await asyncio.sleep(0.15)

    asyncio.run(run())
    assert len(order) >= 5
    pairs = {(order[i], order[i + 1]) for i in range(0, len(order) - 1, 2)}
    assert ("a", "b") in pairs and ("b", "a") in pairs


def test_shared_ticker_unregister_stops_fires() -> None:
    """A removed client stops ticking; remaining clients continue."""
    ticker = ahc._ClientTicker()
    kept = _FakeTickerClient("kept", 0.02)
    dropped = _FakeTickerClient("dropped", 0.02)

    async def run() -> None:
        ticker.register(kept)
        ticker.register(dropped)
        await asyncio.sleep(0.07)
        ticker.unregister(dropped)
        dropped.fires.clear()
        kept.fires.clear()
        await asyncio.sleep(0.07)

    asyncio.run(run())
    assert not dropped.fires
    assert len(kept.fires) >= 1


def _encode_frame(
    client: AgenticHeadlessClient,
    type_id: int,
    payload: bytes,
    *,
    flags: int = 0,
    correlation_id: int = 0,
) -> bytes:
    """Encode one wire frame through the client's own proto codec."""
    lib = client._proto_lib
    encode = lib.kith_proto_encode
    encode.restype = ctypes.c_size_t
    encode.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint16,
        ctypes.c_uint8,
        ctypes.c_uint64,
        ctypes.c_char_p,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    buf = ctypes.create_string_buffer(len(payload) + 64)
    n = int(
        encode(
            client._proto,
            type_id,
            flags,
            correlation_id,
            payload,
            len(payload),
            buf,
            len(buf),
        )
    )
    assert n > 0
    return buf.raw[:n]


def test_request_flush_drains_queued_frames_inline(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """A flush request writes queued frames synchronously, no task between."""
    ahc_client = build_ahc(http_enabled=False, ipc_enabled=False)
    transport = _RecordingTransport()
    assert not transport.chunks
    # Frames queued while no transport exists cannot reach the socket;
    # attaching one and requesting the flush drains them inline.
    ahc_client.submit(_DIRECT_TYPE, b"queued")
    ahc_client._transport = transport  # type: ignore[assignment]
    ahc_client._running = True
    ahc_client._request_flush()
    assert len(transport.chunks) == 1
    assert len(transport.chunks[0]) > 0


def test_submit_releases_borrowed_buffer_without_cyclic_collection(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Submission keeps its buffer through encoding and releases it on return."""
    client = build_ahc(http_enabled=False, ipc_enabled=False)
    payload = b"binary\x00payload\xff"
    buffer_type = ctypes.c_uint8 * len(payload)
    copy_buffer = buffer_type.from_buffer_copy
    references: list[weakref.ReferenceType[Any]] = []

    def observe_buffer(data: bytes) -> Any:
        buffer = copy_buffer(data)
        references.append(weakref.ref(buffer))
        return buffer

    submit_native = client._client_lib.kith_client_submit_interactive

    def encode(handle: Any, command_ptr: Any, command_id: Any) -> int:
        command = command_ptr._obj
        if command.payload_len:
            gc.collect()
            assert references[-1]() is not None
            assert ctypes.string_at(command.payload, command.payload_len) == payload
        return int(submit_native(handle, command_ptr, command_id))

    monkeypatch.setattr(buffer_type, "from_buffer_copy", staticmethod(observe_buffer))
    monkeypatch.setattr(client._client_lib, "kith_client_submit_interactive", encode)
    try:
        client.register_type("state", _DIRECT_TYPE)
        assert client.submit(_DIRECT_TYPE, payload) == 1
        assert len(references) == 1 and references[0]() is None
        assert client._pop_outbound() == _encode_frame(client, _DIRECT_TYPE, payload)
        assert client.submit(_DIRECT_TYPE) == 2
        assert client._pop_outbound() == _encode_frame(client, _DIRECT_TYPE, b"")
        assert client._pop_outbound() is None
    finally:
        client._destroy_engine()


_DIRECT_TYPE = 7
_UNREGISTERED_TYPE = 999


def _drain(client: AgenticHeadlessClient, buf: bytearray) -> None:
    """Feed ``buf`` through the socket-data path; mirror any tail back.

    The client owns the inbound buffer across data_received calls; the
    mirror preserves the assertion surface where unconsumed tail
    bytes stay visible in the caller's bytearray, including when a
    frame raises out of the consumer.
    """
    client._running = True
    try:
        client._on_socket_data(bytes(buf))
    finally:
        del buf[:]
        buf.extend(client._inbuf)
        del client._inbuf[:]


def test_direct_frames_become_events_without_engine_feed(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct-type frames build events in the drain loop; feed never runs."""
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    client.register_type("state", _DIRECT_TYPE)
    feeds: list[int] = []

    def no_feed(_client: object, _frame: object, _now: int) -> int:
        feeds.append(1)
        return 0

    monkeypatch.setattr(client._client_lib, "kith_client_feed_frame", no_feed)
    transport = _RecordingTransport()
    client._transport = transport  # type: ignore[assignment]
    assert client._pop_outbound() is None
    native_pop = client._client_lib.kith_client_pop_outbound
    pops: list[int] = []

    def count_pop(*args: Any) -> int:
        pops.append(1)
        return int(native_pop(*args))

    monkeypatch.setattr(client._client_lib, "kith_client_pop_outbound", count_pop)
    buf = bytearray(
        _encode_frame(client, _DIRECT_TYPE, b"abc") + _encode_frame(client, _DIRECT_TYPE, b"de")
    )
    client._direct_ready = True
    _drain(client, buf)
    assert not feeds
    assert not pops
    assert not buf
    assert [event.payload for event in client._event_history] == [b"abc", b"de"]
    assert all(event.type_id == _DIRECT_TYPE for event in client._event_history)
    assert all(event.ts_mono_ns > 0 for event in client._event_history)
    client.register_type("control", 8)
    _drain(client, bytearray(_encode_frame(client, 8, b"control")))
    assert feeds == [1] and pops == [1]


def test_bootstrap_reentrant_submit_preserves_followup_flush(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """A nested drain cannot hide the next frame queued by native bootstrap."""
    client = build_ahc(http_enabled=False, ipc_enabled=False)
    transport = _RecordingTransport()
    client._transport = transport  # type: ignore[assignment]
    client._running = True

    def advance(frame: ahc.DecodedFrame) -> int:
        assert frame.type_id == 9
        assert client.submit(7, b"nested") == 1
        assert client._outbound_empty
        return 1

    try:
        for type_id in (7, 8, 9, 10):
            client.register_type(f"type-{type_id}", type_id)
        client._configure_bootstrap(
            [
                ahc.BootstrapStep(9, lambda: (7, b"hello"), advance),
                ahc.BootstrapStep(10, lambda: (8, b"next")),
            ]
        )
        assert client._client_lib.kith_client_on_connected(client._client, 100) == 0
        client._request_flush()
        client._on_socket_data(_encode_frame(client, 9, b"reply"))
        assert transport.chunks == [
            _encode_frame(client, 7, b"hello"),
            _encode_frame(client, 7, b"nested"),
            _encode_frame(client, 8, b"next"),
        ]
        assert client._outbound_empty
        assert client._pop_outbound() is None
    finally:
        client._running = False
        client._destroy_engine()


def test_mixed_fast_and_fallback_frames_share_one_consume(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Fallback re-enters at the advanced position; a bad frame keeps the tail."""
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    client.register_type("state", _DIRECT_TYPE)
    direct = _encode_frame(client, _DIRECT_TYPE, b"ok")
    garbage = _encode_frame(client, _UNREGISTERED_TYPE, b"?")
    trailing = _encode_frame(client, _DIRECT_TYPE, b"tail")
    buf = bytearray(direct + garbage + trailing)
    client._direct_ready = True

    with pytest.raises(KithClientError):
        _drain(client, buf)
    # The leading direct frame was dispatched and consumed; the unregistered
    # frame raised from the C decoder; everything from it onward is intact.
    assert [event.payload for event in client._event_history] == [b"ok"]
    assert bytes(buf) == garbage + trailing


def test_correlation_trailer_parsed_on_direct_frames(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """A flagged trailer lands in correlation_id; the payload excludes it."""
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    client.register_type("state", _DIRECT_TYPE)
    frame = _encode_frame(
        client,
        _DIRECT_TYPE,
        b"payload",
        flags=0x02,
        correlation_id=0xDEADBEEFCAFEF00D,
    )
    buf = bytearray(frame)
    client._direct_ready = True
    _drain(client, buf)
    (event,) = client._event_history
    assert event.correlation_id == 0xDEADBEEFCAFEF00D
    assert event.payload == b"payload"
    assert not buf


def test_short_correlation_trailer_matches_native_rejection(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Direct candidates retain the native decoder's trailer validation."""
    outcomes: list[tuple[int, bytes, int]] = []
    for direct_types in (frozenset(), frozenset({_DIRECT_TYPE})):
        client = build_ahc(direct_type_ids=direct_types)
        try:
            client.register_type("state", _DIRECT_TYPE)
            frame = bytearray(_encode_frame(client, _DIRECT_TYPE, b"", flags=0x02))
            frame[6:10] = (7).to_bytes(4, "big")
            frame.pop()
            original = bytes(frame)
            client._direct_ready = True
            with pytest.raises(KithClientError) as error:
                _drain(client, frame)
            outcomes.append((error.value.code, bytes(frame), len(client._event_history)))
            assert bytes(frame) == original
        finally:
            client._running = False
            client._destroy_engine()
    assert outcomes[0] == outcomes[1]
    assert outcomes[0][2] == 0


def test_direct_type_requires_successful_registration(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Deferred registration retains the native unknown-type boundary."""
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    try:
        frame = bytearray(_encode_frame(client, _DIRECT_TYPE, b"payload"))
        original = bytes(frame)
        client._direct_ready = True
        with pytest.raises(KithClientError):
            _drain(client, frame)
        assert bytes(frame) == original and not client._event_history
        with pytest.raises(KithClientError):
            client.register_type("", _DIRECT_TYPE)
        with pytest.raises(KithClientError):
            _drain(client, frame)
        client.register_type("state", (1 << 16) + _DIRECT_TYPE)
        _drain(client, frame)
        assert not frame and [event.payload for event in client._event_history] == [b"payload"]
    finally:
        client._running = False
        client._destroy_engine()


@pytest.mark.parametrize("ping_type_id", [0, 7])
def test_direct_pong_stays_on_native_client_path(
    ping_type_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The configured pong is handled by the engine with or without ping."""
    client = AgenticHeadlessClient(
        "direct-pong",
        http_enabled=False,
        ipc_enabled=False,
        auto_connect=False,
        ping_type_id=ping_type_id,
        pong_type_id=(1 << 16) + 8,
        message_types={"ping": 7, "pong": 8},
        direct_type_ids=frozenset({8}),
    )
    client._running = True
    client._direct_ready = True
    monkeypatch.setattr(ahc, "_now_ms", lambda: 1107)
    try:
        assert client._client_lib.kith_client_on_connected(client._client, 100) == 0
        assert client._client_lib.kith_client_tick(client._client, 1100) == 0
        status = ahc.ClientRuntimeStatus(
            size=ctypes.sizeof(ahc.ClientRuntimeStatus), abi_version=ahc.KITH_ABI_VERSION
        )
        assert (
            client._client_lib.kith_client_runtime_status(client._client, ctypes.byref(status)) == 0
        )
        assert bool(status.awaiting_pong) is bool(ping_type_id)
        client._on_socket_data(_encode_frame(client, 8, (1100).to_bytes(8, "big")))
        assert (
            client._client_lib.kith_client_runtime_status(client._client, ctypes.byref(status)) == 0
        )
        assert not status.awaiting_pong
        assert status.rtt_samples == (1 if ping_type_id else 0)
        assert not client._event_history and client.replication_count() == 0
    finally:
        client._running = False
        client._destroy_engine()


def test_incomplete_direct_tail_waits_for_more_bytes(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """A truncated direct frame stays buffered with no event emitted."""
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    client.register_type("state", _DIRECT_TYPE)
    whole = _encode_frame(client, _DIRECT_TYPE, b"whole")
    buf = bytearray(whole[:-3])
    client._direct_ready = True
    _drain(client, buf)
    assert not client._event_history
    assert bytes(buf) == whole[:-3]


def test_bootstrap_gate_keeps_await_frame_on_engine_path(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before READY a direct-type frame still feeds the engine bootstrap.

    The login handshake awaits the first replication frame, so the fast
    path must stay off until the FSM reports READY; after that the same
    frame shape dispatches without touching the engine.
    """
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    client.register_type("state", _DIRECT_TYPE)
    assert not client._direct_ready
    buf = bytearray(_encode_frame(client, _DIRECT_TYPE, b"handshake"))
    _drain(client, buf)
    # No bootstrap steps configured, so the real engine hands the frame
    # to its user callback: one event, built by the C trampoline.
    assert len(client._event_history) == 1

    feeds: list[int] = []

    def count_feed(_client: object, _frame: object, _now: int) -> int:
        feeds.append(1)
        return 0

    monkeypatch.setattr(client._client_lib, "kith_client_feed_frame", count_feed)
    client._direct_ready = True
    buf = bytearray(_encode_frame(client, _DIRECT_TYPE, b"stream"))
    _drain(client, buf)
    assert not feeds
    assert len(client._event_history) == 2


def test_bad_magic_defers_to_decoder_and_keeps_bytes(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Corrupted header bytes fall to the C decoder and fail loudly there."""
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    client.register_type("state", _DIRECT_TYPE)
    frame = bytearray(_encode_frame(client, _DIRECT_TYPE, b"x"))
    frame[0] ^= 0xFF
    buf = bytearray(frame)
    client._direct_ready = True
    with pytest.raises(KithClientError):
        _drain(client, buf)
    assert not client._event_history
    assert bytes(buf) == bytes(frame)


def test_empty_direct_set_matches_legacy_drain(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without direct types every frame rides the decode-and-feed path."""
    client = build_ahc()
    client.register_type("state", _DIRECT_TYPE)
    feeds: list[int] = []

    def count_feed(_client: object, _frame: object, _now: int) -> int:
        feeds.append(1)
        return 0

    monkeypatch.setattr(client._client_lib, "kith_client_feed_frame", count_feed)
    frames = _encode_frame(client, _DIRECT_TYPE, b"a") + _encode_frame(client, _DIRECT_TYPE, b"b")
    buf = bytearray(frames)
    _drain(client, buf)
    # Both frames reach the engine; the fake feed short-circuits the
    # callback chain, so no events materialize (legacy event building is
    # covered by the bootstrap-gate test's real-feed phase).
    assert feeds == [1, 1]
    assert not client._event_history
    assert not buf


def test_summary_mode_retains_time_spaced_payloads_only(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Summary retention keeps the first frame, then one per interval."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        summary_mode=True,
    )
    client.register_type("state", _DIRECT_TYPE)
    clock = iter([1_000, 1_100, 2_500_000_000])
    monkeypatch.setattr("kith._agent.ahc.time.monotonic_ns", lambda: next(clock))
    payloads = [b"first", b"too-soon", b"later"]
    frames = b"".join(_encode_frame(client, _DIRECT_TYPE, p) for p in payloads)
    buf = bytearray(frames)
    client._direct_ready = True
    _drain(client, buf)
    assert not buf
    assert not client._event_history
    assert not client._pending_push
    assert client.replication_count() == 3
    synthesized = client.recent_events(count=64)
    assert len(synthesized) == 1
    assert synthesized[0].payload == b"later"
    assert synthesized[0].type_id == _DIRECT_TYPE


def test_summary_mode_gates_on_bootstrap_and_rearms(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Pre-READY frames ride the engine path; reconnect clears the slot."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        summary_mode=True,
    )
    client.register_type("state", _DIRECT_TYPE)
    handshake = bytearray(_encode_frame(client, _DIRECT_TYPE, b"handshake"))
    _drain(client, handshake)
    # Pre-READY: the engine consumed the frame and its trampoline built
    # the event; the summary path stayed off.
    assert len(client._event_history) == 1
    assert client.replication_count() == 0
    assert client.recent_events() == []

    stream = bytearray(_encode_frame(client, _DIRECT_TYPE, b"post-ready"))
    client._direct_ready = True
    _drain(client, stream)
    assert client.replication_count() == 1
    assert client.recent_events()[0].payload == b"post-ready"

    client._arm_connection()
    assert client._summary_slot is None
    assert client._summary_last_keep_ns is None
    assert client._direct_ready is False


def test_summary_mode_evidence_window_retains_every_frame(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The evidence window keeps every direct frame; no length gate applies."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        summary_mode=True,
        evidence_budget_bytes=1024,
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    clock = iter((1000, 1100, 1200))
    monkeypatch.setattr("kith._agent.ahc.time.monotonic_ns", lambda: next(clock))
    frames = b"".join(
        _encode_frame(client, _DIRECT_TYPE, payload)
        for payload in (b"short", b"0123456789a", b"0123456789b")
    )
    _drain(client, bytearray(frames))
    payloads = [event.payload for event in client.evidence_frames()]
    assert payloads == [b"short", b"0123456789a", b"0123456789b"]
    assert client.recent_events()[0].payload is client.evidence_frames()[0].payload
    assert not client._inbuf

    client._arm_connection()
    assert client.evidence_frames() == []
    assert client._evidence_bytes == 0


def test_summary_mode_evidence_window_evicts_oldest_under_budget(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """The byte budget evicts oldest-first through a running counter."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        summary_mode=True,
        evidence_budget_bytes=16,
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    payloads = (b"0123456789a", b"0123456789b", b"0123456789c")
    frames = b"".join(_encode_frame(client, _DIRECT_TYPE, payload) for payload in payloads)
    _drain(client, bytearray(frames))
    # Each 11-byte append pushes the window past its 16-byte budget, so
    # the oldest frame evicts and only the newest survives.
    assert [event.payload for event in client.evidence_frames()] == [b"0123456789c"]
    assert client._evidence_bytes == len(b"0123456789c")


def test_evidence_window_rides_the_budget_not_summary_mode(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """A full-retention client with a budget retains the window too."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=64,
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    frames = _encode_frame(client, _DIRECT_TYPE, b"0123456789a")
    _drain(client, bytearray(frames))
    assert [event.payload for event in client.evidence_frames()] == [b"0123456789a"]
    first = client.recent_events()[0]
    assert first.payload is client.evidence_frames()[0].payload
    snapshot = client.snapshot_view_window()
    assert snapshot is not None and snapshot.frames[0][2] is first.payload
    client._inbuf.extend(b"overwrite")
    client._inbuf.clear()
    oversized = b"large" * 20
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, oversized)))
    assert client.evidence_frames() == []
    assert client._evidence_bytes == 0
    assert [event.payload for event in client.recent_events()] == [b"0123456789a", oversized]
    assert snapshot.frames[0][2] == b"0123456789a"
    assert snapshot.evaluate().incomplete is False
    live = client.view_window()
    assert live is not None and live.incomplete is True


def test_view_window_reads_none_until_opened(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """A client whose window was never opened holds no breadth evidence."""
    client = build_ahc(window_id_extractor=lambda type_id, payload: ())
    assert client.view_window() is None


def test_view_window_counts_in_window_frames_from_the_retained_stream(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """The read walks the retained stream within the window bounds.

    Frames the client receives while the window is open join the id set
    and the arrival scan; the extractor runs at read time, so the ingest
    path calls it zero times.
    """
    calls = [0]

    def extractor(type_id: int, payload: bytes) -> list[int]:
        del type_id
        calls[0] += 1
        return [len(payload)]

    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=4096,
        window_id_extractor=extractor,
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(
        client,
        bytearray(_encode_frame(client, _DIRECT_TYPE, b"aaa")),
    )
    _drain(
        client,
        bytearray(_encode_frame(client, _DIRECT_TYPE, b"bb")),
    )
    assert calls[0] == 0
    client.seal_view_window(time.monotonic_ns())
    window = client.view_window()
    assert window is not None
    assert window.distinct_ids == frozenset({3, 2})
    assert calls[0] == 2
    assert list(window.frame_ts_ns) == sorted(window.frame_ts_ns)
    assert len(window.frame_ts_ns) == 2
    assert window.incomplete is False


def test_view_window_keeps_the_deque_breadth_beyond_the_bounds(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """The deque-wide set spans the window bounds; the window set does not.

    Frames received before the window opened stay in the retained
    stream, so the deque set carries their ids while the window set
    holds only in-window arrivals — the split a sparse window read
    needs to separate a young window from a stream that never carried
    the roster.
    """
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=4096,
        window_id_extractor=lambda type_id, payload: [len(payload)],
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"aaa")))
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"b")))
    client.seal_view_window(time.monotonic_ns())
    window = client.view_window()
    assert window is not None
    assert window.distinct_ids == frozenset({1})
    assert window.deque_distinct_ids == frozenset({3, 1})


def test_view_window_deque_breadth_saturates_at_the_cap(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Both id sets stop admitting ids once they hold the cap."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=1_000_000,
        window_id_extractor=lambda type_id, payload: [len(payload)],
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    for size in range(1, _VIEW_WINDOW_ID_CAP + 50):
        _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"x" * size)))
    client.seal_view_window(time.monotonic_ns())
    window = client.view_window()
    assert window is not None
    assert len(window.distinct_ids) == _VIEW_WINDOW_ID_CAP
    assert len(window.deque_distinct_ids) == _VIEW_WINDOW_ID_CAP


def test_view_window_seal_stops_admitting_later_frames(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Frames stamped after the deadline never join the accumulator."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=4096,
        window_id_extractor=lambda type_id, payload: [len(payload)],
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"aaaa")))
    deadline = time.monotonic_ns()
    client.seal_view_window(deadline)
    later = bytearray(_encode_frame(client, _DIRECT_TYPE, b"b"))
    # One ns past the deadline: the ingest stamp exceeds it.
    monkeypatch.setattr("kith._agent.ahc.time.monotonic_ns", lambda: deadline + 1)
    _drain(client, later)
    window = client.view_window()
    assert window is not None
    assert window.distinct_ids == frozenset({4})
    assert len(window.frame_ts_ns) == 1


def test_view_window_survives_reconnection_arm(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """While the window is open the retained stream outlives a reconnect.

    The window's counting basis is the retained stream, so arming a
    fresh connection must not wipe it mid-window — frames delivered
    before the reconnect are exactly the frames the window counts, and
    the read after the reconnect still sees the full in-window stream.
    """
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        summary_mode=True,
        evidence_budget_bytes=1024,
        window_id_extractor=lambda type_id, payload: [1],
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"x")))
    client._arm_connection()
    assert len(client.evidence_frames()) == 1
    client.seal_view_window(time.monotonic_ns())
    window = client.view_window()
    assert window is not None
    assert window.distinct_ids == frozenset({1})
    assert len(window.frame_ts_ns) == 1


def test_view_window_without_extractor_still_scans_arrivals(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """No extractor: the ts list fills for the gap guard, the set stays empty."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=4096,
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"x")))
    window = client.view_window()
    assert window is not None
    assert window.distinct_ids == frozenset()
    assert len(window.frame_ts_ns) == 1
    assert window.incomplete is False


def test_view_window_saturation_stops_extraction(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Once the set holds the cap, the extractor stops being called."""
    calls = [0]

    def extractor(type_id: int, payload: bytes) -> list[int]:
        del type_id, payload
        calls[0] += 1
        return [calls[0]]

    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=65536,
        window_id_extractor=extractor,
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    frames = b"".join(
        _encode_frame(client, _DIRECT_TYPE, b"p") for _ in range(ahc._VIEW_WINDOW_ID_CAP + 5)
    )
    _drain(client, bytearray(frames))
    window = client.view_window()
    assert window is not None
    assert len(window.distinct_ids) == ahc._VIEW_WINDOW_ID_CAP
    assert calls[0] == ahc._VIEW_WINDOW_ID_CAP
    assert len(window.frame_ts_ns) == ahc._VIEW_WINDOW_ID_CAP + 5


def test_view_window_read_is_cached_after_seal(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """The first post-seal read fixes the evidence; subsequent reads reuse it.

    The cache keeps a second read from re-walking the stream, and the
    extractor's call count proves a single walk.
    """
    calls = [0]

    def extractor(type_id: int, payload: bytes) -> list[int]:
        del type_id
        calls[0] += 1
        return [len(payload)]

    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=4096,
        window_id_extractor=extractor,
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"aaaa")))
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"bb")))
    client.seal_view_window(time.monotonic_ns())
    window = client.view_window()
    assert window is not None
    assert window.distinct_ids == frozenset({4, 2})
    assert calls[0] == 2
    assert client.view_window() is window
    assert calls[0] == 2


def test_view_window_snapshot_before_seal_keeps_admitting(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """A pre-seal read is an uncached live snapshot of the growing window."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=4096,
        window_id_extractor=lambda type_id, payload: [len(payload)],
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"aaaa")))
    snapshot = client.view_window()
    assert snapshot is not None
    assert snapshot.distinct_ids == frozenset({4})
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"bb")))
    grown = client.view_window()
    assert grown is not None
    assert grown.distinct_ids == frozenset({4, 2})
    assert len(grown.frame_ts_ns) == 2
    client.seal_view_window(time.monotonic_ns())
    sealed = client.view_window()
    assert sealed is not None
    assert sealed.distinct_ids == frozenset({4, 2})
    assert len(sealed.frame_ts_ns) == 2


def test_view_window_eviction_marks_evidence_incomplete(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """A deque eviction under the window makes the counts a floor.

    The byte budget forces eviction of the earliest frames while the
    window is open, so the read cannot prove it sees every
    in-window frame: the evidence is marked incomplete, the ids read as
    a floor, and the breadth probe reports the client unfounded instead
    of trusting the counts.
    """
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=1,
        window_id_extractor=lambda type_id, payload: [len(payload)],
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"aaaa")))
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"bb")))
    client.seal_view_window(time.monotonic_ns())
    window = client.view_window()
    assert window is not None
    assert window.incomplete is True


def test_view_window_without_budget_marks_evidence_incomplete(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """No byte budget: no retained basis, so the read says so loudly."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        window_id_extractor=lambda type_id, payload: [1],
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"x")))
    client.seal_view_window(time.monotonic_ns())
    window = client.view_window()
    assert window is not None
    assert window.incomplete is True


def test_full_mode_replication_count_trails_history_scan_by_handshake(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """The counter misses only pre-READY frames relative to a history scan."""
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    client.register_type("state", _DIRECT_TYPE)
    pre = bytearray(_encode_frame(client, _DIRECT_TYPE, b"handshake"))
    _drain(client, pre)
    post = bytearray(
        _encode_frame(client, _DIRECT_TYPE, b"a") + _encode_frame(client, _DIRECT_TYPE, b"b")
    )
    client._direct_ready = True
    _drain(client, post)
    scanned = sum(1 for ev in client.recent_events(count=100_000) if ev.type_id == _DIRECT_TYPE)
    assert scanned == 3
    assert client.replication_count() == 2


class _RecordingTransport:
    """Transport stand-in capturing writes and buffer accounting."""

    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.buffered = 0
        self.closing = False

    def write(self, data: bytes) -> None:
        self.chunks.append(data)
        self.buffered += len(data)

    def get_write_buffer_size(self) -> int:
        return self.buffered

    def is_closing(self) -> bool:
        return self.closing

    def close(self) -> None:
        self.closing = True


def test_ping_defaults_match_native_deadline_and_pong_watchdog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Native zero defaults and explicit settings share the same watchdog clock."""
    clock = [100]
    monkeypatch.setattr(ahc, "_now_ms", lambda: clock[0])
    cases = [
        (0, 0, 0, 0),
        (7, 0, 0, 1000),
        (7, 250, 0, 250),
        (7, 0, 70, 1000),
        (1 << 16, 0, 0, 0),
        (7, 1 << 32, 0, 1000),
    ]
    for index, (type_id, interval, timeout, expected_interval) in enumerate(cases):
        client = AgenticHeadlessClient(
            f"ping-default-{index}",
            http_enabled=False,
            ipc_enabled=False,
            auto_connect=False,
            ping_type_id=type_id,
            pong_type_id=8,
            ping_interval_ms=interval,
            pong_timeout_ms=timeout,
        )
        transport = _RecordingTransport()
        client._transport = transport  # type: ignore[assignment]
        client._running = True
        try:
            client.register_type("ping", 7)
            client.register_type("pong", 8)
            assert client._client_lib.kith_client_on_connected(client._client, 100) == 0
            assert client._ping_interval_ms == expected_interval
            assert client._pong_timeout_ms == (timeout or 3 * expected_interval)
            clock[0] = 100 + (expected_interval or 1000)
            client.tick_engine()
            if not expected_interval:
                assert not transport.chunks and not transport.closing
                continue
            assert transport.chunks == [_encode_frame(client, 7, clock[0].to_bytes(8, "big"))]
            clock[0] += client._pong_timeout_ms
            client.tick_engine()
            assert not transport.closing
            clock[0] += 1
            client.tick_engine()
            assert transport.closing
        finally:
            client._running = False
            client._destroy_engine()


def test_stalled_outbound_probe_warns_and_drains(
    build_ahc: Callable[..., AgenticHeadlessClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Queued frames with no drain in progress log the miss and drain."""
    client = build_ahc(http_enabled=False, ipc_enabled=False)
    client.register_type("state", _DIRECT_TYPE)
    client.submit(_DIRECT_TYPE, b"queued")
    transport = _RecordingTransport()
    client._transport = transport  # type: ignore[assignment]
    client._running = True
    with caplog.at_level(logging.WARNING, logger="kith._agent.ahc"):
        client.probe_stalled_outbound()
    assert any("without a flush trigger" in record.message for record in caplog.records)
    assert len(transport.chunks) == 1
    assert len(transport.chunks[0]) > 0


def test_stalled_outbound_probe_silent_when_nothing_queued(
    build_ahc: Callable[..., AgenticHeadlessClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An idle connection produces no warning and writes nothing."""
    client = build_ahc(http_enabled=False, ipc_enabled=False)
    transport = _RecordingTransport()
    client._transport = transport  # type: ignore[assignment]
    client._running = True
    with caplog.at_level(logging.WARNING, logger="kith._agent.ahc"):
        client.probe_stalled_outbound()
    assert not caplog.records
    assert not transport.chunks


def test_stalled_outbound_probe_skips_active_drain(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An in-flight inline drain owns the queue; the probe must not pop."""

    client = build_ahc(http_enabled=False, ipc_enabled=False)
    transport = _RecordingTransport()

    def fail_pop(_client: Any, buf: Any, cap: Any, out_len: Any) -> int:
        del buf, cap, out_len
        raise AssertionError("probe popped while an inline drain owned the queue")

    monkeypatch.setattr(client._client_lib, "kith_client_pop_outbound", fail_pop)
    client._transport = transport  # type: ignore[assignment]
    client._running = True
    client._draining = True
    client.probe_stalled_outbound()


def test_outbound_sweeper_probes_registered_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shared task sweeps every registered client each interval."""
    monkeypatch.setattr(ahc, "_OUTBOUND_IDLE_PROBE_S", 0.02)
    sweeper = ahc._OutboundSweeper()

    class _Probe:
        def __init__(self) -> None:
            self.calls = 0

        def probe_stalled_outbound(self) -> None:
            self.calls += 1

    probes = [_Probe(), _Probe()]

    async def run() -> None:
        for probe in probes:
            sweeper.register(probe)
        await asyncio.sleep(0.075)
        sweeper.unregister(probes[0])
        first, second = probes[0].calls, probes[1].calls
        await asyncio.sleep(0.06)
        assert probes[0].calls == first
        assert probes[1].calls > second

    asyncio.run(run())
    assert probes[0].calls >= 2


def test_high_water_defers_residue_until_resume(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Popping stops at the transport high-water mark; resume restarts it."""
    client = build_ahc(
        http_enabled=False, ipc_enabled=False, direct_type_ids=frozenset({_DIRECT_TYPE})
    )
    client.register_type("state", _DIRECT_TYPE)
    # Queue three frames with no transport attached.
    for i in range(3):
        client.submit(_DIRECT_TYPE, f"f{i}".encode())
    transport = _RecordingTransport()
    # The first write lands with the buffer already at the mark: exactly
    # one frame goes out and the rest stays engine-side.
    transport.buffered = ahc._OUTBOUND_HIGH_WATER
    client._transport = transport  # type: ignore[assignment]
    client._running = True
    client._request_flush()
    assert len(transport.chunks) == 1
    client._direct_ready = True
    assert not client._outbound_empty
    client._on_socket_data(_encode_frame(client, _DIRECT_TYPE, b"received"))
    assert len(transport.chunks) == 2
    # Below low water again, the writable callback drains the residue.
    transport.buffered = 0
    client._on_write_resumed()
    assert len(transport.chunks) == 3


@pytest.mark.parametrize("probe", [False, True], ids=["flush", "probe"])
def test_outbound_growth_preserves_large_correlated_frames_and_backpressure(
    build_ahc: Callable[..., AgenticHeadlessClient],
    probe: bool,
) -> None:
    """Configured wire limits include the trailer and preserve FIFO on resume."""
    wire_limit = (1 << 20) + 257
    requested = 16384 if probe else 64
    client = build_ahc(
        http_enabled=False,
        ipc_enabled=False,
        max_payload=wire_limit,
        outbound_buffer_initial_bytes=requested,
    )
    try:
        client.register_type("state", _DIRECT_TYPE)
        bodies = [b"head", b"x" * (wire_limit - 8), b"tail"]
        frames = []
        for body in bodies:
            frames.append(
                _encode_frame(
                    client,
                    _DIRECT_TYPE,
                    body,
                    flags=ahc.KITH_PROTO_FLAG_CORRELATION,
                    correlation_id=0x123456789ABCDEF0,
                )
            )
            client.submit(
                _DIRECT_TYPE,
                body,
                flags=ahc.KITH_PROTO_FLAG_CORRELATION,
                correlation_id=0x123456789ABCDEF0,
            )
        initial = ctypes.sizeof(client._out_buf)
        assert initial == requested
        assert client._out_limit == wire_limit + ahc.KITH_PROTO_HDR_SIZE
        assert len(frames[1]) == client._out_limit
        transport = _RecordingTransport()
        client._transport = transport  # type: ignore[assignment]
        client._running = True
        if probe:
            client.probe_stalled_outbound()
        else:
            client._request_flush()
        assert transport.chunks == frames[:2]
        grown = ctypes.sizeof(client._out_buf)
        assert initial < grown <= client._out_limit
        assert not client._draining
        transport.buffered = 0
        client._on_write_resumed()
        client._on_write_resumed()
        assert transport.chunks == frames
        assert ctypes.sizeof(client._out_buf) == grown
        assert client._pop_outbound() is None
    finally:
        client._running = False
        client._destroy_engine()


@pytest.mark.parametrize("probe", [False, True], ids=["flush", "probe"])
def test_outbound_growth_memory_failure_retains_queued_frames(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    probe: bool,
) -> None:
    """Failed growth retains the front frame and releases drain ownership."""
    client = build_ahc(
        http_enabled=False, ipc_enabled=False, direct_type_ids=frozenset({_DIRECT_TYPE})
    )
    try:
        client.register_type("state", _DIRECT_TYPE)
        bodies = [b"head", b"x" * 8192, b"tail"]
        frames = [_encode_frame(client, _DIRECT_TYPE, body) for body in bodies]
        for body in bodies:
            client.submit(_DIRECT_TYPE, body)
        original = client._out_buf
        transport = _RecordingTransport()
        client._transport = transport  # type: ignore[assignment]
        client._running = True

        class _FailGrowth:
            def __mul__(self, capacity: int) -> object:
                raise MemoryError(f"scratch growth to {capacity}")

        with monkeypatch.context() as patch:
            patch.setattr(ahc, "_U8", _FailGrowth())
            if probe:
                with caplog.at_level(logging.WARNING, logger="kith._agent.ahc"):
                    client.probe_stalled_outbound()
                assert any("scratch growth" in record.message for record in caplog.records)
            else:
                with pytest.raises(MemoryError, match="scratch growth"):
                    client._request_flush()
        assert client._out_buf is original
        assert not client._draining
        assert transport.chunks == frames[:1]
        assert not client._outbound_empty
        transport.buffered = 0
        client._direct_ready = True
        client._on_socket_data(_encode_frame(client, _DIRECT_TYPE, b"received"))
        client._on_write_resumed()
        assert transport.chunks == frames
        assert client._pop_outbound() is None
        assert not client._draining
    finally:
        client._running = False
        client._destroy_engine()


@pytest.mark.parametrize("max_payload", [-1, 1 << 32])
def test_outbound_payload_limit_rejects_before_creating_handles(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
    max_payload: int,
) -> None:
    """Out-of-range wire limits cannot wrap through ctypes constructor fields."""

    def fail_create(self: AgenticHeadlessClient, maximum: int) -> ctypes.c_void_p:
        raise AssertionError(f"created proto with invalid maximum {maximum}")

    monkeypatch.setattr(AgenticHeadlessClient, "_create_proto", fail_create)
    with pytest.raises(ValueError, match="max_payload"):
        build_ahc(http_enabled=False, ipc_enabled=False, max_payload=max_payload)


@pytest.mark.parametrize("initial_bytes", [0, 1 << 32, 1.5])
def test_outbound_initial_buffer_rejects_before_creating_handles(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
    initial_bytes: int | float,
) -> None:
    """Scratch capacity requires a positive uint32 integer before native setup."""

    def fail_create(self: AgenticHeadlessClient, maximum: int) -> ctypes.c_void_p:
        raise AssertionError(f"created proto with invalid initial capacity {initial_bytes}")

    monkeypatch.setattr(AgenticHeadlessClient, "_create_proto", fail_create)
    with pytest.raises(ValueError, match="outbound_buffer_initial_bytes"):
        build_ahc(
            http_enabled=False,
            ipc_enabled=False,
            outbound_buffer_initial_bytes=initial_bytes,
        )


def test_cli_outbound_initial_buffer_forwarding_and_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI forwards valid capacity and rejects zero before constructing a client."""
    calls: list[dict[str, object]] = []
    lifecycle: list[str] = []

    class _Client:
        def __init__(self, **kwargs: object) -> None:
            calls.append(kwargs)
            self.ipc_socket_path = "/unused"
            self.http_port = 0
            self._running = False

        async def start(self) -> None:
            lifecycle.append("start")

        async def stop(self) -> None:
            lifecycle.append("stop")

    monkeypatch.setattr(ahc, "AgenticHeadlessClient", _Client)
    monkeypatch.setattr(sys, "argv", ["ahc", "--outbound-buffer-initial-bytes", "16384"])
    assert asyncio.run(ahc.main()) == 0
    assert len(calls) == 1
    assert calls[0]["outbound_buffer_initial_bytes"] == 16384
    assert lifecycle == ["start", "stop"]
    monkeypatch.setattr(sys, "argv", ["ahc", "--outbound-buffer-initial-bytes", "0"])
    with pytest.raises(SystemExit) as exc:
        asyncio.run(ahc.main())
    assert exc.value.code == 2
    assert len(calls) == 1
    assert lifecycle == ["start", "stop"]


def test_initial_outbound_allocation_precedes_native_handles(
    build_ahc: Callable[..., AgenticHeadlessClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failed initial scratch allocation creates no native handles."""

    class _Unavailable:
        def __mul__(self, capacity: int) -> object:
            raise MemoryError(f"initial scratch allocation {capacity}")

    def fail_create(self: AgenticHeadlessClient, maximum: int) -> ctypes.c_void_p:
        raise AssertionError(f"created proto before scratch with maximum {maximum}")

    monkeypatch.setattr(ahc, "_U8", _Unavailable())
    monkeypatch.setattr(AgenticHeadlessClient, "_create_proto", fail_create)
    with pytest.raises(MemoryError, match="initial scratch"):
        build_ahc(http_enabled=False, ipc_enabled=False)


def test_eof_received_tears_down_both_directions(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Inbound EOF returns False so asyncio closes the whole transport."""
    client = build_ahc(http_enabled=False, ipc_enabled=False)
    protocol = ahc._ClientProtocol(client)
    assert protocol.eof_received() is False


def test_connection_lost_releases_the_pump(
    build_ahc: Callable[..., AgenticHeadlessClient],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """connection_lost resolves the parked future; errors log a warning."""
    client = build_ahc(http_enabled=False, ipc_enabled=False)

    async def run() -> None:
        loop = asyncio.get_running_loop()
        protocol = ahc._ClientProtocol(client)
        client._closed_future = loop.create_future()
        protocol.connection_lost(None)
        await client._closed_future
        client._closed_future = loop.create_future()
        with caplog.at_level(logging.WARNING, logger="kith._agent.ahc"):
            protocol.connection_lost(RuntimeError("boom"))
        await client._closed_future

    asyncio.run(run())
    assert any(
        "connection error" in record.message and "boom" in record.message
        for record in caplog.records
    )


def test_partial_batch_then_teardown_keeps_consumed_prefix(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Frames dispatched before a drop stay delivered; the tail dies clean.

    The sync consumer always runs to completion — no cancellation
    mid-drain — so a dropped connection loses only unconsumed
    tail bytes and never strands a live ctypes view on the buffer.
    """
    client = build_ahc(direct_type_ids=frozenset({_DIRECT_TYPE}))
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    whole = _encode_frame(client, _DIRECT_TYPE, b"kept")
    partial = _encode_frame(client, _DIRECT_TYPE, b"lost")[:-3]
    client._running = True
    client._on_socket_data(whole + partial)
    assert [event.payload for event in client._event_history] == [b"kept"]
    assert bytes(client._inbuf) == partial
    client._inbuf.clear()
    client._inbuf.extend(b"next-connection")
    assert [event.payload for event in client._event_history] == [b"kept"]


def test_consume_inbound_is_synchronous() -> None:
    """The socket callback can invoke its consumer without awaiting it."""
    assert not inspect.iscoroutinefunction(ahc.AgenticHeadlessClient._consume_inbound)


def test_pump_unregisters_ticker_and_sweeper_on_connection_end(
    build_ahc: Callable[..., AgenticHeadlessClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ending the connection releases both shared registries."""
    client = build_ahc(http_enabled=False, ipc_enabled=False)
    unregistered: list[str] = []
    monkeypatch.setattr(ahc._ENGINE_TICKER, "unregister", lambda _c: unregistered.append("ticker"))
    monkeypatch.setattr(
        ahc._OUTBOUND_SWEEPER, "unregister", lambda _c: unregistered.append("sweeper")
    )

    async def run() -> None:
        loop = asyncio.get_running_loop()
        client._protocol = ahc._ClientProtocol(client)
        client._closed_future = loop.create_future()
        client._closed_future.set_result(None)
        await client._pump()

    asyncio.run(run())
    assert sorted(unregistered) == ["sweeper", "ticker"]


def test_view_window_future_seal_remains_provisional(
    build_ahc: Callable[..., AgenticHeadlessClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Future seals admit frames through the deadline and exclude arrivals beyond it."""
    now = [10]
    monkeypatch.setattr("kith._agent.ahc.time.monotonic_ns", lambda: now[0])
    client = build_ahc(
        evidence_budget_bytes=4096,
        window_id_extractor=lambda type_id, payload: [len(payload)],
    )
    client.begin_view_window()
    client._evidence.append((11, 1, b"a"))
    client.seal_view_window(20)
    early = client.view_window()
    assert early is not None and early.distinct_ids == frozenset({1})
    client._evidence.extend(((20, 1, b"bb"), (21, 1, b"ccc")))
    now[0] = 22
    final = client.view_window()
    assert final is not None and final.distinct_ids == frozenset({1, 2})
    assert final.deque_distinct_ids == frozenset({1, 2, 3})
    assert final.frame_ts_ns == (11, 20)
    assert client.view_window() is final


def test_window_snapshot_survives_retention_mutation(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Captured frames and completeness survive retention eviction."""
    client = build_ahc(
        evidence_budget_bytes=4,
        window_id_extractor=lambda type_id, payload: [len(payload)],
    )
    client.begin_view_window()
    ts = time.monotonic_ns()
    client._append_evidence(ts, 1, b"aaaa")
    client.seal_view_window(ts)
    snapshot = client.snapshot_view_window()
    assert snapshot is not None
    client._append_evidence(ts + 1, 1, b"bb")
    evidence = snapshot.evaluate()
    assert evidence.distinct_ids == frozenset({4})
    assert evidence.frame_ts_ns == (ts,)
    assert evidence.incomplete is False
    live = client.view_window()
    assert live is not None and live.incomplete is True


@pytest.mark.parametrize("operation", ["reseal", "reopen", "retire"])
def test_cooperative_window_decode_does_not_cache_stale_generation(
    build_ahc: Callable[..., AgenticHeadlessClient], operation: str
) -> None:
    """Cooperative decoding never caches across window generation changes."""
    extracted = [0]

    def extractor(type_id: int, payload: bytes) -> list[int]:
        del type_id
        extracted[0] += 1
        return [len(payload)]

    client = build_ahc(
        evidence_budget_bytes=4096,
        window_id_extractor=extractor,
    )
    client.begin_view_window()
    ts = time.monotonic_ns()
    client._evidence.extend((ts, 1, b"a") for _ in range(130))
    client.seal_view_window(ts)

    async def exercise() -> None:
        task = asyncio.create_task(client.view_window_async())
        await asyncio.sleep(0)
        while extracted[0] == 0 and not task.done():
            await asyncio.sleep(0)
        assert 0 < extracted[0] < 130
        assert not task.done()
        if operation == "reopen":
            client.begin_view_window()
        elif operation == "reseal":
            client.seal_view_window(time.monotonic_ns() + 1_000_000_000)
        else:
            client.retire_view_window()
        if operation != "retire":
            client._evidence.append((time.monotonic_ns(), 1, b"bb"))
        old = await task
        assert old is not None and old.distinct_ids == frozenset({1})
        assert client._window_evidence is None
        current = await client.view_window_async()
        if operation == "retire":
            assert current is None
        else:
            assert current is not None and 2 in current.distinct_ids

    asyncio.run(exercise())


def test_retired_window_pauses_raw_retention_until_a_new_window(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Snapshots survive retirement while events and reconnect remain independent."""
    client = build_ahc(
        direct_type_ids=frozenset({_DIRECT_TYPE}),
        evidence_budget_bytes=4096,
        window_id_extractor=lambda type_id, payload: [len(payload)],
    )
    client.register_type("state", _DIRECT_TYPE)
    client._direct_ready = True
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"aaa")))
    client.seal_view_window(time.monotonic_ns())
    snapshot = client.snapshot_view_window()
    assert snapshot is not None
    expected = snapshot.evaluate()
    count = client.replication_count()
    client.retire_view_window()
    generation = client._window_generation
    client.retire_view_window()
    assert client._window_generation == generation
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"bbbb")))
    assert client.evidence_frames() == [] and client._evidence_bytes == 0
    assert client.view_window() is None
    assert client.replication_count() == count + 1
    assert [event.payload for event in client.recent_events()] == [b"aaa", b"bbbb"]
    assert snapshot.evaluate() == expected
    client._arm_connection()
    client._direct_ready = True
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"ccccc")))
    assert client.evidence_frames() == []
    client.begin_view_window()
    _drain(client, bytearray(_encode_frame(client, _DIRECT_TYPE, b"dddddd")))
    assert [event.payload for event in client.evidence_frames()] == [b"dddddd"]
    assert snapshot.evaluate() == expected


@pytest.mark.parametrize("discard", [False, True])
def test_evidence_fifo_matches_deque_across_turnover_and_reuse(discard: bool) -> None:
    """FIFO turnover preserves values and payload references."""
    frames = ahc._EvidenceFrames()
    expected: deque[tuple[int, int, bytes]] = deque()
    for index in range(600):
        payload = bytes([index % 256]) * 7
        frames.append_values(index, index % 65536, payload)
        expected.append((index, index % 65536, payload))
    for _ in range(300):
        original = expected.popleft()
        if discard:
            assert frames.oldest_timestamp() == original[0]
            assert frames.discard_oldest() is original[2]
        else:
            actual = frames.popleft()
            assert actual == original and actual[2] is original[2]
    for index in range(600, 900):
        payload = bytes([index % 256]) * 7
        frames.append_values(index, index, payload)
        expected.append((index, index, payload))
    assert list(frames) == list(expected)
    assert all(a[2] is b[2] for a, b in zip(frames, expected, strict=True))
    assert frames.span_ns() == expected[-1][0] - expected[0][0]
    while expected:
        original = expected.popleft()
        if discard:
            assert frames.oldest_timestamp() == original[0]
            assert frames.discard_oldest() is original[2]
        else:
            assert frames.popleft() == original
    assert len(frames) == 0 and frames.span_ns() is None
    with pytest.raises(IndexError):
        frames.popleft()
    frames.extend([(1, 2, b"shared")])
    frames.clear()
    assert list(frames) == [] and frames.span_ns() is None
    frames.append((3, 4, b"again"))
    assert list(frames) == [(3, 4, b"again")]


def test_evidence_oversized_eviction_preserves_snapshot_after_reconnect(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Snapshots share payloads while live reconnect and eviction stay isolated."""
    client = build_ahc(evidence_budget_bytes=4)
    client.begin_view_window()
    timestamp = time.monotonic_ns()
    first, second = bytes(bytearray(b"aa")), bytes(bytearray(b"bb"))
    client._append_evidence(timestamp, 1, first)
    client._append_evidence(timestamp + 1, 2, second)
    snapshot = client.snapshot_view_window()
    assert snapshot is not None and type(snapshot.frames) is tuple
    assert all(type(frame) is tuple for frame in snapshot.frames)
    assert snapshot.frames[0][2] is first and snapshot.frames[1][2] is second
    client._arm_connection()
    assert tuple(client._evidence) == snapshot.frames
    client._append_evidence(timestamp + 2, 3, b"large")
    assert len(client._evidence) == 0 and client._evidence_bytes == 0
    assert client._evidence_evictions == 3 and client._window_evicted_in_window
    assert snapshot.frames == ((timestamp, 1, first), (timestamp + 1, 2, second))
    assert snapshot.frames[0][2] is first and snapshot.frames[1][2] is second
    assert not snapshot.incomplete


def test_evidence_hot_discard_reads_timestamp_until_first_in_window_eviction(
    build_ahc: Callable[..., AgenticHeadlessClient],
) -> None:
    """Steady eviction preserves counts without repeatedly reading metadata."""
    client = build_ahc(evidence_budget_bytes=1)
    client.begin_view_window()
    timestamp = client._window_start_ns
    assert timestamp is not None
    reads = [0]

    class Metadata(deque[int]):
        @overload
        def __getitem__(self, index: SupportsIndex, /) -> int: ...

        @overload
        def __getitem__(self, index: slice, /) -> Never: ...

        def __getitem__(self, index: SupportsIndex | slice, /) -> int:
            if isinstance(index, slice):
                raise TypeError("deque indices must be integers")
            if index == 0:
                reads[0] += 1
            return super().__getitem__(index)

    client._evidence._metadata = Metadata(client._evidence._metadata)
    client._append_evidence(timestamp - 1, 1, b"a")
    client._append_evidence(timestamp + 1, 1, b"b")
    assert reads == [1] and not bool(client._window_evicted_in_window)
    client._append_evidence(timestamp + 2, 1, b"c")
    assert reads == [2] and client._window_evicted_in_window
    for index in range(3, 100):
        client._append_evidence(timestamp + index, 1, b"d")
    assert reads == [2]
    assert client._evidence_evictions == 99 and client._evidence_bytes == 1
    client._window_open = False
    client._window_evicted_in_window = False
    client._append_evidence(timestamp + 100, 1, b"e")
    assert reads == [2] and not client._window_evicted_in_window
    assert client._evidence_evictions == 100 and client._evidence_bytes == 1


@pytest.mark.parametrize("path", ["cold", "hot"])
@pytest.mark.parametrize("length", [0, 64])
@pytest.mark.parametrize("failure", ["first_metadata", "second_metadata", "payload"])
def test_evidence_fifo_append_failure_preserves_values_and_reuse(
    length: int, failure: str, path: str, build_ahc: Callable[..., AgenticHeadlessClient]
) -> None:
    """Partial appends roll back both FIFOs and re-raise the same failure."""
    client = build_ahc(evidence_budget_bytes=1024) if path == "hot" else None
    frames = client._evidence if client is not None else ahc._EvidenceFrames()
    append = client._append_evidence if client is not None else frames.append_values
    if client is not None:
        client.begin_view_window()
    for index in range(length):
        append(index, 1, bytes([index % 256]))
    before = list(frames)
    client_state = (
        (
            client._evidence_bytes,
            client._evidence_evictions,
            client._window_evicted_in_window,
            client._window_generation,
            client._window_open,
            client._window_start_ns,
        )
        if client is not None
        else None
    )
    calls = [0]
    error = (
        KeyboardInterrupt("append")
        if failure == "payload" and length == 64
        else MemoryError("append")
    )

    class Metadata(deque[int]):
        def append(self, value: int) -> None:
            calls[0] += 1
            if failure == "first_metadata" and calls[0] == 1:
                raise error
            super().append(value)
            if failure == "second_metadata" and calls[0] == 2:
                raise error

    class Payloads(deque[bytes]):
        def append(self, value: bytes) -> None:
            super().append(value)
            calls[0] += 1
            if failure == "payload" and calls[0] == 3:
                raise error

    frames._metadata = Metadata(frames._metadata)
    frames._payloads = Payloads(frames._payloads)
    with pytest.raises(type(error)) as caught:
        append(999, 2, b"new")
    assert caught.value is error
    if client is not None:
        assert (
            client._evidence_bytes,
            client._evidence_evictions,
            client._window_evicted_in_window,
            client._window_generation,
            client._window_open,
            client._window_start_ns,
        ) == client_state
    assert list(frames) == before
    assert all(a[2] is b[2] for a, b in zip(frames, before, strict=True))
    assert len(frames._metadata) == 2 * len(frames._payloads) == 2 * length
    append(1000, 3, b"after")
    assert list(frames) == [*before, (1000, 3, b"after")]
    assert len(frames._metadata) == 2 * len(frames._payloads)

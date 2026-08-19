"""Shared helpers for integration tests: ClientEngine wrapper and data types.

This module is importable from the integration test files via the
``pythonpath`` and ``mypy_path`` entries that include ``tests/integration``.
"""

from __future__ import annotations

import ctypes
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path

import pytest
from examples._common import replay_format as rf

from kith._bridge import Bridge
from kith._generated import client as gen_client
from kith._generated import proto as gen_proto
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import check_error
from kith.proto import Frame


_REPO_ROOT = Path(__file__).resolve().parents[2]
_BUILD_DEBUG = Path(os.environ.get("KITH_BUILD_DIR", str(_REPO_ROOT / "build" / "debug"))).resolve()


def _debug_libs_present() -> bool:
    return (_BUILD_DEBUG / "libkith_server.so").is_file()


needs_build = pytest.mark.skipif(
    not _debug_libs_present(),
    reason="debug build libraries not present; run 'cmake --build build/debug'",
)


def wait_for_replay_records(
    artifact: Path,
    satisfied: Callable[[rf.ReplayDocument], bool],
    *,
    timeout_s: float = 10.0,
) -> rf.ReplayDocument:
    """Poll a live recording's artifact until ``satisfied`` holds on a full parse.

    The recorder flushes its stream after every boundary write, so the
    artifact is readable up to the last drained tick while the server
    runs. A read that overlaps a boundary write parses a torn tail and is
    retried; the wait ends only when a complete parse satisfies the
    condition, so shutdown can follow without a settle sleep discarding a
    still-buffered tail. Returns the first satisfying document.
    """
    deadline = time.monotonic() + timeout_s
    last_note = "no parse succeeded"
    while time.monotonic() < deadline:
        try:
            document = rf.read_document_file(artifact)
            if satisfied(document):
                return document
            last_note = f"{len(document.ticks)} ticks parsed, condition unmet"
        except OSError:
            last_note = "artifact not readable yet"
        except Exception:
            last_note = "parse failed on a torn or malformed artifact"
        time.sleep(0.05)
    raise AssertionError(f"artifact condition unmet after {timeout_s}s ({last_note})")


# ---------------------------------------------------------------------------
# data types returned by ClientEngine accessors
# ---------------------------------------------------------------------------


class BootstrapState(IntEnum):
    """Bootstrap FSM state, mirroring ``kith_client_bootstrap_state``."""

    IDLE = int(gen_client.kith_client_bootstrap_state.KITH_CLIENT_BOOTSTRAP_IDLE)
    RUNNING = int(gen_client.kith_client_bootstrap_state.KITH_CLIENT_BOOTSTRAP_RUNNING)
    READY = int(gen_client.kith_client_bootstrap_state.KITH_CLIENT_BOOTSTRAP_READY)
    FAILED = int(gen_client.kith_client_bootstrap_state.KITH_CLIENT_BOOTSTRAP_FAILED)


@dataclass(frozen=True)
class RuntimeStatus:
    """Runtime status snapshot (keepalive and connection state)."""

    connected: bool
    awaiting_pong: bool
    reconnect_due: bool
    reconnect_attempts: int
    rtt_last_ms: int
    rtt_samples: int


@dataclass(frozen=True)
class ClientEvent:
    """A drained client event."""

    ts_mono_ns: int
    type_id: int
    payload: bytes


# ---------------------------------------------------------------------------
# ClientEngine: ctypes wrapper for libkith_client.so
# ---------------------------------------------------------------------------


class ClientEngine:
    """Ctypes wrapper for the headless client engine (``libkith_client.so``).

    Drives the bootstrap FSM, interactive command injection, and event drain
    through the generated ctypes bindings.  Callback trampolines are stored on
    the instance to prevent garbage collection while the C engine holds the
    function pointers.

    The caller must :meth:`close` the engine (or use a ``with`` block) before
    releasing the borrowed proto handle.
    """

    __slots__ = ("_closed", "_handle", "_lib", "_trampolines")

    def __init__(
        self,
        bridge: Bridge,
        proto_handle: object,
        *,
        ping_type_id: int = 0,
        pong_type_id: int = 0,
        ping_interval_ms: int = 0,
    ) -> None:
        lib = bridge.lib("client")
        params = gen_client.kith_client_params_t(
            size=ctypes.sizeof(gen_client.kith_client_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            ping_type_id=ping_type_id,
            pong_type_id=pong_type_id,
            ping_interval_ms=ping_interval_ms,
        )
        out = ctypes.POINTER(gen_client.kith_client_t)()
        rc = int(
            lib.kith_client_create(
                ctypes.byref(params),
                proto_handle,
                None,
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_client_create")
        self._lib = lib
        self._handle: ctypes._Pointer[gen_client.kith_client] | None = out
        self._closed = False
        self._trampolines: list[object] = []

    # -----------------------------------------------------------------------
    # bootstrap
    # -----------------------------------------------------------------------

    def configure_no_bootstrap(self) -> None:
        """Configure zero bootstrap steps (READY immediately on connect)."""
        rc = int(
            self._lib.kith_client_configure_bootstrap(
                self._handle,
                None,
                ctypes.c_uint32(0),
            )
        )
        check_error(rc, "kith_client_configure_bootstrap")

    def configure_simple_bootstrap(self, enter_type_id: int, await_type_id: int) -> None:
        """Configure a one-step bootstrap: send ``enter_type_id``, await ``await_type_id``.

        The step's on_enter produces a header-only frame with the given type
        id; on_reply is null so any matching frame auto-advances to READY.
        """
        trampoline = self._make_enter_trampoline(enter_type_id)
        step_arr = (gen_client.kith_client_bootstrap_step_t * 1)()
        step_arr[0].await_type_id = await_type_id
        step_arr[0].on_enter = trampoline
        # on_reply and ctx are zero-initialized (null → auto-advance, no ctx).
        rc = int(
            self._lib.kith_client_configure_bootstrap(
                self._handle,
                step_arr,
                ctypes.c_uint32(1),
            )
        )
        check_error(rc, "kith_client_configure_bootstrap")

    def bootstrap_state(self) -> BootstrapState:
        """Return the current bootstrap FSM state."""
        status = gen_client.kith_client_bootstrap_status_t(
            size=ctypes.sizeof(gen_client.kith_client_bootstrap_status_t),
            abi_version=gen_version.KITH_ABI_VERSION,
        )
        rc = int(self._lib.kith_client_bootstrap_status(self._handle, ctypes.byref(status)))
        check_error(rc, "kith_client_bootstrap_status")
        return BootstrapState(int(status.state))

    # -----------------------------------------------------------------------
    # connection
    # -----------------------------------------------------------------------

    def on_connected(self, now_ms: int = 1000) -> None:
        """Signal connection established; start the bootstrap FSM."""
        rc = int(self._lib.kith_client_on_connected(self._handle, ctypes.c_uint64(now_ms)))
        check_error(rc, "kith_client_on_connected")

    def on_disconnected(self, now_ms: int = 2000) -> None:
        """Signal connection lost; schedule reconnect."""
        rc = int(self._lib.kith_client_on_disconnected(self._handle, ctypes.c_uint64(now_ms)))
        check_error(rc, "kith_client_on_disconnected")

    def runtime_status(self) -> RuntimeStatus:
        """Return the keepalive and connection status snapshot.

        Pre-fills the reserved slots with sentinels and asserts them
        unchanged after the call: the runtime writes the status fields it
        owns and leaves the caller's reserved bytes alone.
        """
        status = gen_client.kith_client_runtime_status_t(
            size=ctypes.sizeof(gen_client.kith_client_runtime_status_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            reserved_u8=0xA5,
            reserved_u32=0x5A5A5A5A,
        )
        rc = int(self._lib.kith_client_runtime_status(self._handle, ctypes.byref(status)))
        check_error(rc, "kith_client_runtime_status")
        assert status.reserved_u8 == 0xA5
        assert status.reserved_u32 == 0x5A5A5A5A
        return RuntimeStatus(
            connected=bool(status.connected),
            awaiting_pong=bool(status.awaiting_pong),
            reconnect_due=bool(status.reconnect_due),
            reconnect_attempts=int(status.reconnect_attempts),
            rtt_last_ms=int(status.rtt_last_ms),
            rtt_samples=int(status.rtt_samples),
        )

    # -----------------------------------------------------------------------
    # inbound / outbound
    # -----------------------------------------------------------------------

    def pop_outbound(self, buf_cap: int = 65536) -> bytes | None:
        """Pop the next encoded outbound frame, or ``None`` when the queue is empty."""
        buf = (ctypes.c_uint8 * buf_cap)()
        frame_len = ctypes.c_uint32(0)
        rc = int(
            self._lib.kith_client_pop_outbound(
                self._handle,
                buf,
                ctypes.c_uint32(buf_cap),
                ctypes.byref(frame_len),
            )
        )
        if rc == -int(gen_types.kith_error.KITH_EAGAIN):
            return None
        check_error(rc, "kith_client_pop_outbound")
        return bytes(buf[: int(frame_len.value)])

    def feed_frame(self, frame: Frame, now_ms: int = 1000) -> None:
        """Feed a decoded :class:`~kith.proto.Frame` to the engine."""
        c_frame = gen_proto.kith_proto_frame_t()
        c_frame.type_id = frame.type_id
        c_frame.flags = frame.flags
        c_frame.has_correlation = frame.has_correlation
        c_frame.correlation_id = frame.correlation_id
        c_frame.payload_len = len(frame.payload)
        if frame.payload:
            buf = ctypes.create_string_buffer(frame.payload, len(frame.payload))
            c_frame.payload = ctypes.cast(buf, ctypes.c_void_p)
        else:
            c_frame.payload = None
        rc = int(
            self._lib.kith_client_feed_frame(
                self._handle,
                ctypes.byref(c_frame),
                ctypes.c_uint64(now_ms),
            )
        )
        check_error(rc, "kith_client_feed_frame")

    # -----------------------------------------------------------------------
    # interactive commands
    # -----------------------------------------------------------------------

    def submit_interactive(
        self,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int:
        """Submit an interactive command; return the assigned command id."""
        payload_buf = ctypes.create_string_buffer(payload, len(payload)) if payload else None
        cmd = gen_client.kith_client_command_t(
            type_id=type_id,
            flags=flags,
            correlation_id=correlation_id,
            payload=ctypes.cast(payload_buf, ctypes.c_void_p) if payload_buf else None,
            payload_len=len(payload),
        )
        cmd_id = ctypes.c_uint64(0)
        rc = int(
            self._lib.kith_client_submit_interactive(
                self._handle,
                ctypes.byref(cmd),
                ctypes.byref(cmd_id),
            )
        )
        check_error(rc, "kith_client_submit_interactive")
        return int(cmd_id.value)

    # -----------------------------------------------------------------------
    # events
    # -----------------------------------------------------------------------

    def publish_event(self, type_id: int, payload: bytes = b"") -> None:
        """Publish an event to the engine's event ring.

        Safe to call from within the frame handler callback.
        """
        event = gen_client.kith_client_event_t(
            size=ctypes.sizeof(gen_client.kith_client_event_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            ts_mono_ns=0,
            type_id=type_id,
            payload_len=len(payload),
        )
        if payload:
            event.payload[: len(payload)] = payload
        rc = int(self._lib.kith_client_publish_event(self._handle, ctypes.byref(event)))
        check_error(rc, "kith_client_publish_event")

    def set_frame_handler(self, handler: Callable[[int, int, bytes], None]) -> None:
        """Set a runtime frame handler for non-bootstrap, non-pong frames.

        The handler receives ``(type_id, flags, payload_bytes)``.
        """
        trampoline = self._make_frame_handler_trampoline(handler)
        self._lib.kith_client_set_frame_handler(self._handle, trampoline, None)

    def drain_events(self, max_events: int = 16) -> list[ClientEvent]:
        """Drain up to ``max_events`` events from the event ring."""
        events = (gen_client.kith_client_event_t * max_events)()
        count = ctypes.c_size_t(0)
        rc = int(
            self._lib.kith_client_drain_events(
                self._handle,
                events,
                ctypes.c_size_t(max_events),
                ctypes.byref(count),
            )
        )
        check_error(rc, "kith_client_drain_events")
        result: list[ClientEvent] = []
        for i in range(int(count.value)):
            ev = events[i]
            payload_len = int(ev.payload_len)
            payload = bytes(ev.payload[:payload_len]) if payload_len else b""
            result.append(
                ClientEvent(
                    ts_mono_ns=int(ev.ts_mono_ns),
                    type_id=int(ev.type_id),
                    payload=payload,
                )
            )
        return result

    # -----------------------------------------------------------------------
    # tick
    # -----------------------------------------------------------------------

    def tick(self, now_ms: int) -> None:
        """Drive the keepalive and reconnect timers."""
        rc = int(self._lib.kith_client_tick(self._handle, ctypes.c_uint64(now_ms)))
        check_error(rc, "kith_client_tick")

    # -----------------------------------------------------------------------
    # trampoline factories
    # -----------------------------------------------------------------------

    def _make_enter_trampoline(self, type_id: int) -> object:
        """Create a step-enter trampoline that sends a header-only frame."""

        def on_enter(
            _client: ctypes._Pointer[gen_client.kith_client],
            _ctx: int,
            out_type_id: ctypes._Pointer[ctypes.c_uint16],
            _out_payload: ctypes._Pointer[ctypes.c_uint8],
            _payload_cap: int,
            out_payload_len: ctypes._Pointer[ctypes.c_uint],
        ) -> int:
            out_type_id[0] = type_id
            out_payload_len[0] = 0
            return 0

        trampoline = gen_client.kith_client_step_enter_fn(on_enter)
        self._trampolines.append(trampoline)
        return trampoline

    def _make_frame_handler_trampoline(
        self,
        handler: Callable[[int, int, bytes], None],
    ) -> object:
        """Create a frame-handler trampoline that extracts the frame fields."""

        def on_frame(
            _client: ctypes._Pointer[gen_client.kith_client],
            _ctx: int,
            frame_ptr: ctypes._Pointer[gen_proto.kith_proto_frame_t],
        ) -> None:
            frame = frame_ptr[0]
            payload_len = int(frame.payload_len)
            addr = frame.payload
            payload = bytes(ctypes.string_at(addr, payload_len)) if payload_len and addr else b""
            handler(int(frame.type_id), int(frame.flags), payload)

        trampoline = gen_client.kith_client_frame_fn(on_frame)
        self._trampolines.append(trampoline)
        return trampoline

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the client handle. Idempotent."""
        if self._closed:
            return
        self._lib.kith_client_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> ClientEngine:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __del__(self) -> None:
        if getattr(self, "_closed", True):
            return
        self.close()

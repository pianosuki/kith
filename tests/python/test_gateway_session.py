"""Build-gated unit tests for the gateway :class:`~kith.gateway.Session` window.

Constructs a :class:`~kith.gateway.Gateway` from the borrowed-handle facades
(``Proto``, ``Sim``, ``Fabric``) and a raw net handle, so the per-subscriber
window surface is exercised through the public Python boundary without
booting the full server. The cases mirror ``tests/c/test_gateway_session.c``:
the window starts empty, ``window_add`` drives the shared cache refcount,
``window_remove``/``window_clear`` drop it back, and ``window_count`` reports
the size. The shared cache statistics are read back through the gateway
facade to confirm the window mutations reach the cache.
"""

from __future__ import annotations

import ctypes
import socket
import struct
import threading
from collections.abc import Iterator

import pytest
from _build_gate import _BUILD_DEBUG, needs_build

from kith._bridge import Bridge, load, reset
from kith._generated import configure as configure_all
from kith._generated import gateway as gen_gateway
from kith._generated import net as gen_net
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import KithNetworkError, KithStateError, check_error
from kith.fabric import CellKey, Fabric
from kith.gateway import (
    Gateway,
    PinnedSession,
    Session,
    SessionInfo,
    SessionType,
    tiered_delivery_config,
)
from kith.proto import Proto
from kith.sim import Sim
from kith.worker import Worker


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test."""
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


def _make_net(bridge: Bridge, proto: Proto) -> object:
    """Create a net handle via raw ctypes (no public Net facade exists)."""
    params = gen_net.kith_net_params_t(
        size=ctypes.sizeof(gen_net.kith_net_params_t),
        abi_version=gen_version.KITH_ABI_VERSION,
    )
    out = ctypes.POINTER(gen_net.kith_net_t)()
    rc = int(
        bridge.lib("net").kith_net_create(
            ctypes.byref(params),
            proto.handle,
            None,
            ctypes.byref(out),
        )
    )
    check_error(rc, "kith_net_create")
    return out


def _make_conn(bridge: Bridge, net: object) -> object:
    """Accept one loopback connection on a listening net handle."""
    rc = int(bridge.lib("net").kith_net_listen(net, b"127.0.0.1", 0))
    check_error(rc, "kith_net_listen")
    listener_fd = int(bridge.lib("net").kith_net_listener_fd(net))
    listener = socket.fromfd(listener_fd, socket.AF_INET, socket.SOCK_STREAM)
    try:
        host, port = listener.getsockname()[:2]
    finally:
        listener.close()
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(5.0)
    client.connect((host, port))
    out = ctypes.POINTER(gen_net.kith_net_conn_t)()
    rc = int(bridge.lib("net").kith_net_accept(net, ctypes.byref(out)))
    check_error(rc, "kith_net_accept")
    client.close()
    return out


@needs_build
class TestSessionWindow:
    def test_borrowed_session_view_close_is_a_true_noop(self) -> None:
        bridge = load()
        configure_all(bridge)
        proto = Proto(bridge=bridge)
        sim = Sim(bridge=bridge)
        fabric = Fabric(sim, bridge=bridge)
        net = _make_net(bridge, proto)
        try:
            gw = Gateway(net, fabric, proto, bridge=bridge)
            try:
                conn = _make_conn(bridge, net)
                try:
                    session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=1)
                    try:
                        # The dispatch trampoline hands handlers borrowed views
                        # like this one; a handler-side close must leave the
                        # view usable and the owner's release below intact.
                        view = Session(bridge, gw.handle, session.handle, borrowed=True)
                        view.close()
                        assert view.handle is not None
                        assert view.window_count() == 0
                    finally:
                        session.close()
                finally:
                    bridge.lib("net").kith_net_conn_close(conn)
                    bridge.lib("net").kith_net_conn_release(conn)
            finally:
                gw.close()
        finally:
            bridge.lib("net").kith_net_destroy(net)
            fabric.close()
            sim.close()
            proto.close()

    def test_window_lifecycle_drives_shared_cache(self) -> None:
        bridge = load()
        configure_all(bridge)
        proto = Proto(bridge=bridge)
        sim = Sim(bridge=bridge)
        fabric = Fabric(sim, bridge=bridge)
        net = _make_net(bridge, proto)
        try:
            gw = Gateway(net, fabric, proto, bridge=bridge)
            try:
                stats = gw.cache_stats()
                assert stats.subscribed_cell_count == 0

                conn = _make_conn(bridge, net)
                try:
                    session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=1)
                    try:
                        assert session.window_count() == 0

                        cell_a = CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
                        cell_b = CellKey(zone=1, cell_x=1, cell_y=0, cell_z=0, lod=0)
                        session.window_add(cell_a)
                        session.window_add(cell_b)
                        assert session.window_count() == 2
                        assert gw.cache_stats().subscribed_cell_count == 2

                        # Idempotent re-add: window size and cache count unchanged.
                        session.window_add(cell_a)
                        assert session.window_count() == 2
                        assert gw.cache_stats().subscribed_cell_count == 2

                        session.window_remove(cell_a)
                        assert session.window_count() == 1
                        assert gw.cache_stats().subscribed_cell_count == 1

                        # Removing a cell not in the window is a no-op.
                        session.window_remove(cell_a)
                        assert session.window_count() == 1

                        session.window_clear()
                        assert session.window_count() == 0
                        assert gw.cache_stats().subscribed_cell_count == 0
                    finally:
                        session.close()
                finally:
                    bridge.lib("net").kith_net_conn_close(conn)
                    bridge.lib("net").kith_net_conn_release(conn)
            finally:
                gw.close()
        finally:
            bridge.lib("net").kith_net_destroy(net)
            fabric.close()
            sim.close()
            proto.close()

    def test_populate_binds_and_seeds_in_one_step(self) -> None:
        bridge = load()
        configure_all(bridge)
        proto = Proto(bridge=bridge)
        sim = Sim(bridge=bridge)
        fabric = Fabric(sim, bridge=bridge)
        net = _make_net(bridge, proto)
        try:
            gw = Gateway(net, fabric, proto, bridge=bridge)
            try:
                conn = _make_conn(bridge, net)
                try:
                    session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=1)
                    try:
                        # One call carries the startup contract's identity and
                        # subscription halves; duplicate cells in the seed
                        # list are idempotent.
                        cell_a = CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
                        cell_b = CellKey(zone=1, cell_x=1, cell_y=0, cell_z=0, lod=0)
                        session.populate(42, [cell_a, cell_b, cell_a])
                        info = session.info()
                        assert info.actor_id == 42
                        assert session.window_count() == 2
                        assert gw.cache_stats().subscribed_cell_count == 2

                        # A bind-only populate rebinds without touching the
                        # window.
                        session.populate(43)
                        assert session.info().actor_id == 43
                        assert session.window_count() == 2
                    finally:
                        session.close()
                finally:
                    bridge.lib("net").kith_net_conn_close(conn)
                    bridge.lib("net").kith_net_conn_release(conn)
            finally:
                gw.close()
        finally:
            bridge.lib("net").kith_net_destroy(net)
            fabric.close()
            sim.close()
            proto.close()


@needs_build
class TestDeliveryConfigParams:
    def test_tiered_config_image_layout(self) -> None:
        image = tiered_delivery_config(reduced_interval_ms=250)
        config = gen_gateway.kith_gateway_tiered_config_t.from_buffer_copy(image)
        assert config.size == ctypes.sizeof(gen_gateway.kith_gateway_tiered_config_t)
        assert config.abi_version == gen_version.KITH_ABI_VERSION
        assert config.full_interval_ms == 0
        assert config.reduced_interval_ms == 250
        assert config.crowd_interval_ms == 500
        assert config.max_gap_ms == 1000

    def test_gateway_accepts_delivery_strategy_and_config(self) -> None:
        bridge = load()
        configure_all(bridge)
        proto = Proto(bridge=bridge)
        sim = Sim(bridge=bridge)
        fabric = Fabric(sim, bridge=bridge)
        net = _make_net(bridge, proto)
        try:
            gw = Gateway(
                net,
                fabric,
                proto,
                bridge=bridge,
                delivery_strategy="tiered",
                delivery_config=tiered_delivery_config(),
            )
            try:
                assert gw.cache_stats().cell_count == 0
            finally:
                gw.close()
        finally:
            bridge.lib("net").kith_net_destroy(net)
            fabric.close()
            sim.close()
            proto.close()


def _make_conn_keep(bridge: Bridge, net: object) -> tuple[object, socket.socket]:
    """Accept one loopback connection and keep the client socket open."""
    rc = int(bridge.lib("net").kith_net_listen(net, b"127.0.0.1", 0))
    check_error(rc, "kith_net_listen")
    listener_fd = int(bridge.lib("net").kith_net_listener_fd(net))
    listener = socket.fromfd(listener_fd, socket.AF_INET, socket.SOCK_STREAM)
    try:
        host, port = listener.getsockname()[:2]
    finally:
        listener.close()
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(5.0)
    client.connect((host, port))
    out = ctypes.POINTER(gen_net.kith_net_conn_t)()
    rc = int(bridge.lib("net").kith_net_accept(net, ctypes.byref(out)))
    check_error(rc, "kith_net_accept")
    return out, client


@needs_build
class TestSessionDeliveryTotals:
    # The per-session delivery-health read: a fresh session folds nothing,
    # the accessor returns the four-field totals record, and the pin's read
    # is the same counters (the pin keeps the session alive; both reads
    # partition the gateway totals).

    def test_fresh_session_totals_are_zero_and_the_pin_reads_the_same(
        self,
    ) -> None:
        bridge = load()
        configure_all(bridge)
        proto = Proto(bridge=bridge)
        sim = Sim(bridge=bridge)
        fabric = Fabric(sim, bridge=bridge)
        net = _make_net(bridge, proto)
        try:
            gw = Gateway(net, fabric, proto, bridge=bridge)
            try:
                conn = _make_conn(bridge, net)
                try:
                    session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=1)
                    try:
                        totals = session.delivery_totals()
                        assert (totals.enqueued, totals.dropped) == (0, 0)
                        assert (totals.events_enqueued, totals.suppressed) == (0, 0)

                        gateway_totals = gw.delivery_totals()
                        assert totals.enqueued <= gateway_totals.enqueued

                        pinned = session.pin()
                        try:
                            assert pinned.delivery_totals() == totals
                        finally:
                            pinned.close()
                    finally:
                        session.close()
                finally:
                    bridge.lib("net").kith_net_conn_close(conn)
                    bridge.lib("net").kith_net_conn_release(conn)
            finally:
                gw.close()
        finally:
            bridge.lib("net").kith_net_destroy(net)
            fabric.close()
            sim.close()
            proto.close()


class TestSessionSend:
    # The gateway wrapper's send is the composition-root mirror of the
    # session dispatch view's send; these pins drive it against a real
    # loopback connection through the same raw-net fixture the window
    # tests use. A session created here is owned by the test, which is
    # the reference the send contract requires.

    def _boot(self) -> tuple[Bridge, Proto, Sim, Fabric, object, Gateway]:
        bridge = load()
        configure_all(bridge)
        proto = Proto(bridge=bridge)
        proto.register_type("note", 1101)
        sim = Sim(bridge=bridge)
        fabric = Fabric(sim, bridge=bridge)
        net = _make_net(bridge, proto)
        gw = Gateway(net, fabric, proto, bridge=bridge)
        return bridge, proto, sim, fabric, net, gw

    @staticmethod
    def _teardown(
        bridge: Bridge,
        proto: Proto,
        sim: Sim,
        fabric: Fabric,
        net: object,
        gw: Gateway,
    ) -> None:
        gw.close()
        bridge.lib("net").kith_net_destroy(net)
        fabric.close()
        sim.close()
        proto.close()

    def test_send_round_trip_reaches_the_peer_socket(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        try:
            conn, client = _make_conn_keep(bridge, net)
            try:
                session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=2)
                try:
                    session.bind_actor(5)
                    gw.send(session, 1101, b"hello")
                    check_error(
                        int(bridge.lib("net").kith_net_conn_write(conn)), "kith_net_conn_write"
                    )
                    hdr = client.recv(10, socket.MSG_WAITALL)
                    assert len(hdr) == 10
                    magic, version, flags, msg_type, wire_len = struct.unpack(">2sBBHI", hdr)
                    assert magic == b"KT"
                    assert version == 1
                    assert flags == 0
                    assert msg_type == 1101
                    assert wire_len == 5
                    body = client.recv(wire_len, socket.MSG_WAITALL)
                    assert body == b"hello"
                finally:
                    session.close()
            finally:
                bridge.lib("net").kith_net_conn_close(conn)
                bridge.lib("net").kith_net_conn_release(conn)
                client.close()
        finally:
            self._teardown(bridge, proto, sim, fabric, net, gw)

    def test_send_backpressure_raises_network_error(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        try:
            conn, client = _make_conn_keep(bridge, net)
            try:
                session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=2)
                try:
                    session.bind_actor(5)
                    # The queue fills at 42 bytes per send until the byte
                    # count reaches the high watermark; the next send is
                    # refused with backpressure and raises the transport
                    # family at the call site.
                    sent = 0
                    with pytest.raises(KithNetworkError) as exc_info:
                        while True:
                            sent += 1
                            assert sent < 100_000
                            gw.send(session, 1101, b"x" * 32)
                    assert exc_info.value.code == gen_types.kith_error.KITH_EAGAIN
                finally:
                    session.close()
            finally:
                bridge.lib("net").kith_net_conn_close(conn)
                bridge.lib("net").kith_net_conn_release(conn)
                client.close()
        finally:
            self._teardown(bridge, proto, sim, fabric, net, gw)

    def test_send_on_closed_session_raises_state_error(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        try:
            conn, client = _make_conn_keep(bridge, net)
            try:
                session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=2)
                session.close()
                # The owned session is destroyed above; the view's handle
                # is gone and the send refuses in the lifecycle family
                # before touching any C surface.
                with pytest.raises(KithStateError) as exc_info:
                    session.send(1101, b"late")
                assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE
            finally:
                bridge.lib("net").kith_net_conn_close(conn)
                bridge.lib("net").kith_net_conn_release(conn)
                client.close()
        finally:
            self._teardown(bridge, proto, sim, fabric, net, gw)


def _make_extra_conn(bridge: Bridge, net: object) -> tuple[object, socket.socket]:
    """Connect and accept one more loopback connection on a listening net."""
    listener_fd = int(bridge.lib("net").kith_net_listener_fd(net))
    listener = socket.fromfd(listener_fd, socket.AF_INET, socket.SOCK_STREAM)
    host, port = listener.getsockname()[:2]
    listener.close()
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(5.0)
    client.connect((host, port))
    out = ctypes.POINTER(gen_net.kith_net_conn_t)()
    rc = int(bridge.lib("net").kith_net_accept(net, ctypes.byref(out)))
    check_error(rc, "kith_net_accept")
    return out, client


@needs_build
class TestSessionLifecycle:
    # The lifecycle surface's Python pins: the destroyed-session callback
    # registered through the facade (pool-dispatched, fires with the
    # destroyed session's identity), the pin/release pair (a pinned session
    # stays sendable past the owner's destroy while its connection lives),
    # the session-count mirror, and the roster snapshot. The boot/teardown
    # shapes are TestSessionSend's: same raw-net fixture, owned sessions.

    @staticmethod
    def _boot() -> tuple[Bridge, Proto, Sim, Fabric, object, Gateway]:
        bridge = load()
        configure_all(bridge)
        proto = Proto(bridge=bridge)
        proto.register_type("note", 1101)
        sim = Sim(bridge=bridge)
        fabric = Fabric(sim, bridge=bridge)
        net = _make_net(bridge, proto)
        gw = Gateway(net, fabric, proto, bridge=bridge)
        return bridge, proto, sim, fabric, net, gw

    @staticmethod
    def _teardown(
        bridge: Bridge,
        proto: Proto,
        sim: Sim,
        fabric: Fabric,
        net: object,
        gw: Gateway,
    ) -> None:
        gw.close()
        bridge.lib("net").kith_net_destroy(net)
        fabric.close()
        sim.close()
        proto.close()

    def test_destroyed_callback_fires_with_identity(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        # The destroyed callback registers pool-bound (PYTHON); the
        # fixture attaches a one-worker pool so the notification reaches a
        # worker instead of dropping into the lifecycle gauge.
        worker = Worker(bridge=bridge, worker_count=1, task_capacity=16)
        check_error(
            int(bridge.lib("gateway").kith_gateway_attach_worker_pool(gw.handle, worker.handle)),
            "kith_gateway_attach_worker_pool",
        )
        try:
            seen: list[SessionInfo] = []
            done = threading.Event()

            def on_destroyed(info: SessionInfo) -> None:
                seen.append(info)
                done.set()

            gw.on_session_destroyed(on_destroyed)
            conn, client = _make_conn_keep(bridge, net)
            try:
                session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=8)
                expected = session.info()
                session.close()
                # The destroy submits the pool task; the event is the
                # deadline poll over the worker's run.
                assert done.wait(5.0), "destroyed callback did not fire"
                assert len(seen) == 1
                assert seen[0].session_id == expected.session_id
                assert seen[0].principal_id == 8
                assert gw.session_count == 0
            finally:
                bridge.lib("net").kith_net_conn_close(conn)
                bridge.lib("net").kith_net_conn_release(conn)
                client.close()
        finally:
            worker.close()
            self._teardown(bridge, proto, sim, fabric, net, gw)

    def test_pin_keeps_session_sendable_past_owner_destroy(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        try:
            conn, client = _make_conn_keep(bridge, net)
            try:
                session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=3)
                pinned = session.pin()
                assert isinstance(pinned, PinnedSession)
                # The owner destroy consumes the owner reference; the pin
                # keeps the session's memory and its connection reference
                # alive, and the connection is still open, so the send is
                # legal and leaves on the next flush.
                session.close()
                pinned.send(1101, b"late")
                check_error(int(bridge.lib("net").kith_net_conn_write(conn)), "kith_net_conn_write")
                hdr = client.recv(10, socket.MSG_WAITALL)
                assert len(hdr) == 10
                wire_type = struct.unpack(">2sBBHI", hdr)[3]
                assert wire_type == 1101
                assert client.recv(4, socket.MSG_WAITALL) == b"late"
                assert pinned.info().principal_id == 3
                pinned.close()
                # After the release the wrapper's handle is gone: the send
                # refuses in the lifecycle family before touching C.
                with pytest.raises(KithStateError) as exc_info:
                    pinned.send(1101, b"after-release")
                assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE
            finally:
                bridge.lib("net").kith_net_conn_close(conn)
                bridge.lib("net").kith_net_conn_release(conn)
                client.close()
        finally:
            self._teardown(bridge, proto, sim, fabric, net, gw)

    def test_pin_from_closed_session_raises_state_error(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        try:
            conn, client = _make_conn_keep(bridge, net)
            try:
                session = gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=4)
                session.close()
                with pytest.raises(KithStateError) as exc_info:
                    session.pin()
                assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE
            finally:
                bridge.lib("net").kith_net_conn_close(conn)
                bridge.lib("net").kith_net_conn_release(conn)
                client.close()
        finally:
            self._teardown(bridge, proto, sim, fabric, net, gw)

    def test_session_count_tracks_create_and_destroy(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        try:
            assert gw.session_count == 0
            conns_clients = []
            sessions = []
            try:
                # One listen; each extra loopback connection is a fresh
                # connect + accept against the already-listening net.
                for principal in (11, 12):
                    conn, client = (
                        _make_conn_keep(bridge, net)
                        if principal == 11
                        else _make_extra_conn(bridge, net)
                    )
                    conns_clients.append((conn, client))
                    sessions.append(
                        gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=principal)
                    )
                assert gw.session_count == 2
                sessions[0].close()
                assert gw.session_count == 1
                sessions[1].close()
                assert gw.session_count == 0
            finally:
                for conn, client in conns_clients:
                    bridge.lib("net").kith_net_conn_close(conn)
                    bridge.lib("net").kith_net_conn_release(conn)
                    client.close()
        finally:
            self._teardown(bridge, proto, sim, fabric, net, gw)

    def test_session_snapshot_lists_the_live_roster(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        try:
            assert gw.session_snapshot() == []
            conns_clients = []
            sessions = []
            try:
                for principal in (11, 12):
                    conn, client = (
                        _make_conn_keep(bridge, net)
                        if principal == 11
                        else _make_extra_conn(bridge, net)
                    )
                    conns_clients.append((conn, client))
                    sessions.append(
                        gw.create_session(conn, SessionType.SUBSCRIBER, principal_id=principal)
                    )
                roster = gw.session_snapshot()
                assert len(roster) == 2
                # The C copy is unordered; the roster is compared by
                # identity fields, not position.
                assert {info.principal_id for info in roster} == {11, 12}
                assert {info.session_id for info in roster} == {
                    session.info().session_id for session in sessions
                }
                assert all(info.actor_id == 0 for info in roster)
                assert all(info.type == int(SessionType.SUBSCRIBER) for info in roster)
                sessions[0].close()
                assert gw.session_count == 1
                assert len(gw.session_snapshot()) == 1
                sessions[1].close()
                assert gw.session_snapshot() == []
            finally:
                for conn, client in conns_clients:
                    bridge.lib("net").kith_net_conn_close(conn)
                    bridge.lib("net").kith_net_conn_release(conn)
                    client.close()
        finally:
            self._teardown(bridge, proto, sim, fabric, net, gw)

    def test_off_session_destroyed_detaches_registration(self) -> None:
        bridge, proto, sim, fabric, net, gw = self._boot()
        try:
            gw.on_session_destroyed(lambda info: None)
            assert gw._destroyed_trampoline is not None
            gw.off_session_destroyed()
            assert gw._destroyed_trampoline is None
            assert gw._destroyed_handler is None
            # Idempotent: a second off is a clean C-side success.
            gw.off_session_destroyed()
        finally:
            self._teardown(bridge, proto, sim, fabric, net, gw)

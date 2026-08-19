"""Integration test: boot a real server from Python and connect a client.

Boots a :class:`kith.Server` on an ephemeral port with the embedded topology,
verifies the TCP listener accepts a connection, drives the headless client
engine through ``libkith_client.so`` (bootstrap, interactive command, outbound
pop, tick, event drain), and shuts the server down from the test thread. The
server's run loop drives the reactor, which runs the periodic tick; the client
engine is transport-agnostic, so the test harness pumps frames between the
socket and the engine.
"""

from __future__ import annotations

import socket
import struct
import threading
import time
from collections.abc import Callable

from _helpers import BootstrapState, ClientEngine, needs_build

from kith import (
    Actor,
    ArtifactKey,
    CellKey,
    PinnedSession,
    Server,
    ServerStatus,
    Session,
    SessionInfo,
)
from kith._bridge import Bridge
from kith.exceptions import KithError, KithProtocolError
from kith.proto import Proto


def _status(server: Server) -> ServerStatus:
    """Read the handle state through a fresh call (avoids mypy narrowing)."""
    return server.status


def _frame(msg_type: int, payload: bytes) -> bytes:
    """Encode one wire frame: magic, version, flags, big-endian type and
    length header followed by the payload."""
    return struct.pack(">2sBBHI", b"KT", 1, 0, msg_type, len(payload)) + payload


def _recv_frame(sock: socket.socket) -> tuple[int, bytes]:
    """Read one wire frame off the socket (10-byte header, then payload)."""
    hdr = sock.recv(10, socket.MSG_WAITALL)
    assert len(hdr) == 10
    magic, version, flags, msg_type, wire_len = struct.unpack(">2sBBHI", hdr)
    assert magic == b"KT"
    assert version == 1
    assert flags == 0
    payload = sock.recv(wire_len, socket.MSG_WAITALL)
    assert len(payload) == wire_len
    return msg_type, payload


@needs_build
class TestServerClientConnect:
    def test_server_listens_and_client_engine_lifecycle(
        self,
        make_client: Callable[..., ClientEngine],
        bridge: Bridge,
    ) -> None:
        # Bind an OS-assigned ephemeral port and read it back from the handle
        # so the client connects to the real endpoint without a probe-and-
        # close race on a free port.
        server = Server(listen_port=0, topology="embedded")
        port = server.listen_port
        try:
            assert _status(server) is ServerStatus.CREATED

            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", port))
            try:
                with Proto(bridge=bridge) as proto:
                    proto.register_type("move", 1100)
                    client = make_client(proto.handle)
                    try:
                        client.configure_no_bootstrap()
                        assert client.bootstrap_state() is BootstrapState.IDLE

                        client.on_connected()
                        assert client.bootstrap_state() is BootstrapState.READY

                        cmd_id = client.submit_interactive(1100, b"hello")
                        assert cmd_id >= 1

                        frame = client.pop_outbound()
                        assert frame is not None
                        assert frame[0:2] == b"KT"

                        sock.sendall(frame)

                        client.tick(now_ms=2000)

                        assert client.drain_events() == []

                        client.on_disconnected()
                        status = client.runtime_status()
                        assert status.connected is False
                    finally:
                        client.close()
            finally:
                sock.close()

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_discrete_send_round_trip_from_pool_handler(self) -> None:
        server = Server(listen_port=0, topology="embedded", handler_table_size=2048)
        server.register_proto_type("ping", 1100)
        server.register_proto_type("pong", 1101)

        done = threading.Event()
        observed: dict[str, object] = {}

        def echo(msg_type: int, payload: bytes, session: Session) -> None:
            try:
                # The pool-dispatched handler holds the dispatch reference
                # for its duration, which is exactly the send contract's
                # sanctioned context: the reply goes out on the session's
                # connection and the reactor flushes it on the next tick's
                # arm pass.
                session.send(1101, b"pong-" + payload)
            except KithError as exc:
                observed["error"] = exc
            finally:
                done.set()

        server.register_message_handler(1100, echo)
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", server.listen_port))
            try:
                sock.sendall(_frame(1100, b"ping"))
                assert done.wait(5.0), "handler did not run"
                assert "error" not in observed, observed["error"]
                reply_type, reply_payload = _recv_frame(sock)
                assert reply_type == 1101
                assert reply_payload == b"pong-ping"
            finally:
                sock.close()

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_broadcast_round_trip_from_pool_handler(self) -> None:
        # The cell-scoped broadcast end to end: a pool-dispatched handler
        # binds and seeds the submitting session's window, then submits a
        # broadcast for the cell; the run loop's next tick pass fans one
        # frame to the covering session's connection and the reactor
        # flushes it to the peer socket. The session is the sole recipient
        # derived by the framework — the handler builds no roster.
        server = Server(listen_port=0, topology="embedded", handler_table_size=2048)
        server.register_proto_type("chat", 1103)
        server.register_proto_type("chat_event", 1104)

        done = threading.Event()
        errors: list[KithError] = []

        def chat(msg_type: int, payload: bytes, session: Session) -> None:
            del msg_type
            try:
                session.bind_actor(1717)
                cell = CellKey(zone=9, cell_x=0, cell_y=0, cell_z=0, lod=0)
                session.window_add(cell)
                # The submit is legal from the pool handler: the request
                # queue is internally synchronized. The frame is not on
                # the wire yet — the next tick's broadcast drain fans it.
                server.broadcast_cell(cell, 1104, b"cell:" + payload)
            except KithError as exc:
                errors.append(exc)
            finally:
                done.set()

        server.register_message_handler(1103, chat)
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", server.listen_port))
            try:
                sock.sendall(_frame(1103, b"hello"))
                assert done.wait(5.0), "handler did not run"
                assert not errors, errors
                event_type, event_payload = _recv_frame(sock)
                assert event_type == 1104
                assert event_payload == b"cell:hello"
            finally:
                sock.close()

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_session_delivery_totals_fold_after_replication_round_trip(self) -> None:
        # The full session-to-delivery contract over a real socket: the pool
        # handler binds, seeds the window, publishes the actor's artifact,
        # and bumps the cell; the tick's delivery pass composes the view and
        # delivers it, folding the per-session totals. The handler pins the
        # session so the test reads the totals after the dispatch reference
        # is gone.
        server = Server(
            listen_port=0,
            topology="embedded",
            handler_table_size=2048,
            replication_type_id=1200,
        )
        server.register_proto_type("login", 1102)

        pins: list[PinnedSession] = []
        errors: list[KithError] = []
        done = threading.Event()

        def login(msg_type: int, payload: bytes, session: Session) -> None:
            del msg_type, payload
            try:
                session.bind_actor(4242)
                cell = CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
                session.window_add(cell)
                server.publish_artifact(
                    ArtifactKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0),
                    Actor(id=4242, pos_x=0, pos_y=0, pos_z=0),
                )
                server.publish_cell_product(cell, 1)
                pins.append(session.pin())
            except KithError as exc:
                errors.append(exc)
            finally:
                done.set()

        server.register_message_handler(1102, login)
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", server.listen_port))
            try:
                sock.sendall(_frame(1102, struct.pack("<Q", 991)))
                assert done.wait(5.0), "handler did not run"
                assert not errors, errors
                assert pins, "handler did not pin the session"
                pinned = pins[0]

                deadline = time.monotonic() + 5.0
                totals = pinned.delivery_totals()
                while totals.enqueued == 0 and time.monotonic() < deadline:
                    time.sleep(0.01)
                    totals = pinned.delivery_totals()
                assert totals.enqueued > 0, "the delivery pass never folded"

                # Partition: the one session's sums never exceed the
                # gateway's totals.
                gateway_totals = server.delivery_totals()
                assert totals.enqueued <= gateway_totals.enqueued
                assert totals.dropped <= gateway_totals.dropped
            finally:
                sock.close()

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            if pins:
                pins[0].close()
            server.close()

    def test_populate_completes_the_startup_contract_in_one_call(self) -> None:
        # The one-call form of the session-to-delivery startup contract: a
        # pool handler populates the session (identity and window seed in
        # one atomic step), publishes the actor's artifact, and bumps the
        # cell; replication flows with no separate bind or window calls in
        # the handler. The handler pins the session so the test reads the
        # totals after the dispatch reference is gone.
        server = Server(
            listen_port=0,
            topology="embedded",
            handler_table_size=2048,
            replication_type_id=1201,
        )
        server.register_proto_type("login", 1103)

        pins: list[PinnedSession] = []
        errors: list[KithError] = []
        done = threading.Event()

        def login(msg_type: int, payload: bytes, session: Session) -> None:
            del msg_type, payload
            try:
                cell = CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
                session.populate(4242, [cell])
                server.publish_artifact(
                    ArtifactKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0),
                    Actor(id=4242, pos_x=0, pos_y=0, pos_z=0),
                )
                server.publish_cell_product(cell, 1)
                pins.append(session.pin())
            except KithError as exc:
                errors.append(exc)
            finally:
                done.set()

        server.register_message_handler(1103, login)
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", server.listen_port))
            try:
                sock.sendall(_frame(1103, struct.pack("<Q", 991)))
                assert done.wait(5.0), "handler did not run"
                assert not errors, errors
                assert pins, "handler did not pin the session"
                pinned = pins[0]

                # The populated session composes and delivers: the one-call
                # form reaches the same fold the per-cell path does.
                deadline = time.monotonic() + 5.0
                totals = pinned.delivery_totals()
                while totals.enqueued == 0 and time.monotonic() < deadline:
                    time.sleep(0.01)
                    totals = pinned.delivery_totals()
                assert totals.enqueued > 0, "the delivery pass never folded"
            finally:
                sock.close()

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            if pins:
                pins[0].close()
            server.close()

    def test_view_max_subjects_bounds_the_delivered_set(self) -> None:
        # The facade kwarg sizes the per-subscriber view set end to end:
        # with a capacity of 3 the binding session composes itself plus the
        # two closest candidates (the self subject occupies one slot), and
        # the candidates past the budget never reach the peer. Five
        # candidates keep the count at the crowd regime's entry point, so
        # the just-past-cap truncation path alone decides the delivered
        # set — the absences are structural, not timing.
        server = Server(
            listen_port=0,
            topology="embedded",
            handler_table_size=2048,
            replication_type_id=1202,
            view_max_subjects=3,
        )
        server.register_proto_type("login", 1105)

        pins: list[PinnedSession] = []
        errors: list[KithError] = []
        done = threading.Event()

        def login(msg_type: int, payload: bytes, session: Session) -> None:
            del msg_type, payload
            try:
                cell = CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0)
                session.populate(4242, [cell])
                server.publish_artifact(
                    ArtifactKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0),
                    Actor(id=4242, pos_x=0, pos_y=0, pos_z=0),
                )
                for actor_id in range(1, 6):
                    server.publish_artifact(
                        ArtifactKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0),
                        Actor(
                            id=actor_id,
                            pos_x=actor_id * 65536,
                            pos_y=0,
                            pos_z=0,
                        ),
                    )
                server.publish_cell_product(cell, 1)
                pins.append(session.pin())
            except KithError as exc:
                errors.append(exc)
            finally:
                done.set()

        server.register_message_handler(1105, login)
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(("127.0.0.1", server.listen_port))
            try:
                sock.sendall(_frame(1105, struct.pack("<Q", 992)))
                assert done.wait(5.0), "handler did not run"
                assert not errors, errors
                assert pins, "handler did not pin the session"

                expected = {4242, 1, 2}
                seen: set[int] = set()
                deadline = time.monotonic() + 5.0
                while seen != expected and time.monotonic() < deadline:
                    try:
                        msg_type, payload = _recv_frame(sock)
                    except TimeoutError:
                        continue
                    if msg_type == 1202 and len(payload) >= 8:
                        seen.add(struct.unpack(">Q", payload[:8])[0])
                assert seen == expected, seen
            finally:
                sock.close()

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            if pins:
                pins[0].close()
            server.close()

    def test_disconnect_callback_reports_destroyed_session(self) -> None:
        # The session lifecycle surface end-to-end: a real client connects,
        # the pool-dispatched message handler records its dispatch view's
        # session id, and the client's disconnect drives the wire's close
        # path, which destroys the session and fires the destroyed-session
        # callback on a pool worker with the same identity. The session
        # count mirror reads the table from the test thread the whole way.
        server = Server(listen_port=0, topology="embedded", handler_table_size=2048)
        server.register_proto_type("ping", 1100)
        server.register_proto_type("pong", 1101)

        handler_ran = threading.Event()
        destroyed = threading.Event()
        seen: dict[str, object] = {}

        def echo(msg_type: int, payload: bytes, session: Session) -> None:
            seen["session_id"] = session.info().session_id
            seen["count_in_handler"] = server.session_count
            handler_ran.set()

        def on_destroyed(info: SessionInfo) -> None:
            seen["destroyed_session_id"] = info.session_id
            destroyed.set()

        server.register_message_handler(1100, echo)
        server.on_session_destroyed(on_destroyed)
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", server.listen_port))
            try:
                # The accept runs on the reactor's readiness pass, so the
                # count read polls the mirror until the wire's accept
                # creates the session.
                deadline = time.monotonic() + 5.0
                while server.session_count != 1 and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert server.session_count == 1
                sock.sendall(_frame(1100, b"ping"))
                assert handler_ran.wait(5.0), "handler did not run"
                assert seen["count_in_handler"] == 1
                # The client's disconnect is the framework's real teardown
                # path: HUP on the polled fd closes the connection, the
                # wire destroys the session, and the pool worker runs the
                # callback.
                sock.close()
                assert destroyed.wait(5.0), "destroyed callback did not fire"
                assert seen["destroyed_session_id"] == seen["session_id"]
                assert server.session_count == 0
            except BaseException:
                sock.close()
                raise

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_discrete_send_oversize_raises_protocol_error_in_handler(self) -> None:
        server = Server(listen_port=0, topology="embedded", handler_table_size=2048)
        server.register_proto_type("ping", 1100)
        server.register_proto_type("big", 1102)

        done = threading.Event()
        observed: dict[str, object] = {}

        def on_ping(msg_type: int, payload: bytes, session: Session) -> None:
            try:
                # One byte past the proto's 1 MiB max_payload: the encode
                # step refuses the frame before any queue is touched, and
                # the boundary raises the protocol family at the call site.
                session.send(1102, b"x" * (1024 * 1024 + 1))
            except KithProtocolError as exc:
                observed["error"] = exc
            finally:
                done.set()

        server.register_message_handler(1100, on_ping)
        try:
            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", server.listen_port))
            try:
                sock.sendall(_frame(1100, b""))
                assert done.wait(5.0), "handler did not run"
                assert isinstance(observed.get("error"), KithProtocolError)
            finally:
                sock.close()

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_server_shutdown_from_thread_stops_run_loop(
        self,
    ) -> None:
        server = Server(listen_port=0, topology="distributed")
        try:
            assert _status(server) is ServerStatus.CREATED

            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

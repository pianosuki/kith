"""Integration test: client bootstrap and interactive exchange through a socket.

Drives the headless client engine through ``libkith_client.so`` against a
loopback peer (a Python TCP server that speaks the proto wire format).  The
test harness pumps frames between the socket and the engine: it pops encoded
outbound frames from the client and writes them to the socket, reads bytes
from the socket, decodes them via a :class:`~kith.proto.Proto` handle, and
feeds the decoded frames back to the client.  This exercises the full
bootstrap FSM roundtrip, interactive command injection, the runtime frame
handler, event drain, and keepalive ping/pong — all through real socket I/O
and the C client binding.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable

from _helpers import (
    BootstrapState,
    ClientEngine,
    ClientEvent,
    needs_build,
)

from kith._bridge import Bridge
from kith._generated import types as gen_types
from kith.exceptions import KithProtocolError
from kith.proto import Frame, Proto


def _read_frame(sock: socket.socket, proto: Proto) -> Frame:
    """Read one complete proto frame from the socket."""
    buf = bytearray()
    while True:
        data = sock.recv(4096)
        if not data:
            raise ConnectionError("peer closed before a complete frame arrived")
        buf.extend(data)
        try:
            return proto.decode(bytes(buf))
        except KithProtocolError as exc:
            if exc.code != gen_types.kith_error.KITH_EAGAIN:
                raise


def _peer_exchange(
    listen_sock: socket.socket,
    proto: Proto,
    *,
    login_ack_type: int,
    echo_reply_type: int,
    ping_type: int,
    pong_type: int,
) -> None:
    """Accept one connection and respond to login, echo, and ping frames."""
    conn, _ = listen_sock.accept()
    conn.settimeout(5.0)
    try:
        while True:
            try:
                frame = _read_frame(conn, proto)
            except (ConnectionError, OSError) as _exc:
                break
            if frame.type_id == 1100:
                ack = proto.encode(login_ack_type, b"ok")
                conn.sendall(ack)
            elif frame.type_id == 1102:
                reply = proto.encode(echo_reply_type, frame.payload)
                conn.sendall(reply)
            elif frame.type_id == ping_type:
                pong = proto.encode(pong_type, frame.payload)
                conn.sendall(pong)
            else:
                pass
    finally:
        conn.close()


@needs_build
class TestClientBootstrapSocket:
    def test_client_session_through_socket(
        self,
        make_client: Callable[..., ClientEngine],
        bridge: Bridge,
    ) -> None:
        """Bootstrap to READY, interactive command, event drain through a socket."""
        with Proto(bridge=bridge) as proto:
            proto.register_type("login", 1100)
            proto.register_type("login_ack", 1101)
            proto.register_type("echo", 1102)
            proto.register_type("echo_reply", 1103)

            listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listen_sock.bind(("127.0.0.1", 0))
            listen_sock.listen(1)
            port = listen_sock.getsockname()[1]

            peer = threading.Thread(
                target=_peer_exchange,
                args=(listen_sock, proto),
                kwargs={
                    "login_ack_type": 1101,
                    "echo_reply_type": 1103,
                    "ping_type": 1200,
                    "pong_type": 1201,
                },
                daemon=True,
            )
            peer.start()

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            try:
                sock.connect(("127.0.0.1", port))

                client = make_client(proto.handle)
                try:
                    client.configure_simple_bootstrap(
                        enter_type_id=1100,
                        await_type_id=1101,
                    )
                    assert client.bootstrap_state() is BootstrapState.IDLE

                    client.on_connected(now_ms=0)
                    assert client.bootstrap_state() is BootstrapState.RUNNING

                    login_frame = client.pop_outbound()
                    assert login_frame is not None
                    assert login_frame[0:2] == b"KT"
                    sock.sendall(login_frame)

                    ack = _read_frame(sock, proto)
                    assert ack.type_id == 1101
                    client.feed_frame(ack, now_ms=10)
                    assert client.bootstrap_state() is BootstrapState.READY

                    events: list[ClientEvent] = []

                    def on_frame(type_id: int, _flags: int, payload: bytes) -> None:
                        client.publish_event(type_id, payload)
                        events.append(
                            ClientEvent(
                                ts_mono_ns=int(time.monotonic_ns()),
                                type_id=type_id,
                                payload=payload,
                            )
                        )

                    client.set_frame_handler(on_frame)

                    cmd_id = client.submit_interactive(1102, b"world")
                    assert cmd_id >= 1

                    echo_frame = client.pop_outbound()
                    assert echo_frame is not None
                    sock.sendall(echo_frame)

                    reply = _read_frame(sock, proto)
                    assert reply.type_id == 1103
                    assert reply.payload == b"world"
                    client.feed_frame(reply, now_ms=20)

                    drained = client.drain_events()
                    assert len(drained) == 1
                    assert drained[0].type_id == 1103
                    assert drained[0].payload == b"world"

                    client.on_disconnected(now_ms=30)
                finally:
                    client.close()
            finally:
                sock.close()
                peer.join(timeout=5.0)
                listen_sock.close()

    def test_keepalive_ping_pong(
        self,
        make_client: Callable[..., ClientEngine],
        bridge: Bridge,
    ) -> None:
        """Tick drives a ping; the peer's pong clears the awaiting-pong flag."""
        with Proto(bridge=bridge) as proto:
            proto.register_type("ping", 1200)
            proto.register_type("pong", 1201)

            listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listen_sock.bind(("127.0.0.1", 0))
            listen_sock.listen(1)
            port = listen_sock.getsockname()[1]

            peer = threading.Thread(
                target=_peer_exchange,
                args=(listen_sock, proto),
                kwargs={
                    "login_ack_type": 1101,
                    "echo_reply_type": 1103,
                    "ping_type": 1200,
                    "pong_type": 1201,
                },
                daemon=True,
            )
            peer.start()

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            try:
                sock.connect(("127.0.0.1", port))

                client = make_client(
                    proto.handle,
                    ping_type_id=1200,
                    pong_type_id=1201,
                    ping_interval_ms=100,
                )
                try:
                    client.on_connected(now_ms=0)
                    assert client.bootstrap_state() is BootstrapState.READY

                    status = client.runtime_status()
                    assert status.connected is True
                    assert status.awaiting_pong is False

                    client.tick(now_ms=100)
                    ping_frame = client.pop_outbound()
                    assert ping_frame is not None
                    assert ping_frame[0:2] == b"KT"
                    sock.sendall(ping_frame)

                    pong = _read_frame(sock, proto)
                    assert pong.type_id == 1201
                    client.feed_frame(pong, now_ms=110)

                    status = client.runtime_status()
                    assert status.awaiting_pong is False
                    assert status.rtt_samples >= 1

                    client.on_disconnected(now_ms=200)
                finally:
                    client.close()
            finally:
                sock.close()
                peer.join(timeout=5.0)
                listen_sock.close()

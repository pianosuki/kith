"""Integration test: the minimal server end-to-end.

Boots a :class:`kith.Server` with the same surface the one-liner
``examples.minimal.server`` uses (an OS-assigned ephemeral gateway port and
the embedded topology, no game-owned registration), verifies the gateway
listener accepts a TCP connection, and shuts the server down from the test
thread. The minimal server registers no wire types, no handlers, no model,
no zone, no query, and no control route: this is the thinnest end-to-end
slice, strictly thinner than ``free_movement/`` (which adds one model + one
handler + four control routes).
"""

from __future__ import annotations

import socket
import threading
import time
from pathlib import Path

import pytest
from _helpers import needs_build

from kith import Server, ServerStatus


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _status(server: Server) -> ServerStatus:
    return server.status


@needs_build
class TestMinimalServer:
    def test_gateway_accepts_connection_with_no_game_wiring(self) -> None:
        # Build the facade with the same surface the one-liner uses: an
        # OS-assigned ephemeral gateway port and the embedded topology, and
        # no game-owned registration at all.
        server = Server(listen_port=0, topology="embedded")
        port = server.listen_port
        try:
            assert _status(server) is ServerStatus.CREATED
            assert port != 0

            run_thread = threading.Thread(target=server.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            # The gateway listener accepts a TCP connection. Opening a socket
            # proves the listener is live; the minimal server does not decode
            # or dispatch frames, so no wire handshake is exercised here.
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", port))
            try:
                assert sock.getpeername()[1] == port
            finally:
                sock.close()

            server.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.close()

    def test_minimal_main_entry_point_boots_and_shuts_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Drive examples.minimal.server.main() on its own thread. main()
        # enters Server.serve(), which parks the calling thread in a
        # sigtimedwait loop that also breaks when the run-loop worker exits;
        # the test reaches into the facade the entry point constructed and
        # requests shutdown directly, so no signal is delivered. The
        # thread's context manager closes the facade when serve() returns.
        import examples.minimal.server as minimal

        captured: dict[str, Server] = {}

        original_init = minimal.Server.__init__

        def _capture_init(self: Server, *args: object, **kwargs: object) -> None:
            original_init(self, *args, **kwargs)  # type: ignore[arg-type]
            captured["server"] = self

        monkeypatch.setattr(minimal.Server, "__init__", _capture_init)

        thread = threading.Thread(target=minimal.main, daemon=True)
        thread.start()
        try:
            # Wait for main() to construct the facade and enter serve().
            deadline = time.monotonic() + 5.0
            while "server" not in captured and time.monotonic() < deadline:
                time.sleep(0.01)
            assert "server" in captured
            facade = captured["server"]
            assert facade.listen_port != 0

            # Request graceful shutdown from this thread; the run-loop
            # worker exits, serve()'s wait breaks on worker death, and the
            # context manager in main() releases the handle.
            facade.shutdown()
        finally:
            thread.join(timeout=5.0)
            assert not thread.is_alive(), "run loop did not exit after shutdown"

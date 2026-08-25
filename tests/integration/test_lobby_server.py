"""Integration test: the lobby server end-to-end.

Boots the :mod:`examples.lobby.server`, verifies the gateway listener
accepts a TCP connection, drives the control plane
(``login``/``join``/``chat``/``leave``/``status``), and confirms the
lobby state (members, rooms, chat log, presence outbox) changes at each
step. No simulation model, no fabric cell product, no AOI subscription:
this is a non-spatial slice on Gateway + Control + DB only.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from pathlib import Path
from typing import Any, cast
from urllib import error, request

import pytest
from _helpers import needs_build
from examples.lobby.server import LobbyServer

from kith import ServerStatus


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _status(server: LobbyServer) -> ServerStatus:
    assert server._server is not None
    return server._server.status


def _post_json(url: str, body: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _get_json(url: str) -> dict[str, Any]:
    with request.urlopen(url, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


@needs_build
class TestLobbyServer:
    def test_login_join_chat_leave_end_to_end(self) -> None:
        server = LobbyServer()
        facade, gateway_port, control_port = server.start()
        try:
            assert _status(server) is ServerStatus.CREATED

            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            # The gateway listener accepts a TCP connection. The lobby has
            # no sim/fabric/AOI; this proves the gateway listener is live.
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", gateway_port))
            try:
                assert sock.getpeername()[1] == gateway_port
            finally:
                sock.close()

            base = f"http://127.0.0.1:{control_port}"

            # Login two members.
            m1 = _post_json(f"{base}/login", {"principal_id": 100})
            assert int(m1["member_id"]) == 1
            m2 = _post_json(f"{base}/login", {"principal_id": 200})
            assert int(m2["member_id"]) == 2

            # Member 1 joins "town"; the room has one member.
            join1 = _post_json(f"{base}/join", {"member_id": 1, "room": "town"})
            assert join1["joined"] is True
            assert join1["members"] == [1]

            # Member 2 joins "town"; the room has two members.
            join2 = _post_json(f"{base}/join", {"member_id": 2, "room": "town"})
            assert join2["members"] == [1, 2]

            # List rooms: "town" has [1, 2].
            rooms = _get_json(f"{base}/rooms")
            assert rooms["rooms"] == {"town": [1, 2]}

            # Member 1 sends chat; the chat log records it.
            chat = _post_json(f"{base}/chat", {"member_id": 1, "room": "town", "text": "hello"})
            assert chat["sent"] is True
            log = _get_json(f"{base}/chat_log?room=town")
            assert len(log["log"]) == 1
            assert log["log"][0]["text"] == "hello"

            # The outbox holds the composed presence notifications: two
            # KIND_JOIN and one KIND_CHAT delivered to the room members.
            outbox = _get_json(f"{base}/outbox")
            assert len(outbox["outbox"]) == 3

            # Member 2 leaves "town"; the room has one member.
            leave = _post_json(f"{base}/leave", {"member_id": 2, "room": "town"})
            assert leave["left"] is True
            assert leave["members"] == [1]

            # Presence query for "town" shows member 1.
            presence = _get_json(f"{base}/presence?room=town")
            assert presence["room"] == "town"
            assert len(presence["presence"]) == 1
            assert int(presence["presence"][0]["member_id"]) == 1

            # Member 1 sets status to away (1); the status route returns
            # updated=True.
            status = _post_json(f"{base}/status", {"member_id": 1, "status": 1})
            assert status["updated"] is True
            members = _get_json(f"{base}/members")
            assert int(members["members"][0]["status"]) == 1

            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

    def test_chat_from_non_member_returns_404(self) -> None:
        server = LobbyServer()
        facade, _gateway_port, control_port = server.start()
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)

            base = f"http://127.0.0.1:{control_port}"
            _post_json(f"{base}/login", {"principal_id": 100})

            with pytest.raises(error.HTTPError) as exc_info:
                _post_json(f"{base}/chat", {"member_id": 1, "room": "town", "text": "hi"})
            assert exc_info.value.code == 404
            exc_info.value.close()

            facade.shutdown()
            run_thread.join(timeout=5.0)
        finally:
            server.stop()

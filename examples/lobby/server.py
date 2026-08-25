"""Lobby server: presence, rooms, and chat on Gateway + Control + DB only.

Boots a Server that registers the lobby wire catalog, five
inbound handlers (login, room_join, room_leave, chat_room, presence_update),
and control-plane routes that drive the same lobby state the wire handlers
mutate. No simulation model is instantiated, no zone is reserved, no fabric
cell product is published, and no AOI subscription is made: the lobby runs on
Gateway + Control + DB only, proving the planes are a pickable toolkit and a
non-spatial game needs no Sim, Fabric, or AOI.

The control-plane routes are the harness surface: the agentic scenarios and
the integration test drive presence, rooms, and chat through HTTP, while the
wire types carry the same mutations a real lobby client would send. The
``presence`` (S2C) type is composed by the handlers and recorded in an
OutboxSink; the control plane exposes the outbox so a harness can
observe what the server would deliver to each session.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from examples.lobby import handlers, messages
from examples.lobby.handlers import LobbyState, OutboxSink

from kith import Server
from kith.control import Request, Response


class LobbyServer:
    """The lobby server: Gateway + Control + DB, no Sim/Fabric/AOI.

    Owns the Server facade, the LobbyState, and the
    OutboxSink. The wire handlers mutate the state and compose
    ``presence`` payloads; the control routes mutate the state directly and
    return JSON for the harness.
    """

    __slots__ = ("_server", "_sink", "_state")

    def __init__(self) -> None:
        self._state = LobbyState()
        self._sink = OutboxSink()
        self._server: Server | None = None

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def start(self) -> tuple[Server, int, int]:
        """Build the server, register the surface, and return it ready to run.

        Returns the facade, the gateway port, and the control-plane port so a
        caller can connect a wire client and issue control commands. The
        caller drives run on its own thread and calls
        stop to shut down.
        """
        # replication_type_id=0: the lobby has no sim/fabric view composer,
        # so the gateway's per-tick delivery path is not used. The ``presence``
        # S2C type is composed by the handlers and recorded in the outbox sink.
        server = Server(
            topology="embedded",
            listen_port=0,
            tick_hz=10,
            handler_table_size=4096,
            replication_type_id=0,
        )
        self._server = server

        messages.register(server)
        handlers.register(server, state=self._state, sink=self._sink)

        server.register_control_route("POST", "/login", self._route_login)
        server.register_control_route("POST", "/join", self._route_join)
        server.register_control_route("POST", "/leave", self._route_leave)
        server.register_control_route("POST", "/chat", self._route_chat)
        server.register_control_route("POST", "/status", self._route_status)
        server.register_control_route("GET", "/members", self._route_members)
        server.register_control_route("GET", "/rooms", self._route_rooms)
        server.register_control_route("GET", "/presence", self._route_presence)
        server.register_control_route("GET", "/chat_log", self._route_chat_log)
        server.register_control_route("GET", "/outbox", self._route_outbox)

        return server, server.listen_port, server.control_port

    def stop(self) -> None:
        """Release the server facade and the lobby state."""
        if self._server is not None:
            self._server.shutdown()
            self._server.close()
            self._server = None
        self._sink.clear()

    # -----------------------------------------------------------------------
    # control-plane routes
    # -----------------------------------------------------------------------

    def _route_login(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        principal_id = int(body["principal_id"])
        member = self._state.get_or_create_member(principal_id)
        self._write_json(resp, 200, _member_dict(member))

    def _route_join(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        member_id = int(body["member_id"])
        room = str(body["room"])
        if not self._state.join_room(room, member_id):
            self._write_json(resp, 200, {"joined": False, "room": room})
            return
        self._notify_room(room, handlers.KIND_JOIN, member_id)
        self._write_json(
            resp,
            200,
            {"joined": True, "room": room, "members": self._state.room_members(room)},
        )

    def _route_leave(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        member_id = int(body["member_id"])
        room = str(body["room"])
        if not self._state.leave_room(room, member_id):
            self._write_json(resp, 200, {"left": False, "room": room})
            return
        self._notify_room(room, handlers.KIND_LEAVE, member_id)
        self._write_json(
            resp,
            200,
            {"left": True, "room": room, "members": self._state.room_members(room)},
        )

    def _route_chat(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        member_id = int(body["member_id"])
        room = str(body["room"])
        text = str(body["text"])
        if member_id not in self._state.room_members(room):
            self._write_json(resp, 404, {"error": "member not in room"})
            return
        self._state.record_chat(room, member_id, text)
        self._notify_room(room, handlers.KIND_CHAT, member_id, text=text)
        self._write_json(resp, 200, {"sent": True, "room": room})

    def _route_status(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        member_id = int(body["member_id"])
        status = int(body["status"])
        if not self._state.set_status(member_id, status):
            self._write_json(resp, 200, {"updated": False})
            return
        for room in self._state.room_names():
            if member_id in self._state.room_members(room):
                self._notify_room(room, handlers.KIND_STATUS, member_id)
        self._write_json(resp, 200, {"updated": True})

    def _route_members(self, req: Request, resp: Response) -> None:
        members = [_member_dict(m) for m in self._state.members()]
        self._write_json(resp, 200, {"members": members})

    def _route_rooms(self, req: Request, resp: Response) -> None:
        rooms = {room: self._state.room_members(room) for room in self._state.room_names()}
        self._write_json(resp, 200, {"rooms": rooms})

    def _route_presence(self, req: Request, resp: Response) -> None:
        room = req.path.rpartition("=")[2] if "room=" in req.path else ""
        if room:
            members = [self._state.member(mid) for mid in self._state.room_members(room)]
            presence = [_member_dict(m) for m in members if m]
            self._write_json(resp, 200, {"room": room, "presence": presence})
            return
        rooms = {r: self._state.room_members(r) for r in self._state.room_names()}
        self._write_json(resp, 200, {"rooms": rooms})

    def _route_chat_log(self, req: Request, resp: Response) -> None:
        room = req.path.rpartition("=")[2] if "room=" in req.path else ""
        log = self._state.chat_log(room if room else None)
        entries = [{"room": r, "member_id": mid, "text": t} for r, mid, t in log]
        self._write_json(resp, 200, {"log": entries})

    def _route_outbox(self, req: Request, resp: Response) -> None:
        del req
        entries = [
            {"session_ids": list(e.session_ids), "payload_hex": e.payload.hex()}
            for e in self._sink.entries()
        ]
        self._write_json(resp, 200, {"outbox": entries})

    # -----------------------------------------------------------------------
    # helpers
    # -----------------------------------------------------------------------

    def _notify_room(self, room: str, kind: int, member_id: int, *, text: str = "") -> None:
        member = self._state.member(member_id)
        if member is None:
            return
        payload = handlers.encode_presence(
            kind=kind,
            member_id=member_id,
            display_name=member.display_name,
            status=member.status,
            text=text,
        )
        self._sink.deliver(self._state.room_members(room), payload)

    @staticmethod
    def _write_json(resp: Response, status: int, body: dict[str, Any]) -> None:
        resp.status(status, "application/json")
        resp.body(json.dumps(body).encode("utf-8"))


def _read_json(req: Request) -> dict[str, Any]:
    return json.loads(req.body.decode("utf-8")) if req.body else {}


def _member_dict(member: handlers.Member) -> dict[str, Any]:
    return asdict(member)


def main() -> None:
    """Boot the lobby server and block until interrupted.

    A wire client connects to the gateway port; a harness drives the control
    plane. The integration test drives the same surface programmatically.
    """
    server = LobbyServer()
    facade, gateway_port, control_port = server.start()
    print(f"lobby: gateway={gateway_port} control={control_port}", flush=True)
    try:
        facade.serve()
    finally:
        server.stop()


if __name__ == "__main__":
    main()

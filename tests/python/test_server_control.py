"""Unit tests for the server control-plane client."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import pytest
from tools.agent.server_control import ServerControlClient, ServerControlError


HandlerFn = Callable[
    [str, str, dict[str, str], bytes],
    tuple[int, str, bytes, bool],
]


class _HttpServer:
    """A minimal asyncio HTTP/1.1 server for client tests.

    Dispatches each request to a handler returning
    ``(status, content_type, body_bytes, keep_alive)``.
    """

    def __init__(self, handler: HandlerFn) -> None:
        self._handler = handler
        self._server: asyncio.AbstractServer | None = None
        self.host = "127.0.0.1"
        self.port = 0
        self.requests: list[tuple[str, str, dict[str, str], bytes]] = []
        self._next_conn_id: int = 0
        #: The server-side connection id that served each request, in
        #: request order — the identity a keep-alive contract is judged on.
        self.request_conn_ids: list[int] = []

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, host=self.host, port=0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn_id = self._next_conn_id
        self._next_conn_id += 1
        try:
            while True:
                request_line = await reader.readline()
                if not request_line:
                    break
                parts = request_line.decode("iso-8859-1").split()
                if len(parts) < 2:
                    break
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
                    body = await reader.readexactly(int(cl))
                self.requests.append((method, path, headers, body))
                self.request_conn_ids.append(conn_id)
                status, content_type, resp_body, keep_alive = self._handler(
                    method, path, headers, body
                )
                reason = {200: "OK", 404: "Not Found", 500: "Server Err"}.get(status, "OK")
                head = (
                    f"HTTP/1.1 {status} {reason}\r\n"
                    f"Content-Type: {content_type}\r\n"
                    f"Content-Length: {len(resp_body)}\r\n"
                    f"Connection: {'keep-alive' if keep_alive else 'close'}\r\n"
                    f"\r\n"
                ).encode("iso-8859-1")
                writer.write(head + resp_body)
                await writer.drain()
                if not keep_alive:
                    break
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as _exc:
            pass
        finally:
            writer.close()


def _handler(
    method: str, path: str, _headers: dict[str, str], body: bytes
) -> tuple[int, str, bytes, bool]:
    if path == "/metrics":
        return 200, "text/plain", b"kith_connections 5\n", True
    if path == "/health":
        return 200, "application/json", b'{"status":"ok"}', True
    if path == "/api/state":
        return 200, "application/json", b'{"zone":"alpha"}', True
    if path == "/api/error":
        return 500, "application/json", b'{"err":"boom"}', True
    if path == "/api/notjson":
        return 200, "application/json", b"not-json{", True
    if path == "/api/array":
        return 200, "application/json", b"[1,2,3]", True
    if method == "POST" and path == "/api/cmd":
        decoded = json.loads(body.decode("utf-8"))
        decoded["echoed"] = True
        return 200, "application/json", json.dumps(decoded).encode("utf-8"), True
    return 404, "application/json", b'{"err":"not found"}', True


@contextlib.asynccontextmanager
async def _server_and_client(
    handler: HandlerFn = _handler,
) -> AsyncIterator[tuple[_HttpServer, ServerControlClient]]:
    server = _HttpServer(handler)
    await server.start()
    client = ServerControlClient(host=server.host, port=server.port, timeout_s=2.0)
    try:
        yield server, client
    finally:
        await client.close()
        await server.stop()


def _run(
    coro: Coroutine[Any, Any, object],
) -> None:
    asyncio.run(coro)


# ---------------------------------------------------------------------------
# metrics / get / post / health
# ---------------------------------------------------------------------------


def test_metrics_returns_text() -> None:
    async def run() -> None:
        async with _server_and_client() as (_server, client):
            text = await client.metrics()
            assert "kith_connections 5" in text

    _run(run())


def test_get_returns_json_object() -> None:
    async def run() -> None:
        async with _server_and_client() as (_server, client):
            result = await client.get("/api/state")
            assert result == {"zone": "alpha"}

    _run(run())


def test_health_returns_json() -> None:
    async def run() -> None:
        async with _server_and_client() as (_server, client):
            result = await client.health()
            assert result == {"status": "ok"}

    _run(run())


def test_post_echoes_body() -> None:
    async def run() -> None:
        async with _server_and_client() as (_server, client):
            result = await client.post("/api/cmd", {"x": 1})
            assert result == {"x": 1, "echoed": True}

    _run(run())


# ---------------------------------------------------------------------------
# error paths
# ---------------------------------------------------------------------------


def test_get_raises_on_http_error() -> None:
    async def run() -> None:
        async with _server_and_client() as (_server, client):
            with pytest.raises(ServerControlError, match="HTTP 500"):
                await client.get("/api/error")

    _run(run())


def test_get_raises_on_non_json() -> None:
    async def run() -> None:
        async with _server_and_client() as (_server, client):
            with pytest.raises(ServerControlError, match="non-JSON"):
                await client.get("/api/notjson")

    _run(run())


def test_get_raises_on_non_object_json() -> None:
    async def run() -> None:
        async with _server_and_client() as (_server, client):
            with pytest.raises(ServerControlError, match="expected object"):
                await client.get("/api/array")

    _run(run())


# ---------------------------------------------------------------------------
# keep-alive and reconnect
# ---------------------------------------------------------------------------


def test_keep_alive_reuses_connection() -> None:
    async def run() -> None:
        async with _server_and_client() as (server, client):
            await client.get("/api/state")
            await client.get("/api/state")
            await client.get("/api/state")
            assert len(server.requests) == 3
            # The reuse contract is connection identity, not request count:
            # one server-side connection served all three requests.
            assert len(set(server.request_conn_ids)) == 1

    _run(run())


def test_reconnects_after_mid_stream_drop() -> None:
    """A server that closes after the first response forces a reconnect."""

    def handler(
        _method: str, path: str, _headers: dict[str, str], _body: bytes
    ) -> tuple[int, str, bytes, bool]:
        # Always close the connection (keep_alive=False) to force the
        # client to reconnect on every request.
        if path == "/api/state":
            return 200, "application/json", b'{"zone":"alpha"}', False
        return 404, "application/json", b'{"err":"no"}', False

    async def run() -> None:
        async with _server_and_client(handler) as (server, client):
            first = await client.get("/api/state")
            second = await client.get("/api/state")
            assert first == {"zone": "alpha"}
            assert second == {"zone": "alpha"}
            # Two requests, two server-side connections: the counter
            # records the reconnect the keep-alive contract forbids.
            assert len(set(server.request_conn_ids)) == 2

    _run(run())


def test_connect_failure_raises() -> None:
    async def run() -> None:
        client = ServerControlClient(host="127.0.0.1", port=1, timeout_s=0.5)
        try:
            with pytest.raises(ServerControlError, match="connect"):
                await client.get("/api/state")
        finally:
            await client.close()

    _run(run())


def test_oversized_body_drops_connection_so_next_request_resyncs() -> None:
    """A parse failure mid-body leaves unread bytes; the next request must reconnect.

    The first response claims a 2 MiB body and delivers 32 bytes, so the
    client raises its size error with the stream desynced. Without dropping
    the keep-alive connection there, the second request reads the stale
    body bytes as a status line and every subsequent request fails forever.
    """

    async def run() -> None:
        huge_served = False

        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal huge_served
            try:
                while True:
                    request_line = await reader.readline()
                    if not request_line:
                        break
                    path = request_line.decode("iso-8859-1").split()[1]
                    while True:
                        line = await reader.readline()
                        if line in (b"\r\n", b"\n", b""):
                            break
                    if path == "/huge" and not huge_served:
                        huge_served = True
                        head = (
                            "HTTP/1.1 200 OK\r\n"
                            "Content-Type: application/json\r\n"
                            "Content-Length: 2000000\r\n"
                            "Connection: keep-alive\r\n\r\n"
                        ).encode("iso-8859-1")
                        writer.write(head + b"x" * 32)
                    else:
                        body = b'{"ok": true}'
                        head = (
                            "HTTP/1.1 200 OK\r\n"
                            "Content-Type: application/json\r\n"
                            f"Content-Length: {len(body)}\r\n"
                            "Connection: keep-alive\r\n\r\n"
                        ).encode("iso-8859-1")
                        writer.write(head + body)
                    await writer.drain()
            except (ConnectionError, asyncio.IncompleteReadError) as _exc:
                del _exc
            finally:
                writer.close()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        sockets = server.sockets
        assert sockets is not None
        port = sockets[0].getsockname()[1]
        client = ServerControlClient(host="127.0.0.1", port=port, timeout_s=2.0)
        try:
            with pytest.raises(ServerControlError, match="exceeds"):
                await client.get("/huge")
            reply = await client.get("/api/state")
            assert reply == {"ok": True}
        finally:
            await client.close()
            server.close()
            await server.wait_closed()

    _run(run())


# ---------------------------------------------------------------------------
# defaults and protocol satisfaction
# ---------------------------------------------------------------------------


def test_default_host_and_port() -> None:
    client = ServerControlClient()
    assert client.host == "127.0.0.1"
    assert client.port == 8080


def test_satisfies_server_control_protocol() -> None:
    from tools.agent.assertions import ServerControl

    client = ServerControlClient()
    assert isinstance(client, ServerControl)

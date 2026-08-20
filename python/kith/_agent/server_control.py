"""Server control-plane client for the agentic harness.

An async HTTP/1.1 client over raw asyncio streams (no third-party
dependency; the framework is stdlib-only). It satisfies the
:class:`~tools.agent.assertions.ServerControl` protocol structurally —
``metrics`` and ``get`` — so server-side assertions
(:func:`~tools.agent.assertions.assert_server_metric`,
:func:`~tools.agent.assertions.assert_server_state`) compose against it
without inheriting the protocol.

The client is generic and type-id-agnostic: it exposes ``metrics`` (the
Prometheus text scrape), ``get`` (a JSON control-plane GET), ``post`` (a
JSON control-plane POST), and ``health``. No game-specific command
endpoints live here; a scenario that needs a game-specific control call
issues it through ``get``/``post`` with the game's path.

A single keep-alive TCP connection is reused across requests and
reconnected lazily on loss, so the assertion poll loop (50ms cadence)
does not pay a per-request connect cost. Requests are serialized by an
``asyncio.Lock`` so the shared connection sees one request/response at a
time.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Mapping
from typing import Final


_DEFAULT_HOST: Final[str] = "127.0.0.1"
_DEFAULT_PORT: Final[int] = 8080
_DEFAULT_TIMEOUT_S: Final[float] = 10.0
_MAX_BODY_BYTES: Final[int] = 1 << 20
_READ_CHUNK: Final[int] = 8192

_HTTP_READ_ERRORS: Final[tuple[type[BaseException], ...]] = (
    asyncio.IncompleteReadError,
    ConnectionError,
    OSError,
)


class ServerControlError(Exception):
    """Raised when a control-plane request fails (transport or HTTP status)."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status: int | None = status


class ServerControlClient:
    """An async HTTP/1.1 client for the server control plane.

    Maintains one keep-alive connection to ``host:port`` (default
    ``127.0.0.1:8080``) and serializes requests over it. Reconnects
    lazily when the connection is lost or has not been opened.
    """

    def __init__(
        self,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        *,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout_s = timeout_s
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    async def metrics(self) -> str:
        """Return the Prometheus text-format metrics scrape."""
        status, _, body = await self._request("GET", "/metrics", accept="text/plain")
        if status != 200:
            raise ServerControlError(f"metrics returned HTTP {status}", status=status)
        return body.decode("utf-8", errors="replace")

    async def get(self, path: str) -> Mapping[str, object]:
        """Return the JSON response for a control-plane GET ``path``."""
        status, _, body = await self._request("GET", path, accept="application/json")
        if status != 200:
            raise ServerControlError(f"GET {path} returned HTTP {status}", status=status)
        return _decode_json(path, body)

    async def post(self, path: str, body: Mapping[str, object]) -> Mapping[str, object]:
        """POST a JSON ``body`` to ``path`` and return the JSON response."""
        payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
        status, _, resp = await self._request(
            "POST",
            path,
            accept="application/json",
            content_type="application/json",
            body=payload,
        )
        if status != 200:
            raise ServerControlError(f"POST {path} returned HTTP {status}", status=status)
        return _decode_json(path, resp)

    async def health(self) -> Mapping[str, object]:
        """Return the JSON response of the ``/health`` endpoint."""
        return await self.get("/health")

    async def close(self) -> None:
        """Close the keep-alive connection if open."""
        await self._drop_connection()

    # -----------------------------------------------------------------------
    # internals
    # -----------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        accept: str,
        content_type: str | None = None,
        body: bytes | None = None,
    ) -> tuple[int, str, bytes]:
        """Issue one HTTP/1.1 request, reconnecting once on transport loss.

        Returns ``(status_code, content_type, body_bytes)``. Serializes
        requests over the shared connection via the instance lock.
        """
        async with self._lock:
            try:
                await self._ensure_connected()
                await self._send_request(method, path, accept, content_type, body)
                return await self._read_response()
            except _HTTP_READ_ERRORS:
                await self._drop_connection()
                await self._ensure_connected()
                try:
                    await self._send_request(method, path, accept, content_type, body)
                    return await self._read_response()
                except _HTTP_READ_ERRORS as exc2:
                    await self._drop_connection()
                    raise ServerControlError(f"{method} {path} failed: {exc2}") from exc2
            except ServerControlError:
                # A parse or size failure leaves the stream at an unknown
                # offset; the keep-alive connection is dropped so the next
                # request resyncs instead of reading the stale bytes.
                await self._drop_connection()
                raise

    async def _ensure_connected(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            return
        await self._drop_connection()
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=self._timeout_s,
            )
        except OSError as exc:
            raise ServerControlError(f"connect to {self._host}:{self._port} failed: {exc}") from exc

    async def _drop_connection(self) -> None:
        writer = self._writer
        self._writer = None
        self._reader = None
        if writer is not None:
            writer.close()
            with contextlib.suppress(*_HTTP_READ_ERRORS):
                await writer.wait_closed()

    async def _send_request(
        self,
        method: str,
        path: str,
        accept: str,
        content_type: str | None,
        body: bytes | None,
    ) -> None:
        assert self._writer is not None
        lines = [
            f"{method} {path} HTTP/1.1",
            f"Host: {self._host}:{self._port}",
            f"Accept: {accept}",
            "Connection: keep-alive",
        ]
        if body is not None:
            lines.append(f"Content-Type: {content_type or 'application/json'}")
            lines.append(f"Content-Length: {len(body)}")
        head = ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1")
        self._writer.write(head)
        if body:
            self._writer.write(body)
        await self._writer.drain()

    async def _read_response(self) -> tuple[int, str, bytes]:
        assert self._reader is not None
        status_line = await asyncio.wait_for(self._reader.readline(), timeout=self._timeout_s)
        if not status_line:
            # An empty status line means the server closed the keep-alive
            # connection between requests. Treat it as a transport closure
            # so the request retry path reconnects and tries once more.
            raise ConnectionError("server closed the keep-alive connection")
        status = _parse_status_code(status_line)
        content_length = await self._read_headers()
        body = await self._read_body(content_length)
        return status, "", body

    async def _read_headers(self) -> int | None:
        assert self._reader is not None
        content_length: int | None = None
        while True:
            line = await asyncio.wait_for(self._reader.readline(), timeout=self._timeout_s)
            if line in (b"\r\n", b"\n", b""):
                break
            key, _, value = line.decode("iso-8859-1").partition(":")
            if key.strip().lower() == "content-length":
                try:
                    content_length = int(value.strip())
                except ValueError:
                    content_length = None
        return content_length

    async def _read_body(self, content_length: int | None) -> bytes:
        assert self._reader is not None
        if content_length is not None:
            if content_length > _MAX_BODY_BYTES:
                raise ServerControlError(f"response body exceeds {_MAX_BODY_BYTES} bytes")
            return await asyncio.wait_for(
                self._reader.readexactly(content_length), timeout=self._timeout_s
            )
        # No Content-Length: read until the server closes the connection,
        # then drop the keep-alive: the server closed the connection.
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = await asyncio.wait_for(self._reader.read(_READ_CHUNK), timeout=self._timeout_s)
            if not chunk:
                break
            total += len(chunk)
            if total > _MAX_BODY_BYTES:
                raise ServerControlError(f"response body exceeds {_MAX_BODY_BYTES} bytes")
            chunks.append(chunk)
        await self._drop_connection()
        return b"".join(chunks)


def _parse_status_code(status_line: bytes) -> int:
    """Extract the integer status code from an HTTP/1.1 status line."""
    parts = status_line.decode("iso-8859-1").split(None, 2)
    if len(parts) < 2:
        raise ServerControlError(f"malformed status line: {status_line!r}")
    try:
        return int(parts[1])
    except ValueError as exc:
        raise ServerControlError(f"malformed status code in {status_line!r}") from exc


def _decode_json(path: str, body: bytes) -> dict[str, object]:
    """Decode a JSON response body, requiring a top-level object."""
    text = body.decode("utf-8", errors="replace")
    try:
        decoded = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ServerControlError(f"{path} returned non-JSON body: {exc}") from exc
    if not isinstance(decoded, dict):
        raise ServerControlError(f"{path} returned JSON {type(decoded).__name__}, expected object")
    return decoded

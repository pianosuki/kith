"""Protocol plane wrapper.

The protocol plane frames messages on the wire: a 10-byte header, the
payload, and an optional 8-byte correlation-ID trailer. It owns a
message-type registry that maps names to numeric ids; both peers register the
same name at the same id before exchanging frames of that type. This module
wraps the C surface into Python types: encode returns ``bytes``, decode
returns a :class:`Frame` dataclass with a copied payload, and the registry
accessors take and return plain strings and ints.

A :class:`Proto` owns its C handle and releases it through :meth:`close`.
"""

from __future__ import annotations

import contextlib
import ctypes
from dataclasses import dataclass
from types import TracebackType

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import proto as gen_proto
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import KithProtocolError, check_error


__all__ = ["Frame", "MsgFlag", "Proto"]

_PROTO_ERROR: gen_types.kith_error = gen_types.kith_error.KITH_EPROTO


class MsgFlag:
    """Header flag bits for :meth:`Proto.encode`."""

    CORRELATION = int(gen_proto.kith_proto_flag.KITH_PROTO_FLAG_CORRELATION)


@dataclass(frozen=True)
class Frame:
    """A decoded wire-protocol frame.

    The payload is copied out of the decode buffer so the caller need not keep
    the input bytes alive.

    Attributes:
        type_id: Registered message-type id.
        flags: Header flag bits.
        has_correlation: True when a correlation-ID trailer was present.
        correlation_id: The 64-bit correlation ID, host byte order.
        payload: Payload bytes (not counting the correlation trailer).
    """

    type_id: int
    flags: int
    has_correlation: bool
    correlation_id: int
    payload: bytes


class Proto:
    """Wire-protocol codec and type registry wrapping a ``kith_proto_t``.

    Args:
        max_payload: Maximum payload size accepted by decode and produced by
            encode, in bytes (excludes the 10-byte header); 0 selects the
            default (1 MiB).
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the proto handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_proto_t`` handle; :meth:`close` releases it.
    """

    __slots__ = ("_bridge", "_closed", "_handle")

    def __init__(
        self,
        *,
        max_payload: int = 0,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._closed = True

        params = gen_proto.kith_proto_params_t(
            size=ctypes.sizeof(gen_proto.kith_proto_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            max_payload=max_payload,
        )
        out = ctypes.POINTER(gen_proto.kith_proto_t)()
        rc = int(
            loaded.lib("proto").kith_proto_create(
                ctypes.byref(params),
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_proto_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_proto_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    # -----------------------------------------------------------------------
    # registry
    # -----------------------------------------------------------------------

    def register_type(self, name: str, type_id: int) -> None:
        """Register a message type by name with an explicit numeric id.

        Both peers register the same name at the same id before exchanging
        frames of that type. Game-specific types use ids at or above
        ``KITH_PROTO_TYPE_USER_BASE`` (1000).

        Args:
            name: The message type's registry name.
            type_id: The numeric id bound to ``name``.

        Raises:
            KithProtocolError: When ``type_id`` is already registered to a
                different name (``KITH_EEXIST``).

        Thread safety:
            @thread_safety safe — concurrent calls on the same handle are
            serialized.
        """
        rc = int(
            self._bridge.lib("proto").kith_proto_register_type_id(
                self._handle,
                name.encode("utf-8"),
                ctypes.c_uint16(type_id),
            )
        )
        check_error(rc, f"kith_proto_register_type_id: {name}", exc=KithProtocolError)

    def lookup_type(self, name: str) -> int:
        """Return the registered id for ``name``.

        Args:
            name: The registered type name to look up.

        Raises:
            KithNotFoundError: When ``name`` is not registered.

        Thread safety:
            @thread_safety safe.
        """
        out = ctypes.c_uint16()
        rc = int(
            self._bridge.lib("proto").kith_proto_lookup_type(
                self._handle,
                name.encode("utf-8"),
                ctypes.byref(out),
            )
        )
        check_error(rc, f"kith_proto_lookup_type: {name}")
        return int(out.value)

    def type_name(self, type_id: int) -> str | None:
        """Return the registered name for ``type_id``, or ``None`` when unregistered.

        Args:
            type_id: The registered type id to name.

        Thread safety:
            @thread_safety safe.
        """
        raw = self._bridge.lib("proto").kith_proto_type_name(
            self._handle,
            ctypes.c_uint16(type_id),
        )
        if not raw:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    # -----------------------------------------------------------------------
    # codec
    # -----------------------------------------------------------------------

    def encode(
        self,
        type_id: int,
        payload: bytes,
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> bytes:
        """Encode a frame to ``bytes``.

        Args:
            type_id: The message-type id written into the header; the
                codec does not consult the registry on encode.
            payload: The frame's payload bytes.
            flags: Header flag bits (``MsgFlag``); ``MsgFlag.CORRELATION``
                adds the 8-byte correlation trailer.
            correlation_id: The correlation value written as the trailer
                when the correlation flag is set; ignored otherwise.

        Raises:
            KithProtocolError: When the frame exceeds the configured max
                payload.

        Thread safety:
            @thread_safety safe — concurrent calls on the same handle are
            serialized.
        """
        payload_buf = ctypes.create_string_buffer(payload, len(payload)) if payload else None
        cap = self._measure_frame(type_id, flags, correlation_id, payload_buf, len(payload))
        if cap == 0:
            raise KithProtocolError(
                _PROTO_ERROR,
                f"kith_proto_encode: type_id={type_id} exceeds max payload",
            )
        out = (ctypes.c_ubyte * cap)()
        written = int(
            self._bridge.lib("proto").kith_proto_encode(
                self._handle,
                ctypes.c_uint16(type_id),
                ctypes.c_uint8(flags),
                ctypes.c_uint64(correlation_id),
                payload_buf,
                ctypes.c_uint32(len(payload)),
                out,
                ctypes.c_size_t(cap),
            )
        )
        if written == 0:
            raise KithProtocolError(
                _PROTO_ERROR,
                f"kith_proto_encode: type_id={type_id} exceeds max payload",
            )
        return bytes(out[:written])

    def decode(self, buf: bytes) -> Frame:
        """Decode the leading frame from ``buf``.

        Args:
            buf: The received bytes.

        Raises:
            KithProtocolError: When ``buf`` holds an incomplete frame
                (``KITH_EAGAIN``) or a malformed frame (``KITH_EPROTO``).

        Thread safety:
            @thread_safety safe — concurrent calls on the same handle are
            serialized.
        """
        view = (ctypes.c_ubyte * len(buf)).from_buffer_copy(buf) if buf else None
        frame = gen_proto.kith_proto_frame_t()
        consumed = ctypes.c_size_t(0)
        rc = int(
            self._bridge.lib("proto").kith_proto_decode(
                self._handle,
                view,
                ctypes.c_size_t(len(buf)),
                ctypes.byref(frame),
                ctypes.byref(consumed),
            )
        )
        check_error(rc, "kith_proto_decode", exc=KithProtocolError)
        payload_len = int(frame.payload_len)
        if payload_len and frame.payload:
            payload = bytes(ctypes.string_at(frame.payload, payload_len))
        else:
            payload = b""
        return Frame(
            type_id=int(frame.type_id),
            flags=int(frame.flags),
            has_correlation=bool(frame.has_correlation),
            correlation_id=int(frame.correlation_id),
            payload=payload,
        )

    def correlation_hex(self, correlation_id: int) -> str:
        """Format a correlation ID as a 16-character lowercase hex string.

        Args:
            correlation_id: The correlation value to format.

        Thread safety:
            @thread_safety safe — stateless.
        """
        buf = ctypes.create_string_buffer(17)
        self._bridge.lib("proto").kith_proto_correlation_hex(ctypes.c_uint64(correlation_id), buf)
        return buf.value.decode("ascii")

    # -----------------------------------------------------------------------
    # internal
    # -----------------------------------------------------------------------

    def _measure_frame(
        self,
        type_id: int,
        flags: int,
        correlation_id: int,
        payload_buf: object,
        payload_len: int,
    ) -> int:
        return int(
            self._bridge.lib("proto").kith_proto_encode(
                self._handle,
                ctypes.c_uint16(type_id),
                ctypes.c_uint8(flags),
                ctypes.c_uint64(correlation_id),
                payload_buf,
                ctypes.c_uint32(payload_len),
                None,
                ctypes.c_size_t(0),
            )
        )

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the proto handle. Idempotent.

        Thread safety:
            @thread_safety unsafe — no register/lookup/encode/decode may be
            in flight on the handle when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("proto").kith_proto_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> Proto:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        if getattr(self, "_closed", True):
            return
        with contextlib.suppress(Exception):
            self.close()

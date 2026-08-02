"""Typed configuration source wrapper.

A :class:`Config` wraps a ``kith_config_t`` handle built from a ``KEY=VALUE``
file and (optionally) the process environment under a namespace prefix. The
handle is an immutable snapshot taken at construction time; the typed
accessors parse, range-check, and report a missing key so required fields
surface at startup rather than silently taking a default.

The wrapper owns the underlying handle and releases it through :meth:`close`
(also invoked by the context manager and ``__del__``). When a :class:`Config`
is passed to :class:`kith.Server`, the server borrows the handle for the
duration of its own lifetime and the wrapper must outlive the server handle;
the server keeps the wrapper alive and closes it after destroying its handle.
"""

from __future__ import annotations

import contextlib
import ctypes
from pathlib import Path
from types import TracebackType
from typing import Final

from kith import _bridge
from kith._generated import config as gen_config
from kith._generated import configure as configure_all
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import KithConfigError, check_error


__all__ = ["Config"]


_ENOENT: Final[int] = int(gen_types.kith_error.KITH_ENOENT)


class Config:
    """Typed, layered configuration source.

    Construction builds the immutable snapshot from the file layer (when
    ``file_path`` names a readable file) and the environment layer (when
    ``env_prefix`` is set); environment entries override file entries with
    the same key. A missing file is not an error: the source is built from
    the environment alone (or empty when neither layer is supplied). An
    unreadable file reports :class:`KithConfigError` carrying ``KITH_EIO``.

    The typed accessors translate ``KITH_ENOENT`` (key absent) into the
    supplied ``default`` and translate parse and range failures into
    :class:`KithConfigError` so a malformed value surfaces at startup.

    Example::

        with kith.Config(file_path="game.env") as cfg:
            tick = cfg.get_u32("tick_hz", default=20)

    Args:
        file_path: Path to a ``KEY=VALUE`` file, or ``None`` to load the
            environment only. The format is one entry per line; ``#`` starts a
            comment and surrounding double quotes on the value are optional.
        env_prefix: Environment-variable namespace prefix, or ``None`` to
            load the file only. An empty prefix is passed through and
            matches every environment variable's name, mirroring the C
            source's non-NULL contract. Every env var whose name starts
            with this prefix is snapshotted under the key formed by
            stripping the prefix; env entries override file entries with
            the same key.
        bridge: A loaded :class:`kith._bridge.Bridge`. When ``None`` the
            process-wide singleton is loaded.

    Raises:
        KithConfigError: When the configuration source cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — the source is an immutable snapshot taken at
        construction.

    Ownership:
        Owns its ``kith_config_t`` handle; :meth:`close` releases it.
    """

    __slots__ = ("_bridge", "_closed", "_handle")

    def __init__(
        self,
        *,
        file_path: str | Path | None = None,
        env_prefix: str | None = None,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._closed = True

        params = gen_config.kith_config_params_t(
            size=ctypes.sizeof(gen_config.kith_config_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            env_prefix=env_prefix.encode("utf-8") if env_prefix is not None else None,
            file_path=str(file_path).encode("utf-8") if file_path is not None else None,
        )
        out = ctypes.POINTER(gen_config.kith_config_t)()
        rc = int(
            loaded.lib("config").kith_config_create(
                ctypes.byref(params),
                None,
                ctypes.byref(out),
            )
        )
        if file_path is not None:
            context = f"kith_config_create: {file_path}"
        else:
            context = "kith_config_create"
        check_error(rc, context, exc=KithConfigError)
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_config_t*`` for passing to a C function.

        The pointer aliases storage owned by this wrapper; the wrapper must
        outlive any borrowed reference.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    # -----------------------------------------------------------------------
    # typed accessors
    # -----------------------------------------------------------------------

    def has(self, key: str) -> bool:
        """Return whether ``key`` is present in the source.

        Args:
            key: The configuration key to look up.

        Thread safety:
            @thread_safety safe — the source is immutable after construction.
        """
        return bool(
            self._bridge.lib("config").kith_config_has(
                self._handle,
                key.encode("utf-8"),
            )
        )

    def get_string(self, key: str, default: str | None = None) -> str | None:
        """Return ``key`` as a string, or ``default`` when the key is absent.

        Args:
            key: The configuration key to look up.
            default: Returned when the key is absent.

        Raises:
            KithConfigError: When the source or key is invalid.

        Thread safety:
            @thread_safety safe — the source is immutable after construction.
        """
        out_val = ctypes.c_char_p()
        rc = int(
            self._bridge.lib("config").kith_config_string(
                self._handle,
                key.encode("utf-8"),
                ctypes.byref(out_val),
            )
        )
        if rc == -_ENOENT:
            return default
        check_error(rc, f"kith_config_string: {key}", exc=KithConfigError)
        raw = out_val.value
        return raw.decode("utf-8") if raw is not None else default

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Return ``key`` as a boolean, or ``default`` when the key is absent.

        Accepted spellings in lower, upper, or capitalized form:
        ``true``/``false``, ``1``/``0``, ``yes``/``no``, ``on``/``off``.

        Args:
            key: The configuration key to look up.
            default: Returned when the key is absent.

        Raises:
            KithConfigError: When the value is not a recognized boolean
                spelling.

        Thread safety:
            @thread_safety safe — the source is immutable after construction.
        """
        out_val = ctypes.c_bool()
        rc = int(
            self._bridge.lib("config").kith_config_bool(
                self._handle,
                key.encode("utf-8"),
                ctypes.byref(out_val),
            )
        )
        if rc == -_ENOENT:
            return default
        check_error(rc, f"kith_config_bool: {key}", exc=KithConfigError)
        return bool(out_val.value)

    def get_u16(
        self,
        key: str,
        *,
        default: int = 0,
        minimum: int = 0,
        maximum: int = 0xFFFF,
    ) -> int:
        """Return ``key`` as an unsigned 16-bit integer in ``[minimum, maximum]``.

        Args:
            key: The configuration key to look up.
            default: Returned when the key is absent.
            minimum: Inclusive lower bound of the accepted range.
            maximum: Inclusive upper bound of the accepted range.

        Raises:
            KithConfigError: When the value is not a base-10 integer
                (``KITH_EINVAL``) or falls outside the range (``KITH_ERANGE``).

        Thread safety:
            @thread_safety safe — the source is immutable after construction.
        """
        out_val = ctypes.c_uint16()
        rc = int(
            self._bridge.lib("config").kith_config_u16(
                self._handle,
                key.encode("utf-8"),
                ctypes.c_uint16(minimum),
                ctypes.c_uint16(maximum),
                ctypes.byref(out_val),
            )
        )
        if rc == -_ENOENT:
            return default
        check_error(rc, f"kith_config_u16: {key}", exc=KithConfigError)
        return int(out_val.value)

    def get_u32(
        self,
        key: str,
        *,
        default: int = 0,
        minimum: int = 0,
        maximum: int = 0xFFFFFFFF,
    ) -> int:
        """Return ``key`` as an unsigned 32-bit integer in ``[minimum, maximum]``.

        Args:
            key: The configuration key to look up.
            default: Returned when the key is absent.
            minimum: Inclusive lower bound of the accepted range.
            maximum: Inclusive upper bound of the accepted range.

        Raises:
            KithConfigError: When the value is not a base-10 integer
                (``KITH_EINVAL``) or falls outside the range (``KITH_ERANGE``).

        Thread safety:
            @thread_safety safe — the source is immutable after construction.
        """
        out_val = ctypes.c_uint32()
        rc = int(
            self._bridge.lib("config").kith_config_u32(
                self._handle,
                key.encode("utf-8"),
                ctypes.c_uint32(minimum),
                ctypes.c_uint32(maximum),
                ctypes.byref(out_val),
            )
        )
        if rc == -_ENOENT:
            return default
        check_error(rc, f"kith_config_u32: {key}", exc=KithConfigError)
        return int(out_val.value)

    def get_u64(
        self,
        key: str,
        *,
        default: int = 0,
        minimum: int = 0,
        maximum: int = 0xFFFFFFFFFFFFFFFF,
    ) -> int:
        """Return ``key`` as an unsigned 64-bit integer in ``[minimum, maximum]``.

        Args:
            key: The configuration key to look up.
            default: Returned when the key is absent.
            minimum: Inclusive lower bound of the accepted range.
            maximum: Inclusive upper bound of the accepted range.

        Raises:
            KithConfigError: When the value is not a base-10 integer
                (``KITH_EINVAL``) or falls outside the range (``KITH_ERANGE``).

        Thread safety:
            @thread_safety safe — the source is immutable after construction.
        """
        out_val = ctypes.c_uint64()
        rc = int(
            self._bridge.lib("config").kith_config_u64(
                self._handle,
                key.encode("utf-8"),
                ctypes.c_uint64(minimum),
                ctypes.c_uint64(maximum),
                ctypes.byref(out_val),
            )
        )
        if rc == -_ENOENT:
            return default
        check_error(rc, f"kith_config_u64: {key}", exc=KithConfigError)
        return int(out_val.value)

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the configuration source. Idempotent.

        Thread safety:
            @thread_safety unsafe — no accessor may be in flight on the
            handle when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("config").kith_config_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> Config:
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

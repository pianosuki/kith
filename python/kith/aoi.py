"""Area-of-interest index wrapper.

The AOI index tracks object positions in a uniform grid and answers sphere and
box queries that visit each intersecting object through a Python callback.
This module wraps the C surface into a Python type: insert/update/remove take
:class:`Object` dataclasses, and the query methods accept ordinary Python
callables.

A :class:`Aoi` owns its C handle and releases it through :meth:`close`. The
ctypes-wrapped visit-callback trampolines are kept alive for the duration of
each query call.
"""

from __future__ import annotations

import contextlib
import ctypes
from collections.abc import Callable
from dataclasses import dataclass
from types import TracebackType

from kith import _bridge
from kith._generated import aoi as gen_aoi
from kith._generated import configure as configure_all
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import check_error


__all__ = ["Aoi", "Box", "Object", "Sphere"]


_ENOENT: int = int(gen_types.kith_error.KITH_ENOENT)


@dataclass
class Object:
    """An AOI object entry.

    Attributes:
        id: Stable object identifier.
        pos_x, pos_y, pos_z: Position in fixed-point units.
        radius: Bounding radius in fixed-point units.
    """

    id: int
    pos_x: int
    pos_y: int
    pos_z: int
    radius: int = 0


@dataclass(frozen=True)
class Sphere:
    """A sphere query volume."""

    cx: int
    cy: int
    cz: int
    radius: int


@dataclass(frozen=True)
class Box:
    """An axis-aligned box query volume."""

    min_x: int
    min_y: int
    min_z: int
    max_x: int
    max_y: int
    max_z: int


def _obj_to_c(obj: Object) -> gen_aoi.kith_aoi_object_t:
    return gen_aoi.kith_aoi_object_t(
        id=ctypes.c_uint64(obj.id),
        pos_x=ctypes.c_int64(obj.pos_x),
        pos_y=ctypes.c_int64(obj.pos_y),
        pos_z=ctypes.c_int64(obj.pos_z),
        radius=ctypes.c_int64(obj.radius),
        user_ptr=None,
    )


def _obj_from_c(o: gen_aoi.kith_aoi_object_t) -> Object:
    return Object(
        id=int(o.id),
        pos_x=int(o.pos_x),
        pos_y=int(o.pos_y),
        pos_z=int(o.pos_z),
        radius=int(o.radius),
    )


class Aoi:
    """Uniform-grid area-of-interest index wrapping a ``kith_aoi_t``.

    Args:
        cell_size: Grid cell size in fixed-point units; 0 selects the default.
        bucket_count: Hash bucket count per cell; 0 selects the default.
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the AOI handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_aoi_t`` handle; :meth:`close` releases it.
    """

    __slots__ = ("_bridge", "_closed", "_handle")

    def __init__(
        self,
        *,
        cell_size: int = 0,
        bucket_count: int = 0,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._closed = True

        params = gen_aoi.kith_aoi_params_t(
            size=ctypes.sizeof(gen_aoi.kith_aoi_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            cell_size=ctypes.c_int64(cell_size),
            bucket_count=bucket_count,
            pad=ctypes.c_uint32(0),
        )
        out = ctypes.POINTER(gen_aoi.kith_aoi_t)()
        rc = int(
            loaded.lib("aoi").kith_aoi_create(
                ctypes.byref(params),
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_aoi_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_aoi_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    # -----------------------------------------------------------------------
    # mutation
    # -----------------------------------------------------------------------

    def insert(self, obj: Object) -> None:
        """Insert a new object. The object id must not already be present.

        Args:
            obj: The object to insert.

        Thread safety:
            @thread_safety unsafe — the index is not synchronized.
        """
        c_obj = _obj_to_c(obj)
        rc = int(
            self._bridge.lib("aoi").kith_aoi_insert(
                self._handle,
                ctypes.byref(c_obj),
            )
        )
        check_error(rc, f"kith_aoi_insert: id={obj.id}")

    def update(self, obj: Object) -> None:
        """Update an existing object's position and radius.

        Args:
            obj: The object carrying the id to update and the
                replacement position and radius.

        Thread safety:
            @thread_safety unsafe — the index is not synchronized.
        """
        c_obj = _obj_to_c(obj)
        rc = int(
            self._bridge.lib("aoi").kith_aoi_update(
                self._handle,
                ctypes.byref(c_obj),
            )
        )
        check_error(rc, f"kith_aoi_update: id={obj.id}")

    def remove(self, object_id: int) -> None:
        """Remove an object by id.

        Args:
            object_id: The id of the object to remove.

        Thread safety:
            @thread_safety unsafe — the index is not synchronized.
        """
        rc = int(
            self._bridge.lib("aoi").kith_aoi_remove(
                self._handle,
                ctypes.c_uint64(object_id),
            )
        )
        check_error(rc, f"kith_aoi_remove: id={object_id}")

    def lookup(self, object_id: int) -> Object | None:
        """Return the object with ``object_id``, or ``None`` when absent.

        Args:
            object_id: The id of the object to return.

        Thread safety:
            @thread_safety unsafe — the index is not synchronized.
        """
        out = gen_aoi.kith_aoi_object_t()
        rc = int(
            self._bridge.lib("aoi").kith_aoi_lookup(
                self._handle,
                ctypes.c_uint64(object_id),
                ctypes.byref(out),
            )
        )
        if rc == -_ENOENT:
            return None
        check_error(rc, f"kith_aoi_lookup: id={object_id}")
        return _obj_from_c(out)

    # -----------------------------------------------------------------------
    # queries
    # -----------------------------------------------------------------------

    def query_sphere(self, sphere: Sphere, visit: Callable[[Object], bool]) -> int:
        """Visit objects intersecting ``sphere``; return the count visited.

        Args:
            sphere: The query volume.
            visit: Called with each intersecting :class:`Object`; return
                ``True`` to continue or ``False`` to stop the scan early.

        Thread safety:
            @thread_safety unsafe — the index is not synchronized; ``visit``
            must not mutate the index. Concurrent queries on one handle race
            the shared index.
        """
        cb, get_count = self._visit_trampoline(visit)
        c_sphere = _sphere_to_c(sphere)
        rc = int(
            self._bridge.lib("aoi").kith_aoi_query_sphere(
                self._handle,
                ctypes.byref(c_sphere),
                cb,
                None,
            )
        )
        check_error(rc, "kith_aoi_query_sphere")
        return get_count()

    def query_box(self, box: Box, visit: Callable[[Object], bool]) -> int:
        """Visit objects intersecting ``box``; return the count visited.

        Args:
            box: The query volume.
            visit: Called with each intersecting :class:`Object`; return
                ``True`` to continue or ``False`` to stop the scan early.

        Thread safety:
            @thread_safety unsafe — the index is not synchronized; ``visit``
            must not mutate the index.
        """
        cb, get_count = self._visit_trampoline(visit)
        c_box = _box_to_c(box)
        rc = int(
            self._bridge.lib("aoi").kith_aoi_query_box(
                self._handle,
                ctypes.byref(c_box),
                cb,
                None,
            )
        )
        check_error(rc, "kith_aoi_query_box")
        return get_count()

    def size(self) -> int:
        """Return the number of objects currently indexed.

        Thread safety:
            @thread_safety unsafe — the index is not synchronized.
        """
        return int(self._bridge.lib("aoi").kith_aoi_size(self._handle))

    # -----------------------------------------------------------------------
    # internal
    # -----------------------------------------------------------------------

    def _visit_trampoline(
        self, visit: Callable[[Object], bool]
    ) -> tuple[object, Callable[[], int]]:
        # Per-call counter so concurrent queries do not share instance
        # state; the C query calls this trampoline synchronously.
        count = 0

        def _trampoline(
            obj_ptr: ctypes._Pointer[gen_aoi.kith_aoi_object],
            _ctx: object,
        ) -> bool:
            nonlocal count
            count += 1
            return bool(visit(_obj_from_c(obj_ptr.contents)))

        return gen_aoi.kith_aoi_visit_fn(_trampoline), lambda: count

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the AOI handle. Idempotent.

        Thread safety:
            @thread_safety unsafe — no query or mutation may be in flight on
            the handle when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("aoi").kith_aoi_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> Aoi:
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


def _sphere_to_c(s: Sphere) -> gen_aoi.kith_aoi_sphere_t:
    return gen_aoi.kith_aoi_sphere_t(
        cx=ctypes.c_int64(s.cx),
        cy=ctypes.c_int64(s.cy),
        cz=ctypes.c_int64(s.cz),
        radius=ctypes.c_int64(s.radius),
    )


def _box_to_c(b: Box) -> gen_aoi.kith_aoi_box_t:
    return gen_aoi.kith_aoi_box_t(
        min_x=ctypes.c_int64(b.min_x),
        min_y=ctypes.c_int64(b.min_y),
        min_z=ctypes.c_int64(b.min_z),
        max_x=ctypes.c_int64(b.max_x),
        max_y=ctypes.c_int64(b.max_y),
        max_z=ctypes.c_int64(b.max_z),
    )

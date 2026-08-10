"""Fabric plane wrapper.

The fabric plane owns the cell-stream store: published cell products keyed
by cell locator (zone, cell_x, cell_y, cell_z, lod), rendered at a requested
product level from the borrowed sim's artifacts, and drained into per-view
subscriptions. This module wraps the publish/remove, snapshot, and
subscription surfaces into Python types.

A :class:`Fabric` is constructed against a borrowed :class:`kith.sim.Sim`
(the fabric queries the sim for artifacts to render). :class:`Subscription`
wraps a cell-key set and drains changed cell products.

Both wrappers own their C handles and release them through :meth:`close`.
"""

from __future__ import annotations

import contextlib
import ctypes
from dataclasses import dataclass
from types import TracebackType

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import fabric as gen_fabric
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import check_error


__all__ = ["CellKey", "CellProduct", "Fabric", "FabricArtifact", "ProductLevel", "Subscription"]


_ENOENT: int = int(gen_types.kith_error.KITH_ENOENT)
_ERANGE: int = int(gen_types.kith_error.KITH_ERANGE)
"""Buffer-too-small outcome; the call reports the required size on retry."""

# Automatic growth attempts when a snapshot outgrows its buffer before the
# error is surfaced to the caller.
_SNAPSHOT_GROW_ATTEMPTS: int = 3


class ProductLevel:
    """Product level a snapshot is rendered at."""

    FULL = int(gen_fabric.kith_fabric_product_level.KITH_FABRIC_LEVEL_FULL)
    REDUCED = int(gen_fabric.kith_fabric_product_level.KITH_FABRIC_LEVEL_REDUCED)
    CROWD = int(gen_fabric.kith_fabric_product_level.KITH_FABRIC_LEVEL_CROWD)


@dataclass(frozen=True)
class CellKey:
    """Cell locator for a fabric cell product.

    Attributes:
        zone: Zone identifier.
        cell_x, cell_y, cell_z: Cell coordinates in fixed-point units.
        lod: Level-of-detail tier.
    """

    zone: int
    cell_x: int
    cell_y: int
    cell_z: int
    lod: int


@dataclass(frozen=True)
class CellProduct:
    """A fabric cell product header."""

    key: CellKey
    authority_epoch: int
    publish_seq: int
    product_level: int
    actor_count: int


@dataclass(frozen=True)
class FabricArtifact:
    """An actor state rendered into a cell snapshot.

    ``update_seq`` is the publisher-minted counter the artifact carried at
    publish time (0 when never minted); ``product_level`` is the level the
    snapshot was rendered at. Reduced and crowd renderings zero both — they
    do not vouch for per-actor coverage.
    """

    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    input_tick: int
    update_seq: int = 0
    product_level: int = 0


def _key_to_c(key: CellKey) -> gen_fabric.kith_fabric_cell_key_t:
    return gen_fabric.kith_fabric_cell_key_t(
        zone=ctypes.c_uint32(key.zone),
        cell_x=ctypes.c_int32(key.cell_x),
        cell_y=ctypes.c_int32(key.cell_y),
        cell_z=ctypes.c_int32(key.cell_z),
        lod=ctypes.c_uint8(key.lod),
        pad=(ctypes.c_uint8 * 3)(),
    )


def _cell_product_from_c(p: gen_fabric.kith_fabric_cell_product_t) -> CellProduct:
    return CellProduct(
        key=CellKey(
            zone=int(p.key.zone),
            cell_x=int(p.key.cell_x),
            cell_y=int(p.key.cell_y),
            cell_z=int(p.key.cell_z),
            lod=int(p.key.lod),
        ),
        authority_epoch=int(p.authority_epoch),
        publish_seq=int(p.publish_seq),
        product_level=int(p.product_level),
        actor_count=int(p.actor_count),
    )


def _artifact_from_c(a: gen_fabric.kith_fabric_artifact_t) -> FabricArtifact:
    return FabricArtifact(
        actor_id=int(a.actor_id),
        pos_x=int(a.pos_x),
        pos_y=int(a.pos_y),
        pos_z=int(a.pos_z),
        vel_x=int(a.vel_x),
        vel_y=int(a.vel_y),
        vel_z=int(a.vel_z),
        input_tick=int(a.input_tick),
        update_seq=int(a.update_seq),
        product_level=int(a.product_level),
    )


class Fabric:
    """Cell-stream store wrapping a ``kith_fabric_t``.

    Args:
        sim: A :class:`kith.sim.Sim` the fabric borrows for artifact rendering.
        cell_bucket_count: Cell store hash bucket count; 0 selects the default.
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the fabric handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_fabric_t`` handle; :meth:`close` releases it,
        closing any subscription left open first.
    """

    __slots__ = ("_bridge", "_closed", "_handle", "_subs")

    def __init__(
        self,
        sim: object,
        *,
        cell_bucket_count: int = 0,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._subs: list[Subscription] = []
        self._closed = True

        sim_handle = sim.handle if hasattr(sim, "handle") else sim
        params = gen_fabric.kith_fabric_params_t(
            size=ctypes.sizeof(gen_fabric.kith_fabric_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            cell_bucket_count=cell_bucket_count,
        )
        out = ctypes.POINTER(gen_fabric.kith_fabric_t)()
        rc = int(
            loaded.lib("fabric").kith_fabric_create(
                ctypes.byref(params),
                sim_handle,
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_fabric_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_fabric_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    # -----------------------------------------------------------------------
    # publish/remove
    # -----------------------------------------------------------------------

    def publish(self, key: CellKey, authority_epoch: int) -> int:
        """Publish a cell product, returning the assigned ``publish_seq``.

        Args:
            key: The cell whose product to publish.
            authority_epoch: The publisher's current authority epoch.

        Raises:
            KithStateError: When ``authority_epoch`` is stale (``KITH_EPERM``).

        Thread safety:
            @thread_safety safe — the cell store and subscription fanout are
            internally synchronized.
        """
        c_key = _key_to_c(key)
        out_seq = ctypes.c_uint64(0)
        rc = int(
            self._bridge.lib("fabric").kith_fabric_publish(
                self._handle,
                ctypes.byref(c_key),
                ctypes.c_uint32(authority_epoch),
                ctypes.byref(out_seq),
            )
        )
        check_error(rc, "kith_fabric_publish")
        return int(out_seq.value)

    def remove_cell(self, key: CellKey) -> None:
        """Remove a cell product from the stream.

        Args:
            key: The cell whose product to remove.

        Thread safety:
            @thread_safety safe.
        """
        c_key = _key_to_c(key)
        rc = int(
            self._bridge.lib("fabric").kith_fabric_remove_cell(
                self._handle,
                ctypes.byref(c_key),
            )
        )
        check_error(rc, "kith_fabric_remove_cell")

    def remove_zone(self, zone: int) -> None:
        """Remove all cell products for one zone.

        Args:
            zone: The zone whose cell products to remove.

        Thread safety:
            @thread_safety safe.
        """
        rc = int(
            self._bridge.lib("fabric").kith_fabric_remove_zone(
                self._handle,
                ctypes.c_uint32(zone),
            )
        )
        check_error(rc, "kith_fabric_remove_zone")

    # -----------------------------------------------------------------------
    # snapshot/product
    # -----------------------------------------------------------------------

    def snapshot_cell(
        self, key: CellKey, level: int, *, capacity: int = 64
    ) -> list[FabricArtifact]:
        """Return artifacts in one cell rendered at ``level``.

        Each entry is a :class:`FabricArtifact` carrying the rendered actor
        state: position and velocity in fixed-point units, the input tick,
        the publisher-minted ``update_seq``, and the product level the
        snapshot was rendered at. Order is unspecified but deterministic for
        a given sequence of store mutations; callers that require a specific
        order sort the returned list. When the cell holds more than
        ``capacity`` artifacts at a full or reduced level, the buffer grows
        to the required size reported by the call and the snapshot is
        retried, so the returned list is complete.

        Args:
            key: The cell to snapshot.
            level: The product level to render the snapshot at.
            capacity: Initial buffer size for the returned artifacts.

        Thread safety:
            @thread_safety safe — concurrent calls serialize on an internal
            snapshot scratch mutex inside the fabric.
        """
        c_key = _key_to_c(key)
        lib = self._bridge.lib("fabric")
        size = int(capacity)
        buf = (gen_fabric.kith_fabric_artifact_t * size)()
        out_count = ctypes.c_size_t(0)
        rc = int(
            lib.kith_fabric_snapshot_cell(
                self._handle,
                ctypes.byref(c_key),
                ctypes.c_uint32(level),
                buf,
                ctypes.c_size_t(size),
                ctypes.byref(out_count),
            )
        )
        for _ in range(_SNAPSHOT_GROW_ATTEMPTS):
            if rc != -_ERANGE:
                break
            size = int(out_count.value)
            buf = (gen_fabric.kith_fabric_artifact_t * size)()
            rc = int(
                lib.kith_fabric_snapshot_cell(
                    self._handle,
                    ctypes.byref(c_key),
                    ctypes.c_uint32(level),
                    buf,
                    ctypes.c_size_t(size),
                    ctypes.byref(out_count),
                )
            )
        check_error(rc, f"kith_fabric_snapshot_cell (required size {size})")
        return [_artifact_from_c(buf[i]) for i in range(int(out_count.value))]

    def snapshot_zone_cells(self, zone: int, lod: int, *, capacity: int = 256) -> list[CellProduct]:
        """Return cell product headers for all cells in one zone at one lod.

        Args:
            zone: The zone whose cells to snapshot.
            lod: The level of detail to read.
            capacity: Buffer size for the returned headers; a zone
                holding more returns the first ``capacity`` and silently
                drops the surplus.

        Thread safety:
            @thread_safety safe.
        """
        buf = (gen_fabric.kith_fabric_cell_product_t * capacity)()
        out_count = ctypes.c_size_t(0)
        rc = int(
            self._bridge.lib("fabric").kith_fabric_snapshot_zone_cells(
                self._handle,
                ctypes.c_uint32(zone),
                ctypes.c_uint8(lod),
                buf,
                ctypes.c_size_t(capacity),
                ctypes.byref(out_count),
            )
        )
        check_error(rc, "kith_fabric_snapshot_zone_cells")
        return [_cell_product_from_c(buf[i]) for i in range(int(out_count.value))]

    def cell_product(self, key: CellKey) -> CellProduct | None:
        """Return the product header for one cell, or ``None`` when absent.

        Args:
            key: The cell whose product header to return.

        Thread safety:
            @thread_safety safe.
        """
        c_key = _key_to_c(key)
        out = gen_fabric.kith_fabric_cell_product_t()
        rc = int(
            self._bridge.lib("fabric").kith_fabric_cell_product(
                self._handle,
                ctypes.byref(c_key),
                ctypes.byref(out),
            )
        )
        if rc == -_ENOENT:
            return None
        check_error(rc, "kith_fabric_cell_product")
        return _cell_product_from_c(out)

    def product_count(self) -> int:
        """Return the total number of cell products retained.

        Thread safety:
            @thread_safety safe.
        """
        return int(self._bridge.lib("fabric").kith_fabric_product_count(self._handle))

    # -----------------------------------------------------------------------
    # subscription
    # -----------------------------------------------------------------------

    def create_subscription(self) -> Subscription:
        """Create a cell-key subscription drained against this fabric.

        The fabric tracks the subscription and closes it before releasing its
        own handle, so the C contract (a subscription is destroyed before the
        fabric it was created from) holds even when the subscription is not
        closed explicitly.

        Thread safety:
            @thread_safety safe — the subscription registry is internally
            synchronized.

        Ownership:
            Returns an owned subscription: the caller closes it through
            :meth:`Subscription.close`; :meth:`Fabric.close` closes any
            subscription left open.
        """
        out = ctypes.POINTER(gen_fabric.kith_fabric_subscription_t)()
        rc = int(
            self._bridge.lib("fabric").kith_fabric_create_subscription(
                self._handle,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_fabric_create_subscription")
        sub = Subscription(self._bridge, out, self._handle)
        self._subs.append(sub)
        return sub

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the fabric handle. Idempotent.

        Closes every subscription created from this fabric first, so the C
        contract (subscriptions are destroyed before the fabric they borrow)
        holds even when subscriptions are not closed explicitly.

        Thread safety:
            @thread_safety unsafe — no publish/subscribe/snapshot/drain may
            be in flight on the fabric when this is called.
        """
        if self._closed:
            return
        for sub in self._subs:
            sub.close()
        self._subs.clear()
        self._bridge.lib("fabric").kith_fabric_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> Fabric:
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


class Subscription:
    """A cell-key subscription drained against a :class:`Fabric`.

    Add cell keys to track, then call :meth:`drain` to collect the cell
    products that changed.

    Ownership:
        Owns its C handle separately from the fabric: :meth:`close`
        destroys it, and :meth:`Fabric.close` closes any subscription
        left open.
    """

    __slots__ = ("_bridge", "_closed", "_fabric", "_handle")

    def __init__(self, bridge: _bridge.Bridge, handle: object, fabric: object) -> None:
        self._bridge = bridge
        self._handle = handle
        self._fabric = fabric
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_fabric_subscription_t*``.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    def add(self, key: CellKey) -> None:
        """Track ``key`` for change notifications.

        Args:
            key: The cell key to track.

        Thread safety:
            @thread_safety safe — the interest set is internally synchronized.
        """
        c_key = _key_to_c(key)
        rc = int(
            self._bridge.lib("fabric").kith_fabric_subscription_add(
                self._handle,
                ctypes.byref(c_key),
            )
        )
        check_error(rc, "kith_fabric_subscription_add")

    def remove(self, key: CellKey) -> None:
        """Stop tracking ``key``.

        Args:
            key: The cell key to stop tracking.

        Thread safety:
            @thread_safety safe — the interest set is internally synchronized.
        """
        c_key = _key_to_c(key)
        rc = int(
            self._bridge.lib("fabric").kith_fabric_subscription_remove(
                self._handle,
                ctypes.byref(c_key),
            )
        )
        check_error(rc, "kith_fabric_subscription_remove")

    def size(self) -> int:
        """Return the number of tracked cell keys.

        Thread safety:
            @thread_safety safe.
        """
        return int(self._bridge.lib("fabric").kith_fabric_subscription_size(self._handle))

    def drain(self) -> list[CellProduct]:
        """Return every changed cell product header, clearing the change queue.

        A product that arrives between the sizing probe and the copy stays
        queued for the next call. A cell removed from the stream before its
        product is collected produces no entry.

        Thread safety:
            @thread_safety safe — the interest set and cell store are
            internally synchronized.
        """
        lib = self._bridge.lib("fabric")
        pending = ctypes.c_size_t(0)
        rc = int(
            lib.kith_fabric_drain(
                self._fabric,
                self._handle,
                None,
                ctypes.c_size_t(0),
                ctypes.byref(pending),
            )
        )
        check_error(rc, "kith_fabric_drain")
        if pending.value == 0:
            return []
        buf = (gen_fabric.kith_fabric_cell_product_t * pending.value)()
        out_count = ctypes.c_size_t(0)
        rc = int(
            lib.kith_fabric_drain(
                self._fabric,
                self._handle,
                buf,
                ctypes.c_size_t(pending.value),
                ctypes.byref(out_count),
            )
        )
        check_error(rc, "kith_fabric_drain")
        return [_cell_product_from_c(buf[i]) for i in range(int(out_count.value))]

    def close(self) -> None:
        """Release the subscription handle. Idempotent.

        Thread safety:
            @thread_safety unsafe — the subscription must not be used by
            another thread when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("fabric").kith_fabric_subscription_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> Subscription:
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

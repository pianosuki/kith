"""Coordination plane wrapper.

The coordination plane resolves cell authority, reports cell density, and
drives split/merge decisions across the cluster membership through a
coordination bus. The bus is a separate handle the coord borrows; this
module wraps both as Python types.

A :class:`Coord` borrows a :class:`CoordBus` (or ``None`` for the embedded
single-instance topology). Both own their C handles and release them through
:meth:`close`; the bus must outlive the coord and be closed after it.
"""

from __future__ import annotations

import contextlib
import ctypes
from dataclasses import dataclass
from enum import IntEnum
from types import TracebackType

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import coord as gen_coord
from kith._generated import fabric as gen_fabric
from kith._generated import version as gen_version
from kith.exceptions import check_error
from kith.fabric import CellKey


__all__ = [
    "Authority",
    "BusEvent",
    "BusMemberStatus",
    "BusTransport",
    "Coord",
    "CoordBus",
    "RebalanceContract",
]


class BusTransport(IntEnum):
    """Coordination bus transport backend."""

    LOOPBACK = int(gen_coord.kith_coord_bus_transport.KITH_COORD_BUS_TRANSPORT_LOOPBACK)


@dataclass(frozen=True)
class Authority:
    """Resolved authority for a cell."""

    instance_id: int
    authority_epoch: int


@dataclass(frozen=True)
class BusMemberStatus:
    """A bus member's heartbeat and load snapshot."""

    instance_id: int
    heartbeat_ms: int
    owned_cell_count: int
    active_input_count: int


@dataclass(frozen=True)
class BusEvent:
    """A drained coordination bus event.

    Attributes:
        event_type: One of ``BusEventType.REBALANCE``, ``MEMBERSHIP``, or
            ``SNAPSHOT_REQUEST`` (the raw ``kith_coord_bus_event_type`` value).
        zone: Zone identifier the event concerns.
        source_instance_id: Member that published the event.
        timestamp_ms: Monotonic timestamp; always 0 on the loopback
            transport, which does not stamp it.
        payload: Event payload bytes (copied), or empty.
    """

    event_type: int
    zone: int
    source_instance_id: int
    timestamp_ms: int
    payload: bytes


@dataclass(frozen=True)
class RebalanceContract:
    """A cell authority rebalance payload carried by the bus.

    The contract moves a cell's authority from ``source_instance_id`` to
    ``target_instance_id`` at ``authority_epoch``. A ``target_instance_id``
    of 0 clears the override (merge back to hash-fallback distribution).

    Attributes:
        key: Cell being rebalanced.
        source_instance_id: Prior authority instance (0 = was hash-fallback).
        target_instance_id: New authority instance (0 = clear override).
        authority_epoch: New authority epoch (stale-product guard).
    """

    key: CellKey
    source_instance_id: int
    target_instance_id: int
    authority_epoch: int

    @classmethod
    def from_bytes(cls, payload: bytes) -> RebalanceContract:
        """Reconstruct a contract from a drained bus event payload.

        Args:
            payload: The ``BusEvent.payload`` bytes from a ``REBALANCE`` event.

        Raises:
            ValueError: When ``payload`` is shorter than the C struct layout.
        """
        if len(payload) < ctypes.sizeof(gen_coord.kith_coord_rebalance_contract_t):
            raise ValueError(f"rebalance contract payload too short: {len(payload)} bytes")
        c = gen_coord.kith_coord_rebalance_contract_t.from_buffer_copy(payload)
        return cls(
            key=CellKey(
                zone=int(c.key.zone),
                cell_x=int(c.key.cell_x),
                cell_y=int(c.key.cell_y),
                cell_z=int(c.key.cell_z),
                lod=int(c.key.lod),
            ),
            source_instance_id=int(c.source_instance_id),
            target_instance_id=int(c.target_instance_id),
            authority_epoch=int(c.authority_epoch),
        )

    def to_bytes(self) -> bytes:
        """Pack the contract as bytes for :meth:`CoordBus.publish`.

        Returns:
            The C struct layout serialized to bytes.
        """
        c = _rebalance_contract_to_c(self)
        return bytes(ctypes.string_at(ctypes.byref(c), ctypes.sizeof(c)))


def _cell_key_to_c(key: CellKey) -> gen_fabric.kith_fabric_cell_key_t:
    return gen_fabric.kith_fabric_cell_key_t(
        zone=ctypes.c_uint32(key.zone),
        cell_x=ctypes.c_int32(key.cell_x),
        cell_y=ctypes.c_int32(key.cell_y),
        cell_z=ctypes.c_int32(key.cell_z),
        lod=ctypes.c_uint8(key.lod),
        pad=(ctypes.c_uint8 * 3)(),
    )


def _rebalance_contract_to_c(
    contract: RebalanceContract,
) -> gen_coord.kith_coord_rebalance_contract_t:
    return gen_coord.kith_coord_rebalance_contract_t(
        key=_cell_key_to_c(contract.key),
        source_instance_id=ctypes.c_uint32(contract.source_instance_id),
        target_instance_id=ctypes.c_uint32(contract.target_instance_id),
        authority_epoch=ctypes.c_uint32(contract.authority_epoch),
        pad=ctypes.c_uint32(0),
    )


class CoordBus:
    """Coordination bus wrapping a ``kith_coord_bus_t``.

    Args:
        instance_id: Local instance identifier.
        transport: Bus transport backend (loopback for single-process).
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the bus handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_coord_bus_t`` handle; :meth:`close` releases it.
    """

    __slots__ = ("_bridge", "_closed", "_handle")

    def __init__(
        self,
        *,
        instance_id: int = 0,
        transport: BusTransport = BusTransport.LOOPBACK,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._closed = True

        params = gen_coord.kith_coord_bus_params_t(
            size=ctypes.sizeof(gen_coord.kith_coord_bus_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            instance_id=instance_id,
            transport=int(transport),
            pad=ctypes.c_uint32(0),
        )
        out = ctypes.POINTER(gen_coord.kith_coord_bus_t)()
        rc = int(
            loaded.lib("coord").kith_coord_bus_create(
                ctypes.byref(params),
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_coord_bus_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_coord_bus_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    def instance_id(self) -> int:
        """Return the bus's local instance identifier.

        Thread safety:
            @thread_safety unsafe.
        """
        return int(self._bridge.lib("coord").kith_coord_bus_instance_id(self._handle))

    def member_count(self) -> int:
        """Return the number of active bus members.

        Thread safety:
            @thread_safety unsafe.
        """
        return int(self._bridge.lib("coord").kith_coord_bus_member_count(self._handle))

    def member_status(self, member_index: int) -> BusMemberStatus:
        """Return the status snapshot for bus member ``member_index``.

        Args:
            member_index: The membership-table index of the member to
                read.

        Thread safety:
            @thread_safety unsafe.
        """
        out = gen_coord.kith_coord_bus_member_status_t()
        rc = int(
            self._bridge.lib("coord").kith_coord_bus_member_status(
                self._handle,
                ctypes.c_uint32(member_index),
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_coord_bus_member_status")
        return BusMemberStatus(
            instance_id=int(out.instance_id),
            heartbeat_ms=int(out.heartbeat_ms),
            owned_cell_count=int(out.owned_cell_count),
            active_input_count=int(out.active_input_count),
        )

    def add_member(self, instance_id: int) -> None:
        """Register an additional cluster member on the bus.

        The bus is created with one member (the local instance); this extends
        the membership table so the split/merge coordinator can pick a real
        target instance when local cell density exceeds the split threshold.
        Two coord handles borrowing one loopback bus see the same membership,
        the density-driven evaluator fires, and rebalance contracts published
        on the bus transfer cell authority across the instance boundary.

        Args:
            instance_id: Instance ID of the joining member. Must not be 0 and
                must not match an existing member.

        Raises:
            KithStateError: When ``instance_id`` is already a member.
            KithError: When ``instance_id`` is 0 or the membership table cannot
                grow.

        Thread safety:
            @thread_safety unsafe — no member_status/tick/split evaluation
            may be in flight on the bus when this is called.
        """
        rc = int(
            self._bridge.lib("coord").kith_coord_bus_add_member(
                self._handle,
                ctypes.c_uint32(instance_id),
            )
        )
        check_error(rc, "kith_coord_bus_add_member")

    def tick(self, now_ms: int) -> None:
        """Advance the bus membership clock.

        Args:
            now_ms: The current monotonic time in milliseconds.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread.
        """
        rc = int(
            self._bridge.lib("coord").kith_coord_bus_tick(
                self._handle,
                ctypes.c_uint64(now_ms),
            )
        )
        check_error(rc, "kith_coord_bus_tick")

    def subscribe(self, zone: int) -> None:
        """Subscribe to a zone's bus events.

        Events are filtered by the zone they were published for, not by
        the publisher instance.

        Args:
            zone: The zone whose bus events this subscription drains.

        Thread safety:
            @thread_safety unsafe.
        """
        rc = int(
            self._bridge.lib("coord").kith_coord_bus_subscribe(
                self._handle,
                ctypes.c_uint32(zone),
            )
        )
        check_error(rc, "kith_coord_bus_subscribe")

    def unsubscribe(self, zone: int) -> None:
        """Stop subscribing to a zone's bus events. Idempotent.

        Args:
            zone: The zone whose bus events this subscription stops
                draining.

        Thread safety:
            @thread_safety unsafe.
        """
        rc = int(
            self._bridge.lib("coord").kith_coord_bus_unsubscribe(
                self._handle,
                ctypes.c_uint32(zone),
            )
        )
        check_error(rc, "kith_coord_bus_unsubscribe")

    def publish(self, event_type: int, zone: int, payload: bytes = b"") -> None:
        """Publish a bus event of ``event_type`` for ``zone``.

        Args:
            event_type: The event type id published on the bus.
            zone: The zone the event is published for; subscriptions
                filter on it.
            payload: The event's payload bytes.

        Thread safety:
            @thread_safety unsafe.
        """
        buf = ctypes.create_string_buffer(payload, len(payload)) if payload else None
        rc = int(
            self._bridge.lib("coord").kith_coord_bus_publish(
                self._handle,
                ctypes.c_uint32(event_type),
                ctypes.c_uint32(zone),
                buf,
                ctypes.c_uint32(len(payload)),
            )
        )
        check_error(rc, "kith_coord_bus_publish")

    def drain(self, *, capacity: int = 64) -> list[BusEvent]:
        """Return pending bus events, clearing the queue.

        Args:
            capacity: Buffer size for the returned events; events past
                it are dropped.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread.
        """
        buf = (gen_coord.kith_coord_bus_event_t * capacity)()
        out_count = ctypes.c_size_t(0)
        rc = int(
            self._bridge.lib("coord").kith_coord_bus_drain(
                self._handle,
                buf,
                ctypes.c_size_t(capacity),
                ctypes.byref(out_count),
            )
        )
        check_error(rc, "kith_coord_bus_drain")
        events: list[BusEvent] = []
        for i in range(int(out_count.value)):
            ev = buf[i]
            payload_len = int(ev.payload_len)
            if payload_len and ev.payload:
                payload = bytes(ctypes.string_at(ev.payload, payload_len))
            else:
                payload = b""
            events.append(
                BusEvent(
                    event_type=int(ev.event_type),
                    zone=int(ev.zone),
                    source_instance_id=int(ev.source_instance_id),
                    timestamp_ms=int(ev.timestamp_ms),
                    payload=payload,
                )
            )
        return events

    def close(self) -> None:
        """Release the bus handle. Idempotent.

        Thread safety:
            @thread_safety unsafe — no publish/drain/subscribe/tick may be
            in flight on the bus when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("coord").kith_coord_bus_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> CoordBus:
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


class Coord:
    """Coordination plane wrapping a ``kith_coord_t``.

    Args:
        bus: A :class:`CoordBus` the coord borrows, or ``None`` for the
            embedded single-instance topology (the bus is not required).
        instance_id: Local instance identifier.
        cell_bucket_count: Authority table hash bucket count; 0 selects default.
        density_stride: Density-report stride in ticks; 0 selects default.
        split_threshold: Cell density that triggers a split; 0 selects default.
        merge_threshold: Cell density that triggers a merge; 0 selects default.
        split_min_dwell_ms: Minimum time in ms a cell stays at or above
            the split threshold before the split executes; 0 selects
            default.
        merge_min_dwell_ms: Minimum time in ms a cell stays at or below
            the merge threshold before the merge executes; 0 selects
            default.
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the coord handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_coord_t`` handle; :meth:`close` releases it. The
        bus passed as ``bus`` stays the caller's.
    """

    __slots__ = ("_bridge", "_closed", "_handle")

    def __init__(
        self,
        bus: CoordBus | None = None,
        *,
        instance_id: int = 0,
        cell_bucket_count: int = 0,
        density_stride: int = 0,
        split_threshold: int = 0,
        merge_threshold: int = 0,
        split_min_dwell_ms: int = 0,
        merge_min_dwell_ms: int = 0,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._closed = True

        bus_handle = bus.handle if bus is not None else None
        params = gen_coord.kith_coord_params_t(
            size=ctypes.sizeof(gen_coord.kith_coord_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            instance_id=instance_id,
            cell_bucket_count=cell_bucket_count,
            density_stride=density_stride,
            split_threshold=split_threshold,
            merge_threshold=merge_threshold,
            split_min_dwell_ms=ctypes.c_uint64(split_min_dwell_ms),
            merge_min_dwell_ms=ctypes.c_uint64(merge_min_dwell_ms),
            pad=ctypes.c_uint32(0),
        )
        out = ctypes.POINTER(gen_coord.kith_coord_t)()
        rc = int(
            loaded.lib("coord").kith_coord_create(
                ctypes.byref(params),
                bus_handle,
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_coord_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_coord_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    # -----------------------------------------------------------------------
    # authority
    # -----------------------------------------------------------------------

    def authority(self, key: CellKey) -> Authority:
        """Resolve the authority for ``key``.

        Args:
            key: The cell whose authority to resolve.

        Thread safety:
            @thread_safety unsafe.
        """
        c_key = _cell_key_to_c(key)
        out = gen_coord.kith_coord_authority_t()
        rc = int(
            self._bridge.lib("coord").kith_coord_authority(
                self._handle,
                ctypes.byref(c_key),
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_coord_authority")
        return Authority(instance_id=int(out.instance_id), authority_epoch=int(out.authority_epoch))

    def set_authority(self, key: CellKey, instance_id: int) -> None:
        """Set a cell's authority override, bumping the authority epoch.

        Args:
            key: The cell whose authority to override.
            instance_id: The instance the cell's authority moves to.

        Thread safety:
            @thread_safety unsafe.
        """
        c_key = _cell_key_to_c(key)
        rc = int(
            self._bridge.lib("coord").kith_coord_set_authority(
                self._handle,
                ctypes.byref(c_key),
                ctypes.c_uint32(instance_id),
            )
        )
        check_error(rc, "kith_coord_set_authority")

    def clear_authority(self, key: CellKey) -> None:
        """Clear a cell's authority override.

        Args:
            key: The cell whose authority override to clear.

        Thread safety:
            @thread_safety unsafe.
        """
        c_key = _cell_key_to_c(key)
        rc = int(
            self._bridge.lib("coord").kith_coord_clear_authority(
                self._handle,
                ctypes.byref(c_key),
            )
        )
        check_error(rc, "kith_coord_clear_authority")

    def on_rebalance(self, contract: RebalanceContract) -> None:
        """Apply an incoming rebalance contract received from the bus.

        When ``contract.target_instance_id`` is 0, the override is cleared
        (merge); otherwise the override is set to the target instance with
        the contract's authority epoch (split). The composition root drains
        the bus, extracts rebalance events, and calls this method to apply
        each contract so the local authority table converges with the
        source instance's decision.

        Args:
            contract: The rebalance contract drained from a bus event.

        Raises:
            KithError: On an apply failure.

        Thread safety:
            @thread_safety unsafe.
        """
        c_contract = _rebalance_contract_to_c(contract)
        rc = int(
            self._bridge.lib("coord").kith_coord_on_rebalance(
                self._handle,
                ctypes.byref(c_contract),
            )
        )
        check_error(rc, "kith_coord_on_rebalance")

    # -----------------------------------------------------------------------
    # density / tick
    # -----------------------------------------------------------------------

    def report_density(self, key: CellKey, actor_count: int, now_ms: int) -> None:
        """Report cell density for split/merge tracking.

        Args:
            key: The cell the density is reported for.
            actor_count: The actor count currently in the cell.
            now_ms: The current monotonic time in milliseconds; band
                entries stamp their dwell timers from it.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread.
        """
        c_key = _cell_key_to_c(key)
        rc = int(
            self._bridge.lib("coord").kith_coord_report_density(
                self._handle,
                ctypes.byref(c_key),
                ctypes.c_uint32(actor_count),
                ctypes.c_uint64(now_ms),
            )
        )
        check_error(rc, "kith_coord_report_density")

    def tick(self, now_ms: int) -> None:
        """Advance the split/merge coordinator.

        Args:
            now_ms: The current monotonic time in milliseconds.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread.
        """
        rc = int(
            self._bridge.lib("coord").kith_coord_tick(
                self._handle,
                ctypes.c_uint64(now_ms),
            )
        )
        check_error(rc, "kith_coord_tick")

    def cell_count(self) -> int:
        """Return the total number of tracked cells.

        Thread safety:
            @thread_safety unsafe.
        """
        return int(self._bridge.lib("coord").kith_coord_cell_count(self._handle))

    def owned_cell_count(self, instance_id: int) -> int:
        """Return the number of cells owned by ``instance_id``.

        Args:
            instance_id: The instance whose owned cell count to return.

        Thread safety:
            @thread_safety unsafe.
        """
        return int(
            self._bridge.lib("coord").kith_coord_owned_cell_count(
                self._handle,
                ctypes.c_uint32(instance_id),
            )
        )

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the coord handle. Idempotent.

        Thread safety:
            @thread_safety unsafe — no authority/snapshot/tick may be in
            flight on the coord when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("coord").kith_coord_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> Coord:
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

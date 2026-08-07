"""Simulation plane wrapper.

The sim plane steps actor state forward at a configured tick rate and
publishes the resulting per-cell artifacts into the fabric. It owns an
artifact store keyed by cell locator (zone, cell_x, cell_y, cell_z, lod) and
a model registry. The registry path targets C sim models compiled against
the versioned vtable contract; Python sim models are wired through the
handler worker pool, not this registry.

This module wraps the read-side of the artifact store (snapshot and product
queries) and the authority-side publish/remove calls into Python types. The
exposed-layout value types (artifact keys, actors, artifacts, cell products)
are mirrored as dataclasses so callers handle plain Python objects.

A :class:`Sim` owns its C handle and releases it through :meth:`close`.
"""

from __future__ import annotations

import contextlib
import ctypes
from dataclasses import dataclass
from types import TracebackType

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import sim as gen_sim
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import check_error


__all__ = [
    "Actor",
    "Artifact",
    "ArtifactKey",
    "CellProduct",
    "Sim",
    "SimInput",
    "SimModel",
    "SimModelConfig",
]


_ENOENT: int = int(gen_types.kith_error.KITH_ENOENT)
_ERANGE: int = int(gen_types.kith_error.KITH_ERANGE)
"""Buffer-too-small outcome; the call reports the required size on retry."""

# Automatic growth attempts when a snapshot outgrows its buffer before the
# error is surfaced to the caller.
_SNAPSHOT_GROW_ATTEMPTS: int = 3


@dataclass(frozen=True)
class ArtifactKey:
    """Cell locator for a sim artifact.

    Attributes:
        zone: Zone identifier.
        cell_x, cell_y, cell_z: Cell coordinates in fixed-point units.
        lod: Level-of-detail tier.
        authority_epoch: Authority epoch the publishing instance claims.
        publish_seq: Assigned by the store on publish; ignored on input.
    """

    zone: int
    cell_x: int
    cell_y: int
    cell_z: int
    lod: int
    authority_epoch: int = 0
    publish_seq: int = 0


@dataclass(frozen=True)
class Artifact:
    """A published actor state within a cell.

    ``update_seq`` is the publisher-minted counter the actor carried at
    publish time (0 when never minted).
    """

    key: ArtifactKey
    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    input_tick: int
    update_seq: int = 0


@dataclass(frozen=True)
class CellProduct:
    """A cell product header (per-cell summary)."""

    zone: int
    cell_x: int
    cell_y: int
    cell_z: int
    lod: int
    authority_epoch: int
    publish_seq: int
    actor_count: int


def _key_to_c(key: ArtifactKey) -> gen_sim.kith_sim_artifact_key_t:
    return gen_sim.kith_sim_artifact_key_t(
        zone=ctypes.c_uint32(key.zone),
        cell_x=ctypes.c_int32(key.cell_x),
        cell_y=ctypes.c_int32(key.cell_y),
        cell_z=ctypes.c_int32(key.cell_z),
        lod=ctypes.c_uint8(key.lod),
        pad=(ctypes.c_uint8 * 3)(),
        authority_epoch=ctypes.c_uint32(key.authority_epoch),
        publish_seq=ctypes.c_uint64(key.publish_seq),
    )


def _artifact_from_c(a: gen_sim.kith_sim_artifact_t) -> Artifact:
    return Artifact(
        key=ArtifactKey(
            zone=int(a.key.zone),
            cell_x=int(a.key.cell_x),
            cell_y=int(a.key.cell_y),
            cell_z=int(a.key.cell_z),
            lod=int(a.key.lod),
            authority_epoch=int(a.key.authority_epoch),
            publish_seq=int(a.key.publish_seq),
        ),
        actor_id=int(a.actor_id),
        pos_x=int(a.pos_x),
        pos_y=int(a.pos_y),
        pos_z=int(a.pos_z),
        vel_x=int(a.vel_x),
        vel_y=int(a.vel_y),
        vel_z=int(a.vel_z),
        input_tick=int(a.input_tick),
        update_seq=int(a.update_seq),
    )


def _cell_product_from_c(p: gen_sim.kith_sim_cell_product_t) -> CellProduct:
    return CellProduct(
        zone=int(p.zone),
        cell_x=int(p.cell_x),
        cell_y=int(p.cell_y),
        cell_z=int(p.cell_z),
        lod=int(p.lod),
        authority_epoch=int(p.authority_epoch),
        publish_seq=int(p.latest_publish_seq),
        actor_count=int(p.actor_count),
    )


class Sim:
    """Simulation artifact store wrapping a ``kith_sim_t``.

    Args:
        tick_hz: Simulation tick rate in Hz; 0 selects the default (20).
        artifact_bucket_count: Artifact store hash bucket count; 0 selects the
            default (4096).
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the sim handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_sim_t`` handle; :meth:`close` releases it.
    """

    __slots__ = ("_bridge", "_closed", "_handle")

    def __init__(
        self,
        *,
        tick_hz: int = 0,
        artifact_bucket_count: int = 0,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._closed = True

        params = gen_sim.kith_sim_params_t(
            size=ctypes.sizeof(gen_sim.kith_sim_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            tick_hz=tick_hz,
            artifact_bucket_count=artifact_bucket_count,
        )
        out = ctypes.POINTER(gen_sim.kith_sim_t)()
        rc = int(
            loaded.lib("sim").kith_sim_create(
                ctypes.byref(params),
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_sim_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_sim_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    # -----------------------------------------------------------------------
    # authority side
    # -----------------------------------------------------------------------

    def publish(
        self,
        key: ArtifactKey,
        *,
        actor_id: int,
        pos: tuple[int, int, int],
        vel: tuple[int, int, int] = (0, 0, 0),
        input_tick: int = 0,
        update_seq: int = 0,
    ) -> int:
        """Publish or update an actor's state as a cell artifact.

        Returns the ``publish_seq`` assigned by the store.

        Args:
            key: Cell locator (zone, cell, lod) to publish into.
            actor_id: The actor the artifact describes.
            pos: Position in Q16.16 fixed-point.
            vel: Velocity in Q16.16 fixed-point.
            input_tick: Last input tick applied to the actor.
            update_seq: Publisher-minted movement-application counter
                (0 = never minted); copied verbatim into the artifact.

        Returns:
            The assigned publish sequence number.

        Thread safety:
            @thread_safety safe — the artifact store is internally synchronized.
        """
        c_key = _key_to_c(key)
        actor = gen_sim.kith_sim_actor_t(
            id=ctypes.c_uint64(actor_id),
            pos_x=ctypes.c_int64(pos[0]),
            pos_y=ctypes.c_int64(pos[1]),
            pos_z=ctypes.c_int64(pos[2]),
            vel_x=ctypes.c_int64(vel[0]),
            vel_y=ctypes.c_int64(vel[1]),
            vel_z=ctypes.c_int64(vel[2]),
            input_tick=ctypes.c_uint32(input_tick),
            flags=ctypes.c_uint32(0),
            update_seq=ctypes.c_uint32(update_seq),
        )
        out_seq = ctypes.c_uint64(0)
        rc = int(
            self._bridge.lib("sim").kith_sim_publish_artifact(
                self._handle,
                ctypes.byref(c_key),
                ctypes.byref(actor),
                ctypes.byref(out_seq),
            )
        )
        check_error(rc, "kith_sim_publish_artifact")
        return int(out_seq.value)

    def remove_actor(self, actor_id: int) -> None:
        """Remove all artifacts for one actor across all cells.

        Args:
            actor_id: The actor whose artifacts to remove.

        Thread safety:
            @thread_safety safe — the artifact store is internally synchronized.
        """
        rc = int(
            self._bridge.lib("sim").kith_sim_remove_artifact(
                self._handle,
                ctypes.c_uint64(actor_id),
            )
        )
        check_error(rc, "kith_sim_remove_artifact")

    def remove_zone(self, zone: int) -> None:
        """Remove all artifacts for one zone.

        Args:
            zone: The zone whose artifacts to remove.

        Thread safety:
            @thread_safety safe — the artifact store is internally synchronized.
        """
        rc = int(
            self._bridge.lib("sim").kith_sim_remove_zone(
                self._handle,
                ctypes.c_uint32(zone),
            )
        )
        check_error(rc, "kith_sim_remove_zone")

    # -----------------------------------------------------------------------
    # read side
    # -----------------------------------------------------------------------

    def artifact_count(self) -> int:
        """Return the total number of artifacts currently retained.

        Thread safety:
            @thread_safety safe — the artifact store is internally synchronized.
        """
        return int(self._bridge.lib("sim").kith_sim_artifact_count(self._handle))

    def snapshot_cell(self, key: ArtifactKey, *, capacity: int = 64) -> list[Artifact]:
        """Return all artifacts in one cell.

        Order is unspecified but deterministic for a given sequence of
        store mutations; callers that require a specific order sort the
        returned list. When the cell holds more than ``capacity``
        artifacts, the buffer grows to the required size reported by the
        call and the snapshot is retried, so the returned list is complete.

        Args:
            key: The cell to snapshot.
            capacity: Initial buffer size for the returned artifacts.

        Thread safety:
            @thread_safety safe — the artifact store is internally
            synchronized.
        """
        c_key = _key_to_c(key)
        lib = self._bridge.lib("sim")
        size = int(capacity)
        buf = (gen_sim.kith_sim_artifact_t * size)()
        out_count = ctypes.c_size_t(0)
        rc = int(
            lib.kith_sim_snapshot_cell(
                self._handle,
                ctypes.byref(c_key),
                buf,
                ctypes.c_size_t(size),
                ctypes.byref(out_count),
            )
        )
        for _ in range(_SNAPSHOT_GROW_ATTEMPTS):
            if rc != -_ERANGE:
                break
            size = int(out_count.value)
            buf = (gen_sim.kith_sim_artifact_t * size)()
            rc = int(
                lib.kith_sim_snapshot_cell(
                    self._handle,
                    ctypes.byref(c_key),
                    buf,
                    ctypes.c_size_t(size),
                    ctypes.byref(out_count),
                )
            )
        check_error(rc, f"kith_sim_snapshot_cell (required size {size})")
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
            @thread_safety safe — the artifact store is internally synchronized.
        """
        buf = (gen_sim.kith_sim_cell_product_t * capacity)()
        out_count = ctypes.c_size_t(0)
        rc = int(
            self._bridge.lib("sim").kith_sim_snapshot_zone_cells(
                self._handle,
                ctypes.c_uint32(zone),
                ctypes.c_uint8(lod),
                buf,
                ctypes.c_size_t(capacity),
                ctypes.byref(out_count),
            )
        )
        check_error(rc, "kith_sim_snapshot_zone_cells")
        return [_cell_product_from_c(buf[i]) for i in range(int(out_count.value))]

    def cell_product(self, key: ArtifactKey) -> CellProduct | None:
        """Return the product header for one cell, or ``None`` when it is empty.

        Args:
            key: The cell whose product header to return.

        Raises:
            KithError: On a non-ENOENT failure.

        Thread safety:
            @thread_safety safe — the artifact store is internally synchronized.
        """
        c_key = _key_to_c(key)
        out = gen_sim.kith_sim_cell_product_t()
        rc = int(
            self._bridge.lib("sim").kith_sim_cell_product(
                self._handle,
                ctypes.byref(c_key),
                ctypes.byref(out),
            )
        )
        if rc == -_ENOENT:
            return None
        check_error(rc, "kith_sim_cell_product")
        return _cell_product_from_c(out)

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the sim handle. Idempotent.

        Thread safety:
            @thread_safety unsafe — no register/create/publish/snapshot may
            be in flight on the sim when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("sim").kith_sim_destroy(self._handle)
        self._handle = None
        self._closed = True

    def __enter__(self) -> Sim:
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


# ---------------------------------------------------------------------------
# model instance wrapper and value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SimModelConfig:
    """Creation parameters for a sim model instance.

    Fields mirror ``kith_sim_config_t``; the model implementation converts
    them to fixed-point at init. A field left at 0 selects the model's
    default for that field.

    Attributes:
        base_speed: Base movement speed in model units per tick.
        run_speed: Run movement speed (when the run flag is set on an input).
        accel: Acceleration in model units per tick per tick.
        decel: Deceleration in model units per tick per tick.
        move_eps: Minimum movement threshold (below it, no publishable move).
        collision_radius: Collision circle radius in model units.
    """

    base_speed: int = 0
    run_speed: int = 0
    accel: int = 0
    decel: int = 0
    move_eps: float = 0.0
    collision_radius: int = 0


@dataclass
class Actor:
    """One actor's live simulation state, stepped in place by a model.

    Positions and velocities are Q16.16 fixed-point (``int64_t`` with 16
    fractional bits), matching the C ``kith_sim_actor_t`` representation.

    Attributes:
        id: Opaque actor identifier (caller-supplied, treated as a key).
        pos_x, pos_y, pos_z: Position in Q16.16 fixed-point.
        vel_x, vel_y, vel_z: Velocity in Q16.16 fixed-point (0 on Z for 2D).
        input_tick: Last input tick applied to this actor.
        flags: Movement flags (bit 0 = run; model-specific bits start at 8).
        update_seq: Publisher-minted monotone movement-application counter;
            models increment it once per applied movement input (0 = never
            minted). Copied verbatim into published artifacts.
    """

    id: int
    pos_x: int = 0
    pos_y: int = 0
    pos_z: int = 0
    vel_x: int = 0
    vel_y: int = 0
    vel_z: int = 0
    input_tick: int = 0
    flags: int = 0
    update_seq: int = 0


@dataclass(frozen=True)
class SimInput:
    """One movement input applied to one actor.

    Attributes:
        input_tick: Monotonic input tick number for this actor.
        move_x, move_y, move_z: Normalized move component [-32767, 32767].
        flags: Input flags (bit 0 = run).
    """

    input_tick: int = 0
    move_x: int = 0
    move_y: int = 0
    move_z: int = 0
    flags: int = 0


def _model_config_to_c(cfg: SimModelConfig) -> gen_sim.kith_sim_config_t:
    return gen_sim.kith_sim_config_t(
        size=ctypes.sizeof(gen_sim.kith_sim_config_t),
        abi_version=gen_version.KITH_ABI_VERSION,
        base_speed=ctypes.c_uint32(cfg.base_speed),
        run_speed=ctypes.c_uint32(cfg.run_speed),
        accel=ctypes.c_uint32(cfg.accel),
        decel=ctypes.c_uint32(cfg.decel),
        move_eps=ctypes.c_float(cfg.move_eps),
        collision_radius=ctypes.c_uint32(cfg.collision_radius),
    )


def _actor_to_c(a: Actor) -> gen_sim.kith_sim_actor_t:
    return gen_sim.kith_sim_actor_t(
        id=ctypes.c_uint64(a.id),
        pos_x=ctypes.c_int64(a.pos_x),
        pos_y=ctypes.c_int64(a.pos_y),
        pos_z=ctypes.c_int64(a.pos_z),
        vel_x=ctypes.c_int64(a.vel_x),
        vel_y=ctypes.c_int64(a.vel_y),
        vel_z=ctypes.c_int64(a.vel_z),
        input_tick=ctypes.c_uint32(a.input_tick),
        flags=ctypes.c_uint32(a.flags),
        update_seq=ctypes.c_uint32(a.update_seq),
    )


def _actor_from_c(a: gen_sim.kith_sim_actor_t) -> Actor:
    return Actor(
        id=int(a.id),
        pos_x=int(a.pos_x),
        pos_y=int(a.pos_y),
        pos_z=int(a.pos_z),
        vel_x=int(a.vel_x),
        vel_y=int(a.vel_y),
        vel_z=int(a.vel_z),
        input_tick=int(a.input_tick),
        flags=int(a.flags),
        update_seq=int(a.update_seq),
    )


def _input_to_c(i: SimInput) -> gen_sim.kith_sim_input_t:
    return gen_sim.kith_sim_input_t(
        input_tick=ctypes.c_uint32(i.input_tick),
        move_x=ctypes.c_int16(i.move_x),
        move_y=ctypes.c_int16(i.move_y),
        move_z=ctypes.c_int16(i.move_z),
        flags=ctypes.c_uint8(i.flags),
    )


class SimModel:
    """A borrowed view over a ``kith_sim_model_t`` instance.

    Exposes the step / apply_input / load_behavior surface the game drives
    each tick.

    Args:
        bridge: A loaded :class:`kith._bridge.Bridge`.
        handle: The ``kith_sim_model_t*`` to view.
        owned: When True, :meth:`close` destroys the handle (the view owns
            it). When False (the facade-borrowed case), close is a no-op.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Borrowed by default: the facade owns the model instance and
        releases it on close, so :meth:`close` is a no-op and a borrowed
        view never frees the facade's model. A view constructed with
        ``owned=True`` owns the handle and :meth:`close` destroys it.
    """

    __slots__ = ("_bridge", "_closed", "_handle", "_owned")

    def __init__(self, bridge: _bridge.Bridge, handle: object, *, owned: bool = False) -> None:
        self._bridge = bridge
        self._handle = handle
        self._owned = owned
        self._closed = False

    @property
    def handle(self) -> object:
        """The underlying ``kith_sim_model_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    def step(self, actors: list[Actor], dt_ms: int) -> list[Actor]:
        """Advance ``actors`` by ``dt_ms`` milliseconds, returning the new state.

        The model modifies the actor array in place; a fresh list of
        :class:`Actor` copies is returned so the caller never shares mutable
        state with the C array.

        Args:
            actors: The actors to advance.
            dt_ms: The simulation timestep in milliseconds.

        Thread safety:
            @thread_safety unsafe — the model is not synchronized.
        """
        if not actors:
            return []
        buf = (_actor_to_c(a) for a in actors)
        arr = (gen_sim.kith_sim_actor_t * len(actors))(*buf)
        rc = int(
            self._bridge.lib("sim").kith_sim_model_step(
                self._handle,
                arr,
                ctypes.c_size_t(len(actors)),
                ctypes.c_uint32(dt_ms),
            )
        )
        check_error(rc, "kith_sim_model_step")
        return [_actor_from_c(arr[i]) for i in range(len(actors))]

    def apply_input(self, actor: Actor, inp: SimInput) -> Actor:
        """Apply one input to one actor, returning the updated actor.

        Args:
            actor: The actor to update.
            inp: The input to apply.

        Thread safety:
            @thread_safety unsafe — the model is not synchronized.
        """
        c_actor = _actor_to_c(actor)
        c_input = _input_to_c(inp)
        rc = int(
            self._bridge.lib("sim").kith_sim_model_apply_input(
                self._handle,
                ctypes.byref(c_actor),
                ctypes.byref(c_input),
            )
        )
        check_error(rc, "kith_sim_model_apply_input")
        return _actor_from_c(c_actor)

    def load_behavior(self, path: str | None) -> None:
        """Load a behavior grid from ``path``, or clear it when ``path`` is None.

        Args:
            path: File path of the behavior grid to load, or ``None`` to
                clear the current grid.

        Thread safety:
            @thread_safety unsafe — no step/apply_input/load_behavior may be
            in flight on the model.
        """
        c_path = path.encode("utf-8") if path is not None else None
        rc = int(self._bridge.lib("sim").kith_sim_model_load_behavior(self._handle, c_path))
        check_error(rc, "kith_sim_model_load_behavior")

    def close(self) -> None:
        """Release the handle when owned; a no-op for a borrowed view.

        A borrowed close leaves the view untouched so the owning facade's
        release still runs when it closes; marking the view closed without
        releasing would make that flip-to-owned release skip the underlying
        handle and leak the model instance.

        Thread safety:
            @thread_safety unsafe — no step/apply_input/load_behavior may be
            in flight on the model when this is called.
        """
        if self._closed:
            return
        if self._owned:
            self._bridge.lib("sim").kith_sim_model_destroy(self._handle)
            self._handle = None
            self._closed = True

    def __enter__(self) -> SimModel:
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

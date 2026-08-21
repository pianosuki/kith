"""ctypes loader for the spatial game library's native movement-apply core.

Wraps ``libspatial_native_core`` (examples/spatial/native_core.c): the C
core owns the actor table, the stripe-locked apply sequences, the per-tick
dirty-cell set with its authority epochs, the identity-gate drop counter,
and the replay record buffer, so the movement path never enters the Python
interpreter. The composition root builds one instance with the server's
borrowed plane handles, registers the pool-dispatched movement handler
on the gateway, and drives the control-plane accessors and the
per-tick flush through this module.

The two ctypes structures below mirror the C definitions in
``examples/spatial/native_core.h`` (game-owned ABI, not framework surface);
every framework type passes through the generated bindings in
sim. A mismatch fails loudly at the first call, not
silently: the core validates ``size`` and ``abi_version`` at create.

Payload and plane-handle lifetimes: the plane handles are borrowed from the
facade and must outlive the core (the composition root closes the core after
the server facade, so the gateway is quiesced before the core is released).
"""

from __future__ import annotations

import ctypes
import os
from pathlib import Path
from typing import Any, Final, cast

from examples._common.replay_format import MoveEvent

from kith import Actor, SimInput
from kith._generated import sim as gen_sim
from kith._generated import version as gen_version
from kith.exceptions import check_error


__all__ = ["SpatialNative", "load_lib"]


# Must mirror struct spatial_move_record in native_core.h (26 payload bytes
# padded to 32 by 8-byte alignment).
class _MoveRecord(ctypes.Structure):
    """One applied-movement replay record (the C core's v1 MoveEvent echo)."""

    _fields_ = (
        ("actor_id", ctypes.c_uint64),
        ("input_tick", ctypes.c_uint32),
        ("move_x", ctypes.c_int16),
        ("move_y", ctypes.c_int16),
        ("move_z", ctypes.c_int16),
        ("flags", ctypes.c_uint8),
        ("pad", ctypes.c_uint8 * 7),
    )


# The C struct is 26 payload bytes padded to 32 (8-byte alignment).
assert ctypes.sizeof(_MoveRecord) == 32


# Must mirror struct spatial_native_params in native_core.h.
class _Params(ctypes.Structure):
    """Creation parameters for the native core (mirrors native_core.h)."""

    _fields_ = (
        ("size", ctypes.c_uint32),
        ("abi_version", ctypes.c_uint32),
        ("zone_id", ctypes.c_uint32),
        ("cell_radius", ctypes.c_uint32),
        ("cell_size_q16", ctypes.c_uint64),
        ("max_actors", ctypes.c_uint32),
        ("sim", ctypes.c_void_p),
        ("model", ctypes.c_void_p),
        ("fabric", ctypes.c_void_p),
        ("reserved", ctypes.c_void_p * 4),
    )


_LIB_NAME: Final[str] = "libspatial_native_core.so"


def _locate_lib() -> Path:
    """Resolve the built core's path.

    Searches ``$KITH_SPATIAL_NATIVE``, then ``$KITH_BUILD_DIR/examples``,
    then the repo's release and debug preset build dirs, in that order.
    """
    env = os.environ.get("KITH_SPATIAL_NATIVE", "").strip()
    if env:
        path = Path(env)
        if not path.is_file():
            raise FileNotFoundError(f"KITH_SPATIAL_NATIVE names no file: {env}")
        return path
    candidates: list[Path] = []
    build_dir = Path(__file__).resolve().parents[2] / "build"
    if build_env := os.environ.get("KITH_BUILD_DIR", ""):
        candidates.append(Path(build_env) / "examples" / _LIB_NAME)
    for preset in ("release", "debug"):
        candidates.append(build_dir / preset / "examples" / _LIB_NAME)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "libspatial_native_core not found; build it with 'cmake --build <build-dir> "
        "--target spatial_native_core' or set KITH_SPATIAL_NATIVE"
    )


def load_lib() -> ctypes.CDLL:
    """Load the native core shared library and bind its surface."""
    lib = ctypes.CDLL(str(_locate_lib()))
    lib.spatial_native_create.argtypes = (ctypes.POINTER(_Params), ctypes.c_void_p)
    lib.spatial_native_create.restype = ctypes.c_int
    lib.spatial_native_destroy.argtypes = (ctypes.c_void_p,)
    lib.spatial_native_destroy.restype = None
    lib.spatial_native_register_movement_handler.argtypes = (
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_uint16,
    )
    lib.spatial_native_register_movement_handler.restype = ctypes.c_int
    lib.spatial_native_actor_insert.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(gen_sim.kith_sim_actor_t),
    )
    lib.spatial_native_actor_insert.restype = ctypes.c_int
    lib.spatial_native_actor_teleport.argtypes = (
        ctypes.c_void_p,
        ctypes.c_uint64,
        ctypes.c_int64,
        ctypes.c_int64,
        ctypes.c_int64,
        ctypes.POINTER(gen_sim.kith_sim_actor_t),
    )
    lib.spatial_native_actor_teleport.restype = ctypes.c_int
    lib.spatial_native_actor_get.argtypes = (
        ctypes.c_void_p,
        ctypes.c_uint64,
        ctypes.POINTER(gen_sim.kith_sim_actor_t),
    )
    lib.spatial_native_actor_get.restype = ctypes.c_int
    lib.spatial_native_actor_snapshot.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(gen_sim.kith_sim_actor_t),
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
    )
    lib.spatial_native_actor_snapshot.restype = ctypes.c_int
    lib.spatial_native_apply.argtypes = (
        ctypes.c_void_p,
        ctypes.c_uint64,
        ctypes.POINTER(gen_sim.kith_sim_input_t),
        ctypes.c_uint32,
        ctypes.POINTER(gen_sim.kith_sim_actor_t),
    )
    lib.spatial_native_apply.restype = ctypes.c_int
    lib.spatial_native_mark_actor_cell_dirty.argtypes = (ctypes.c_void_p, ctypes.c_uint64)
    lib.spatial_native_mark_actor_cell_dirty.restype = ctypes.c_int
    lib.spatial_native_gate_drops.argtypes = (ctypes.c_void_p,)
    lib.spatial_native_gate_drops.restype = ctypes.c_uint64
    lib.spatial_native_flush.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(_MoveRecord),
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.POINTER(ctypes.c_uint64),
    )
    lib.spatial_native_flush.restype = ctypes.c_int
    return lib


def _actor_to_c(actor: Actor) -> gen_sim.kith_sim_actor_t:
    return gen_sim.kith_sim_actor_t(
        id=actor.id,
        pos_x=actor.pos_x,
        pos_y=actor.pos_y,
        pos_z=actor.pos_z,
        vel_x=actor.vel_x,
        vel_y=actor.vel_y,
        vel_z=actor.vel_z,
        input_tick=actor.input_tick,
        flags=actor.flags,
        update_seq=actor.update_seq,
    )


def _actor_from_c(c: gen_sim.kith_sim_actor_t) -> Actor:
    return Actor(
        id=int(c.id),
        pos_x=int(c.pos_x),
        pos_y=int(c.pos_y),
        pos_z=int(c.pos_z),
        vel_x=int(c.vel_x),
        vel_y=int(c.vel_y),
        vel_z=int(c.vel_z),
        input_tick=int(c.input_tick),
        flags=int(c.flags),
        update_seq=int(c.update_seq),
    )


def _as_void_p(handle: object) -> ctypes.c_void_p:
    """Normalize a borrowed ctypes pointer (or int) to ``c_void_p``."""
    if isinstance(handle, ctypes.c_void_p):
        return handle
    if isinstance(handle, int):
        return ctypes.c_void_p(handle)
    cdata = cast("ctypes._Pointer[Any]", handle)
    return ctypes.c_void_p(ctypes.cast(cdata, ctypes.c_void_p).value)


class SpatialNative:
    """The native movement-apply core, loaded and wired for one server.

    Owns the C core handle; every method is a thin ctypes crossing over the
    surface in ``native_core.h``. The movement wire handler registers on the
    gateway with the pool-dispatched flag and runs entirely in C;
    this class carries the control-plane surface (spawn/teleport/query) and
    the per-tick flush that drains the dirty-cell set and the replay record
    buffer.

    Args:
        sim: Borrowed ``kith_sim_t*`` from the facade.
        model: The sim model the apply sequence drives through its vtable.
        fabric: Borrowed ``kith_fabric_t*`` the flush publishes into.
        zone_id: Zone id every actor publishes into.
        cell_size: Cell span per axis in Q16.16 (the Python geometry's
            ``floor(pos / cell_size)``).
        cell_radius: Chebyshev radius of the crossing window diff.
        max_actors: Actor-table capacity; ids are bounded by this.

    Raises:
        KithError: When the core rejects the create parameters.
    """

    # A full tick at the gate point drains ~420 records; the drain buffer
    # holds an order of magnitude more so the loop below finishes in one
    # call at every honest operating point.
    _FLUSH_CAP: Final[int] = 8192

    def __init__(
        self,
        *,
        sim: object,
        model: object,
        fabric: object,
        zone_id: int,
        cell_size: int,
        cell_radius: int,
        max_actors: int,
    ) -> None:
        self._lib = load_lib()
        params = _Params(
            size=ctypes.sizeof(_Params),
            abi_version=gen_version.KITH_ABI_VERSION,
            zone_id=zone_id,
            cell_radius=cell_radius,
            cell_size_q16=cell_size,
            max_actors=max_actors,
            sim=_as_void_p(sim),
            model=_as_void_p(model),
            fabric=_as_void_p(fabric),
        )
        out = ctypes.c_void_p()
        rc = int(self._lib.spatial_native_create(ctypes.byref(params), ctypes.byref(out)))
        check_error(rc, "spatial_native_create")
        self._handle: ctypes.c_void_p | None = out
        self._max_actors = max_actors
        self._buffer = (_MoveRecord * self._FLUSH_CAP)()

    def register_movement_handler(self, gateway: object, msg_type: int) -> None:
        """Register the pool-dispatched movement handler on the gateway.

        Args:
            gateway: The server's borrowed gateway handle.
            msg_type: The wire message type the C handler owns.
        """
        rc = int(
            self._lib.spatial_native_register_movement_handler(
                self._handle, _as_void_p(gateway), msg_type
            )
        )
        check_error(rc, "spatial_native_register_movement_handler")

    def actor_insert(self, actor: Actor) -> None:
        """Insert a freshly allocated actor and publish its initial state."""
        c_actor = _actor_to_c(actor)
        rc = int(self._lib.spatial_native_actor_insert(self._handle, ctypes.byref(c_actor)))
        check_error(rc, "spatial_native_actor_insert")

    def actor_teleport(self, actor_id: int, pos_x: int, pos_y: int, pos_z: int) -> Actor | None:
        """Reposition one actor and return the updated state, or ``None``."""
        out = gen_sim.kith_sim_actor_t()
        rc = int(
            self._lib.spatial_native_actor_teleport(
                self._handle,
                ctypes.c_uint64(actor_id),
                ctypes.c_int64(pos_x),
                ctypes.c_int64(pos_y),
                ctypes.c_int64(pos_z),
                ctypes.byref(out),
            )
        )
        if rc == -1:
            return None
        check_error(rc, "spatial_native_actor_teleport")
        return _actor_from_c(out)

    def actor_get(self, actor_id: int) -> Actor | None:
        """Return one actor's live state, or ``None`` when absent."""
        out = gen_sim.kith_sim_actor_t()
        rc = int(self._lib.spatial_native_actor_get(self._handle, actor_id, ctypes.byref(out)))
        if rc == -1:
            return None
        check_error(rc, "spatial_native_actor_get")
        return _actor_from_c(out)

    def actor_snapshot(self) -> list[Actor]:
        """Return every inserted actor, quiesced against applies."""
        snapshot = (gen_sim.kith_sim_actor_t * self._max_actors)()
        count = ctypes.c_uint32(0)
        check_error(
            int(
                self._lib.spatial_native_actor_snapshot(
                    self._handle, snapshot, self._max_actors, ctypes.byref(count)
                )
            ),
            "spatial_native_actor_snapshot",
        )
        return [_actor_from_c(snapshot[i]) for i in range(count.value)]

    def apply(self, actor_id: int, inp: SimInput, *, dt_ms: int = 50) -> Actor | None:
        """Apply one movement input on the control path (no gate, no diff)."""
        c_input = gen_sim.kith_sim_input_t(
            input_tick=inp.input_tick,
            move_x=inp.move_x,
            move_y=inp.move_y,
            move_z=inp.move_z,
            flags=inp.flags,
        )
        out = gen_sim.kith_sim_actor_t()
        rc = int(
            self._lib.spatial_native_apply(
                self._handle, actor_id, ctypes.byref(c_input), dt_ms, ctypes.byref(out)
            )
        )
        if rc == -1:
            return None
        check_error(rc, "spatial_native_apply")
        return _actor_from_c(out)

    def mark_actor_cell_dirty(self, actor_id: int) -> bool:
        """Mark the actor's current cell dirty for the next flush."""
        rc = int(self._lib.spatial_native_mark_actor_cell_dirty(self._handle, actor_id))
        if rc == -1:
            return False
        check_error(rc, "spatial_native_mark_actor_cell_dirty")
        return True

    @property
    def gate_drops(self) -> int:
        """Movement frames dropped by the identity gate."""
        return int(self._lib.spatial_native_gate_drops(self._handle))

    def flush(self) -> list[MoveEvent]:
        """Drain one per-tick flush: dirty cells, then buffered records.

        The dirty cells are bumped exactly once per tick (stale-epoch
        publishes suppressed and counted inside the core); the buffered
        replay records return in apply-completion order. The drain is a
        snapshot: records appended after it wait for the next tick, so this
        call is bounded. Returns the drained records in replay order.
        """
        bumped = ctypes.c_uint64(0)
        eperm = ctypes.c_uint64(0)
        events: list[MoveEvent] = []
        more = ctypes.c_uint32(1)
        while more.value:
            count = ctypes.c_uint32(0)
            more = ctypes.c_uint32(0)
            rc = int(
                self._lib.spatial_native_flush(
                    self._handle,
                    ctypes.cast(self._buffer, ctypes.POINTER(_MoveRecord)),
                    self._FLUSH_CAP,
                    ctypes.byref(count),
                    ctypes.byref(more),
                    ctypes.byref(bumped),
                    ctypes.byref(eperm),
                )
            )
            check_error(rc, "spatial_native_flush")
            for i in range(count.value):
                rec = self._buffer[i]
                events.append(
                    MoveEvent(
                        actor_id=int(rec.actor_id),
                        input_tick=int(rec.input_tick),
                        move_x=int(rec.move_x),
                        move_y=int(rec.move_y),
                        move_z=int(rec.move_z),
                        flags=int(rec.flags),
                    )
                )
        return events

    def close(self) -> None:
        """Destroy the C core; idempotent."""
        if self._handle is not None:
            self._lib.spatial_native_destroy(self._handle)
            self._handle = None

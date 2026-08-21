#!/usr/bin/env python3
"""Deterministic simulation replay and record-verification tool.

Drives the kith simulation shared library through the same public ABI a
composition root uses, applying a recorded input stream and printing a
rolling FNV-1a hash of the live actor set. A second run on the same record
yields the same hash sequence; a hash that differs from a pinned baseline
flags a simulation-behavior regression without reasoning about the internals.

Inputs come in two encodings, distinguished by sniffing the leading bytes:

- Binary (``docs/guides/replay_format.md``): the compact versioned container
  produced by the recording call sites — per-tick records, pinned hash
  expectations, and generator checkpoints. Preferred.
- Text (legacy, kept for old artifacts): one whitespace-separated event per
  line (``<tick> SPAWN|MOVE|DESPAWN ...``); blank lines and ``#`` comments
  are ignored. Positions and velocities are raw Q16.16 int64 values; move
  components are normalized to [-32767, 32767].

Modes (the first argument selects one; omitting it means ``play``, which
keeps historical invocations working):

- ``play`` — replay a record and print ``tick=N hash=HEX`` lines; optional
  ``--expect-hash``/``--expect-file`` compare against pinned values.
- ``verify`` — replay plus expectation checking: explicit CLI expectations
  override the record's embedded EXPECTED_HASH records; with neither source
  the mode refuses to run, and so does a non-positive ``--hash-every``
  (no hash could ever be emitted to compare against). Exit status reports
  the verdict.
- ``diff`` — logical comparison of two records (any encoding on either
  side): per-tick event sequences, generator checkpoints, metadata, and
  pinned expectations. No simulation library needed.
- ``transcode`` — rewrite a legacy text record as a binary artifact,
  optionally embedding pinned expectations from a ``tick=N hash=HEX``
  sidecar produced by a prior ``play`` run.

Events are applied at their recorded tick in stored order; at each tick the
due events are applied, then every live actor is stepped once at the
configured tick rate. The record's tick rate is authoritative for binary
inputs; ``--tick-hz`` overrides only when passed explicitly.

Exit status: 0 when the requested comparison succeeds, 1 on a mismatch or
replay error, 2 on a usage or parse error.
"""

from __future__ import annotations

import argparse
import ctypes
import os
import struct
import sys
from collections.abc import Sequence
from itertools import groupby
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from examples._common.replay_format import (  # noqa: E402 — path bootstrap above
    DespawnEvent,
    ExpectedHash,
    MoveEvent,
    ReplayDocument,
    ReplayFormatError,
    ReplayHeader,
    SpawnEvent,
    TickRecord,
    diff_documents,
    encode_document,
    read_document,
    sniff_is_binary,
)


KITH_ABI_VERSION: int = 1
FIX_SHIFT: int = 16
DEFAULT_TICK_HZ: int = 20
MAX_ACTORS: int = 1 << 16
MAX_REPORTED_MISSING: int = 8

LIB_BASENAME = "libkith_sim.so.1"

FNV_OFFSET: int = 0xCBF29CE484222325
FNV_PRIME: int = 0x100000001B3
U64_MASK: int = 0xFFFFFFFFFFFFFFFF

_COMMANDS = ("play", "verify", "diff", "transcode")


class KithSimError(Exception):
    """Raised when a kith simulation call returns a negative error code."""


class RecordError(Exception):
    """Raised on a malformed record line."""


class SimParams(ctypes.Structure):
    """kith_sim_params_t (size-versioned). Defaults are selected by zero."""

    _fields_ = [
        ("size", ctypes.c_uint32),
        ("abi_version", ctypes.c_uint32),
        ("tick_hz", ctypes.c_uint32),
        ("artifact_bucket_count", ctypes.c_uint32),
        ("reserved", ctypes.c_void_p * 8),
    ]


class SimConfig(ctypes.Structure):
    """kith_sim_config_t (model physics constants)."""

    _fields_ = [
        ("size", ctypes.c_uint32),
        ("abi_version", ctypes.c_uint32),
        ("base_speed", ctypes.c_uint32),
        ("run_speed", ctypes.c_uint32),
        ("accel", ctypes.c_uint32),
        ("decel", ctypes.c_uint32),
        ("move_eps", ctypes.c_float),
        ("collision_radius", ctypes.c_uint32),
        ("reserved", ctypes.c_void_p * 8),
    ]


class SimActor(ctypes.Structure):
    """kith_sim_actor_t. Positions and velocities are Q16.16 int64."""

    _fields_ = [
        ("id", ctypes.c_uint64),
        ("pos_x", ctypes.c_int64),
        ("pos_y", ctypes.c_int64),
        ("pos_z", ctypes.c_int64),
        ("vel_x", ctypes.c_int64),
        ("vel_y", ctypes.c_int64),
        ("vel_z", ctypes.c_int64),
        ("input_tick", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
    ]


class SimInput(ctypes.Structure):
    """kith_sim_input_t. Move components are normalized to [-32767, 32767]."""

    _fields_ = [
        ("input_tick", ctypes.c_uint32),
        ("move_x", ctypes.c_int16),
        ("move_y", ctypes.c_int16),
        ("move_z", ctypes.c_int16),
        ("flags", ctypes.c_uint8),
    ]


def find_sim_lib(explicit: str | None) -> Path:
    """Locate libkith_sim.so.1 by explicit path, env var, or build tree walk."""
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise KithSimError(f"library not found: {explicit}")
        return path

    env = os.environ.get("KITH_SIM_LIB")
    if env:
        path = Path(env)
        if not path.is_file():
            raise KithSimError(f"library not found: {env}")
        return path

    candidates = [
        _REPO_ROOT / "build" / "debug" / LIB_BASENAME,
        _REPO_ROOT / "build" / "release" / LIB_BASENAME,
        _REPO_ROOT / "cmake-build-debug" / LIB_BASENAME,
        _REPO_ROOT / "cmake-build-release" / LIB_BASENAME,
    ]
    for cand in candidates:
        if cand.is_file():
            return cand

    system = ctypes.CDLL(LIB_BASENAME)
    if system:
        return Path(LIB_BASENAME)

    raise KithSimError(f"could not locate {LIB_BASENAME}")


def load_sim_bindings(path: Path) -> ctypes.CDLL:
    """Load libkith_sim and declare argtypes/restype for the called symbols."""
    lib = ctypes.CDLL(str(path))

    lib.kith_sim_create.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.kith_sim_create.restype = ctypes.c_int

    lib.kith_sim_destroy.argtypes = [ctypes.c_void_p]
    lib.kith_sim_destroy.restype = None

    lib.kith_sim_create_model.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.kith_sim_create_model.restype = ctypes.c_int

    lib.kith_sim_model_destroy.argtypes = [ctypes.c_void_p]
    lib.kith_sim_model_destroy.restype = None

    lib.kith_sim_model_step.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(SimActor),
        ctypes.c_size_t,
        ctypes.c_uint32,
    ]
    lib.kith_sim_model_step.restype = ctypes.c_int

    lib.kith_sim_model_apply_input.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(SimActor),
        ctypes.POINTER(SimInput),
    ]
    lib.kith_sim_model_apply_input.restype = ctypes.c_int

    lib.kith_sim_model_load_behavior.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.kith_sim_model_load_behavior.restype = ctypes.c_int

    return lib


def check_rc(rc: int, what: str) -> int:
    """Assert a kith call returned non-negative; raise on a negative error code."""
    if rc < 0:
        raise KithSimError(f"{what} failed: kith error {-rc}")
    return rc


def create_sim(lib: ctypes.CDLL) -> ctypes.c_void_p:
    """Create a sim handle with default params (built-in models registered)."""
    handle = ctypes.c_void_p()
    rc = check_rc(lib.kith_sim_create(None, None, ctypes.byref(handle)), "kith_sim_create")
    if rc != 0 or not handle.value:
        raise KithSimError("kith_sim_create returned a null handle")
    return handle


def destroy_sim(lib: ctypes.CDLL, handle: ctypes.c_void_p) -> None:
    """Destroy a sim handle (NULL-safe)."""
    lib.kith_sim_destroy(handle)


def create_model(
    lib: ctypes.CDLL,
    sim: ctypes.c_void_p,
    name: str,
    cfg: SimConfig | None,
) -> ctypes.c_void_p:
    """Instantiate a built-in model by name."""
    handle = ctypes.c_void_p()
    cfg_ptr = ctypes.byref(cfg) if cfg is not None else None
    rc = check_rc(
        lib.kith_sim_create_model(sim, name.encode(), cfg_ptr, None, ctypes.byref(handle)),
        "kith_sim_create_model",
    )
    if rc != 0 or not handle.value:
        raise KithSimError(f"kith_sim_create_model({name!r}) returned a null handle")
    return handle


def build_config(args: argparse.Namespace) -> SimConfig | None:
    """Build a SimConfig when any physics flag is set; otherwise None (defaults)."""
    if not any(
        (
            args.base_speed,
            args.run_speed,
            args.accel,
            args.decel,
            args.collision_radius is not None,
            args.move_eps is not None,
        )
    ):
        return None
    cfg = SimConfig()
    cfg.size = ctypes.sizeof(SimConfig)
    cfg.abi_version = KITH_ABI_VERSION
    cfg.base_speed = args.base_speed
    cfg.run_speed = args.run_speed
    cfg.accel = args.accel
    cfg.decel = args.decel
    cfg.move_eps = args.move_eps if args.move_eps is not None else 0.0
    cfg.collision_radius = args.collision_radius if args.collision_radius is not None else 0
    return cfg


def parse_int(token: str, what: str, lo: int, hi: int) -> int:
    """Parse a signed integer token within [lo, hi]."""
    try:
        value = int(token)
    except ValueError as exc:
        raise RecordError(f"{what}: expected integer, got {token!r}") from exc
    if value < lo or value > hi:
        raise RecordError(f"{what}: {value} out of range [{lo}, {hi}]")
    return value


def parse_record(path: Path) -> list[tuple[int, SpawnEvent | MoveEvent | DespawnEvent]]:
    """Parse a legacy text record into ``(tick, event)`` pairs in file order."""
    pairs: list[tuple[int, SpawnEvent | MoveEvent | DespawnEvent]] = []
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        tokens = line.split()
        try:
            pairs.append(parse_event(tokens))
        except RecordError as exc:
            raise RecordError(f"{path}:{lineno}: {exc}") from exc
    return pairs


def parse_event(tokens: Sequence[str]) -> tuple[int, SpawnEvent | MoveEvent | DespawnEvent]:
    """Parse one whitespace-split event line into a ``(tick, event)`` pair."""
    if len(tokens) < 3:
        raise RecordError("expected at least tick, kind, actor_id")
    tick = parse_int(tokens[0], "tick", 0, 2**63 - 1)
    kind = tokens[1]
    actor_id = parse_int(tokens[2], "actor_id", 0, 2**64 - 1)

    if kind == "SPAWN":
        if len(tokens) != 10:
            raise RecordError("SPAWN needs pos_x pos_y pos_z vel_x vel_y vel_z flags")
        event: SpawnEvent | MoveEvent | DespawnEvent = SpawnEvent(
            actor_id=actor_id,
            pos_x=parse_int(tokens[3], "pos_x", -(2**63), 2**63 - 1),
            pos_y=parse_int(tokens[4], "pos_y", -(2**63), 2**63 - 1),
            pos_z=parse_int(tokens[5], "pos_z", -(2**63), 2**63 - 1),
            vel_x=parse_int(tokens[6], "vel_x", -(2**63), 2**63 - 1),
            vel_y=parse_int(tokens[7], "vel_y", -(2**63), 2**63 - 1),
            vel_z=parse_int(tokens[8], "vel_z", -(2**63), 2**63 - 1),
            flags=parse_int(tokens[9], "flags", 0, 2**32 - 1),
        )
        return tick, event
    if kind == "MOVE":
        if len(tokens) != 8:
            raise RecordError("MOVE needs input_tick move_x move_y move_z flags")
        return tick, MoveEvent(
            actor_id=actor_id,
            input_tick=parse_int(tokens[3], "input_tick", 0, 2**32 - 1),
            move_x=parse_int(tokens[4], "move_x", -(2**15), 2**15 - 1),
            move_y=parse_int(tokens[5], "move_y", -(2**15), 2**15 - 1),
            move_z=parse_int(tokens[6], "move_z", -(2**15), 2**15 - 1),
            flags=parse_int(tokens[7], "flags", 0, 2**8 - 1),
        )
    if kind == "DESPAWN":
        if len(tokens) != 3:
            raise RecordError("DESPAWN takes only tick and actor_id")
        return tick, DespawnEvent(actor_id=actor_id)
    raise RecordError(f"unknown event kind: {kind!r}")


def text_groups(
    pairs: Sequence[tuple[int, SpawnEvent | MoveEvent | DespawnEvent]],
) -> list[TickRecord]:
    """Group ``(tick, event)`` pairs into per-tick records.

    The sort is stable, so file order breaks ties within a tick exactly as
    the legacy reader always applied them.
    """
    ordered = sorted(pairs, key=lambda pair: pair[0])
    return [
        TickRecord(tick, tuple(event for _, event in group))
        for tick, group in groupby(ordered, key=lambda pair: pair[0])
    ]


class ActorStore:
    """Dense actor array plus an id->slot map, mirroring the C artifact store."""

    def __init__(self) -> None:
        self.buf: ctypes.Array[SimActor] = (SimActor * MAX_ACTORS)()
        self.slot: dict[int, int] = {}
        self.live: int = 0

    def spawn(self, event: SpawnEvent) -> None:
        """Append an actor; reject duplicates."""
        if event.actor_id in self.slot:
            raise RecordError(f"SPAWN: actor {event.actor_id} already exists")
        if self.live >= MAX_ACTORS:
            raise KithSimError(f"actor capacity {MAX_ACTORS} exceeded")
        idx = self.live
        node = self.buf[idx]
        node.id = event.actor_id
        node.pos_x = event.pos_x
        node.pos_y = event.pos_y
        node.pos_z = event.pos_z
        node.vel_x = event.vel_x
        node.vel_y = event.vel_y
        node.vel_z = event.vel_z
        node.input_tick = 0
        node.flags = event.flags
        self.slot[event.actor_id] = idx
        self.live += 1

    def apply_move(self, lib: ctypes.CDLL, model: ctypes.c_void_p, event: MoveEvent) -> None:
        """Apply one input to a live actor via the model ABI."""
        idx = self.slot.get(event.actor_id)
        if idx is None:
            raise RecordError(f"MOVE: actor {event.actor_id} not spawned")
        inp = SimInput()
        inp.input_tick = event.input_tick
        inp.move_x = event.move_x
        inp.move_y = event.move_y
        inp.move_z = event.move_z
        inp.flags = event.flags
        check_rc(
            lib.kith_sim_model_apply_input(model, ctypes.byref(self.buf[idx]), ctypes.byref(inp)),
            "kith_sim_model_apply_input",
        )

    def despawn(self, event: DespawnEvent) -> None:
        """Remove an actor, swapping the last live actor into its slot."""
        idx = self.slot.pop(event.actor_id, None)
        if idx is None:
            return
        last = self.live - 1
        if idx != last:
            self.buf[idx] = self.buf[last]
            last_id = self.buf[idx].id
            self.slot[int(last_id)] = idx
        self.live = last

    def step(self, lib: ctypes.CDLL, model: ctypes.c_void_p, dt_ms: int) -> None:
        """Step every live actor by dt_ms milliseconds."""
        if self.live == 0:
            return
        check_rc(
            lib.kith_sim_model_step(model, self.buf, self.live, dt_ms),
            "kith_sim_model_step",
        )

    def hash(self) -> int:
        """FNV-1a 64 over the actor set sorted by id (big-endian, two's complement)."""
        ordered = sorted(self.slot, key=lambda aid: (aid,))
        h = FNV_OFFSET
        h = _fnv1a_update(h, struct.pack(">Q", len(ordered)))
        for aid in ordered:
            node = self.buf[self.slot[aid]]
            h = _fnv1a_update(
                h,
                struct.pack(
                    ">QqqqqqqII",
                    aid,
                    node.pos_x,
                    node.pos_y,
                    node.pos_z,
                    node.vel_x,
                    node.vel_y,
                    node.vel_z,
                    node.input_tick,
                    node.flags,
                ),
            )
        return h & U64_MASK


def _fnv1a_update(hash_val: int, data: bytes) -> int:
    """Fold a byte string into an FNV-1a 64 accumulator."""
    for b in data:
        hash_val ^= b
        hash_val = (hash_val * FNV_PRIME) & U64_MASK
    return hash_val


def load_record(path: Path) -> tuple[list[TickRecord], ReplayDocument | None]:
    """Load a record in either encoding.

    Returns the per-tick groups plus the parsed binary document when the
    input was binary (None for legacy text, which carries no header).
    """
    data = path.read_bytes()
    if sniff_is_binary(data):
        document = read_document(data, source=str(path))
        return list(document.ticks), document
    return text_groups(parse_record(path)), None


def resolve_tick_hz(args: argparse.Namespace, document: ReplayDocument | None) -> int:
    """Resolve the tick cadence: an explicit flag wins, else the record header."""
    if args.tick_hz is not None:
        return int(args.tick_hz)
    if document is not None:
        return document.header.tick_hz
    return DEFAULT_TICK_HZ


def replay(
    lib: ctypes.CDLL,
    groups: Sequence[TickRecord],
    args: argparse.Namespace,
    tick_hz: int,
) -> list[tuple[int, int]]:
    """Replay the record's events and return (tick, hash) pairs at hash ticks."""
    sim = create_sim(lib)
    try:
        cfg = build_config(args)
        model = create_model(lib, sim, args.model, cfg)
        try:
            if args.behavior:
                path_bytes = os.fsencode(args.behavior)
                check_rc(
                    lib.kith_sim_model_load_behavior(model, path_bytes),
                    "kith_sim_model_load_behavior",
                )

            store = ActorStore()
            dt_ms = (1000 + tick_hz // 2) // tick_hz
            max_tick = max((group.tick for group in groups), default=0)
            tick_limit = args.ticks if args.ticks is not None else max_tick + 1

            index = 0
            hashes: list[tuple[int, int]] = []
            for tick in range(tick_limit):
                while index < len(groups) and groups[index].tick == tick:
                    for event in groups[index].events:
                        apply_event(lib, model, store, event)
                    index += 1
                store.step(lib, model, dt_ms)
                if args.hash_every > 0 and tick % args.hash_every == 0:
                    hashes.append((tick, store.hash()))
            return hashes
        finally:
            lib.kith_sim_model_destroy(model)
    finally:
        destroy_sim(lib, sim)


def apply_event(
    lib: ctypes.CDLL,
    model: ctypes.c_void_p,
    store: ActorStore,
    event: SpawnEvent | MoveEvent | DespawnEvent,
) -> None:
    """Dispatch one event to the actor store or the model."""
    if isinstance(event, SpawnEvent):
        store.spawn(event)
    elif isinstance(event, MoveEvent):
        store.apply_move(lib, model, event)
    elif isinstance(event, DespawnEvent):
        store.despawn(event)


def load_expect_file(path: Path) -> dict[int, int]:
    """Load expected ``tick=N hash=HEX`` lines into a tick->hash map."""
    expected: dict[int, int] = {}
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        tokens = line.replace("=", " ").split()
        if len(tokens) != 4 or tokens[0] != "tick" or tokens[2] != "hash":
            raise RecordError(f"{path}:{lineno}: expected 'tick=N hash=HEX'")
        expected[int(tokens[1])] = int(tokens[3], 16)
    return expected


def expectation_map(args: argparse.Namespace, document: ReplayDocument | None) -> dict[int, int]:
    """Resolve the active expectation map: CLI flags override embedded records."""
    if args.expect_file:
        return load_expect_file(Path(args.expect_file))
    if args.expect_hash:
        return {}
    if document is not None and document.expected_hashes:
        return {e.tick: e.hash for e in document.expected_hashes}
    return {}


def print_hashes(hashes: list[tuple[int, int]], quiet: bool) -> None:
    """Emit the rolling ``tick=N hash=HEX`` lines unless suppressed."""
    if quiet:
        return
    for tick, h in hashes:
        print(f"tick={tick} hash={h:016x}")


def compare_against_map(
    hashes: list[tuple[int, int]],
    expected: dict[int, int],
    *,
    require_coverage: bool,
) -> int:
    """Compare emitted hashes against an expectation map; return 0 or 1.

    A tick is only covered when it appears on both sides: an emitted tick
    without an expectation fails when ``require_coverage`` is set, and an
    expected tick that was never emitted fails regardless.
    """
    for tick, h in hashes:
        want = expected.get(tick)
        if want is None:
            if require_coverage:
                print(f"error: no expected hash for tick {tick}", file=sys.stderr)
                return 1
            continue
        if h != want:
            print(
                f"error: tick {tick} hash {h:016x} != expected {want:016x}",
                file=sys.stderr,
            )
            return 1
    missing = sorted(set(expected) - {tick for tick, _ in hashes})
    for tick in missing[:MAX_REPORTED_MISSING]:
        print(
            f"error: expected hash for tick {tick} was never emitted (check --hash-every)",
            file=sys.stderr,
        )
    if len(missing) > MAX_REPORTED_MISSING:
        print(
            f"error: {len(missing) - MAX_REPORTED_MISSING} more expected ticks were never emitted",
            file=sys.stderr,
        )
    return 1 if missing else 0


def run_play_or_verify(args: argparse.Namespace) -> int:
    """Replay the record and report the outcome for the play/verify modes."""
    if args.command == "verify" and args.hash_every <= 0:
        print(
            "error: verify requires a positive --hash-every to emit comparable hashes",
            file=sys.stderr,
        )
        return 2
    groups, document = load_record(args.input)
    if not groups:
        print(f"error: no events in {args.input}", file=sys.stderr)
        return 1

    lib_path = find_sim_lib(args.lib)
    lib = load_sim_bindings(lib_path)
    hashes = replay(lib, groups, args, resolve_tick_hz(args, document))

    if args.command == "play":
        print_hashes(hashes, args.quiet)
        if args.expect_file:
            status = compare_against_map(
                hashes, load_expect_file(Path(args.expect_file)), require_coverage=True
            )
            if status != 0:
                return status
        elif args.expect_hash:
            return _compare_final_hash(hashes, int(args.expect_hash, 16))
        return 0

    expected = expectation_map(args, document)
    if not expected and not args.expect_hash:
        print(
            "error: verify found no expectations (embed EXPECTED_HASH records "
            "or pass --expect-hash/--expect-file)",
            file=sys.stderr,
        )
        return 2
    print_hashes(hashes, args.quiet)
    if args.expect_hash:
        status = _compare_final_hash(hashes, int(args.expect_hash, 16))
        return status
    return compare_against_map(hashes, expected, require_coverage=True)


def _compare_final_hash(hashes: list[tuple[int, int]], want: int) -> int:
    """Compare the final emitted hash against one pinned value."""
    if not hashes:
        print("error: no hashes emitted to compare (check --hash-every)", file=sys.stderr)
        return 1
    got = hashes[-1][1]
    if got != want:
        print(
            f"error: final hash {got:016x} != expected {want:016x}",
            file=sys.stderr,
        )
        return 1
    return 0


def run_diff(args: argparse.Namespace) -> int:
    """Logically compare two records; exit 0 identical, 1 different."""
    left_groups, left_doc = load_record(args.input)
    right_groups, right_doc = load_record(args.input_b)
    left = left_doc or _text_document(left_groups)
    right = right_doc or _text_document(right_groups)
    differences = diff_documents(left, right)
    if not differences:
        if not args.quiet:
            print("records are logically identical")
        return 0
    for line in differences:
        print(line)
    return 1


def _text_document(groups: list[TickRecord]) -> ReplayDocument:
    """Wrap legacy text groups as a headerless document for the differ."""
    return ReplayDocument(
        header=ReplayHeader(tick_hz=DEFAULT_TICK_HZ, flags=0),
        meta=None,
        ticks=tuple(groups),
        expected_hashes=(),
        rng_checkpoints=(),
    )


def run_transcode(args: argparse.Namespace) -> int:
    """Rewrite a legacy text record as a binary artifact."""
    data = args.input.read_bytes()
    if sniff_is_binary(data):
        print(f"error: {args.input} is already binary", file=sys.stderr)
        return 2
    groups = text_groups(parse_record(args.input))
    if not groups:
        print(f"error: no events in {args.input}", file=sys.stderr)
        return 1

    expected: dict[int, int] = {}
    if args.expectations_from:
        expected = load_expect_file(Path(args.expectations_from))

    document = ReplayDocument(
        header=ReplayHeader(tick_hz=args.tick_hz, flags=0),
        meta={"model": args.model},
        ticks=tuple(groups),
        expected_hashes=tuple(ExpectedHash(tick, h) for tick, h in sorted(expected.items())),
        rng_checkpoints=(),
    )
    args.output.write_bytes(encode_document(document))
    if not args.quiet:
        print(
            f"wrote {args.output}: {len(document.ticks)} ticks, "
            f"{len(document.expected_hashes)} expectations"
        )
    return 0


def _add_sim_options(parser: argparse.ArgumentParser) -> None:
    """Register the simulation-driving options shared by play and verify."""
    parser.add_argument("--input", type=Path, required=True, help="record file to replay")
    parser.add_argument(
        "--model",
        default="tile2d",
        choices=["tile2d", "free2d"],
        help="model to instantiate",
    )
    parser.add_argument("--lib", default=None, help="path to libkith_sim.so.1")
    parser.add_argument(
        "--tick-hz",
        type=int,
        default=None,
        help="tick rate override (binary records default to their header)",
    )
    parser.add_argument(
        "--ticks", type=int, default=None, help="tick count (default: max tick + 1)"
    )
    parser.add_argument(
        "--hash-every",
        type=int,
        default=0,
        help="emit a hash every N ticks (verify requires a positive value)",
    )
    parser.add_argument("--behavior", type=Path, default=None, help="tile2d behavior grid path")
    parser.add_argument("--base-speed", type=int, default=0, help="override base_speed")
    parser.add_argument("--run-speed", type=int, default=0, help="override run_speed")
    parser.add_argument("--accel", type=int, default=0, help="override accel")
    parser.add_argument("--decel", type=int, default=0, help="override decel")
    parser.add_argument("--move-eps", type=float, default=None, help="override move_eps")
    parser.add_argument(
        "--collision-radius", type=int, default=None, help="override collision_radius"
    )


def _add_expectation_options(parser: argparse.ArgumentParser) -> None:
    """Register the pinned-expectation options shared by play and verify."""
    parser.add_argument("--expect-hash", default=None, help="expected final hash (hex)")
    parser.add_argument(
        "--expect-file", default=None, help="file of expected 'tick=N hash=HEX' lines"
    )


def build_parser() -> argparse.ArgumentParser:
    """Construct the argparse parser with the four modes."""
    parser = argparse.ArgumentParser(
        description="Deterministic simulation replay, verification, and diff tool.",
    )
    subparsers = parser.add_subparsers(dest="command")

    play = subparsers.add_parser("play", help="replay a record and print rolling hashes")
    _add_sim_options(play)
    _add_expectation_options(play)
    play.add_argument("--quiet", action="store_true", help="suppress hash output")

    verify = subparsers.add_parser(
        "verify", help="replay and check embedded or CLI-pinned expectations"
    )
    _add_sim_options(verify)
    _add_expectation_options(verify)
    verify.add_argument("--quiet", action="store_true", help="suppress hash output")

    diff = subparsers.add_parser("diff", help="logically compare two records")
    diff.add_argument("--input", type=Path, required=True, help="first record")
    diff.add_argument("--input-b", type=Path, required=True, help="second record")
    diff.add_argument("--quiet", action="store_true", help="suppress the identical notice")

    transcode = subparsers.add_parser(
        "transcode", help="rewrite a legacy text record as a binary artifact"
    )
    transcode.add_argument("--input", type=Path, required=True, help="text record to read")
    transcode.add_argument("--output", type=Path, required=True, help="binary record to write")
    transcode.add_argument(
        "--model", default="tile2d", choices=["tile2d", "free2d"], help="model recorded in META"
    )
    transcode.add_argument(
        "--tick-hz", type=int, default=DEFAULT_TICK_HZ, help="tick rate for the header"
    )
    transcode.add_argument(
        "--expectations-from",
        default=None,
        help="sidecar of 'tick=N hash=HEX' lines to embed as EXPECTED_HASH records",
    )
    transcode.add_argument("--quiet", action="store_true", help="suppress the summary line")
    return parser


def main(argv: Sequence[str]) -> int:
    """Dispatch one mode and translate failures to the exit contract."""
    rest = list(argv[1:])
    if rest and rest[0] not in _COMMANDS:
        rest.insert(0, "play")
    elif not rest:
        rest = ["play", "--help"]

    args = build_parser().parse_args(rest)
    if getattr(args, "tick_hz", None) is not None and args.tick_hz <= 0:
        print("error: --tick-hz must be positive", file=sys.stderr)
        return 2
    if getattr(args, "hash_every", 0) < 0:
        print("error: --hash-every must be non-negative", file=sys.stderr)
        return 2
    handlers = {
        "play": run_play_or_verify,
        "verify": run_play_or_verify,
        "diff": run_diff,
        "transcode": run_transcode,
    }
    try:
        return handlers[args.command](args)
    except KithSimError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except RecordError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ReplayFormatError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

"""Integration test: the stripe-sharded spatial handlers stay deterministic
under concurrent dispatch.

Drives the real handler set and the real tile2d model from several threads
with a fixed per-actor input schedule delivered in randomized arrival
orders, then asserts the concurrent final state equals a single-threaded
reference run of the same per-actor sequences: the model's final state is a
function of the per-actor input sequences, not of their cross-actor
interleaving, which is the order-independence the deterministic-simulation
contract needs from the handler's lock topology.

The arrival model crosses threads for the same actor on purpose: each
actor's spawn is issued by one thread and its schedule is applied by
another, with the apply thread starting before the spawn so its first
attempts race the spawn's insert-plus-publish stripe hold (attempts before
the spawn lands are dropped by the handler, exactly as an unknown-actor
wire frame is, and the schedule retries until accepted, so the input set is
preserved). The server's own tick thread drains the dirty-cell flush
concurrently with the applies and a chat-marking thread runs alongside, so
the bookkeeping lock's snapshot-vs-mark seam is exercised too. The recorded
artifact is parsed back with the strict reader, whose canonical-order check
fails the test if concurrent arrival ever reordered a tick's events.

The seed pins the arrival orders: the comparison is reproducible in CI. Any
failure under any seed is a real defect; the seed decides which interleaving
surfaces it first.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Sequence
from pathlib import Path

from _helpers import needs_build, wait_for_replay_records
from examples._common.replay_format import MoveEvent, ReplayDocument, read_document_file
from examples.embedded.server import EmbeddedServer
from examples.spatial import messages
from examples.spatial.handlers import encode_chat

from kith import Actor, CellKey, Server, ServerStatus, SimInput


_SEED: int = 20260828
_THREADS: int = 4
_ACTORS: int = 48
_APPLIES_PER_ACTOR: int = 12


class _StubSessionInfo:
    """Session metadata stub satisfying the handler's info protocol."""

    actor_id: int = 0
    session_id: int = 0


class _StubSession:
    """Session stub satisfying the handler's session protocol."""

    def populate(self, actor_id: int, cells: Sequence[CellKey]) -> None:
        del actor_id, cells

    def bind_actor(self, actor_id: int) -> None:
        del actor_id

    def info(self) -> _StubSessionInfo:
        return _StubSessionInfo()

    def window_clear(self) -> None:
        return

    def window_add(self, key: CellKey) -> None:
        del key

    def window_remove(self, key: CellKey) -> None:
        del key


def _actor_schedule(actor_id: int) -> list[SimInput]:
    """Return the fixed per-actor input sequence every run applies.

    Directions alternate by actor id so neighboring actors cross cells in
    opposing directions, maximizing cell-boundary overlap between concurrently
    applied schedules.
    """
    move_x = 200 if actor_id % 2 else -200
    move_y = 200 if (actor_id >> 1) % 2 else -200
    return [
        SimInput(input_tick=tick, move_x=move_x, move_y=move_y, move_z=0, flags=0)
        for tick in range(1, _APPLIES_PER_ACTOR + 1)
    ]


def _wait_running(facade: Server, timeout: float = 5.0) -> None:
    """Block until the facade reaches RUNNING or the deadline expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if facade.status is ServerStatus.RUNNING:
            return
        time.sleep(0.01)
    raise RuntimeError("embedded server did not reach RUNNING")


def _per_actor_moves(doc: ReplayDocument) -> dict[int, int]:
    """Count MoveEvents per actor across the artifact's ticks."""
    recorded: dict[int, int] = {}
    for tick_record in doc.ticks:
        for event in tick_record.events:
            if isinstance(event, MoveEvent):
                recorded[event.actor_id] = recorded.get(event.actor_id, 0) + 1
    return recorded


def _shutdown(
    embedded: EmbeddedServer,
    facade: Server,
    run_thread: threading.Thread | None,
) -> None:
    """Shut the stack down in the composition root's canonical order."""
    facade.shutdown()
    if run_thread is not None:
        run_thread.join(timeout=5.0)
    embedded.stop()


def _run_concurrent(record_path: Path) -> dict[int, Actor]:
    """Apply the fixed schedules from several threads; return the final table.

    Each actor's spawn is issued by the thread owning its id modulo the
    thread count; its schedule is applied by the next thread around the
    ring, whose attempts begin before the spawn lands so the first applies
    contend with the spawn's stripe hold. Attempted applies before the
    spawn completes return ``None`` (the handler drops unknown-actor frames)
    and the schedule retries that input until it is accepted, preserving
    the per-actor input sequence. Raises the first worker exception after
    all threads join.
    """
    embedded = EmbeddedServer(record_path=record_path)
    facade, _gateway_port, _control_port = embedded.start()
    run_thread = threading.Thread(target=facade.run, daemon=True)
    run_thread.start()
    _wait_running(facade)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()
    applied: dict[int, int] = {}

    def fail(exc: BaseException) -> None:
        with errors_lock:
            errors.append(exc)

    def run_worker(thread_index: int) -> None:
        try:
            handlers = embedded.handlers
            rng = random.Random(_SEED + 10_000 + thread_index)
            for actor_id in range(1, _ACTORS + 1):
                if actor_id % _THREADS == thread_index:
                    handlers.spawn_actor(actor_id)
            apply_ids = [
                actor_id
                for actor_id in range(1, _ACTORS + 1)
                if actor_id % _THREADS == (thread_index + 1) % _THREADS
            ]
            rng.shuffle(apply_ids)
            for actor_id in apply_ids:
                for inp in _actor_schedule(actor_id):
                    stepped = handlers.apply_movement(actor_id, inp)
                    deadline = time.monotonic() + 10.0
                    while stepped is None:
                        if time.monotonic() > deadline:
                            msg = f"actor {actor_id} never accepted an input"
                            raise RuntimeError(msg)
                        # Sample the spawn window repeatedly: each attempt
                        # either reads the absent actor under its stripe or
                        # blocks on the stripe the spawn holds across the
                        # insert-plus-publish.
                        time.sleep(0.0001)
                        stepped = handlers.apply_movement(actor_id, inp)
                applied[actor_id] = _APPLIES_PER_ACTOR
                time.sleep(rng.random() * 0.002)
        except BaseException as exc:  # re-raised on the main thread after join
            fail(exc)

    stop_chat = threading.Event()
    chat_rng = random.Random(_SEED + 20_000)

    def chat_worker() -> None:
        try:
            handlers = embedded.handlers
            session = _StubSession()
            while not stop_chat.is_set():
                actor_id = chat_rng.randrange(1, _ACTORS + 1)
                handlers.on_chat(
                    messages.CHAT_TYPE,
                    encode_chat(actor_id, "ping"),
                    session,
                )
        except BaseException as exc:  # re-raised on the main thread after join
            fail(exc)

    workers = [
        threading.Thread(target=run_worker, args=(index,), daemon=True) for index in range(_THREADS)
    ]
    chat = threading.Thread(target=chat_worker, daemon=True)
    for thread in workers:
        thread.start()
    chat.start()
    for thread in workers:
        thread.join()
    # Every apply_movement returns after buffering its record (applied ⇒
    # buffered); the wait proves the tick boundaries drained them, which
    # the recorder's tail-dropping close then cannot lose. No record call
    # site fires after the workers joined — the chat worker only marks
    # cells dirty — so the satisfied census stays the final census.
    wait_for_replay_records(
        record_path,
        lambda doc: (
            _per_actor_moves(doc) == dict.fromkeys(range(1, _ACTORS + 1), _APPLIES_PER_ACTOR)
        ),
    )
    stop_chat.set()
    chat.join(timeout=5.0)
    table = {actor.id: actor for actor in embedded.handlers.actor_states()}
    _shutdown(embedded, facade, run_thread)
    if errors:
        raise errors[0]
    assert len(applied) == _ACTORS
    return table


def _run_reference() -> dict[int, Actor]:
    """Apply the same per-actor sequences single-threaded; return the table."""
    embedded = EmbeddedServer()
    facade, _gateway_port, _control_port = embedded.start()
    handlers = embedded.handlers
    for actor_id in range(1, _ACTORS + 1):
        handlers.spawn_actor(actor_id)
    for actor_id in range(1, _ACTORS + 1):
        for inp in _actor_schedule(actor_id):
            handlers.apply_movement(actor_id, inp)
    table = {actor.id: actor for actor in handlers.actor_states()}
    _shutdown(embedded, facade, None)
    return table


@needs_build
def test_concurrent_schedules_match_the_single_threaded_reference(tmp_path: Path) -> None:
    """Concurrent arrival produces the reference state and a canonical artifact."""
    record_path = tmp_path / "concurrent.krpl"
    concurrent = _run_concurrent(record_path)
    reference = _run_reference()

    assert set(concurrent) == set(reference)
    for actor_id, actor in concurrent.items():
        expected = reference[actor_id]
        assert actor.pos_x == expected.pos_x, f"actor {actor_id} pos_x diverged"
        assert actor.pos_y == expected.pos_y, f"actor {actor_id} pos_y diverged"
        assert actor.vel_x == expected.vel_x, f"actor {actor_id} vel_x diverged"
        assert actor.vel_y == expected.vel_y, f"actor {actor_id} vel_y diverged"
        assert actor.input_tick == expected.input_tick, f"actor {actor_id} input_tick diverged"
        assert actor.update_seq == expected.update_seq, f"actor {actor_id} update_seq diverged"

    document = read_document_file(record_path)
    assert document.ticks, "the concurrent run recorded no ticks"
    recorded = _per_actor_moves(document)
    assert recorded == dict.fromkeys(range(1, _ACTORS + 1), _APPLIES_PER_ACTOR)

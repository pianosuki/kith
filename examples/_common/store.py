"""Generic async game-state store, with an in-memory default backing.

A game-state store is a small key/value surface a game uses to persist
state it wants to survive a server restart. The interface is generic on
purpose: it carries no game vocabulary, so a one-actor-per-account game
keys the store by principal id and stores the actor's serialized state,
while a game that owns a roster of characters keys the store by character
id instead. The roster is one consumer of this store, not the store's
contract.

The default backing is in-memory and ships with the examples — imported
as examples._common.store, not from the kith package — so a reference
game boots with no external dependency (the zero-setup story).
A game that wants durable persistence registers a Postgres-backed
implementation alongside it; the two are interchangeable behind this
interface (dependency injection), mirroring the framework's own
config-struct pattern of one contract with switchable backings.

The methods are coroutines so the in-memory and Postgres backings share
a shape: the in-memory ones resolve immediately, while a Postgres
backing awaits an async query reply. Callers drive the coroutines with a
running event loop; the store itself holds no loop reference.
"""

from __future__ import annotations

import threading
from typing import Protocol


__all__ = [
    "GameStateStore",
    "InMemoryStore",
]


class GameStateStore(Protocol):
    """Async byte-key/byte-value game-state persistence.

    Keys and values are opaque byte strings; the store assigns no
    semantics to their contents. A return of ``None`` from
    get denotes a missing key, distinct from a stored zero-length
    value. Implementations are safe to call from the framework's Python
    worker pool; callers that share a store across sessions do
    not need extra synchronization.
    """

    async def get(self, key: bytes) -> bytes | None:
        """Return the stored value for ``key``, or ``None`` if absent."""
        ...

    async def put(self, key: bytes, value: bytes) -> None:
        """Store ``value`` under ``key``, replacing any prior value."""
        ...

    async def delete(self, key: bytes) -> None:
        """Remove ``key``; deleting an absent key is not an error."""
        ...


class InMemoryStore:
    """In-memory GameStateStore backing (the default).

    A dict guarded by a Lock. The async methods perform
    no I/O and resolve on the first step, so a caller awaiting them under a
    running loop pays only the coroutine overhead. The lock makes the store
    safe under free-threaded Python, where the gateway worker pool may
    dispatch concurrent handlers that share one store instance.

    Values are stored by reference; a caller that mutates a ``bytes``
    object after handing it to the store sees no effect (``bytes`` is
    immutable), and a caller that reuses a key receives the same object
    the store holds.
    """

    __slots__ = ("_lock", "_values")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[bytes, bytes] = {}

    async def get(self, key: bytes) -> bytes | None:
        with self._lock:
            return self._values.get(key)

    async def put(self, key: bytes, value: bytes) -> None:
        with self._lock:
            self._values[key] = value

    async def delete(self, key: bytes) -> None:
        with self._lock:
            self._values.pop(key, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._values)

# ADR-0026: The gateway carries the session lifecycle surface

**Status:** Accepted

## Context

A session's lifetime is visible to a game only indirectly. Every
destruction funnels through one entry and the session is refcounted across
the reactor→worker dispatch handoff, so a disconnect is already an ordered,
race-free teardown — but no surface reports it. A game that keeps
per-session state (rosters, seats, per-session caches) learns of a
disconnect only by polling, and the documented probe reads a field that
never changes, so uncleaned per-session state grows under churn. Sessions
are also unholdable: the dispatch reference a handler runs under dies when
the handler returns, and no primitive exists to keep a session alive past
it, so a game-built recipient set — a roster the game addresses later —
cannot be constructed. The send record anticipated this gap and
named the acquisition and enumeration surface as its own decision.

## Decision

The gateway carries the session lifecycle as three surfaces. Death is an
event: a destroyed-session callback registered on the gateway, fired from
the destruction entry exactly once per session destroyed while the gateway
handle is alive — after the session is detached from the table and its
subscriptions, before the owner reference drops. The callback receives the
session's identity, never the session itself: the session is dying, and
the off-thread dispatch kinds cannot hold a reference to it. Registration
follows the handler machinery — unflagged callbacks run inline on the
reactor thread, pool-flagged and Python-bound callbacks dispatch through
the attached worker pool under the same contract as message handlers — and
a lifecycle notification that cannot be delivered counts in its own
dropped-lifecycle gauge, not the dispatch-drop gauge: a lost disconnect
notice is silent state leakage, not backpressure.

Sessions are holdable: a public acquire/release pair on the session's
dispatch refcount, mirroring the connection reference pair. Acquire is
legal only where a reference is already held — inside a handler under the
dispatch reference, or on the reactor thread — because the dispatch
reference dies when the handler returns; a session pinned this way
survives the reactor's destroy and is freed when the last reference
releases. A pinned session's send follows the send contract: a closed
connection raises in the lifecycle family.

Roster visibility ships as a count and a snapshot. The count is a mirror
gauge of the table, readable from any thread; the snapshot is a copy-out
of live sessions' identity records, reactor-thread-only like the table it
reads. The table itself stays unsynchronized and reactor-thread-only; no
lock grows onto it.

## Consequences

Positive — per-session game state becomes reclaimable on disconnect (the
uncleaned-roster growth under churn ends), a game-built recipient set is
constructible through pins taken inside handlers, and the death signal
replaces the probe pattern the previous documentation taught. The surfaces
compose with the send record exactly as it anticipated.

Negative — the acquire discipline is documented, not enforced: acquiring
from a context that holds no reference is undefined, and the contract text
is the only fence. Lifecycle notifications are best-effort at teardown: a
callback dispatched to a pool that is gone, or refused at interpreter
finalization, is dropped and counted, so state cleanup keyed on the
callback does not run in those windows. Enumeration stays
reactor-thread-only, so a facade game builds its roster from callbacks and
pins rather than by listing.

# ADR-0028: Session populate is one atomic step

**Status:** Accepted

## Context

A session comes into existence empty: the gateway creates it when the
transport connects, before any identity is known. The delivery startup
contract then populates the session in steps — bind it to a subscriber
actor, then seed its subscription window — as separate calls, each
serialized by the session's window mutex. Between the bind and the seed
the session is bound but tracks no cells, and the seed-failure census
publishes exactly that state as its signature condition, so an observer
cannot distinguish a populate in progress from a failed seed. The same
ordering hazard exists on the game side, where a matchmaker creates a
group before its first member populates and a game-side sweep may retire
the empty group first. The sweep is the game's own: the framework
provides no idle sweep, and sessions die only on close paths.

## Decision

Populate is one call and one critical section: it binds the session to
its subscriber actor and seeds its subscription window inside a single
hold of the session's window mutex. The bind publishes first, then the
seed, preserving the composition invalidation ordering. The seed reuses
the per-cell add contract: a capacity failure leaves the landed cells in
place and retains the failed ones for the tick pass's retry, and the call
reports the failure. There is no rollback — landing a cell subscribes it,
and staging a transaction by un-subscribing landed cells would fight the
healing backstop. After this step the bound-without-subscriptions state
exists only as a seed failure: the census gauge's meaning sharpens from
"populate in progress or seed failure" to "seed failure", which is what
makes a game-side sweep safe to gate on the gauge.

The sweep stays game-side: no idle sweep, no group concept, and no
sweep-ordering hook join the framework. A game that retires empty groups
owns the ordering of its own observations against its own population
steps; the operator guide carries that contract. The surface is the
subscriber session only: no pinned-session mirror, no server-level
mirror, and no combined create-and-populate — the transport creates
sessions in every shipped shape, and the join step runs where the
dispatch reference already lives.

## Consequences

Positive — populating a session is one observable step: no intermediate
bound-without-subscriptions state, an unambiguous census gauge, and the
identity-plus-subscriptions pairing enforced at the API instead of by
caller discipline. The join path reads as one call in the guides and in
the shipped reference.

Negative — a partially failed seed is a legal outcome: the call returns
the capacity error with some cells landed and reports no per-cell
breakdown; per-cell precision stays with the per-cell add call. A
bind-only form (an empty cell list) is legal and produces the
bound-without-subscriptions state deliberately; the census names it as a
seed failure until the caller seeds.

# ADR-0001: Five-plane world stream fabric architecture

**Status:** Accepted

## Context

An earlier monolithic, actor-centered MMO server proved that a
reactor/connection-centric area-of-interest (AOI) model hits an N² ceiling:
every connection tries to observe every other, and relevance is computed too
late. Evolving that architecture toward distinct planes — simulation, world
streams, gateway edge caching, player-relevance composition, control/session,
and coordination — broke past the ceiling at scale. This framework
consolidates that into five planes: the gateway edge cache and the
player-relevance composer merge into the Gateway plane as subsystems.

## Decision

The framework's default runtime topology is the five-plane fabric. Plane
contracts and invariants are defined in the architecture documentation and
enforced automatically by a plane-invariant checker in the verification gate.
Session, bootstrap, and control traffic share the single reactor with
world-stream replication (ADR-0011), not a second reactor. The two are
isolated by dispatch routing, not by thread: Python-bound service handlers
are offloaded to the worker pool so the reactor never blocks on Python
(ADR-0004), and inline C handlers are non-blocking by contract. Control-plane
load therefore cannot stall live publish on the reactor thread. A dedicated
second reactor for session traffic is not part of this design; it would
require a superseding ADR. Bootstrap builds its initial view
deterministically from published cell streams rather than racing live
updates.

## Consequences

Positive — breaks the N² ceiling and scales horizontally. Negative — more
modules and stricter boundaries; the plane checker is the guardrail. The
coordination plane's ownership map, split/merge coordinator, and inter-node
bus are designed to be shardable and leader-electable for very large
clusters. The initial implementation may use a single backing-store instance
and a single coordinator; the contract (the coordinator owns the cell
ownership map) does not change when the backing store is sharded or the
coordinator is leader-elected. This is a deployment concern, not an
architectural one.

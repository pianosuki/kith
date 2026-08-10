# ADR-0003: Split/merge transfers sim authority and fabric publish authority with overlapping cell-stream handoff

**Status:** Accepted

## Context

Moving authority metadata slower than the publish workload leaves a single
instance as the bottleneck. In an earlier monolithic, actor-centered MMO
server, actor-by-actor handoff emitted a despawn event to observers, causing
despawn/respawn churn inside the AOI under load — a visible interruption at
the seams.

## Decision

A split/merge moves simulation authority, fabric publish ownership, and
gateway subscription metadata as one coordinated ownership update, using an
overlapping cell-stream handoff rather than a hard atomic cut. The outgoing
authority continues publishing the affected cell streams for a bounded overlap
window after the new authority begins, each guarded by an authority epoch. The
new authority's product sequence supersedes the old once it overtakes it;
epoch-guarded rejection of stale publishes prevents duplication. Gateways
retain the same subscription identity across the handoff; the backing cell
owner changes underneath them with no user-visible despawn or reset. No
despawn event is emitted to clients in normal same-zone migration. The overlap
window is a config tunable.

## Consequences

Positive — cross-instance traffic is cell-stream-oriented rather than
actor-by-actor, and gameplay at the seams is uninterrupted (no visible freeze,
despawn, or reset during cell migration). Negative — a bounded window of
duplicate publish work during handoff, mitigated by a short overlap window and
the epoch guard rejecting stale products once superseded.

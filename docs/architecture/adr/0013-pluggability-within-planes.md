# ADR-0013: Pluggability is within planes, not across invariant-breaking topologies

**Status:** Accepted

## Context

The framework must be generic enough to support different architectures.
Fully-pluggable "any architecture" would reintroduce the N² ceiling by
allowing invariant violations.

## Decision

The framework ships one default topology (the five-plane fabric) and switchable
topologies (embedded, distributed). Within a topology, individual planes have
pluggable implementations via the registry pattern, but every implementation
must respect its plane's invariants. The plane checker rejects
invariant-violating implementations.

## Consequences

Real flexibility (embedded for small games, distributed for MMO) without the
risk of silently reintroducing the reactor-centric AOI model. Negative — each
pluggable plane carries a registry surface and a plugin contract that must
track the plane's invariants as they evolve. Registries shipped: sim models,
database queries, protocol message types, and control-plane routes.
Composer-policy and fabric-storage swaps remain within-plane
extension points.

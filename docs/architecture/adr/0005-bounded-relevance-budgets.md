# ADR-0005: Bounded relevance budgets and representation tiers are first-class

**Status:** Accepted

## Context

"Every player sees full live state of every other player" does not scale. In
an earlier monolithic, actor-centered MMO server, LOD/crowd proxies were
treated as emergency fallbacks rather than the normal contract.

## Decision

Every player's send set is always budgeted (Ring A interactive core, Ring B
local presence, Ring C crowd aggregate). Representation tiers
(full/reduced/crowd) are the normal contract, not an overload mode. The
shipped default at v1.0.0 is 512 subjects per subscriber; a game tunes its
own budget through the gateway params, and changing the shipped default is a
superseding decision, not an implementation detail.

## Consequences

Per-player delivery scales with the capped budget rather than the zone
population; extreme density degrades by design. Positive — delivery cost is
a function of the budget, the fixed-size delivery record, and the 20 Hz tick
rate, never of the zone population. Negative — at full density the budget is
a delivery cost, not a fidelity cap: games that expect dense pileups tune
the budget down or lean on representation tiers rather than carrying the
default.

The budget also feeds the per-tick reactor-thread total (the compose cost
scales with the budget); the budget-to-gate reconciliation and the
per-instance fidelity ceiling it implies are in
`docs/architecture/performance_budgets.md`.

# ADR-0030: The composition root forwards the view tuning knobs

**Status:** Accepted

## Context

The gateway's per-subscriber view is bounded and cadenced by three creation
parameters: the view-set subject capacity (shipped default 512), the view
refresh interval, and the cell-cache refresh interval. ADR-0005 makes the
bounded relevance budget first-class — a game sizes its own budget through
the gateway params — and the delivery cadence the two intervals gate is the
framework's freshness control under load.

On the composition-root shape the tuning surface was unreachable. The
wiring derives both refresh intervals from the tick interval (a refresh
interval wider than the tick leaves inputs published inside the gap
undelivered until the next refresh) and forwards no view capacity, so the
facade could not size the view set: a dense deployment's subscriber views
truncated at the shipped default with no reachable ceiling, while the
standalone gateway shape accepted the same three knobs directly. The two
shapes disagreed about who may tune, not about what the knobs mean.

## Decision

The composition root's creation parameters carry the same three knobs — the
view-set subject capacity and the two refresh intervals — and forward them
to the gateway it creates. Zero keeps the shipped law per field: the
capacity selects the gateway's default, and the intervals select the tick
interval, the derivation the composition root applies. An explicit interval
passes through unclamped; a caller setting an interval wider than its tick
opts into inputs published inside the gap landing on the next refresh, the
same contract the delivery wait budget's explicit values follow. The
per-tick composition budget stays derived and is not one of the forwarded
knobs — its half-tick default is the gateway's own law.

## Consequences

Positive — dense deployments size the view set and the refresh cadence
without leaving the facade, and the standalone and embedded shapes accept
the same three knobs; the truncation semantics (closest-first selection,
then the crowd regime) and the view high-watermark gauges stay the
gateway's own unchanged surface.

Negative — three more fields ride the composition root's size-versioned
creation struct (additive). An explicit interval below the tick sharpens
freshness at the cost of more composition passes, and the wider-than-tick
staleness is the caller's documented choice rather than a guarded error.

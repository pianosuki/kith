# ADR-0002: Publish products are immutable and cell-scoped

**Status:** Accepted

## Context

Per-actor, per-connection publish duplicates work before relevance is known.
The unit of publication must be the cell stream.

## Decision

Publish products are immutable once published, keyed by zone, 3D cell
coordinates, a level-of-detail index, an authority epoch, and a monotonically
increasing publish sequence. The cell key is 3D-native from inception
(ADR-0016). Supersession happens in the fabric, not in per-connection queues.

## Consequences

Positive — publish cost is per-cell rather than per-connection, supersession
is a fabric concern, and consumers depend on stable products. Negative —
immutability places revision discipline in the fabric, which must own
supersession. Gateways subscribe to cell streams; they never rescan raw zone
state.

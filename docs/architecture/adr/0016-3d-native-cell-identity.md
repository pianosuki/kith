# ADR-0016: Cell identity is 3D-native from inception

**Status:** Accepted

## Context

The framework's goal is to support any spatial MMO, including 3D. An audit of
an earlier monolithic, actor-centered MMO server found that a large share of its
architecture was fundamentally 2D-coupled (sim, physics, tile grid, wire
protocol, DB schema, AOI selection) because the canonical cell key was a 2D
coordinate pair with an overloaded field meaning both subcell LOD refinement
and, in the sim layer, a discrete z-floor index. The critical finding: the streaming and
clustering machinery (fabric, gateway cache, split/merge, cluster bus, cell
table, router, representation policy) is coordinate-agnostic — it is parametric
over a cell identity and moves bytes without knowing what the cell coordinates
mean. The 2D coupling was concentrated in the key-shape choice, not in the
machinery. Baking 2D into the cell key would force a fabric rewrite for 3D;
making the cell key 3D-native from inception makes 3D a sim-model plugin.

## Decision

The fabric's canonical cell identity is zone plus 3D cell coordinates plus a
level-of-detail index from inception, where the level-of-detail index is a
subcell refinement index — not a vertical axis, renamed from the earlier
server's overloaded field to remove the z-conflation. 2D simulation models
project onto this key by setting the vertical coordinate to zero; 3D models
use a non-zero vertical coordinate natively. The spatial query library ships
sphere and box (AABB) queries over that key; a planar population selects
circularly under the sphere query at the zero vertical coordinate. The subscription window is an explicit set of 3D-native cell
keys, so its geometry belongs to the caller: a 2D wiring populates it with
cells at the zero vertical coordinate, a 3D wiring with a slab of cells — the window mechanism itself
is coordinate-agnostic.

## Consequences

Positive — 3D MMOs are a sim-model plugin rather than a fabric rewrite; the
fabric, gateway cache, split/merge, cluster bus, and cell table are 3D-capable
by construction. Negative — one extra component in the cell key and a slightly
more general window function; the first sim models are unaffected because they
set the vertical coordinate to zero and select with sphere queries over planar
populations. The overloaded
field rename removes a real source of confusion between the subcell refinement
axis and the vertical axis.

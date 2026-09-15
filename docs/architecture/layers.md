# Dependency Layers

Every module has both a **runtime plane** (who owns what data — see
`docs/architecture/planes.md`) and a **dependency layer** (which way
`#include` may point). The two are independent axes: a module's layer says
nothing about its plane, and its plane says nothing about its layer.

The dependency layer governs `#include` direction. A module may only include
headers from the same or a lower dependency layer, regardless of plane. This
keeps the dependency graph acyclic and prevents high-level wiring from leaking
into low-level infrastructure. The rule is enforced by
`tools/check_module_layers.py`.

## The layers

| Layer | Name | Modules | Runtime plane(s) |
|---|---|---|---|
| 0 | foundation | `framework`, `util`, `config`, `logger`, `metrics` | shared (no plane) |
| 1 | infrastructure | `net`, `proto`, `worker` | `net` → Gateway (transport); `proto`/`worker` shared |
| 2 | services | `reactor`, `aoi`, `sim`, `fabric`, `gateway`, `coord`, `control`, `client`, `state`, `db` | `sim` → Sim; `fabric` → Fabric; `gateway` → Gateway; `coord` → Coord; `control` → Control; `reactor` shared; `aoi` shared; `state`/`db` shared; `client` (client side, not a plane) |
| 3 | composition | `server` | wires all planes, selects topology |
| 4 | game | `examples/*`, user code | game-specific (Python or C) |

`framework` is a pseudo-module for the cross-cutting public headers that live
directly under `include/kith/` (`api.h`, `version.h`, `types.h`, `kith.h`).
Every real module depends on them, so they sit at the foundation layer.

## Rule

A module at layer N may include headers from modules at layer N or lower. An
edge that points upward (layer N → layer > N) is a violation.

Foundation modules are shared: they serve the planes and may be included by
any higher layer, and the same holds for the shared modules at the higher
layers — the plane column above records each module's binding. Service-layer
modules may include shared modules and same-or-lower services within the
plane contract (see `docs/architecture/planes.md` and
`tools/plane_rules.yaml` for the cross-plane include restrictions that layer
on top of this rule).

## Enforcement

`tools/check_module_layers.py` scans `include/` and `src/` for `#include "mod/..."`
edges and verifies each one is same-layer or downward. The committed pre-commit
hook runs it as `check-module-layers` over `include src`. Header-only
third-party dependencies and external modules are excluded by name.

The layer check governs direction. The plane check governs data-flow ownership
expressed through includes (for example, `sim` must not include `gateway` even
though both are layer 2). The two checks are complementary: passing one does
not imply passing the other.

## Why a separate axis

A runtime plane is a statement about *data* — which plane owns authoritative
state. A dependency layer is a statement about *code* — which module may depend
on which. Folding them into one axis conflates the two: a foundation module
would need a plane, and a plane module would need a layer it does not naturally
have. Keeping them separate lets the framework reason about scaling (planes)
and buildability (layers) independently.

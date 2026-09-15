# Runtime Topologies

The framework ships two runtime topologies, selected by configuration. The
plane contracts (see `docs/architecture/planes.md`) are identical in both;
only the wiring changes. Game logic written against the plane contracts runs
unmodified in either topology.

## embedded

All planes run in one process. `coord` is a no-op stub (a single cell, a single
owner, no inter-node bus). `fabric` uses in-memory storage.

Use the embedded topology for small games, development, testing, and the
`examples/embedded/` demo. It has no Redis or cluster dependencies —
a single process links against `libkith_server.so` and runs.

## distributed

The default. Planes may run in the same or in separate processes, connected via
the `coord/bus`. Gateway and sim shards scale independently. Fabric storage is
the in-memory append-only ring; durable cross-process state lives in the state
plane (Redis) and the db plane (Postgres), which the distributed wiring
configures.

The distributed topology is the one that breaks the N² ceiling: publish work
is fan-out to subscribed gateways only, and authoritative work is sharded
across sim instances by cell ownership. Coordination owns the cell ownership
map and moves simulation authority and fabric publish authority together
during split/merge (ADR-0003).

## Switching

Switching topology is a configuration change, not a code change. The server
composition root reads the configured topology and wires the planes
accordingly:

- **embedded** — instantiate one of each plane in-process; pass the in-memory
  fabric storage and the no-op coord into the wiring.
- **distributed** — instantiate the configured number of gateway, sim, and
  fabric instances; connect them through the `coord/bus`; wire the durable
  stores (state plane, db plane) that the deployment configures.

A module written against a plane's contract does not know which topology it is
running in. The plane boundaries hold in both.

## Scaling gates

Before claiming the N² ceiling is broken, the framework must pass the
two-tier scaling gates documented in `AGENTS.md` §4.4 and ADR-0020:

- **Dense-1000 (embedded) — stress gate.** 1000 dense actors on the
  embedded topology certify session integrity and liveness under the
  pileup (`session_ok >= 99%`, `selected_clients >= 95%`,
  `bootstrap_ms_p95 < 3000`, no crash, no corruption). The
  delivery-fidelity metrics (`move_missing_ratio_certified` with its
  `mm_event` companion, `continuity_flicker`)
  are reported, not thresholded; the embedded topology is the
  single-instance stress test, not the fidelity claim.
- **Distributed-2000 — fidelity gate.** 2000 distributed actors must meet
  the full §4.4 thresholds per instance (`session_ok >= 99%`,
  `selected_clients >= 95%`, `move_missing_ratio_certified < 10%`,
  `bootstrap_ms_p95 < 3000`, no continuity flicker) with no single instance
  the fixed publish bottleneck under normal spread. The distributed
  topology is the one that breaks the N² ceiling, so it is the topology the
  fidelity claim is made for. The embedded topology is not expected to
  meet the distributed gate; it exists for development and small
  deployments. The gate runs 1000 clients/instance, above the measured
  per-instance ceiling of the Python movement-apply regime; the certified
  native-apply configuration reads 0.00% certified move-missing at that
  load, and the capacity model records both regimes
  (`docs/architecture/performance_budgets.md`).

## References

- `docs/architecture/planes.md` — plane contracts (unchanged across topologies).
- `docs/architecture/layers.md` — dependency layers (orthogonal to topology).
- `docs/architecture/performance_budgets.md` — per-operation budgets and the
  per-instance fidelity capacity model.
- ADR-0001 — five-plane world stream fabric architecture.
- ADR-0003 — split/merge with overlapping cell-stream handoff.
- ADR-0013 — pluggability is within planes, not across invariant-breaking
  topologies.
- ADR-0020 — two-tier scaling gates (dense-1000 stress + distributed-2000
  fidelity).

# Changelog

All notable changes to kith are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0]

The first stable release of kith: a scalable server framework for real-time,
stateful multiplayer worlds. A C23 core owns the performance-critical systems
— transport, the io_uring reactor, spatial indexing, the world stream fabric,
and the simulation — and a Python layer hosts game logic, with C simulation
models as shared libraries plugging into the same ABI. The framework ships
one opinionated default runtime topology, the five-plane world stream fabric
(Sim, Fabric, Gateway, Coord, Control), built around interest management:
the world is divided into cells that publish state once, and gateways
subscribe to cells and compose a per-player view. Embedded and distributed
topologies run the same game code, the simulation is deterministic with
replay and hash-regression coverage, and guides for Python extension, C
simulation models, the wire protocol, operations, scaling, replay, and
agentic testing document the whole surface.

### Scaling gates

Claims about scale are certified by two gate classes, with the certified
configuration part of the claim. The procedure lives in
`docs/guides/scaling_checklist.md`; the numbers here are the release
thresholds.

**dense-1000** (embedded, stress gate) — 1000 actors on one embedded server:
`session_ok >= 99%`, `selected_clients >= 95%`, `bootstrap_ms_p95 < 3000 ms`,
no crash or corruption. Delivery-fidelity metrics are reported, not
thresholded: the embedded single-cell pileup is the adversarial case, not
the topology the fidelity claim is made for.

**distributed-2000** (fidelity gate) — 2000 actors across two server
instances, per instance: `session_ok >= 99%`, `selected_clients >= 95%`,
`move_missing_ratio_certified < 10%`, `bootstrap_ms_p95 < 3000 ms`, no
continuity flicker. The certified move-missing reading is the
publisher-minted `update_seq` delta between self echoes; the event-count
metric is reported alongside it and floors under saturation.

The certified configuration: free-threaded CPython 3.14t (GIL disabled),
the native C movement-apply path, two delivery-executor workers, one load
driver per instance cohort, pinned cores per the recorded reference
envelope. The per-operation budgets and the per-instance capacity model
that records both apply-path regimes live in
`docs/architecture/performance_budgets.md`.

### Out-of-box visual example

The released wheel ships a runnable example alongside the library:
`kith.examples.visual` boots an embedded-topology world — a seamless plain,
ambient actors under the same momentum model the players use, cell-scoped
chat, a metrics route — with one command and no written code, and the
console entry point `kith-visual` lists and launches its scenarios (crowd,
density, churn, membership, reconnect, tick ladder, soak). The graphical
client renders the world through the certified harness client engine and
carries the measurement HUD — per-actor staleness, arrival jitter, input
echo, the snap-event log, and the live delivery counters; its window
renderer rides an opt-in extra, so the base install stays dependency-free.
The agentic client engine itself moved into the package under a private
home, re-exported at its existing tooling path, so the shipped example and
the certified harness drive one implementation.

### Known residuals

Measured, sub-threshold, and named — statements of scope, not open defects:

- The single-driver load-driver configuration still shows occasional
  sub-threshold delivery pauses at roughly half their measured amplitude
  (one asyncio loop driving the full cohort delays the movement cadence).
  The certified recipe runs one driver process per instance cohort, and
  the certified runs read zero continuity flicker.
- The gateway delivery executor's batch-release pause is measured in gate
  traces; the pause has no attributed trigger.
- Gate-run fidelity ledgers exist in two formats; numbers from the earlier
  format are not directly comparable with later runs. The scaling guide's
  measurement caveats carry the boundary.
- The distributed gate certifies horizontal scaling: one server process
  per instance with per-instance loopback coordination buses. Cross-machine
  coordination transport is not part of the certified claim; the
  `KITH_COORD_BUS_TRANSPORT` enum's additive extension point is the seam
  for it.
- On the Python movement-apply path, per-instance fidelity holds to roughly
  300 clients per instance before compose-budget deferrals breach the
  missing-move bar; the native C apply path — the certified configuration —
  reads 0.00% at 1000 per instance. The capacity model records both regimes
  and what raises each.

### Platform

kith is Linux-only. The reactor runs on io_uring; the build links
`liburing`, `libpq`, and `libhiredis`; release wheels bundle the framework's
own shared libraries and keep those three dynamically linked. Production
builds are expected to compile with the hardening flags the project ships
(full RELRO, stack protector, `-D_FORTIFY_SOURCE=3`, PIE, CFI where the
toolchain supports it).

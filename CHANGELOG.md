# Changelog

All notable changes to kith are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-11-06

### Added

- Public model storage creation, state access and destruction support custom
  simulation models compiled against installed headers without private layouts.

- Native pooled tick callbacks admit one queued or executing invocation at a
  time, count refused ticks and drain accepted work before server destruction.

- Precise tick-clock conversions retain fractional periods across logical steps;
  existing integral-period conversions retain their results.

- A public Python server handle supports application-owned native extensions and
  persistent native state. The spatial example uses this public boundary.

- Python 3.15 compatibility coverage includes standard and free-threaded
  interpreters alongside Python 3.14, with installed-wheel checks for each.

### Deprecated

- The original tick-clock millisecond and nanosecond conversions retain their
  integral-period results throughout 1.x. Use the precise variants for elapsed
  time at the configured rate. The original conversions are scheduled for
  removal in 2.0.0.

### Fixed

- Gateway and session objects preserve their required alignment with allocators
  that provide the documented fundamental alignment.
- Sessions retain their delivery callbacks across registry growth and final
  release after gateway destruction.
- Direct client dispatch preserves native registration, correlation-trailer
  validation and configured pong handling.
- Architecture and binding checks fail on C parser errors; gateway analysis uses
  its build feature definitions.
- Python sanitizer jobs use their configured compiler and retain compiler,
  runtime and interpreter identities, with explicit unavailable metadata when
  collection cannot run.
- Reactor deletion retires readiness callback ownership even when backend
  cancellation fails. Poll and cancellation identities remain distinct across
  replacement registrations.
- Redis connection failure detaches the hiredis context before another readiness
  phase can use it. Pending and subsequent commands report connection loss
  instead of reusing the released context.
- Python transaction cancellation retains session ownership through native
  completion and rollback. Failed rollback reserves the connection until
  database closure; cleanup failures are reported without masking the original
  error or returning an open transaction to the pool.
- Closing a server from its own Python handler now raises a state error before
  teardown. Native worker-pool self-destruction aborts before freeing live work;
  callbacks request shutdown and an owning thread performs destruction.
- Server deadlines at rates through 1000 Hz follow an epoch-anchored rational
  grid instead of accumulating period truncation and callback delay. Stalls skip
  missed deadlines without unbounded catch-up work.
- Load-harness movement uses anchored input slots, reports incomplete offered
  load, and stops counter sampling on failure or cancellation.
- Load-harness collection waits for fixed deadlines and decodes shared frozen
  observations cooperatively, keeping reception active during reporting.
- Final harness captures retire live raw retention after sealed deadlines while
  preserving frozen observations and continued event delivery.
- Headless clients grow outbound scratch storage from a configurable starting
  size, preserving larger configured frames and queued-frame order.
- Direct client retention shares immutable payloads; load-harness depth
  reporting decodes only the observed actor's state records.
- Client command submission releases temporary encoding buffers on return
  without creating cyclic ownership.
- Direct replication avoids polling an unchanged empty outbound queue; pending
  output retains its existing flush triggers.
- Headless clients derive the default pong timeout from the resolved ping
  interval, including the native default selected by a zero interval.
- Spatial breadth decoding scans packed identity and membership fields together.
- The custom-model guide and installed-consumer tests exercise the public
  allocation, allocator-failure and destruction contracts.

### Changed

- Tiered delivery reuses decision-time ledger lookups when committing an
  admitted batch, preserving record order and retry behavior.
- Integration guides specify timer versus logical-step time, native callback
  ordering and lifetime, Python copying versus persistent native batches,
  individual versus aggregate relevance, and publication refresh latency.
- Architecture policy consistently permits explicit contract-only amendments,
  preserves published record numbers, and rejects implementation references
  inside decisions. A separate implementation map retains useful locations.
  Pinned development-only Markdown formatting enforces consistent prose and
  structural formatting in the gate.
- Refresh development and build dependencies, pinned CI actions and runtime
  containers. Compiler and binding-generator versions are selected separately;
  retained supported-channel exceptions are recorded with their validation.

## [1.0.1] - 2026-10-19

### Fixed

- Local binary wheels carry the build platform tag. Portable release wheels
  bundle and audit native dependencies against the declared manylinux policy,
  including redistribution notices and the dependency closure in the SBOM.
- Installed-wheel smoke checks require the package's own native libraries and
  matching Python and C release versions.

### Changed

- Refresh the reproducible development, visual-example and build toolchains,
  container images and artifact actions to compatible maintained versions.

## [1.0.0] - 2026-09-21

The first stable release of kith: a scalable server framework for real-time,
stateful multiplayer worlds. A C23 core owns the performance-critical systems:
transport, the io_uring reactor, spatial indexing, the world stream fabric, and
the simulation. A Python layer hosts game logic, with C simulation models as
shared libraries plugging into the same ABI. The framework ships one opinionated
default runtime topology, the five-plane world stream fabric (Sim, Fabric,
Gateway, Coord, Control), built around interest management: the world is divided
into cells that publish state once, and gateways subscribe to cells and compose
a per-player view. Embedded and distributed topologies run the same game code,
the simulation is deterministic with replay and hash-regression coverage, and
guides for Python extension, C simulation models, the wire protocol, operations,
scaling, replay, and agentic testing document the whole surface.

### Scaling gates

Claims about scale are certified by two gate classes, with the certified
configuration part of the claim. The procedure lives in
`docs/guides/scaling_checklist.md`; the numbers here are the release thresholds.

**dense-1000** (embedded, stress gate) runs 1000 actors on one embedded server:
`session_ok >= 99%`, `selected_clients >= 95%`, `bootstrap_ms_p95 < 3000 ms`, no
crash or corruption. Delivery-fidelity metrics are reported, not thresholded:
the embedded single-cell pileup is the adversarial case, not the topology the
fidelity claim is made for.

**distributed-2000** (fidelity gate) runs 2000 actors across two server
instances, per instance: `session_ok >= 99%`, `selected_clients >= 95%`,
`move_missing_ratio_certified < 10%`, `bootstrap_ms_p95 < 3000 ms`, no
continuity flicker. The certified move-missing reading is the publisher-minted
`update_seq` delta between self echoes; the event-count metric is reported
alongside it and floors under saturation.

The certified configuration: free-threaded CPython 3.14t (GIL disabled), the
native C movement-apply path, two delivery-executor workers, one load driver per
instance cohort, pinned cores per the recorded reference envelope. The
per-operation budgets and the per-instance capacity model that records both
apply-path regimes live in `docs/architecture/performance_budgets.md`.

The release certification ran on the release tree under this configuration on
the recorded reference host, 2026-09-21: distributed-2000 twice back-to-back,
both passing every threshold per instance (session_ok 100.00%, selected_clients
100.00% of 2000 eligible, move_missing_ratio_certified 0.00%, no continuity
flicker, bootstrap_ms_p95 304 ms and 331 ms), and dense-1000 once, passing its
stress gate (session_ok 100.00%, selected_clients 100.00% of 996 eligible,
bootstrap_ms_p95 470 ms; its delivery-fidelity readings are the
reported-not-thresholded dense semantics described above).

### Out-of-box visual example

The released wheel ships a runnable example alongside the library:
`kith.examples.visual` boots an embedded-topology world (a seamless plain,
ambient actors under the same momentum model the players use, cell-scoped chat,
a metrics route) with one command and no written code, and the console entry
point `kith-visual` lists and launches its scenarios (crowd, density, churn,
membership, reconnect, tick ladder, soak). The graphical client renders the
world through the certified harness client engine and carries the measurement
HUD: per-actor staleness, arrival jitter, input echo, the snap-event log, and
the live delivery counters; its window renderer is an opt-in extra, so the base
install stays dependency-free. The agentic client engine itself moved into the
package under a private namespace, re-exported at its existing tooling path, so
the shipped example and the certified harness drive one implementation.

### Known residuals

These measured, sub-threshold residuals define the release's scope and are not
open defects:

- The single-driver load-driver configuration still shows occasional
  sub-threshold delivery pauses at roughly half their measured amplitude (one
  asyncio loop driving the full cohort delays the movement cadence). The
  certified recipe runs one driver process per instance cohort, and the
  certified runs read zero continuity flicker.
- The gateway delivery executor's batch-release pause is measured in gate
  traces; the pause has no attributed trigger.
- Gate-run fidelity ledgers exist in two formats; numbers from the earlier
  format are not directly comparable with later runs. The scaling guide's
  measurement caveats carry the boundary.
- The distributed gate certifies horizontal scaling: one server process per
  instance with per-instance loopback coordination buses. Cross-machine
  coordination transport is not part of the certified claim; the
  `KITH_COORD_BUS_TRANSPORT` enum's additive extension point is the seam for it.
- On the Python movement-apply path, per-instance fidelity holds to roughly 300
  clients per instance before compose-budget deferrals breach the missing-move
  bar; the native C apply path (the certified configuration) reads 0.00% at 1000
  per instance. The capacity model records both regimes and what raises each.

### Platform

kith is Linux-only. The reactor runs on io_uring; the build links `liburing`,
`libpq`, and `libhiredis`; release wheels bundle the framework's own shared
libraries and keep those three dynamically linked. Production builds are
expected to compile with the hardening flags the project ships (full RELRO,
stack protector, `-D_FORTIFY_SOURCE=3`, PIE, CFI where the toolchain supports
it).

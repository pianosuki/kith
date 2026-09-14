# Scaling Checklist

The numeric scaling gates the framework must pass before the N² ceiling is
claimed broken are enforced (`AGENTS.md` §4.4); the harness that drives
them is `tools/agent/load_harness.py`.

The harness is a dedicated headless-client load generator, separate from the
closed-loop scenarios in `tools/agent/runner.py` (which drive two or three
clients). It registers N headless clients against a fresh server, walks them
through the login bootstrap, drives a movement phase at a fixed cadence,
samples the §4.4 metrics from the real replication stream the gateway
delivers, and renders a pass/fail report. It reuses the production
`AgenticHeadlessClient` over the real wire, so the gate exercises the full
stack — TCP accept, wire dispatch, sim publish, fabric replication, gateway
delivery — through the code path whose scaling is being gated.

## The two gates

`AGENTS.md` §4.4 defines the scaling-gate contract (ADR-0020): gates
come in two classes — stress and fidelity — and the embedded topology
is the stress class while the distributed topology is the fidelity
class. Reaching an intermediate actor count (for example, 900
green actors) is not sufficient, and a passing dense-1000 stress gate
alone does not claim the N² ceiling is broken; the framework requires the
distributed-2000 fidelity gate to claim it.

| Gate | Profile | Topology | Pass criterion |
|---|---|---|---|
| Dense 1000 (stress) | `dense-1000` (1000 actors, 5 s, 20 Hz input) | embedded (single instance) | session integrity under the pileup: `session_ok >= 99%`, `selected_clients >= 95%`, `bootstrap_ms_p95 < 3000`, no crash, no corruption. The delivery-fidelity metrics (`move_missing_ratio`, `continuity_flicker`) are reported, not thresholded. |
| Distributed 2000 (fidelity) | `distributed-2000` (2000 actors, 5 s, 20 Hz input) | distributed (multiple instances plus a coordination bus) | the full §4.4 thresholds hold per instance (`session_ok >= 99%`, `selected_clients >= 95%`, `move_missing_ratio_certified < 10%`, `bootstrap_ms_p95 < 3000`, no continuity flicker), and no single instance is the fixed publish bottleneck under normal spread. |

The measured capacity table in
`docs/architecture/performance_budgets.md` records what each delivery
and apply regime has carried per instance on the recorded reference
envelope.

The dense single-cell pileup is the adversarial case the embedded topology
is not the fidelity claim for: at a 512-subject view budget and 20 Hz input
it demands ~696 MB/s of server egress (the shipped 68-byte subject record,
ADR-0023's widening) and more than one core of work on
the single-threaded reactor. Real backends hand the dense pileup to
game-design layering and to partitioning (the distributed topology), not
to a single gateway's fidelity guarantees. The stress gate still catches
crashes, session loss, bootstrap regressions, and view-composition failure
under load; it stops short of certifying 20 Hz delivery fidelity on the
single instance.

The distributed gate requires a distributed-topology host factory (multiple
instances). The full gate runs against the multi-process
`tools.agent.subprocess_cluster_host.subprocess_cluster_factory`: one
embedded-server OS process per instance, so the harness's asyncio
client-drive loop is decoupled from the servers' worker-pool GILs. Each
instance runs the embedded topology with its own coordination bus (a real
cross-process coord transport is a separate, additive capability the gate
does not exercise — the gate validates horizontal scaling, not
bus-driven split/merge). The in-process
`tools.agent.distributed_host.distributed_host_factory` (two embedded
servers on a shared loopback coordination bus via `add_member`) remains
for the smoke profile and the cross-instance split-merge integration test.
The profile and thresholds are declared so the gate is defined; running it
via `--subprocess-cluster` drives the full 2000-actor profile against the
multi-process cluster.

## Running the harness

The harness ships as a CLI. Run it from the repository root after the C
libraries are built (`cmake --preset debug && cmake --build build/debug`;
the certified gate runs against the release tree, `cmake --preset release
&& cmake --build --preset release`):

```
KITH_LIB=build/release KITH_PROTO_LIB=build/release/libkith_proto.so.1 \
KITH_CLIENT_LIB=build/release/libkith_client.so.1 \
python -m tools.agent.load_harness --profile dense-1000 --embedded
KITH_NATIVE_APPLY=1 KITH_LIB=build/release KITH_PROTO_LIB=build/release/libkith_proto.so.1 \
KITH_CLIENT_LIB=build/release/libkith_client.so.1 \
python3.14t -m tools.agent.load_harness --profile distributed-2000 \
  --subprocess-cluster --delivery-workers 2 --dual-drivers
```

The distributed-2000 fidelity gate runs under free-threaded Python
(`python3.14t`, GIL disabled) on the native movement-apply path — the
certified configuration ADR-0020 defines: the profile's
`python_workers=8` assumes the N-worker pool ADR-0004 reserves for
free-threaded builds, and the `move_missing_ratio_certified < 10%` bar
is met only under that configuration. A standard (GIL) interpreter run is a
regression baseline, not a gating configuration. The dense-1000 stress
gate and the smoke profiles run under either interpreter (the dense gate
gates on session integrity, not delivery fidelity, so the GIL's
single-worker ceiling does not gate it).

The gate invocation carries `KITH_NATIVE_APPLY=1` (the native
movement-apply path, ADR-0024: without it each server falls back to the
Python apply path and the handler pool overflows under the certified
load), `--delivery-workers 2` (the per-instance delivery-executor
threads, ADR-0022), and `--dual-drivers` (one driver process per
instance cohort). The dual-driver split is part of the certified shape:
a single driver process concentrates all 2000 clients on one asyncio
loop whose inbound replication processing runs at full capacity, and one
oversized loop iteration can delay the movement cadence past the
continuity-flicker threshold — a driver-side measurement artifact, not a
server delivery defect. Two cohorts halve each loop's inbound load.

The invocation also pins the library resolution: the servers take
`KITH_LIB` (the library directory) and the driver-side
`AgenticHeadlessClient`s take `KITH_PROTO_LIB` and `KITH_CLIENT_LIB` as
explicit versioned file paths (`libkith_proto.so.1`,
`libkith_client.so.1`). Absent the pins the loader's build walk prefers
`build/debug`, so a tree holding both build types runs the certified
gate on the debug libraries.

The distributed-2000 profile also selects the built-in `tiered` delivery
preset (ADR-0021): its move-missing bar exists to measure the suppression
lever's effect, so a run on the factory-default `full` preset would gate
on nothing new. Every profile keeps the factory default unless it says
otherwise; an explicit `--delivery-strategy full` re-baselines the same
drive on the default preset when a tiered-vs-full comparison is wanted.
The report records the strategy under test in both renderers.

Every report stamps the resolved movement-apply regime and a derived
certified-shape verdict, computed after the environment is resolved (the
apply knob is a shell environment assignment on the subprocess paths, so
it never appears in the argv stamp): JSON `shape.apply_regime` /
`shape.certified_shape` / `shape.deviations`, and in the markdown an
`Apply regime:` line, a `Certified shape:` line, and a
NOT-A-CERTIFIED-SHAPE banner naming each deviation when the run departs
from the documented certified configuration. The stamp never fails a
run: the documented baselines (GIL runs, Python-apply ladders,
full-preset re-baselines) are legitimate non-certified shapes, and
certification is judged by the evidence consumer: only
`certified_shape: true` reports count as certified evidence.

The two hosts select the movement-apply path differently. A
`--subprocess-cluster` server inherits `KITH_NATIVE_APPLY=1` through its
environment; an `--embedded` host takes the explicit `--native-apply`
flag. The dense certified configuration holds the Python apply path: the
certified dense runs drove that path, and the gate's evidence stays
comparable only under the configuration it was certified with. The
native path on the embedded host exists for diagnostics and comparison
(the C handler pool, ADR-0024), not as the dense gate's recipe. The
report's `Apply regime:` stamp resolves from whichever source applies:
the environment for `--subprocess-cluster`, the flag for `--embedded`.

Flags:

| Flag | Values | Meaning |
|---|---|---|
| `--profile` / `-p` | `dense-1000`, `distributed-2000`, `smoke-dense`, `smoke-distributed` | Scaling profile to drive (default `dense-1000`). |
| `--embedded` | flag | Boot an in-process embedded server per run, instead of targeting an external server. |
| `--distributed` | flag | Boot an in-process multi-instance cluster (shared coordination bus) per run, for the smoke profile and split-merge validation. |
| `--subprocess-cluster` | flag | Boot one embedded-server OS process per instance and drive it from this process, decoupling the loop from the servers' GILs. The full distributed-2000 gate uses this path. |
| `--instances` | int | Instance count for `--subprocess-cluster` (default 2). |
| `--clients` | int | Override the profile actor count with a lighter drive. Worker pool, instance count, cadence, and duration stay from the profile, so a smaller count re-baselines the same server configuration. Defaults to the profile's actor count. |
| `--python-workers` | int | Override the profile handler-worker pool size. Resolution order: this flag, then the `KITH_PYTHON_WORKERS` environment variable, then the profile's pool size, then the server default (1 under the GIL). An explicit 0 requests the server default. |
| `--delivery-strategy` | strategy name | Override the profile's gateway delivery strategy (for example `full` or `tiered`). Defaults to the profile's own strategy; the distributed-2000 profile selects `tiered`. |
| `--delivery-workers` | int | Per-instance delivery-executor thread count (ADR-0022); requires `--subprocess-cluster`. |
| `--dual-drivers` | flag | Drive the cluster with two driver processes, one per instance cohort, at a fixed movement-window offset. Part of the certified distributed-2000 shape: it halves each driver loop's inbound replication load so a batch burst cannot delay the movement cadence past the flicker threshold. |
| `--native-apply` | flag | Run the movement-apply path on the C handler pool (ADR-0024) for `--embedded` hosts; the default keeps the Python apply path the embedded host is certified under. Requires `--embedded`. |
| `--server-affinity` | CPU list | Pin the `--subprocess-cluster` servers to these CPUs via a `taskset -c` argv wrapper (requires `--subprocess-cluster`). Default: no pinning. |
| `--proc-trace-output` | path | Write the raw `/proc` sampler trace and the per-client arrival series as JSON, keeping every sample the report's diagnostic tables summarize. |
| `--host` | host string | External server host (default `127.0.0.1`; ignored with `--embedded`/`--distributed`/`--subprocess-cluster`). |
| `--port` | int | External server gateway port (default `7777`; ignored with `--embedded`/`--distributed`/`--subprocess-cluster`). |
| `--format` | `md`, `json` | Output format printed to stdout (default `md`). |
| `--output` | path | Write the report to this file (stdout otherwise). |

Exit status: `0` when the gate passes, `1` when it fails.

Watching a run live: from a second terminal, `python -m tools.agent.live_view`
renders each instance's actor positions and publish-path counters while a
run executes. It discovers the cluster's server processes through `/proc`
(or takes `--hosts host:port,host:port`) and polls the control plane
read-only, GET requests only. The default sample mode reads a rotating
200-actor window per instance through single-actor reads with at most one
request in flight, so its control-plane load stays in the microsecond
class a certified run already tolerates; `--full-roster` pages the whole
roster per tick and is for non-certified viewing. The view is an observer
convenience, not part of the gate: killing it mid-run changes nothing
about the run or its report, and `--record PATH` (outside the run's
artifact tree) logs the frames it rendered as JSONL.

### Targeting an external server

Without `--embedded`, the harness targets a server at `--host`/`--port` and
builds one `AgenticHeadlessClient` per registered client through the
reference game's `make_ahc`. Use this to gate a server running a different
build (a release build, a different machine, a containerized deployment)
the embedded host cannot reach.

### Pinned-reference runs at scale

Comparative measurements (a worker-count sweep, a before/after lever
check) are only as good as their invariances: one variable changes per
comparison, and everything else is held constant and recorded. At the
distributed gate's operating point this needs host preparation beyond the
build:

- **Release build, pinned loader path.** Configure with
  `cmake --preset release && cmake --build --preset release` and export
  `KITH_LIB=build/release`: the loader prefers `build/debug` when both
  exist, and a Debug build multiplies every C hot path's cost
  several-fold.
- **File-descriptor ceiling.** Raise `ulimit -n` (65536 covers a
  2000-client run) before driving; the default soft limit (1024) fails
  with EMFILE near ~1000 clients.
- **Ring memory and the kernel entry cap.** The reactor backend creates
  one completion ring per instance with `max_fds + 16` submission slots,
  which the kernel rounds up to the next power of two. Ring pages are
  memcg-accounted kernel memory: since Linux 5.12 (commit `26bfa89e25f4`)
  the SQ/CQ ring arrays are charged to the cgroup memory allocator via
  `__GFP_ACCOUNT` rather than to `RLIMIT_MEMLOCK`, which since 5.12
  applies only to io_uring registered buffers (which the net layer does
  not use). Provision cgroup `memory.max`, not memlock, for the ring.
  The reactor passes `IORING_SETUP_CLAMP` (kernel flag since v5.6), so a
  `max_fds` that pushes the requested entry count above the kernel cap
  (32768 SQ entries) is clamped to that cap instead of failing; the
  actual post-clamp ring sizes are queryable via
  `kith_reactor_ring_sizes`. Ring creation retries `EINTR` up to eight
  times; setup failures surface as `KITH_EINVAL` (rejected parameters or
  unsupported flag), `KITH_EPERM` (io_uring disabled by the
  `kernel.io_uring_disabled` sysctl or a seccomp/LSM filter), or
  `KITH_ENOMEM` (allocation failure under cgroup pressure).
- **Free-threaded interpreter.** Run the fidelity gate under a
  free-threaded venv's interpreter directly (`.venv-ft/bin/python`,
  GIL disabled) per ADR-0020.
- **SMT topology before pinning.** On simultaneous-multithreading hosts
  cpu lists are enumerated pair-style (`{N,N+8}` on 8-core/16-thread
  desktops: cpu0's siblings are `0,8`). A literal split like driver
  `0-7` / servers `8-15` therefore SMT-shares every physical core
  between driver and servers. Read
  `/sys/devices/system/cpu/cpu0/topology/thread_siblings_list` first and
  split by physical core — driver `taskset -c 0-1,8-9`, servers
  `--server-affinity 2-7,10-15` — so the pinned sets are truly
  core-disjoint.
- **Frequency side-channel.** Governor behavior poisons tail metrics
  invisibly. Sample per-cpu frequency alongside any comparative run (one
  epoch-stamped `/proc/cpuinfo` line every ~100 ms into a side file); a
  clean run shows the active-max at boost clock for effectively all
  samples with no sustained sub-nominal stretch, which excludes
  downclock post-hoc without touching the measured path.
- **Raw traces over reruns.** Pass `--proc-trace-output` on every
  instrumented run: the persisted JSON keeps the `/proc` sampler trace
  and the arrival series, so scheduler-level questions (who ran, who was
  preempted, when frames arrived relative to tick boundaries) are
  answerable later without re-running the gate.
- **Record machine state.** Governor, core count, RAM, and an idle check
  belong in the run record next to the report; unrecorded host state
  invalidates cross-run comparison.

**Worker-pool sizing.** The worker pool is not free parallelism. During
synchronized input waves the pool competes for the instance's CPU slice,
and oversubscription turns into nonvoluntary-switch storms that stall
delivery punctuality while leaving average throughput intact. The
measured operating point on a 12-logical-CPU server slice (two
instances): pools of 12–16 workers per instance flicker, 8 workers pass
cleanly, with identical wave compute across the sweep — the switch
volume, not the work, forms the gaps. Size the pool so workers plus
reactors stay at or below roughly 1.5x the server slice's logical CPUs;
treat larger pools as throughput experiments against the punctuality
metrics, not defaults.

### Every-commit smoke profile

`smoke-dense` (24 actors, 0.4 s) and `smoke-distributed` (24 actors across
two instances, 0.4 s) are reduced profiles that validate the harness
machinery end-to-end (multi-client login, movement, sampling,
gate-metric computation, report rendering) without the 1000- or
2000-connection wall-clock cost of the full gate. They are driven by the
integration test `tests/integration/test_scaling_gates.py` every commit,
against relaxed thresholds that assert the harness produces a well-formed
report, not that the ceiling is broken. The full `dense-1000` and
`distributed-2000` profiles run in the weekly tier
(`AGENTS.md` §4.1).

## The thresholds

The two gate classes gate on different metrics (ADR-0020). The dense-1000
stress gate (`DENSE_THRESHOLDS`) certifies session integrity and liveness;
the distributed-2000 fidelity gate (`DISTRIBUTED_THRESHOLDS`) certifies the
full §4.4 thresholds per instance. The smoke profiles use relaxed
thresholds that validate the machinery, not the ceiling.

### Dense-1000 stress thresholds (`DENSE_THRESHOLDS`)

| Metric | Threshold | What it measures |
|---|---|---|
| `session_ok` | `>= 99%` | The fraction of registered clients that stayed connected and reached the bootstrap `READY` state. A client that dropped or never finished bootstrap fails this. |
| `selected_clients` | `>= 95%` | The fraction of clients that received an `actor_state` for a non-self actor through the replication stream. In the dense single-cell topology a connected subscriber receives a composed batch including other actors every tick, so a client that bootstrapped but saw no other actor indicates a view-composition or delivery failure. |
| `bootstrap_ms_p95` | `< 3000` | The 95th-percentile bootstrap latency in milliseconds, from client start to bootstrap `READY`. |
| `move_missing_ratio_certified` | reported | The fraction of submitted movement inputs the source did not certify as applied (ADR-0023's counter-delta rule). **Reported, not thresholded** on the stress gate: a degradation is visible in the report without failing the gate. Measured on a deeply instrumented sample (16 clients) whose event history holds the full movement-window stream. |
| `mm_event` | reported | The companion event-count reading (distinct echoed input ticks against submitted inputs). Reported, not thresholded, on the stress gate. |
| `continuity_flicker` | reported | Whether any instrumented client's own-actor stream showed a gap longer than 200 ms (four sim ticks at 20 Hz) between consecutive `actor_state` frames while the client was continuously submitting input and the actor remained in the same cell. **Reported, not thresholded** on the stress gate. A flicker is the actor being visible, dropped from the stream, then resumed — a view-composition or subscription bug. |

### Distributed-2000 fidelity thresholds (`DISTRIBUTED_THRESHOLDS`)

The dense stress thresholds apply per instance, plus the two
delivery-fidelity metrics become hard failures, plus the inter-instance
spread constraint:

| Metric | Threshold | What it measures |
|---|---|---|
| `session_ok` | `>= 99%` | Per instance — the fraction of that instance's registered clients that stayed connected and reached bootstrap `READY`. |
| `selected_clients` | `>= 95%` | Per instance — the fraction of clients that received a non-self `actor_state` through the replication stream. |
| `move_missing_ratio_certified` | `< 10%` | Per instance — the fraction of submitted movement inputs the source did not certify as applied, measured by the publisher-minted `update_seq` counter delta between self echoes across the movement window plus the post-window drain (ADR-0023). The counter advances once per applied input, so the reading is immune to the rebuild cadence's echo-sampling floor and cannot over-credit dropped inputs: a dropped input never mints. Measured on the deeply instrumented sample (16 clients). A client whose echoes carry no minted counter (a non-minting publisher or stamping disabled) falls back to the event-counting reading for that client and is reported as a certified fallback. |
| `mm_event` | companion | The historical event-count reading (distinct echoed input ticks over the full history against submitted inputs), retained under its own name so published gate history stays comparable across the ADR-0023 boundary. Reported, not gated: under saturation it floors at the rebuild-to-submit ratio (~32% at the gate operating point) even at zero real loss, which is why the certified rule carries the bar. |
| `bootstrap_ms_p95` | `< 3000` | Per instance — the 95th-percentile bootstrap latency in milliseconds. |
| `continuity_flicker` | forbidden | Per instance — no instrumented client's own-actor stream may show a gap longer than **200 ms** (four sim ticks at 20 Hz: three publish cycles plus one tick of delivery slack) between consecutive `actor_state` frames while continuously submitting input in the same cell. The threshold matches the harness constant `_FLICKER_GAP_MS`. |
| `publish_rate_skew` | `<= 3.0x` | The ratio of the highest to the lowest per-instance publish rate under normal spread; no single instance may be the fixed publish bottleneck. |

`session_ok` and `selected_clients` are breadth metrics measured across
every registered client. `move_missing_ratio_certified` (with its
`mm_event` companion) and `continuity_flicker` are
depth metrics measured on the deeply instrumented sample (16 clients, by
default) whose event history is large enough to hold the full
movement-window replication stream; the remaining clients carry a small
history and contribute only to the breadth metrics. The sample is kept
small so a 1000-client run's memory stays bounded; the depth metrics are
binary (flicker / no flicker, observed / missing), so the sample is
statistically meaningful for them.

The `smoke-dense` profile uses relaxed thresholds (`SMOKE_THRESHOLDS`):
`session_ok >= 90%`, `selected_clients >= 50%`,
`move_missing_ratio < 50%`, `bootstrap_ms_p95 < 5000`, and
`continuity_flicker` allowed. These assert the harness produces a sane
report; they do not assert the ceiling is broken.

## Interpreting a report

A report is a pure value object (`GateReport`) holding the profile,
metrics, thresholds, and run metadata. The `passed` property compares
metrics against thresholds; `to_markdown()` and `to_json()` render a
verdict and a per-metric table. A metric gated on the report's tier shows
its threshold with a `yes`/`no` verdict; a metric that is reported but not
gated shows `reported` in the threshold column and a `—` in the pass
column (it cannot fail the gate).

Dense-1000 (stress gate) markdown output:

```
# Scaling Gate: dense-1000 — PASS

**Started:** 2026-08-18T00:00:00+00:00
**Duration:** 6.123s
**Topology:** embedded
**Actors:** 1000 (sampled: 16)

## Metrics vs Thresholds

| Metric | Value | Threshold | Pass |
|---|---:|---:|:---:|
| session_ok | 99.60% | >= 99.00% | yes |
| selected_clients | 96.10% | >= 95.00% | yes |
| move_missing_ratio | 4.20% | reported | — |
| bootstrap_ms_p95 | 410 | < 3000 | yes |
| continuity_flicker | False | allowed | — |
| publish_rate_skew | n/a | n/a | yes |
```

The distributed-2000 (fidelity gate) report gates every metric: the
`move_missing_ratio` row shows `< 10.00%` with a `yes`/`no` verdict, the
`continuity_flicker` row shows `forbidden`, and the `publish_rate_skew`
row shows the `<= 3.00x` bound instead of `n/a`.

The report is the gate's record. A passing distributed-2000 report is the
evidence required to claim the N² ceiling is broken; a passing dense-1000
report certifies stress survival, not delivery fidelity. A failing report
names the metric that failed and by how much, which is the first diagnostic
step. A `FAIL` exit status from the CLI is non-zero, so the gate drops into
CI or a scheduled job as a single command.

## What the gate does and does not exercise

The gate exercises the production server path against real headless clients
over the real wire. It does not:

- **Assert per-tick latency or throughput.** The gate's metrics are about
  session health and replication continuity under load, not microbenchmark
  numbers; those belong to the performance tier (`AGENTS.md` §4.1).
- **Stress the connection plane beyond 1000 connections.** The weekly
  stress tier (`AGENTS.md` §4.1) drives 1000-plus concurrent connections
  against the connection plane directly; the scaling gate drives 1000
  actors through the full replication stack.

## Measurement caveats

The measurement properties below bound what a gate result proves; none is
a defect of the framework under test:

- **In-process harness co-tenancy (dense gate and smoke profiles).** The
  `--embedded` and `--distributed` flags boot the server in the same
  process as the asyncio clients and the metrics computation: the reactor
  thread, the worker threads, the client event loop, the ctypes frame
  callbacks, and the metric sampling share one Python process. The
  harness is simultaneously the load generator, the measurement
  instrument, and a major CPU co-tenant of the system under test. On a
  shared runner this biases the delivery-fidelity metrics (and the
  reactor's headroom) downward; a result that passes in-process is
  conservative, and a regression seen in-process warrants confirmation
  against an out-of-process run before drawing a framework conclusion. The
  full distributed-2000 gate runs out-of-process via
  `--subprocess-cluster` (one OS process per instance), so the harness's
  client-drive loop is decoupled from the servers' worker-pool GILs; the
  dense-1000 gate and the smoke profiles remain in-process (the dense
  gate's single reactor is the binding constraint, not GIL co-tenancy at
  1000 clients on one instance).
- **Cross-process coordination transport.** The `--subprocess-cluster`
  host runs one embedded topology per instance; each instance owns its
  whole cell space and its fabric storage is process-local, and each
  runs its own loopback coordination bus with one member. The gate does
  not drive split/merge, so the bus membership is inert for the gate's
  workload — the gate validates horizontal scaling (per-instance fidelity
  plus publish-rate skew), not cross-instance fabric replication or
  bus-driven authority transfer. A real cross-process coordination
  transport (the `KITH_COORD_BUS_TRANSPORT_*` enum's additive extension
  point) is a
  separate, additive capability for real distributed deployments and is
  not required by the gate's contract (ADR-0020).
- **Interpreter configuration.** The distributed-2000 fidelity gate
  certifies under free-threaded Python (`python3.14t`, GIL disabled) per
  ADR-0020: the `python_workers=8` profile drives 8 worker threads, and
  only a free-threaded build lets them run in parallel (ADR-0004's
  "N workers under a free-threaded build" rule). Under the GIL the same
  profile collapses to one active worker and `move_missing` rises to ~75%
  (~7.5x the 10% bar) — the GIL's structural ceiling, not a framework
  regression. A GIL run is a regression baseline; it does not gate.
- **Build type and machine state.** A Debug build multiplies every C hot
  path's cost several-fold, and a loaded or thermally throttled host swings
  the tail metrics. Pin gate runs to a Release/RelWithDebInfo build; every
  report already stamps the build type and machine state (kernel, CPU
  model and thread count, cpufreq governor, interpreter GIL status) it
  was produced under, so a report whose stamp disagrees with the
  certified envelope is a configuration-drift diagnostic, not a certified
  reading. Prefer the baseline comparator
  (`tools/perf/compare_scaling_baseline.py`) for run-to-run regression on
  a fixed host.
- **Login-burst admission tail.** A slow admission straggler (a client
  stuck seconds in the login burst) is invisible to the bootstrap
  latency metric by construction: the p95 is computed over the stamped
  sample, and the worst few percent never enter it. The known shape is
  admission serialization — `on_login` takes the control lock for actor
  spawn and binding, so a 1000-client burst queues on one lock, a
  contention property rather than a pool-size property. The integrity
  bars still bind such a client: an admission straggler that never
  clears boot fails `session_ok`/`selected_clients`, so the class
  cannot pass the gate silently. It did not trigger in any certified
  run (the certified dense runs read `session_ok` 100% with bootstrap
  p95 488/487 ms against the 3000 ms bar). A run whose census or
  integrity metrics trip on this class calls for re-derivation of the
  admission path, not a threshold change.
- **Tick pipeline latency and the continuity-flicker metric.** The
  per-tick publish cadence (ADR-0014) flushes a tick's dirty cells on the
  tick callback, and the gateway observes the flush on the *next* tick's
  refresh — a deliberate one-tick (50 ms at 20 Hz) pipeline latency that
  collapses per-input fabric traffic to per-tick (the "intended
  coalescing tradeoff" ADR-0014 records). That single tick of latency cannot by itself
  produce a continuity-flicker gap: the gap threshold is 200 ms (four
  sim ticks), so a flicker requires four consecutive publish cycles with
  no observable self-frame, not the one tick the deferred flush adds.
   The gaps are also independent of `move_missing_ratio`: distributed-gate
   runs have produced flicker on instances measuring 0% missing, so the
   boolean tracks delivery punctuality, not miss clustering. The measured
   gap structure at the distributed operating point is instance-wide and
   synchronized — every sampled client of one instance gaps within the
   same few milliseconds, in quasi-periodic trains roughly 230–350 ms
   apart with 200–250 ms durations — a per-instance stall signature, not
   a per-session composition failure. The two criteria therefore measure
   different failure modes; the pipeline lag remains a constant 50 ms
   offset within the delivery budget, not a flicker source.
- **Flicker-ledger leading edge.** The continuity-flicker ledger anchors
  each non-empty own-actor series at its movement-window start, so a late
  first frame registers as a gap.

## Wiring a new gate

A new profile is a `ScalingProfile` (name, actor count, topology, duration,
input cadence, settle window) and a `GateThresholds` entry in
`_PROFILE_THRESHOLDS`. The dense-topology drive (`_drive_dense` in
`tools/agent/load_harness.py`) boots one embedded host, registers and
starts clients through a semaphore-gated gather (each client stamps its
own connect-initiation timestamp for a true per-client bootstrap
latency), polls bootstrap, resolves
own actors against the server's `/query_state` roster, submits movement
inputs at the profile's cadence, and samples the metrics. The
distributed-topology drive (`_drive_distributed`) does the same against a
multi-instance host factory, resolving own actors per instance (each
instance allocates actor ids independently from 1) and additionally
computing `publish_rate_skew` from per-instance delivered-frame counts. A
profile that needs a different drive shape (a different movement pattern,
a different metric) extends the harness; a profile that needs a different
topology supplies a `LoadHostFactory` that boots it.

## References

- `tools/agent/load_harness.py` — the harness, profiles, thresholds, and CLI.
- `tools/agent/embedded_host.py` — the in-process embedded host factory.
- `tools/agent/distributed_host.py` — the in-process distributed host
  factory (multi-instance cluster on a shared coordination bus; smoke
  profile and split-merge validation).
- `tools/agent/subprocess_cluster_host.py` — the multi-process distributed
  host factory (one embedded-server OS process per instance; the full
  distributed-2000 gate).
- `tests/integration/test_scaling_gates.py` — the every-commit smoke
  profile tests (dense + in-process distributed + subprocess cluster).
- `AGENTS.md` §4.1 — the test tier table (weekly placement of the
  full gate).
- `AGENTS.md` §4.4 — the numeric gate definitions and the
  intermediate-count insufficiency rule.
- `docs/architecture/adr/0020-two-tier-scaling-gates.md` — the gate-class
  decision (stress class vs fidelity class) and the certified
  configuration.
- `docs/guides/agentic_headless_client.md` — the headless-client
  harness the load harness reuses.

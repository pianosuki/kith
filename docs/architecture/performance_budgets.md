# Performance Budgets and Capacity Model

This is the definitive budget and capacity model for the framework. The
per-operation budgets below are the per-unit microsecond targets; this
document reconciles them with the §4.4 scaling gates by aggregating them
into the per-tick totals that actually gate the fidelity metrics, and
records the per-instance fidelity ceiling those totals imply. Every
scaling-gate run and every ADR performance claim cites this model and
reports the remaining headroom against it. No automated per-operation
regression lane exists: the budgets bind through the gates' fidelity
metrics (the aggregate lines below are what the gate measures), and the
apply-capacity bench (`tools/perf/`) records the apply-path measurements
the capacity model cites — its every-commit test guards the harness, not
the numbers.

## 1. Per-operation budgets

| Path | Budget | Notes |
|---|---|---|
| Reactor tick (per connection, per message) | < 10 us | The wire-dispatch path on the reactor thread, before any handler runs. |
| AOI query (circle, 100 actors) | < 50 us | Spatial index probe. |
| Sim step (1000 actors) | < 500 us | A tick-batched step of all actors in C, as the deterministic-simulation contract and its per-tick game-logic callback (ADR-0014) specify. The per-input Python stepping the reference game runs is the cadence this budget assumes. |
| Fabric cell bundle build (per cell) | < 100 us | Per-cell publish-product serialization. |
| Gateway relevance compose (per player, budget K) | < 20 us at K=50 | Parameterized by the view budget K. Cost scales ~K log N on the output sort and is K-independent on the candidate scan, so K=512 runs ~1-3x this line, ~20-60 us/player. K=512 is the shipped default (ADR-0005) and the dense-stress and gate operating point; K=50 is the illustrative tuned-down budget. Both are valid. |
| Gateway tiered deliver pass (per player, budget K) | < 25 us at K=512 | Per-session decide + enqueue + commit over the composed set. Suppression compares the serializer's input scalars against the ledger, so a suppressed subject builds no record and serializes nothing; the floor is real send work (serialize, assemble, enqueue for due subjects) plus one ledger search per subject. Measured ~28 us/player/tick averaged over fresh and retained views at the frozen distributed-2000 operating point (1000 clients/instance, all-march scenario, every subject's own actor moving each tick), above the line, which the capacity model's ceiling section records. |
| Partition cell handoff | < 1 ms | Split/merge authority transfer (ADR-0003). |
| Python handler dispatch (C->Python boundary) | < 100 us | Per dispatch. At 20k inputs/sec (1000 actors x 20 Hz) this is 2 cores of Python boundary, served by the ADR-0004 worker pool (N workers under free-threaded Python, 1 under the GIL). A pool-bound handler is never run inline on the reactor as a fallback (ADR-0004): on pool exhaustion the submission fails `KITH_EBUSY` and the drop is counted, never invoked on the reactor. |
| Server egress | derived | K x record_size x N_clients x tick_rate. At K=512, the shipped 68-byte subject record (ADR-0023's widening), 1000 clients, 20 Hz this is ~696 MB/s; at K=50 it is ~68 MB/s. Bounded by the relevance budget (ADR-0005) and by the transport (loopback in tests, the NIC in production). |
| Reactor thread total (per tick) | < one core at the tick rate | The sum: cache refresh + N x compose + N x serialize/frame/enqueue + the socket drain. This is the budget that gates `move_missing_ratio` and `continuity_flicker`. At 20 Hz one tick is 50 ms; the sum must fit one reactor core in that window. |

The first eight lines are per-operation microsecond targets; the last two
are the aggregate quantities the per-operation lines compose into. The
server-egress and reactor-thread-total lines are the two quantities that
gate the fidelity metrics.

## 2. The capacity model

The per-operation budgets compose into a per-tick cost chain. Reading it
end to end turns a gate failure from a mystery into a prediction:

```
inputs/sec -> dispatches/sec -> publishes/sec -> lock acquisitions/sec
          -> pending cells/cycle -> refresh cost/cycle
          -> compose cost/cycle -> serialize bytes/cycle
          -> socket drain/cycle -> reactor thread total/cycle
          -> compare to one core at the tick rate
```

Each arrow is a multiplication by a quantity the architecture fixes:

- inputs/sec = N_actors x input_hz. At the gate (1000 actors, 20 Hz) this is 20k.
- dispatches/sec = inputs/sec. Each input dispatches one handler; at <100 us
  per dispatch the Python boundary alone is 2 cores, served by the worker pool.
- publishes/sec depends on cadence. The per-tick cadence (ADR-0014)
  is one cell-product publish per dirtied cell per tick; the per-input cadence
  is N_actors x input_hz publishes/sec. At 1000 actors the per-input cadence
  is 1000x the per-tick cadence and amplifies fabric-lock traffic by the same
  factor. Cadence is a model lever, not a fixed constant.
- pending cells/cycle = cells dirtied since the last drain. With per-tick
  coalescing this is the cells touched this tick; with per-input publishing
  every active cell is permanently pending and the refresh rebuilds all of
  them every cycle.
- refresh cost/cycle = pending_cells x per_cell_snapshot. The per-cell
  snapshot is O(shard) without a per-cell index and O(cell) with one.
- compose cost/cycle = N_clients x per_player_compose(K).
- serialize bytes/cycle = N_clients x K x record_size; per cycle this is the
  egress line above, divided by the tick rate.
- socket drain/cycle = the reactor-thread time spent pushing the enqueued
  frames into kernel socket buffers. Delivery only enqueues; the wire
  readiness handler drains each output queue with a synchronous writev loop
  bounded per call by `out_drain_cap` (default 64 KiB = two full-budget batch
  frames; `UINT32_MAX` restores the unbounded loop): the first writev of a
  call always executes and the call stops once the bytes it wrote this call
  reach the cap, pacing any residual across subsequent readiness events.
  Each cap-truncated call is counted (`kith_net_write_deferrals_total`). One
  call costs at most about the cap's worth of memcpy; per tick the drained
  bytes still sum to N_clients x bytes/tick.
- reactor thread total/cycle = refresh + compose + serialize/frame/enqueue
  + socket drain, all on the one reactor thread. Workers do not help this
  path; they only add contention to the locks it reads.

### The per-instance fidelity ceiling

In the distributed topology the reactor-thread-total line is dominated by
the per-client delivery and compose terms (the cache refresh is sublinear
or fixed by the spread). One core delivers one tick of work per tick
interval, so there is a ceiling N* at which the reactor-thread-total equals
one core at the tick rate. Above N* the reactor cannot compose and deliver
every tick: the compose-budget pass defers recompositions and the
missed-input ratio climbs. To first order, with a fixed per-session cost,
the missed-input ratio is:

    miss(N) = max(0, 1 - N* / N)

The measured regime departs from this in two ways, recorded below: the
deferral dynamic compounds the loss past the knee, and the per-session
cost itself rises with population density.

Two delivery regimes have been calibrated on this model.

Full-resend delivery (the `full` preset), 2026-08-21: the missing ratio was
0.0% at 500 clients/instance and 49.0% at 1000, tracking the one-parameter
model with N* ~ 500 (Release/LTO, free-threaded Python 3.14.7 with GIL
disabled, 16 workers, the out-of-process subprocess-cluster harness, AMD
Ryzen 7 3700X, `powersave` governor, 62 GiB RAM, `ulimit -n 65536`). The
49% cliff at 1000/instance is the single-reactor delivery pipeline running
at ~2x its tick budget, not a distributed-topology defect and not a flicker
artifact. This calibration predates the tiered preset and the window-scoped
missing-ratio collector; it is the encoding-axis history the tiered preset
was measured against, not the current ceiling.

Tiered delivery with record-granular suppression (ADR-0021), measured
2026-08-27 at the commit that shipped the payload-scalar suppression,
same envelope with the 8-worker profile default:

| Per-instance load | compose deferrals/window (inst 0 / 1) | move_missing | flicker |
|---|---|---|---|
| 200 | 0 / 0 | 0.74% | False |
| 300 | 0 / 0 (two runs) | 5.89% / 10.98% | False |
| 400 | 6085 / 5135 | 18.14% | False |
| 500 | 18915 / 18424 | 27.11% | False |
| 1000 | 70854 / 73030 | 82.46% | True |

Every run: session_ok 100%, bootstrap p95 141-529 ms, publish-rate skew
<= 1.02x, zero write deferrals, zero locate failures, all six points from
the same profile and cadence with only the client count re-baselined. The
2026-08-21 calibration ran 16 handler workers, this ladder the 8-worker
profile default; the worker count is not load-bearing for the comparison —
workers serve the Python dispatch boundary, not the tick-thread
compose/deliver path that sets the ceiling (they add contention to the
locks it reads, never capacity to it). The
compose-budget deferral knee sits between 300 and 400 clients/instance: at
or below 300 the pass admits every due recomposition (zero deferrals); at
400 about 21% of recompositions per tick defer, about 56% at 500, and
about 73-80% at 1000 (the window-tick count T is bounded below by
deferrals divided by sessions — 71 at 1000/instance — and estimated at
88-97 from the movement-window span; the fraction reads
deferrals/(sessions x T) across that range). The deferred session repeats
its retained view, so past the knee the missing ratio grows faster than
the one-parameter model predicts, and the per-session compose+deliver cost
itself rises with density (view occupancy grows with the corridor
population until the 512 budget caps it), which pulls the knee below any
fixed-per-session extrapolation. Below the knee the residual missing ratio
(0.74% at 200; 5.89% and 10.98% across two runs at 300) is not budget
loss: with zero deferrals every due composition ran, and the residual is
observational — its shape matches the one-tick flush observation floor, an
input applied after a compose has read its cell is observed one tick
later, and a second application in between supersedes it whole. That floor
is timing-phase dependent and
run-noisy; it straddled the 10% bar at 300/instance across the two runs.

### The apply-path regimes

The ladder above measures one of two calibrated apply-path regimes, and
the two must not be conflated. In the Python-apply regime the movement
apply runs as a Python handler on the shared worker pool (~1.1 ms per
apply at 1000 clients/instance); at full load the pool saturates and
rejected dispatches compound the ladder's losses. In the native-apply
regime (ADR-0024, the certified gate configuration) the apply runs as a
C pool job (~75 us) and the Python pool holds headroom: applies equal
dispatches with zero dispatch-dropped at 1000 clients/instance. Under
the native-apply regime the certified counter-delta rule (ADR-0023)
reads 0.00% certified move-missing at 1000 clients/instance with no
continuity flicker — the fidelity gate's certified envelope — while
compose-budget deferrals still occur (12-15k per window per instance):
deferral re-delivers a session's retained view, and the certified rule
attributes none of that as input loss where the event-count reading
counts every re-delivered pass. The ladder therefore states the
Python-apply event-count ceiling; the gate certifies the native-apply
counter-delta envelope. Both regimes are real; each number names its
regime and its collector.

The dense single-cell case (all 1000 actors in one cell, K=512) is a
different, worse regime: the compose term becomes O(N^2) and the egress line
hits ~696 MB/s into one process. That is the adversarial case the embedded
topology is not the fidelity claim for (ADR-0020); the relevance budget
(ADR-0005) bounds it by degrading density, not by guaranteeing full-fidelity
pileup replication on one gateway.

### The socket-drain term and the two gating regimes

The per-tick cost chain ends in a term with a per-call bound and no
per-tick one: delivery serializes and enqueues frames into per-connection
output queues, and the wire readiness handler drains each queue on the
reactor thread with a synchronous writev loop. The loop is bounded by
`out_drain_cap` (default 64 KiB = two full-budget batch frames; the first
writev always runs, so a normal single-batch drain is untouched, while a
multi-batch backlog paces across subsequent readiness events). The drain
is counted (`kith_net_write_ns_total`) and so is its pacing: each call the
cap truncates with output still queued increments
`kith_net_write_deferrals_total`, which separates paced drains from drains
that finished within their pass and from kernel-backpressure stops.

Measured at 500 clients/instance (the faithful distributed-2000 replica:
two instances, free-threaded Python 3.14.7, Release/LTO, AMD Ryzen 7 3700X,
`powersave`, 2026-08-22): the drain is the single largest reactor-path
term — 648–681 ms per 5 s movement window per instance, about twice the
compose term (332–391 ms) and about twice the deliver term (319–341 ms).
Counted reactor work rises from ~0.84 s to ~1.36 s of the 5 s window
(~17% → ~27%). The drain paces only when per-call backlog exceeds the
64 KiB cap, and `kith_net_write_deferrals_total` reads 0 in every
instance-window measured at this scale: steady-state backlog is one batch
frame, so the cap ships as forward insurance rather than an active bound.

Kernel accounting during the continuity-flicker cluster windows
(per-process `/proc` sampling at 100 ms, servers and driver sampled
separately) bounds the mechanism from both sides: inside every window the
gapping instance's reactor thread runs at 30–43% duty against a 20%
baseline — elevated but never saturated, never blocked — while the
instance's sixteen handler workers surge to 2–3 cores under thousands of
nonvoluntary context switches, and the single-threaded load driver sits at
100% duty through the whole run. No thread freezes in any window (ruling
out a stop-the-world pause), core frequency never leaves maximum boost
(ruling out governor downclock), and the reactor never goes quiet (ruling
out a blocking call). Two mechanisms fit these signatures: the saturated
driver hiccuping on its read path and delivering queued ticks as a burst,
or free-threaded worker-pool scheduling contention on the server side; a
replica run with the driver pinned to a cpuset disjoint from the servers'
discriminates them.

The two fidelity metrics are gated by different regimes of the same
reactor-thread-total line:

- `move_missing_ratio` is gated by the average per-tick sum. Below the
  compose-budget knee (300-400 clients/instance on the tiered ladder above)
  it sits at the one-tick flush observation floor; above the knee the
  deferral fraction owns it. Cutting compose ~45% (the unchanged-view skip)
  and suppressing already-sent records (ADR-0021) does not move the knee:
  the per-client delivery cost that dominates the chain — serialize +
  enqueue + drain for fresh views — and the compose pass itself both still
  spend the same 25 ms budget clock. With suppression the deliver phase
  spends 7-11% less per window than without it, and the compose phase
  ~25% more (886/970 to 1105/1200 ms per window at 1000 clients/instance),
  the freed budget re-spent on more compositions.
- `continuity_flicker` is gated by sustained multi-tick delivery holes.
  The measured clusters are instance-wide synchronized 200–250 ms holes at
  aligned input-tick ranges with zero frame loss at 500
  clients/instance, where the average per-tick sum still fits one tick.
  The counted reactor-path terms do not explain them: the drain pacing
  never engages at this scale and the reactor thread is not saturated
  inside the windows. The hole body lives outside the reactor-path chain,
  in the region the driver/server cpuset-isolation run bounds. At 1000
  clients/instance the clusters are saturation symptoms instead: the tick
  thread runs at ~99% duty inside every gap window and every sampled
  client shows the same four gaps.

The bounded composition pass bounds the worst-tick compose cost at
`compose_budget_us` regardless of storm magnitude: past the budget a
session with a composed view delivers its retained set — frame cadence is
preserved, staleness is bounded by the head-of-pass guarantee (one pass per
remaining stale session), and `kith_gateway_compose_deferrals_total`
distinguishes budget exhaustion from no-change skips. Engagement is
count-dependent: under the deferral knee it is zero (every due composition
runs), while at the 1000 clients/instance gate point roughly three
quarters of recompositions defer per tick — at that population the budget,
not the compose path's internals, is the binding constraint.

### Validation depth

The model is validated at the gate-metric level above: the cliff location
and the miss ratio at two endpoints, plus the per-phase reconciliation the
reactor-path counters enable. The counters are landed and scraped by every
gate run: per-window refresh/compose/deliver, the socket-drain total, the
compose sub-phase split, the compose-deferral counter, and the
write-deferral counter. The 2026-08-22
reconciliation they produced fixed the drain term's place in the chain
(the largest single reactor-path cost had been absent from the model) and
bounded the flicker mechanism: the counted reactor-path chain does not
contain the hole body, which the driver/server isolation run localizes.

## 3. Budget-to-gate reconciliation

| Gate metric (AGENTS.md §4.4) | Budget line that gates it |
|---|---|
| session_ok, selected_clients, bootstrap_ms_p95 | Connection-accept and handshake path; not on the hot tick path. The reactor-tick-per-message budget bounds handshake dispatch. |
| move_missing_ratio_certified | Reactor thread total. Miss begins when the per-tick sum exceeds one core. |
| continuity_flicker | Sustained multi-tick delivery holes. Measured clusters are instance-wide synchronized 200–250 ms holes at aligned tick ranges with zero loss; the counted reactor-path terms bound but do not explain them (drain pacing inert at default, reactor duty 30–43% inside windows vs 20% baseline). The driver/server cpuset-isolation run localizes the hole body between driver ingest and server-side dispatch contention. |
| publish_rate_skew_max | Per-instance reactor thread total; skew is the ratio of the slowest instance's delivery rate to the fastest. |

The dense-1000 gate (embedded, stress) reports `move_missing_ratio` and
`continuity_flicker` but does not threshold them (ADR-0020): the embedded
single-cell pileup is the adversarial case the backend's fidelity guarantees
are not the answer to, and demanding full-fidelity pileup replication on one
laptop process fights the egress and reactor-thread-total lines rather than
the architecture. The distributed-2000 gate (fidelity) thresholds them per
instance.

### Machine capacity envelope

A gate result is a measurement of a specific machine, not a property of the
framework. Every gate report records: build type (Release/RelWithDebInfo),
core count and clock (a `powersave` governor caps the per-core delivery
rate that N* depends on), RAM, and whether the harness is in-process (a CPU
co-tenant of the system under test) or out-of-process. A run that omits the
machine envelope is not a measurement; the in-process co-tenancy and the
governor state are the two largest confounders in the per-instance ceiling.

The distributed-2000 gate runs 1000 clients/instance (2000 clients across two
instances). That sits roughly three times above the measured per-instance
ceiling on the tiered ladder above: the compose-budget deferral knee sits
between 300 and 400 clients/instance; on the Python-apply event-count
ladder, the 10% bar is met at 200/instance, straddled at 300/instance with
zero deferrals, and breached by the deferral regime from 400/instance up.
The per-tick demand
at 1000/instance is real work — fresh-view frames of up to 512 records
each, the compose scan and sort over the same population, and the socket
drain — all on the one reactor thread. Suppression removes the
wasted serializations: the tiered suppression compares the serializer's
input scalars against the ledger, which eliminates roughly ten million
discarded record serializations per second at this operating point and
runs the deliver phase 7-11% shorter than without suppression. What
raises the ceiling is therefore structural: taking compose or deliver
work off the tick thread, cutting the per-session compose+deliver cost
below its measured shares, or spreading the same clients across more
instances. The
gate stays at 1000/instance as the production fidelity target; the
capacity model records the measured ceiling, the budget line that sets it,
and what raises it.

### The measured capacity table

The model's measured points compose into one planning view: the player
loads measured to date, the regime each ran under, and the outcome.
Every row shares one recorded envelope: AMD Ryzen 7 3700X (8 cores,
16 threads), the out-of-process subprocess-cluster harness, Release/LTO,
free-threaded CPython 3.14.7 with the GIL disabled, the `powersave`
governor as found, 62 GiB RAM, and `ulimit -n 65536`.

The certified configuration is part of the claim (ADR-0020): the native
movement-apply path, two delivery-executor workers, one driver process
per instance cohort, and pinned cores per the recorded reference
envelope; a run outside that shape is a baseline or a diagnostic, not a
certified reading.

| Players | Instances x clients/instance | Delivery / apply regime | Measured outcome | Notes |
|---|---|---|---|---|
| 400 | 2 x 200 | tiered, Python apply, 8 handler workers | move_missing 0.74%, zero compose deferrals, flicker False | below the compose-budget knee; the residual is the one-tick observation floor |
| 600 | 2 x 300 | tiered, Python apply, 8 handler workers | move_missing 5.89% / 10.98% across two runs, zero deferrals, flicker False | straddles the 10% certified bar with zero deferrals |
| 800 | 2 x 400 | tiered, Python apply, 8 handler workers | move_missing 18.14%, about 21% of recompositions deferring | first point past the knee |
| 1000 | 2 x 500 | tiered, Python apply, 8 handler workers | move_missing 27.11%, about 56% of recompositions deferring | |
| 2000 | 2 x 1000 | tiered, Python apply, 8 handler workers | move_missing 82.46%, about 73-80% deferring, flicker True | the Python-apply event-count ceiling |
| 2000 | 2 x 1000 | tiered, native C apply, the certified shape: 8 handler workers, 2 delivery-executor workers, one driver process per instance cohort, servers pinned to 6 physical cores, drivers to 2 | certified move-missing 0.00%, flicker False, zero dispatch-dropped | the fidelity gate's certified envelope; compose deferrals still occur (12-15k per window per instance) and re-deliver retained views |
| 1000 | 1 x 1000 | embedded, single cell, Python apply | stress gate: session_ok 100%, bootstrap p95 487-488 ms; fidelity metrics reported, not thresholded | the adversarial pileup; the ~696 MB/s egress line at K=512 |

The five ladder rows are the tiered-delivery ladder measured 2026-08-27
(the per-instance fidelity ceiling above); the certified
row is the same per-instance load under the native-apply regime's
counter-delta reading; the dense row is the certified dense-1000 stress
point.

## 4. Usage rule

Every scaling-gate run cites this model and reports the remaining headroom
against the per-instance ceiling (N* and the measured miss at the gate's
per-instance load). Every ADR that makes a performance or scaling claim
cites the budget line the claim depends on. A fix to a hot path updates the
model's cost terms and shows the new headroom; a claim that the N^2 ceiling
is broken cites the distributed-2000 result and the N* it implies. The
capacity model is the instrument that turns gate work from ceiling discovery
at 20+ minutes per run into ceiling prediction.

## References

- `AGENTS.md` §4.4 — the numeric gate definitions.
- ADR-0001 — five-plane world-stream fabric architecture.
- ADR-0004 — the C/Python threading boundary; the worker pool and the
  no-inline-fallback conformance rule.
- ADR-0005 — bounded relevance budgets; the 512-subject default.
- ADR-0014 — the deterministic-simulation contract (the tick-batched step).
- ADR-0020 — the scaling-claim gates (stress and fidelity classes) and
  the certified configuration.

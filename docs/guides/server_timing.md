# Server timing and logical simulation time

The server provides rational scheduling for target rates from 1 through 1000 Hz.
A zero argument uses the configuration source's rate, or the 20 Hz default when
no nonzero rate is configured. Values above 1000 remain accepted for
compatibility, including values supplied by the configuration source. They
retain the legacy zero-millisecond interval and cached-time rescheduling:
best-effort execution without a precise frequency guarantee. Their gateway
refresh and wait defaults retain the previous zero-value fallback behavior. This
legacy mode is not a certified high-frequency simulation profile. The rate is a
scheduling target, not a throughput guarantee.

## Timer deadlines

The first callback is scheduled immediately when `kith_server_run` starts. For
rates 1..1000, subsequent deadlines lie on a grid anchored to that run's
monotonic clock sample. Grid slot `n` has the deadline
`epoch + ceil(n * 1000 / tick_hz)` milliseconds. At 60 Hz the spacing alternates
between 16 and 17 ms; at 120 Hz it is 8 or 9 ms. Sixty or 120 grid intervals
span exactly one second. The grid has less than one millisecond of rounding
error and does not accumulate callback delay into its phase. Divisor rates such
as 20 Hz retain their 50 ms spacing.

For rates 1..1000, after maintenance and callback dispatch, the server reads
fresh monotonic time and selects the next strictly future grid slot. A stalled
reactor executes one normal tick when it resumes, skips missed slots, and
schedules one future callback. There is no replay loop or catch-up burst. This
bounds per-wakeup work; simulation that must recover elapsed wall time needs an
application policy with its own work bounds. Worker callbacks have their
separate admission and ordering contract; timer cadence cannot guarantee
callback completion by a deadline.

Timer resolution, OS scheduling, IO, maintenance and callback cost affect actual
wakeups. Rates near the timer resolution are especially sensitive to lateness.
The scheduler does not promise nanosecond wakeups or 1000 completed simulation
steps per second. No server-rate performance qualification follows from the
supported numeric range.

## Logical time and model steps

The callback's `tick` increases once per normal server tick, beginning at one;
shutdown maintenance does not increment it. It counts executed server ticks, not
missed wall-clock grid slots or completed worker callbacks. A callback refusal
can therefore leave gaps in the indices a handler receives. The server does not
automatically invoke a sim model or choose its `dt_ms`.

`kith_sim_model_step` retains its integer-millisecond `dt_ms` contract. For a
deterministic 60 Hz model, a rational remainder accumulator can produce 16, 17,
17 ms repeatedly instead of always passing 16 ms:

```c
/* State belongs to one serialized simulation owner. */
uint32_t remainder = 0;

/* Once for each model step actually performed, with hz in 1..1000. */
remainder += 1000;
uint32_t dt_ms = remainder / hz;
remainder %= hz;
```

Sixty such steps total 1000 ms, independent of timer jitter. The variable
integer durations preserve cumulative time; they are not a fixed fractional
millisecond integrator. A model that requires an exact fixed `1/hz` step can own
that rational time base in native state. Neither approach silently advances
simulation for refused callbacks or skipped wall-clock slots.

The public tick clock offers `kith_tick_clock_elapsed_ms_precise` and
`kith_tick_clock_elapsed_ns_precise`. They calculate
`floor(tick * units_per_second / tick_hz)` without intermediate overflow;
rounding error stays below one output unit. Advance an application-owned clock
once per completed model step, then subtract consecutive elapsed values to
obtain integer step durations. Keep calls serialized with that state. The
original `kith_tick_clock_elapsed_ms` and `kith_tick_clock_elapsed_ns` functions
are deprecated since 1.1.0. They retain their
`tick * floor(units_per_second / tick_hz)` results and remain available
throughout 1.x. They are scheduled for removal in 2.0.0; migrate to the precise
variants before that release.

Migration changes results at rates that do not divide the output unit evenly. At
60 Hz, 60 ticks report 960 ms through the original millisecond conversion and
1000 ms through the precise conversion. Preserve the original calculation where
existing model behavior or recorded results depend on a constant integral step.
Precise conversion can also report overflow earlier because its result is
larger. Nanosecond conversion does not change the model's integer-millisecond
step signature.

Generated Python ctypes bindings retain both conversion families. C compiler
deprecation diagnostics do not become Python runtime warnings.

The replay tool retains its existing constant rounded-millisecond integration
policy. Switching an application to rational step durations does not change that
policy or make existing recordings describe the new duration sequence.

## Publication and delivery

Gateway maintenance runs before the simulation callback is dispatched. Callback
publications become eligible for observation on a later gateway refresh,
normally the next tick if work completes in time. Product updates, cache
refresh, relevance selection, composition and network delivery retain their own
contracts; timer correction does not bypass them.

The default cache/view refresh thresholds and gateway work budgets still use the
minimum spacing `floor(1000 / tick_hz)`. Explicit refresh intervals pass through
unchanged. These are maintenance thresholds and budgets, not the logical model
duration or proof of one publication per simulation step.

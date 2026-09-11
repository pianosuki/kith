# Open Range — the visual example

An out-of-box playable demonstration of the kith framework: one command
boots a living world — a seamless 512×512-unit plain, ambient actors
wandering under the same momentum model the players use, cell-scoped
chat, and a live metrics route — and the graphical client renders it
through the certified harness client engine.

## Install

The wheel runs on Linux x86-64 with glibc 2.38 or newer; older systems
raise a load error at `import kith`.

```
pip install kith-fw            # server, drivers, `kith-visual` launcher
pip install "kith-fw[visual]"  # + the graphical client's window renderer
```

The visual extra pins the window renderer to the exact prerelease the
example is verified against — the base install never pulls it.

From a source checkout, build first (`cmake --preset debug &&
cmake --build build/debug`) and install editable; the example resolves
its shared libraries through the package's normal discovery order.

## Play

One command (installs the visual extra):

```
kith-visual play crowd-in
```

Two terminals (window optional — every scenario runs headless):

```
kith-visual run density-pile          # t1: prints  playground ports: gw=… ctl=…
kith-visual client --port <gw> --control-port <ctl>   # t2: the window
```

`kith-visual serve` boots a quiet world (just the ambient actors);
connect a client whenever you like.

## Scenarios

| scenario | what you feel |
|---|---|
| `crowd-in` | bots converge on your cell — view budget, tier styles, ghosts |
| `density-pile` | bots pile into your cell — full vs tiered delivery budget |
| `churn-storm` | sessions connect/disconnect around you |
| `membership-churn` | bots patrol a cell edge — window churn, no reconnects |
| `kill-reconnect` | the server dies on a cadence; the client and bots rebind |
| `tick-ladder` | the server relaunches down 20/62/125/142 Hz — smoothness per rung |
| `soak` | sustained wander load — watch for drift over time |

## Controls and HUD

- **WASD** move · **Shift (hold)** run · **Enter** chat · **Esc** cancel
- **Tab** toggles RAW / INTERP. RAW draws the last received wire
  position (the wire truth). INTERP draws `now - interp buffer`
  (default 2 ticks; `--interp-ms` overrides). Use RAW when hunting
  rubberbanding and slingshots, INTERP to judge smoothness.
- HUD: fps, records/s, per-actor staleness coloring, arrival jitter,
  the snap-event log (a rendered displacement jump above one tick of
  max run velocity — the rubberband detector), product-tier styles,
  membership ghosts, input-echo lag, ping RTT, and the live
  `/playground/metrics` panel (suppressed, drops, delivery totals).

## Server knobs

`kith-visual serve` and `run` pass the feel axes through to the
composition root: `--tick-hz`, `--delivery-preset full|tiered`,
`--view-max-subjects`, `--view-refresh-ms`, `--cache-refresh-ms`,
`--npcs`, `--replication-batch-type-id`. Movement follows the
publish-choreography law — inputs buffer, the tick steps and publishes —
so movement speed is independent of the input rate and the tick rate.

## Notes

- The client's staleness/ambient coloring reads the server's metrics
  route; pass `--control-port <ctl>` for the full panel.
- `--hud-log PATH` and `--snap-log PATH` opt into JSONL streams of the
  HUD state and snap events (both default off).
- The tick-ladder and kill-reconnect scenarios use the fixed port 7777
  so the window rides server restarts; a stale server on that port
  fails the next spawn loudly — stop it and retry:
  `pkill -f 'kith(\.examples\.visual\.serve[r]|-visual serv[e])'`.
- Linux only (the framework's reactor is io_uring-based).

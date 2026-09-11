"""The visual example's console entry point: list, launch, and play the
Open Range scenarios out of the box.

Commands forward their arguments to the owning module's parser:

- ``list``    the scenario catalog
- ``serve``   a quiet world (ambient actors only)
- ``run``     one scenario: server + bot cohort (headless-capable)
- ``client``  the graphical window against a running server
- ``play``    one command: server + cohort + window

``play`` is the zero-setup path: it boots a server, opens the window,
and runs the scenario around it. The window needs the distribution's
visual extra; every other surface runs on the standard-library-only
core.
"""

from __future__ import annotations

import contextlib
import subprocess
import sys
import time
from collections.abc import Callable


_USAGE = """usage: kith-visual <command> [arguments]

commands:
  list                  the scenario catalog
  serve [server-flags]  a quiet world (ambient actors only)
  run <scenario> [...]  one scenario: server + bot cohort
  client --port <gw>    the graphical window
  play <scenario> [...] one command: server + cohort + window

scenario help: kith-visual run --help
"""

_SCENARIO_CATALOG: tuple[tuple[str, str], ...] = (
    ("crowd-in", "bots converge on your cell — view budget, tiers, ghosts"),
    ("density-pile", "bots pile into your cell — full vs tiered budget"),
    ("churn-storm", "sessions connect/disconnect around you"),
    ("membership-churn", "bots patrol a cell edge — window churn, no reconnects"),
    ("kill-reconnect", "the server dies on a cadence; everything rebinds"),
    ("tick-ladder", "the server relaunches down the tick rungs (20/62/125/142)"),
    ("soak", "sustained wander load — watch for drift over time"),
)

# Scenarios that own a fixed-port server lifecycle (the window rides the
# restarts through that one stable port); every other scenario attaches
# to a server `play` spawns itself.
_FIXED_PORT_SCENARIOS = frozenset({"kill-reconnect", "tick-ladder"})

_CLIENT_EXIT_GRACE_S = 5.0


def _list(rest: list[str]) -> int:
    del rest  # the catalog ignores any extra arguments
    print("Open Range scenarios:")
    for name, description in _SCENARIO_CATALOG:
        print(f"  {name:<18} {description}")
    print("  serve             a quiet world — just you and the ambient actors")
    print()
    print("One-command play:  kith-visual play <scenario>")
    print("Two-terminal flow: kith-visual run <scenario>  +  kith-visual client --port <gw>")
    return 0


def _serve(rest: list[str]) -> int:
    from kith.examples.visual import server

    server.main(rest)
    return 0


def _run(rest: list[str]) -> int:
    from kith.examples.visual import drivers

    return drivers.main(rest)


def _client(rest: list[str]) -> int:
    _require_arcade()
    from kith.examples.visual import client

    client.main(rest)
    return 0


def _play(rest: list[str]) -> int:
    from kith.examples.visual import _launch, drivers

    args = drivers.parse_args(rest)
    if args.scenario in _FIXED_PORT_SCENARIOS:
        # The driver owns the fixed-port server; the window rides the
        # restarts through that one stable port.
        client = _spawn_client(["--port", str(args.port), "--tick-hz", str(args.tick_hz)])
        try:
            return drivers.main(rest)
        finally:
            _stop_client(client)
    server_proc, gw, ctl = _launch.spawn_server(
        tick_hz=args.tick_hz,
        npcs=args.npcs,
        preset=args.preset,
        python_workers=args.python_workers,
    )
    print(f"playground ports: gw={gw} ctl={ctl}", flush=True)
    client = _spawn_client(
        ["--port", str(gw), "--control-port", str(ctl), "--tick-hz", str(args.tick_hz)]
    )
    try:
        return drivers.main([args.scenario, "--attach", f"{gw}:{ctl}", *rest[1:]])
    finally:
        _stop_client(client)
        _launch.stop_server(server_proc)


def _require_arcade() -> None:
    """Fail with the install hint when the visual extra is absent."""
    try:
        import arcade  # noqa: F401
    except ImportError:
        print(
            "the visual client needs the window renderer; install it with:\n"
            "    pip install 'kith-fw[visual]'",
            file=sys.stderr,
        )
        raise SystemExit(2) from None


def _spawn_client(args: list[str]) -> subprocess.Popen[bytes] | None:
    """Open the graphical client as a child; a window that cannot open
    (no display, extra not installed) prints why and play continues
    headless."""
    from kith.examples.visual import _launch

    cmd = [sys.executable, "-m", "kith.examples.visual.client", *args]
    client = subprocess.Popen(
        cmd, env=_launch._child_env(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    time.sleep(2.0)
    if client.poll() is not None:
        assert client.stdout is not None
        output = client.stdout.read().decode("utf-8", "replace")
        client.stdout.close()
        print(f"visual client exited early (continuing headless):\n{output}", flush=True)
        return None
    return client


def _stop_client(client: subprocess.Popen[bytes] | None) -> None:
    if client is None:
        return
    if client.poll() is not None:
        if client.stdout is not None:
            client.stdout.close()
        return
    client.terminate()
    try:
        client.wait(timeout=_CLIENT_EXIT_GRACE_S)
    except subprocess.TimeoutExpired:
        client.kill()
    if client.stdout is not None:
        client.stdout.close()


def main(argv: list[str] | None = None) -> int:
    """Dispatch the console entry point's commands."""
    args = sys.argv[1:] if argv is None else argv
    if not args:
        print(_USAGE)
        return 2
    command, rest = args[0], args[1:]
    handlers: dict[str, Callable[[list[str]], int]] = {
        "list": _list,
        "serve": _serve,
        "run": _run,
        "client": _client,
        "play": _play,
    }
    handler: Callable[[list[str]], int] | None = handlers.get(command)
    if handler is None:
        print(f"kith-visual: unknown command {command!r}\n", file=sys.stderr)
        print(_USAGE, file=sys.stderr)
        return 2
    with contextlib.suppress(KeyboardInterrupt):
        return handler(rest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

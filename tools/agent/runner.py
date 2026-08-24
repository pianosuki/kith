"""Scenario runner and CLI for the agentic harness.

Drives a batch of registered scenarios against a :class:`~tools.agent.scenario.ScenarioHost`
via :func:`~tools.agent.scenario.run_scenario`, collects
:class:`~tools.agent.scenario.ScenarioResult` values, and renders a
:class:`~tools.agent.report.Report`. The execution core
(:func:`run_scenarios`) is host-agnostic and loop-owned: it accepts a
factory that produces a fresh host per scenario, so each scenario runs in
an isolated host (the orchestrator's per-client cursors and registry do
not leak across scenarios) while every scenario shares the single
``asyncio`` loop the caller drives with one ``asyncio.run``.

The CLI selects scenarios by substring (``--filter``), lists the registry
(``--list``), writes JSON / JUnit XML / Markdown to files or stdout
(``--format``), and optionally re-runs on source change (``--watch``). It
constructs an :class:`~tools.agent.orchestrator.Orchestrator` targeting a
server; watch mode polls a directory tree's mtimes with ``os.scandir``
(standard library only — the framework has no runtime dependencies).

The runner reads :func:`~tools.agent.scenario.all_scenarios` for
selection; importing this module also imports the ``tools.agent.scenarios``
package so the built-in ``Scenario`` subclasses register via
``Scenario.__init_subclass__`` and are visible to ``--list`` and
``--filter``. Scenarios registered by any other imported module are visible
the same way.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import os
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

# The scenarios package import registers the built-in Scenario subclasses
# via Scenario.__init_subclass__ so all_scenarios() discovers them; the
# execution core and selection helpers below stay registry-agnostic.
from tools.agent import scenarios as _builtin_scenarios  # noqa: F401
from tools.agent.orchestrator import Orchestrator
from tools.agent.report import Report
from tools.agent.scenario import (
    Scenario,
    ScenarioHost,
    ScenarioResult,
    all_scenarios,
    run_scenario,
)


__all__ = [
    "HostFactory",
    "run_scenarios",
]


HostFactory = Callable[[], ScenarioHost]

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 7777
_DEFAULT_WATCH_PATH = "tools/agent"
_DEFAULT_WATCH_INTERVAL_S = 1.0
_FORMATS = ("md", "json", "junit")


# ---------------------------------------------------------------------------
# execution core
# ---------------------------------------------------------------------------


async def run_scenarios(
    scenarios: Sequence[type[Scenario]],
    host_factory: HostFactory,
) -> list[ScenarioResult]:
    """Run each scenario against a fresh host and return the results.

    A new host is built per scenario so per-client cursors and the client
    registry do not leak between scenarios. If the host exposes ``start``
    and ``shutdown`` callables (the :class:`~tools.agent.orchestrator.Orchestrator`
    does), they bracket the run; a host without them (a test fake) is used
    directly. Every scenario shares the caller's ``asyncio`` loop.
    """
    results: list[ScenarioResult] = []
    for scenario_cls in scenarios:
        host = host_factory()
        await _start_host(host)
        try:
            result = await run_scenario(scenario_cls(), host)
        finally:
            await _stop_host(host)
        results.append(result)
    return results


async def _start_host(host: ScenarioHost) -> None:
    """Start the host if it exposes a ``start`` callable (duck-typed)."""
    start = getattr(host, "start", None)
    if callable(start):
        ret = start()
        if inspect.isawaitable(ret):
            await ret


async def _stop_host(host: ScenarioHost) -> None:
    """Stop the host if it exposes a ``shutdown`` callable (duck-typed)."""
    shutdown = getattr(host, "shutdown", None)
    if callable(shutdown):
        ret = shutdown()
        if inspect.isawaitable(ret):
            await ret


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _select_scenarios(filter_text: str | None) -> list[type[Scenario]]:
    registry = all_scenarios()
    if filter_text is None:
        return [registry[name] for name in sorted(registry)]
    matches = [registry[name] for name in sorted(registry) if filter_text in name]
    return matches


def _orchestrator_factory(host: str, port: int) -> HostFactory:
    """Return a factory that builds a fresh orchestrator targeting ``host:port``.

    The CLI host/port override each client's connect target via a custom
    AHC factory, so ``--host``/``--port`` apply to every scenario regardless
    of the connect defaults baked into a scenario's declarative actions.
    The ``AgenticHeadlessClient`` import is deferred to factory call time so
    importing this module does not load the C shared libraries (tests
    construct fakes and never invoke the factory).
    """

    def factory() -> Orchestrator:
        from tools.agent.ahc import AgenticHeadlessClient

        def ahc_factory(
            instance_id: str,
            connect_host: str,
            connect_port: int,
        ) -> AgenticHeadlessClient:
            del connect_host, connect_port
            return AgenticHeadlessClient(
                instance_id=instance_id,
                host=host,
                port=port,
            )

        return Orchestrator(ahc_factory=ahc_factory)

    return factory


@dataclass(frozen=True, slots=True)
class _RunOutcome:
    results: list[ScenarioResult]
    duration_s: float
    started_at: str


def _run_batch(
    scenarios: Sequence[type[Scenario]],
    host_factory: HostFactory,
) -> _RunOutcome:
    started_at = datetime.now(UTC).isoformat()
    start = time.perf_counter()
    results = asyncio.run(run_scenarios(scenarios, host_factory))
    return _RunOutcome(
        results=results,
        duration_s=time.perf_counter() - start,
        started_at=started_at,
    )


def _write_reports(report: Report, args: argparse.Namespace) -> None:
    if args.output_json:
        _write_text(args.output_json, report.to_json())
    if args.output_junit:
        _write_text(args.output_junit, report.to_junit_xml())
    if args.output_md:
        _write_text(args.output_md, report.to_markdown())


def _write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def _stdout(report: Report, fmt: str) -> None:
    if fmt == "json":
        sys.stdout.write(report.to_json())
    elif fmt == "junit":
        sys.stdout.write(report.to_junit_xml())
    else:
        sys.stdout.write(report.to_markdown())
    sys.stdout.write("\n")


def _build_report(outcome: _RunOutcome) -> Report:
    return Report(
        results=outcome.results,
        started_at=outcome.started_at,
        duration_s=outcome.duration_s,
    )


def _run_once(args: argparse.Namespace, scenarios: Sequence[type[Scenario]]) -> int:
    host_factory = (
        _embedded_factory() if args.embedded else _orchestrator_factory(args.host, args.port)
    )
    outcome = _run_batch(scenarios, host_factory)
    report = _build_report(outcome)
    _write_reports(report, args)
    _stdout(report, args.format)
    return 0 if all(r.passed for r in outcome.results) else 1


def _embedded_factory() -> HostFactory:
    """Return a factory that boots a fresh embedded server per scenario.

    Deferred import so importing this module does not load the C shared
    libraries or the example modules; tests construct fakes and never
    invoke the factory.
    """
    from tools.agent.embedded_host import embedded_host_factory

    return embedded_host_factory()


def _watch(args: argparse.Namespace, scenarios: Sequence[type[Scenario]]) -> int:
    paths = [os.path.abspath(p) for p in args.watch_path]
    print(f"Watching {', '.join(paths)} (interval {args.watch_interval}s); Ctrl-C to exit.")
    snapshot = _scan_mtimes(paths)
    host_factory = (
        _embedded_factory() if args.embedded else _orchestrator_factory(args.host, args.port)
    )
    while True:
        try:
            outcome = _run_batch(scenarios, host_factory)
        except KeyboardInterrupt:
            break
        report = _build_report(outcome)
        _write_reports(report, args)
        _stdout(report, args.format)
        try:
            current = _scan_mtimes(paths)
            while current == snapshot:
                time.sleep(args.watch_interval)
                current = _scan_mtimes(paths)
            snapshot = current
            print("\n--- change detected, re-running ---\n")
        except KeyboardInterrupt:
            break
    return 0


def _scan_mtimes(paths: Sequence[str]) -> dict[str, float]:
    """Return a {path: mtime} snapshot of the files under ``paths``."""
    snapshot: dict[str, float] = {}
    for root in paths:
        if os.path.isfile(root):
            snapshot[root] = os.path.getmtime(root)
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]
            for name in filenames:
                if name.endswith(".pyc"):
                    continue
                full = os.path.join(dirpath, name)
                snapshot[full] = os.path.getmtime(full)
    return snapshot


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kith-scenarios",
        description="Run agentic scenarios against a kith server and render a report.",
    )
    parser.add_argument(
        "--filter",
        "-f",
        type=str,
        default=None,
        help="Substring filter on scenario names (default: all registered).",
    )
    parser.add_argument(
        "--format",
        choices=_FORMATS,
        default="md",
        help="Output format printed to stdout (default: md).",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Write the JSON report to this file.",
    )
    parser.add_argument(
        "--output-junit",
        type=str,
        default=None,
        help="Write the JUnit XML report to this file.",
    )
    parser.add_argument(
        "--output-md",
        type=str,
        default=None,
        help="Write the Markdown report to this file.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=_DEFAULT_HOST,
        help="Server host (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=_DEFAULT_PORT,
        help="Server port (default: 7777).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List registered scenarios and exit.",
    )
    parser.add_argument(
        "--embedded",
        action="store_true",
        help="Boot an in-process embedded server per scenario instead of "
        "targeting an external server with --host/--port.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Re-run scenarios when source files change.",
    )
    parser.add_argument(
        "--watch-path",
        type=str,
        action="append",
        default=None,
        help=f"Directory to watch in --watch mode (default: {_DEFAULT_WATCH_PATH}). Repeatable.",
    )
    parser.add_argument(
        "--watch-interval",
        type=float,
        default=_DEFAULT_WATCH_INTERVAL_S,
        help="Polling interval in seconds for --watch (default: 1.0).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.list:
        names = sorted(all_scenarios())
        if not names:
            print("(no scenarios registered)")
            return 0
        print("Registered scenarios:")
        for name in names:
            print(f"  {name}")
        return 0

    scenarios = _select_scenarios(args.filter)
    if not scenarios:
        available = ", ".join(sorted(all_scenarios())) or "(none)"
        print(f"No scenarios match filter {args.filter!r}; available: {available}", file=sys.stderr)
        return 1

    if args.watch:
        if args.watch_path is None:
            args.watch_path = [_DEFAULT_WATCH_PATH]
        return _watch(args, scenarios)
    return _run_once(args, scenarios)


if __name__ == "__main__":
    raise SystemExit(main())

"""Unit tests for the live scale view tool."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import pytest
from tools.agent import live_view as live_view_module
from tools.agent.live_view import (
    CounterStrip,
    InstanceEndpoint,
    InstanceFrame,
    LiveFrame,
    SampleRotation,
    _backoff_s,
    _counter_strip,
    _instance_record,
    _InstancePoller,
    _next_slot,
    _once_exit_code,
    _pad,
    _parse_hosts,
    _record_frame,
    _Recorder,
    discover_endpoints,
    main,
    once_frame,
    render,
)
from tools.agent.load_harness import PhaseCounters
from tools.agent.server_control import ServerControlError


# ---------------------------------------------------------------------------
# builders and fakes
# ---------------------------------------------------------------------------


def _phase(**overrides: int) -> PhaseCounters:
    fields: dict[str, int] = {
        "refresh_ns": 0,
        "compose_ns": 0,
        "deliver_ns": 0,
        "dispatches": 0,
        "publishes": 0,
        "compose_window_ns": 0,
        "compose_prior_ns": 0,
        "compose_lock_wait_ns": 0,
        "compose_scan_ns": 0,
        "compose_sort_ns": 0,
        "compose_select_ns": 0,
        "compose_skips": 0,
        "compose_deferrals": 0,
        "view_locate_failures": 0,
        "write_ns": 0,
        "write_deferrals": 0,
    }
    fields.update(overrides)
    return PhaseCounters(**fields)


class _FakeControl:
    """Minimal ServerControl double driven by handler callables.

    ``post`` fails the test when reached: the view's GET-only contract
    means no code path may ever call it.
    """

    def __init__(
        self,
        *,
        on_get: Callable[[str], Mapping[str, object]] | None = None,
        on_metrics: Callable[[], str] | None = None,
        delay_s: float = 0.0,
    ) -> None:
        self.get_paths: list[str] = []
        self._on_get = on_get
        self._on_metrics = on_metrics
        self._delay_s = delay_s

    async def get(self, path: str) -> Mapping[str, object]:
        self.get_paths.append(path)
        if self._delay_s:
            await asyncio.sleep(self._delay_s)
        if self._on_get is None:
            return {}
        return self._on_get(path)

    async def metrics(self) -> str:
        if self._on_metrics is None:
            return ""
        return self._on_metrics()

    async def post(self, path: str, body: Mapping[str, object]) -> Mapping[str, object]:
        del path, body
        raise AssertionError("the live view never POSTs")


def _roster_control(
    total: int, *, missing: frozenset[int] = frozenset(), delay_s: float = 0.0
) -> _FakeControl:
    def on_get(path: str) -> Mapping[str, object]:
        if path == "/query_state?offset=0&limit=1":
            return {
                "actors": [{"actor_id": 1, "pos_x": 0, "pos_y": 0}],
                "offset": 0,
                "limit": 1,
                "total": total,
            }
        if path.startswith("/query_state?actor_id="):
            actor_id = int(path.rpartition("=")[2])
            if actor_id in missing:
                raise ServerControlError("actor not found", status=404)
            return {"actor_id": actor_id, "pos_x": actor_id, "pos_y": 0, "pos_z": 0}
        raise AssertionError(f"unexpected control path {path!r}")

    return _FakeControl(on_get=on_get, delay_s=delay_s)


def _endpoint(port: int = 9301, pid: int | None = 100) -> InstanceEndpoint:
    return InstanceEndpoint(host="127.0.0.1", port=port, pid=pid)


def _sample_poller(
    control: _FakeControl, sample_size: int = 10, interval_s: float = 1.0
) -> _InstancePoller:
    return _InstancePoller(
        _endpoint(), control, mode="sample", sample_size=sample_size, interval_s=interval_s
    )


def _full_poller(control: _FakeControl, interval_s: float = 1.0) -> _InstancePoller:
    return _InstancePoller(
        _endpoint(), control, mode="full-roster", sample_size=10, interval_s=interval_s
    )


_DEFAULT_STRIP = CounterStrip(
    dt_s=0.5,
    dispatches_ps=812.4,
    publishes_ps=810.2,
    write_deferrals_ps=0.0,
    deliver_share=0.43,
    compose_share=0.38,
    executor_jobs_ps=780.6,
    compose_deferrals=2,
    delivery_inflight=3,
    worker_inflight=1,
    dispatch_dropped=0,
    delivery_dropped=0,
    tick_dropped=0,
    view_locate_failures=0,
)


def _inst(
    *,
    port: int = 9301,
    pid: int | None = 100,
    actors: tuple[tuple[int, int, int], ...] = ((1, 0, 0), (2, 4, 0), (3, 8, 0)),
    roster_total: int | None = 24,
    window: tuple[int, int] | None = (1, 24),
    sampled: int = 24,
    absent: int = 1,
    counters: CounterStrip | None = None,
    roster_age_s: float | None = 0.1,
    metrics_age_s: float | None = 0.1,
    roster_error: str | None = None,
    metrics_error: str | None = None,
    stale: bool = False,
) -> InstanceFrame:
    return InstanceFrame(
        endpoint=InstanceEndpoint("127.0.0.1", port, pid=pid),
        actors=actors,
        roster_total=roster_total,
        window=window,
        sampled=sampled,
        absent=absent,
        counters=_DEFAULT_STRIP if counters is None else counters,
        roster_age_s=roster_age_s,
        metrics_age_s=metrics_age_s,
        roster_error=roster_error,
        metrics_error=metrics_error,
        stale=stale,
    )


def _frame(instances: Sequence[InstanceFrame], *, mode: str = "sample") -> LiveFrame:
    return LiveFrame(
        ts_mono_ns=42,
        mode="full-roster" if mode == "full-roster" else "sample",
        elapsed_s=63.5,
        sample_size=200,
        interval_s=1.0,
        instances=tuple(instances),
    )


# ---------------------------------------------------------------------------
# discovery
# ---------------------------------------------------------------------------

_TCP_HEADER = (
    "  sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode"
)


def _tcp_row(sl: int, local: str, rem: str, state: int, inode: int) -> str:
    return (
        f"  {sl}: {local} {rem} {state:02X} 00000000:00000000 00:00000000 "
        f"00000000 1000 0 {inode} 1 0000000000000000 100"
    )


def _server_dir(
    root: Path, pid: int, inodes: Sequence[int], *, cmdline: bytes | None = None
) -> None:
    pid_dir = root / str(pid)
    (pid_dir / "fd").mkdir(parents=True, exist_ok=True)
    argv = cmdline if cmdline is not None else b"python\0-m\0examples.embedded.server\0\0"
    (pid_dir / "cmdline").write_bytes(argv)
    for fd, inode in enumerate(inodes):
        os.symlink(f"socket:[{inode}]", pid_dir / "fd" / str(fd))


def _net_tables(root: Path, *, tcp4: Sequence[str] = (), tcp6: Sequence[str] = ()) -> None:
    net = root / "net"
    net.mkdir(parents=True, exist_ok=True)
    (net / "tcp").write_text("\n".join([_TCP_HEADER, *tcp4]) + "\n")
    (net / "tcp6").write_text("\n".join([_TCP_HEADER, *tcp6]) + "\n")


def _established_rows(sl: int, port: int, count: int, inode: int) -> list[str]:
    return [
        _tcp_row(sl + i, f"0100007F:{port:04X}", f"0100007F:{0xAF10 + i:04X}", 0x01, inode + i)
        for i in range(count)
    ]


def test_discovery_classifies_by_established_counts(tmp_path: Path) -> None:
    _net_tables(
        tmp_path,
        tcp4=[
            _tcp_row(0, "00000000:2328", "00000000:0000", 0x0A, 10),
            _tcp_row(1, "00000000:2329", "00000000:0000", 0x0A, 11),
            *_established_rows(2, 9000, 8, 12),
        ],
    )
    _server_dir(tmp_path, 100, list(range(10, 20)))
    endpoints = discover_endpoints(tmp_path)
    assert [(e.port, e.pid) for e in endpoints] == [(9001, 100)]


def test_discovery_orders_instances_by_pid(tmp_path: Path) -> None:
    _net_tables(
        tmp_path,
        tcp4=[
            _tcp_row(0, "00000000:2328", "00000000:0000", 0x0A, 10),
            _tcp_row(1, "00000000:2329", "00000000:0000", 0x0A, 11),
            *_established_rows(2, 9000, 8, 12),
            _tcp_row(20, "00000000:232A", "00000000:0000", 0x0A, 40),
            _tcp_row(21, "00000000:232B", "00000000:0000", 0x0A, 41),
            *_established_rows(22, 9002, 8, 42),
        ],
    )
    _server_dir(tmp_path, 200, list(range(40, 50)))
    _server_dir(tmp_path, 100, list(range(10, 20)))
    endpoints = discover_endpoints(tmp_path)
    assert [(e.port, e.pid) for e in endpoints] == [(9001, 100), (9003, 200)]


def test_discovery_skips_tied_counts(tmp_path: Path) -> None:
    _net_tables(
        tmp_path,
        tcp4=[
            _tcp_row(0, "00000000:2328", "00000000:0000", 0x0A, 10),
            _tcp_row(1, "00000000:2329", "00000000:0000", 0x0A, 11),
            *_established_rows(2, 9000, 8, 12),
            *_established_rows(10, 9001, 8, 30),
        ],
    )
    _server_dir(tmp_path, 100, list(range(10, 20)) + list(range(30, 38)))
    assert discover_endpoints(tmp_path) == ()


def test_discovery_skips_login_ramp_instead_of_guessing(tmp_path: Path) -> None:
    """Early ramp: the control plane out-peers a gateway with no logins yet.

    The few-peer listener is NOT the control plane here — the many-peer
    rule only classifies once the winner clears the gateway peer floor.
    Classifying during the ramp would contact the gateway port.
    """
    _net_tables(
        tmp_path,
        tcp4=[
            _tcp_row(0, "00000000:2328", "00000000:0000", 0x0A, 10),
            _tcp_row(1, "00000000:2329", "00000000:0000", 0x0A, 11),
            *_established_rows(2, 9001, 3, 12),
        ],
    )
    _server_dir(tmp_path, 100, list(range(10, 15)))
    assert discover_endpoints(tmp_path) == ()


def test_discovery_skips_below_floor_winner(tmp_path: Path) -> None:
    _net_tables(
        tmp_path,
        tcp4=[
            _tcp_row(0, "00000000:2328", "00000000:0000", 0x0A, 10),
            _tcp_row(1, "00000000:2329", "00000000:0000", 0x0A, 11),
            *_established_rows(2, 9000, 5, 12),
        ],
    )
    _server_dir(tmp_path, 100, list(range(10, 17)))
    assert discover_endpoints(tmp_path) == ()


def test_discovery_skips_single_listener(tmp_path: Path) -> None:
    _net_tables(tmp_path, tcp4=[_tcp_row(0, "00000000:2328", "00000000:0000", 0x0A, 10)])
    _server_dir(tmp_path, 100, [10])
    assert discover_endpoints(tmp_path) == ()


def test_discovery_skips_non_server_cmdline(tmp_path: Path) -> None:
    _net_tables(
        tmp_path,
        tcp4=[
            _tcp_row(0, "00000000:2328", "00000000:0000", 0x0A, 10),
            _tcp_row(1, "00000000:2329", "00000000:0000", 0x0A, 11),
            _tcp_row(2, "0100007F:2328", "0100007F:AF10", 0x01, 12),
        ],
    )
    _server_dir(tmp_path, 100, [10, 11, 12], cmdline=b"python\0-m\0tools.agent.load_harness\0\0")
    assert discover_endpoints(tmp_path) == ()


def test_discovery_reads_tcp6(tmp_path: Path) -> None:
    _net_tables(
        tmp_path,
        tcp6=[
            _tcp_row(
                0,
                "00000000000000000000000000000000:2328",
                "00000000000000000000000000000000:0000",
                0x0A,
                10,
            ),
            _tcp_row(
                1,
                "00000000000000000000000000000000:2329",
                "00000000000000000000000000000000:0000",
                0x0A,
                11,
            ),
            *[
                _tcp_row(
                    2 + i,
                    f"0000000000000000000000000000000{1 + i}:2328",
                    f"0000000000000000000000000000000{1 + i}:AF1{1 + i}",
                    0x01,
                    12 + i,
                )
                for i in range(8)
            ],
        ],
    )
    _server_dir(tmp_path, 100, list(range(10, 20)))
    assert [(e.port, e.pid) for e in discover_endpoints(tmp_path)] == [(9001, 100)]


def test_discovery_tolerates_malformed_rows(tmp_path: Path) -> None:
    _net_tables(
        tmp_path,
        tcp4=[
            "garbage",
            "  1: only three fields",
            _tcp_row(0, "00000000:2328", "00000000:0000", 0x0A, 10),
            _tcp_row(1, "00000000:2329", "00000000:0000", 0x0A, 11),
            *_established_rows(2, 9000, 8, 12),
        ],
    )
    _server_dir(tmp_path, 100, list(range(10, 20)))
    assert [(e.port, e.pid) for e in discover_endpoints(tmp_path)] == [(9001, 100)]


def test_discovery_empty_tree(tmp_path: Path) -> None:
    _net_tables(tmp_path)
    assert discover_endpoints(tmp_path) == ()


# ---------------------------------------------------------------------------
# rotation, cadence, backoff
# ---------------------------------------------------------------------------


def test_rotation_sweeps_and_wraps() -> None:
    rotation = SampleRotation(200)
    assert rotation.window(1000) == (1, 200)
    rotation.advance(1000)
    assert rotation.window(1000) == (201, 400)
    for _ in range(4):
        rotation.advance(1000)
    assert rotation.window(1000) == (1, 200)


def test_rotation_full_coverage_when_total_fits() -> None:
    rotation = SampleRotation(200)
    assert rotation.window(24) == (1, 24)
    rotation.advance(24)
    assert rotation.window(24) == (1, 24)


def test_rotation_reshapes_on_growth() -> None:
    rotation = SampleRotation(10)
    rotation.advance(5)
    assert rotation.window(25) == (11, 20)
    assert rotation.window(5) == (1, 5)


def test_rotation_empty_roster_is_none() -> None:
    assert SampleRotation(10).window(0) is None


def test_rotation_honors_first_id() -> None:
    assert SampleRotation(10).window(15, base=7) == (7, 16)


def test_rotation_rejects_out_of_cap() -> None:
    with pytest.raises(ValueError):
        SampleRotation(257)
    with pytest.raises(ValueError):
        SampleRotation(0)


def test_next_slot_collapses_missed_slots() -> None:
    assert _next_slot(10.0, 1.0, 9.5) == 10.0
    assert _next_slot(10.0, 1.0, 10.0) == 10.0
    assert _next_slot(10.0, 1.0, 10.2) == 11.0
    assert _next_slot(10.0, 1.0, 12.4) == 13.0


def test_backoff_doubles_and_caps() -> None:
    assert _backoff_s(1.0, 1) == 2.0
    assert _backoff_s(1.0, 3) == 8.0
    assert _backoff_s(1.0, 20) == 8.0


# ---------------------------------------------------------------------------
# counter strip
# ---------------------------------------------------------------------------


def test_first_sample_has_absolutes_only() -> None:
    cur = _phase(
        dispatches=50,
        publishes=40,
        compose_deferrals=2,
        worker_tasks_submitted=7,
        worker_tasks_completed=5,
        delivery_inflight_current=3,
    )
    strip = _counter_strip(None, cur, 10.0)
    assert strip.dt_s == 0.0
    assert strip.dispatches_ps is None
    assert strip.publishes_ps is None
    assert strip.deliver_share is None
    assert strip.compose_deferrals == 2
    assert strip.delivery_inflight == 3
    assert strip.worker_inflight == 2
    # The server records the healthy-at-zero counters only on a nonzero
    # drop delta, so an absent series parses as the healthy zero.
    assert strip.dispatch_dropped == 0


def test_rates_over_scrape_delta() -> None:
    prev = _phase(
        dispatches=100,
        publishes=80,
        refresh_ns=1_000_000_000,
        deliver_ns=400_000_000,
        compose_ns=300_000_000,
        delivery_jobs=200,
    )
    cur = _phase(
        dispatches=200,
        publishes=180,
        refresh_ns=1_500_000_000,
        deliver_ns=450_000_000,
        compose_ns=350_000_000,
        write_deferrals=2,
        delivery_jobs=400,
    )
    strip = _counter_strip((prev, 10.0), cur, 10.5)
    assert strip.dt_s == pytest.approx(0.5)
    assert strip.dispatches_ps == pytest.approx(200.0)
    assert strip.publishes_ps == pytest.approx(200.0)
    assert strip.write_deferrals_ps == pytest.approx(4.0)
    assert strip.deliver_share == pytest.approx(0.1)
    assert strip.compose_share == pytest.approx(0.1)
    assert strip.executor_jobs_ps == pytest.approx(400.0)
    assert strip.compose_deferrals == 0


def test_restart_clamps_negative_deltas() -> None:
    strip = _counter_strip((_phase(dispatches=1000), 10.0), _phase(dispatches=5), 10.5)
    assert strip.dispatches_ps == 0.0


def test_zero_refresh_yields_absent_shares() -> None:
    strip = _counter_strip((_phase(dispatches=10), 1.0), _phase(dispatches=20), 2.0)
    assert strip.deliver_share is None
    assert strip.compose_share is None
    assert strip.dispatches_ps == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# sample-mode ticks
# ---------------------------------------------------------------------------


def test_sample_tick_reads_window_and_advances() -> None:
    control = _roster_control(30)
    poller = _sample_poller(control, sample_size=10)
    asyncio.run(poller.poll_once())
    assert poller._window == (1, 10)
    assert poller._roster_total == 30
    assert len(poller._actors) == 10
    assert poller._actors[0] == (1, 1, 0)
    assert poller._roster_error is None
    asyncio.run(poller.poll_once())
    assert poller._window == (11, 20)


def test_404_renders_absent() -> None:
    poller = _sample_poller(_roster_control(30, missing=frozenset({5})), sample_size=10)
    asyncio.run(poller.poll_once())
    assert poller._sampled == 10
    assert poller._absent == 1
    assert len(poller._actors) == 9
    assert poller._roster_error is None


def test_probe_learns_first_id() -> None:
    def on_get(path: str) -> Mapping[str, object]:
        if path == "/query_state?offset=0&limit=1":
            return {
                "actors": [{"actor_id": 7, "pos_x": 0, "pos_y": 0}],
                "offset": 0,
                "limit": 1,
                "total": 4,
            }
        if path.startswith("/query_state?actor_id="):
            actor_id = int(path.rpartition("=")[2])
            return {"actor_id": actor_id, "pos_x": actor_id, "pos_y": 0}
        raise AssertionError(f"unexpected control path {path!r}")

    poller = _sample_poller(_FakeControl(on_get=on_get), sample_size=10)
    asyncio.run(poller.poll_once())
    assert poller._window == (7, 10)
    assert [actor[0] for actor in poller._actors] == [7, 8, 9, 10]


def test_probe_backcompat_without_total() -> None:
    def on_get(path: str) -> Mapping[str, object]:
        if path == "/query_state?offset=0&limit=1":
            return {
                "actors": [
                    {"actor_id": 1, "pos_x": 1, "pos_y": 0},
                    {"actor_id": 2, "pos_x": 2, "pos_y": 0},
                    {"actor_id": 3, "pos_x": 3, "pos_y": 0},
                ]
            }
        if path.startswith("/query_state?actor_id="):
            actor_id = int(path.rpartition("=")[2])
            return {"actor_id": actor_id, "pos_x": actor_id, "pos_y": 0}
        raise AssertionError(f"unexpected control path {path!r}")

    poller = _sample_poller(_FakeControl(on_get=on_get), sample_size=10)
    asyncio.run(poller.poll_once())
    assert poller._roster_total == 3
    assert poller._window == (1, 3)


def test_deadline_stops_window_mid_tick() -> None:
    poller = _sample_poller(_roster_control(30, delay_s=0.02), sample_size=10, interval_s=0.05)
    asyncio.run(poller.poll_once())
    assert 1 <= poller._sampled < 10
    assert poller._roster_error is None


def test_non_404_error_marks_error_and_commits_partial() -> None:
    def on_get(path: str) -> Mapping[str, object]:
        if path == "/query_state?offset=0&limit=1":
            return {
                "actors": [{"actor_id": 1, "pos_x": 0, "pos_y": 0}],
                "offset": 0,
                "limit": 1,
                "total": 10,
            }
        if path.startswith("/query_state?actor_id="):
            actor_id = int(path.rpartition("=")[2])
            if actor_id == 3:
                raise ServerControlError("boom", status=500)
            return {"actor_id": actor_id, "pos_x": actor_id, "pos_y": 0}
        raise AssertionError(f"unexpected control path {path!r}")

    poller = _sample_poller(_FakeControl(on_get=on_get), sample_size=10)
    asyncio.run(poller.poll_once())
    assert poller._roster_error is not None
    assert "boom" in poller._roster_error
    assert [actor[0] for actor in poller._actors] == [1, 2]


def test_poller_never_posts() -> None:
    control = _roster_control(30)
    poller = _sample_poller(control, sample_size=10)
    asyncio.run(poller.poll_once())
    assert control.get_paths
    # _FakeControl.post raises AssertionError; reaching it fails the test.


# ---------------------------------------------------------------------------
# full-roster ticks
# ---------------------------------------------------------------------------


def _actor_dicts(count: int) -> list[dict[str, int]]:
    return [{"actor_id": i + 1, "pos_x": i, "pos_y": 0, "pos_z": 0} for i in range(count)]


def _paged_control(actors: list[dict[str, int]], *, include_total: bool = True) -> _FakeControl:
    def on_get(path: str) -> Mapping[str, object]:
        if path.startswith("/query_state?offset="):
            params = dict(part.split("=") for part in path.split("?", 1)[1].split("&"))
            offset = int(params["offset"])
            limit = int(params["limit"])
            page = actors[offset : offset + limit]
            body: dict[str, object] = {"actors": page, "offset": offset, "limit": limit}
            if include_total:
                body["total"] = len(actors)
            return body
        raise AssertionError(f"unexpected control path {path!r}")

    return _FakeControl(on_get=on_get)


def test_full_roster_folds_all_pages() -> None:
    poller = _full_poller(_paged_control(_actor_dicts(70)))
    asyncio.run(poller.poll_once())
    assert poller._roster_total == 70
    assert len(poller._actors) == 70
    assert poller._actors[0] == (1, 0, 0)
    assert poller._window is None


def test_full_roster_total_absent_stops_on_short_page() -> None:
    poller = _full_poller(_paged_control(_actor_dicts(20), include_total=False))
    asyncio.run(poller.poll_once())
    assert poller._roster_total == 20
    assert len(poller._actors) == 20


def test_full_roster_malformed_entry_marks_error() -> None:
    actors = _actor_dicts(40)
    del actors[3]["pos_x"]
    poller = _full_poller(_paged_control(actors))
    asyncio.run(poller.poll_once())
    assert poller._roster_error is not None
    assert poller._roster_total is None
    assert poller._actors == ()


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------


def test_render_is_deterministic() -> None:
    frame = _frame([_inst()])
    assert render(frame, 60, 12, color=False) == render(frame, 60, 12, color=False)


def test_render_snapshot() -> None:
    out = render(_frame([_inst()]), 80, 12, color=False)
    dot_row = " " * 4 + "." + " " * 35 + "." + " " * 34 + "." + " " * 4
    assert len(dot_row) == 80
    expected = "\n".join(
        [
            _pad("kith live view  elapsed 01:03.5  mode sample(200)  interval 1s", 80),
            _pad("instance 0  127.0.0.1:9301  pid 100  age 0.1s  win 1-24/24 absent 1", 80),
            " " * 80,
            " " * 80,
            " " * 80,
            dot_row,
            " " * 80,
            " " * 80,
            " " * 80,
            _pad("dispatch 812/s  publish 810/s  wdeferr 0/s  cdeferr 2", 80),
            _pad("deliver 43%  compose 38%  exec 781/s  inflight 3  pool 1", 80),
            _pad("dropped dispatch=0 delivery=0 tick=0 view_locate=0  ok", 80),
        ]
    )
    assert out == expected


def test_render_two_panels() -> None:
    out = render(_frame([_inst(), _inst(port=9302, pid=101)]), 80, 20, color=False)
    lines = out.split("\n")
    assert len(lines) == 20
    assert all(len(line) == 80 for line in lines)
    assert "127.0.0.1:9301" in lines[1]
    assert "127.0.0.1:9302" in lines[1]


def test_render_degenerate_bounds() -> None:
    inst = _inst(actors=((1, 5, 0), (2, 5, 0)), window=(1, 2), sampled=2, absent=0)
    out = render(_frame([inst]), 60, 12, color=False)
    assert "." in out


def test_render_marks_stale() -> None:
    inst = _inst(stale=True, roster_age_s=None)
    assert "STALE" in render(_frame([inst]), 60, 12, color=False)


def test_render_marks_roster_error() -> None:
    inst = _inst(roster_error="ServerControlError: boom")
    out = render(_frame([inst]), 60, 12, color=False)
    assert "ERR" in out
    assert "ServerControlError" in out


def test_render_alert_on_dropped_counters() -> None:
    drops = CounterStrip(
        dt_s=0.5,
        dispatch_dropped=7,
        delivery_dropped=0,
        tick_dropped=0,
        view_locate_failures=0,
    )
    plain = render(_frame([_inst(counters=drops)]), 60, 12, color=False)
    assert "DROPPED" in plain
    assert "dispatch=7" in plain
    colored = render(_frame([_inst(counters=drops)]), 60, 12, color=True)
    assert "\x1b[31m" in colored


def test_render_ok_drops_green() -> None:
    colored = render(_frame([_inst()]), 60, 12, color=True)
    assert "\x1b[32m" in colored
    assert "ok" in colored


def test_render_unobserved_drops_stay_plain() -> None:
    strip = CounterStrip(dt_s=0.5)
    out = render(_frame([_inst(counters=strip)]), 60, 12, color=False)
    assert "n/a" in out
    assert "DROPPED" not in out


def test_render_no_instances() -> None:
    out = render(_frame([]), 60, 12, color=False)
    assert "no instances" in out
    assert len(out.split("\n")) == 12


def test_render_tiny_terminal() -> None:
    out = render(_frame([_inst()]), 20, 6, color=False)
    lines = out.split("\n")
    assert len(lines) == 6
    assert all(len(line) == 20 for line in lines)


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------


def test_record_frame_shape() -> None:
    record = _record_frame(_frame([_inst()]))
    assert set(record) == {"ts_mono_ns", "mode", "instances"}
    assert record["ts_mono_ns"] == 42
    assert record["mode"] == "sample"
    instances = record["instances"]
    assert isinstance(instances, list) and instances
    entry = instances[0]
    assert isinstance(entry, dict)
    assert set(entry) == {"endpoint", "bounds", "actors", "counters"}
    assert entry["endpoint"] == "127.0.0.1:9301"
    assert entry["actors"] == [[1, 0, 0], [2, 4, 0], [3, 8, 0]]
    assert entry["bounds"] is not None
    assert isinstance(entry["counters"], dict)
    json.dumps(record)


def test_instance_record_empty_actors_has_null_bounds() -> None:
    record = _instance_record(_inst(actors=()))
    assert record["bounds"] is None
    assert record["actors"] == []


def test_recorder_appends_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "frames.jsonl"
    recorder = _Recorder(str(path))
    recorder.write(_frame([_inst()]))
    recorder.write(_frame([_inst(port=9302)]))
    recorder.close()
    lines = path.read_text().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["instances"][0]["endpoint"] == "127.0.0.1:9301"
    assert json.loads(lines[1])["instances"][0]["endpoint"] == "127.0.0.1:9302"


# ---------------------------------------------------------------------------
# once pass against a real control-plane client
# ---------------------------------------------------------------------------


class _StubHttpServer:
    """Minimal asyncio HTTP/1.1 server serving canned control-plane replies."""

    def __init__(self, handler: Callable[[str], tuple[int, bytes, str]]) -> None:
        self._handler = handler
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        sockets = self._server.sockets
        assert sockets is not None
        self.port = sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        reasons = {200: "OK", 404: "Not Found"}
        try:
            while True:
                request_line = await reader.readline()
                if not request_line:
                    break
                path = request_line.decode("iso-8859-1").split()[1]
                while True:
                    line = await reader.readline()
                    if line in (b"\r\n", b"\n", b""):
                        break
                status, body, content_type = self._handler(path)
                head = (
                    f"HTTP/1.1 {status} {reasons.get(status, 'OK')}\r\n"
                    f"Content-Type: {content_type}\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    f"Connection: keep-alive\r\n\r\n"
                ).encode("iso-8859-1")
                writer.write(head + body)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError) as _exc:
            del _exc
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()


def _stub_handler(path: str) -> tuple[int, bytes, str]:
    if path == "/query_state?offset=0&limit=1":
        body = json.dumps(
            {
                "actors": [{"actor_id": 1, "pos_x": 0, "pos_y": 0, "pos_z": 0}],
                "offset": 0,
                "limit": 1,
                "total": 8,
            }
        ).encode()
        return 200, body, "application/json"
    if path.startswith("/query_state?actor_id="):
        actor_id = int(path.rpartition("=")[2])
        if actor_id > 8:
            return 404, b'{"error": "actor not found"}', "application/json"
        body = json.dumps(
            {"actor_id": actor_id, "pos_x": actor_id * 2, "pos_y": 0, "pos_z": 0}
        ).encode()
        return 200, body, "application/json"
    if path == "/metrics":
        text = (
            "kith_gateway_dispatches_total 100\n"
            "kith_fabric_publishes_total 90\n"
            "kith_gateway_refresh_ns_total 500000000\n"
            "kith_server_tick_dropped_total 0\n"
            "kith_gateway_dispatch_dropped_total 0\n"
            "kith_gateway_delivery_dropped_total 0\n"
            "kith_gateway_view_locate_failures_total 0\n"
        )
        return 200, text.encode(), "text/plain; version=0.0.4"
    raise AssertionError(f"unexpected path {path}")


def test_once_frame_against_stub_server() -> None:
    async def scenario() -> LiveFrame:
        server = _StubHttpServer(_stub_handler)
        await server.start()
        try:
            return await once_frame([InstanceEndpoint("127.0.0.1", server.port)], sample_size=5)
        finally:
            await server.stop()

    frame = asyncio.run(scenario())
    inst = frame.instances[0]
    assert _once_exit_code(frame) == 0
    assert inst.roster_total == 8
    assert len(inst.actors) == 5
    assert inst.actors[0] == (1, 2, 0)
    assert inst.counters is not None
    assert inst.counters.dispatch_dropped == 0
    assert inst.counters.dispatches_ps is None


def test_once_exit_code_requires_a_reading() -> None:
    unreachable = InstanceFrame(
        endpoint=_endpoint(),
        roster_total=None,
        window=None,
        sampled=0,
        absent=0,
        counters=None,
        stale=True,
    )
    assert _once_exit_code(_frame([unreachable])) == 1
    assert _once_exit_code(_frame([unreachable, _inst()])) == 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_parse_hosts_preserves_order() -> None:
    endpoints = _parse_hosts("127.0.0.1:9001, 127.0.0.1:9003")
    assert [(e.host, e.port) for e in endpoints] == [
        ("127.0.0.1", 9001),
        ("127.0.0.1", 9003),
    ]
    assert all(e.pid is None and e.origin == "hosts" for e in endpoints)


@pytest.mark.parametrize(
    "spec", ["9001", "host:", ":9001", "host:notaport", "host:0", "host:70000", ""]
)
def test_parse_hosts_rejects_malformed(spec: str) -> None:
    with pytest.raises(ValueError):
        _parse_hosts(spec)


def test_cli_rejects_sample_over_cap(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        main(["--sample", "257"])
    assert "--sample" in capsys.readouterr().err


def test_cli_rejects_zero_interval(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        main(["--interval", "0"])
    assert "--interval" in capsys.readouterr().err


def test_main_once_without_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(live_view_module, "discover_endpoints", lambda: ())
    monkeypatch.setattr("os.nice", lambda value: 0)
    assert main(["--once"]) == 1

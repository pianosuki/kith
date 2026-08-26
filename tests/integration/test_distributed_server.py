"""Integration test: the distributed-topology cluster end-to-end.

Boots :class:`examples.distributed.server.DistributedCluster`, verifies both
gateway listeners accept TCP connections, drives the coordination surface
over HTTP (``/authority`` / ``/split`` / ``/merge`` / ``/members``), and
confirms the split/merge transfer contract: a rebalance contract published on
the shared
coord bus is drained and applied by the receiving coord, transferring cell
authority across the instance boundary and back. The launcher's
delivery-preset flags parse without a build; a tiered boot pins the
forwarding — both instances construct under the tiered strategy with the
backstop override, and a login binds under it.
"""

from __future__ import annotations

import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, cast
from urllib import error, request

import pytest
from _helpers import _BUILD_DEBUG, needs_build
from examples.distributed.server import DistributedCluster, _build_parser

from kith import ServerStatus
from kith._generated import gateway as gen_gateway


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _status(server: DistributedCluster) -> list[ServerStatus]:
    return [srv._server.status for srv in server._servers if srv._server is not None]


def _post_json(url: str, body: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _get_json(url: str) -> dict[str, Any]:
    with request.urlopen(url, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _wait_running(cluster: DistributedCluster, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    for srv in cluster._servers:
        while srv._server is not None and srv._server.status is not ServerStatus.RUNNING:
            if time.monotonic() > deadline:
                raise TimeoutError("server did not reach RUNNING state")
            time.sleep(0.01)


@needs_build
class TestDistributedCluster:
    def test_split_merge_transfers_authority(self) -> None:
        cluster = DistributedCluster()
        endpoints = cluster.start()
        threads: list[threading.Thread] = []
        try:
            for facade, _, _ in endpoints:
                t = threading.Thread(target=facade.run, daemon=True)
                t.start()
                threads.append(t)
            _wait_running(cluster)

            # Both gateway listeners accept TCP connections.
            for _, gw_port, _ in endpoints:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect(("127.0.0.1", gw_port))
                try:
                    assert sock.getpeername()[1] == gw_port
                finally:
                    sock.close()

            base = f"http://127.0.0.1:{endpoints[0][2]}"

            # Before any transfer, instance A's coord resolves the cell to
            # its own hash-fallback (instance 1) at epoch 0.
            before = _get_json(f"{base}/authority?zone=1&cell_x=1&cell_y=1&owner=A")
            assert before["instance_id"] == 1
            assert before["authority_epoch"] == 0

            # Split: a rebalance contract published on the bus transfers
            # the cell's authority to instance 2. Both coords converge.
            split = _post_json(f"{base}/split", {"zone": 1, "cell_x": 1, "cell_y": 1})
            assert split["instance_id"] == 2
            assert split["authority_epoch"] >= 1

            # Instance A's coord now resolves the cell to instance 2.
            after_split = _get_json(f"{base}/authority?zone=1&cell_x=1&cell_y=1&owner=A")
            assert after_split["instance_id"] == 2
            assert after_split["authority_epoch"] == split["authority_epoch"]

            # Instance B's coord agrees (it applied the contract).
            after_split_b = _get_json(f"{base}/authority?zone=1&cell_x=1&cell_y=1&owner=B")
            assert after_split_b["instance_id"] == 2
            assert after_split_b["authority_epoch"] == split["authority_epoch"]

            # Merge: a merge contract (target 0) clears the override and
            # reverts to hash-fallback. Instance A resolves back to 1.
            merged = _post_json(f"{base}/merge", {"zone": 1, "cell_x": 1, "cell_y": 1})
            assert merged["instance_id"] == 1
            assert merged["authority_epoch"] == 0

            after_merge = _get_json(f"{base}/authority?zone=1&cell_x=1&cell_y=1&owner=A")
            assert after_merge["instance_id"] == 1
            assert after_merge["authority_epoch"] == 0

            for facade, _, _ in endpoints:
                facade.shutdown()
            for t in threads:
                t.join(timeout=5.0)
        finally:
            cluster.stop()

    def test_members_reports_bus_membership(self) -> None:
        cluster = DistributedCluster()
        endpoints = cluster.start()
        threads: list[threading.Thread] = []
        try:
            for facade, _, _ in endpoints:
                t = threading.Thread(target=facade.run, daemon=True)
                t.start()
                threads.append(t)
            _wait_running(cluster)

            base = f"http://127.0.0.1:{endpoints[0][2]}"
            members = _get_json(f"{base}/members")
            assert isinstance(members["members"], list)
            assert len(members["members"]) == 1
            assert members["members"][0]["instance_id"] == 1

            for facade, _, _ in endpoints:
                facade.shutdown()
            for t in threads:
                t.join(timeout=5.0)
        finally:
            cluster.stop()

    def test_split_missing_cell_returns_400(self) -> None:
        cluster = DistributedCluster()
        endpoints = cluster.start()
        threads: list[threading.Thread] = []
        try:
            for facade, _, _ in endpoints:
                t = threading.Thread(target=facade.run, daemon=True)
                t.start()
                threads.append(t)
            _wait_running(cluster)

            base = f"http://127.0.0.1:{endpoints[0][2]}"
            with pytest.raises(error.HTTPError) as exc_info:
                _post_json(f"{base}/split", {"zone": 1})
            assert exc_info.value.code == 400
            exc_info.value.close()

            for facade, _, _ in endpoints:
                facade.shutdown()
            for t in threads:
                t.join(timeout=5.0)
        finally:
            cluster.stop()

    def test_tiered_delivery_strategy_boots_and_serves(self) -> None:
        cluster = DistributedCluster(delivery_strategy="tiered", tiered_max_gap_ms=5000)
        endpoints = cluster.start()
        threads: list[threading.Thread] = []
        try:
            for facade, _, _ in endpoints:
                t = threading.Thread(target=facade.run, daemon=True)
                t.start()
                threads.append(t)
            _wait_running(cluster)

            # The preset is a cluster-level choice: the strategy name and
            # its backstop override ride construction into both servers, so
            # both gateways are created tiered with the forwarded tuning.
            # Decoding each server's cadence image pins the override.
            for srv in cluster._servers:
                assert srv._delivery_strategy == "tiered"
                config = gen_gateway.kith_gateway_tiered_config_t.from_buffer_copy(
                    srv._tiered_config()
                )
                assert config.max_gap_ms == 5000

            # A login binds a session under the tiered strategy: unknown
            # strategy names fail session creation, so the bind succeeding
            # is the proof the name resolved on instance A's gateway.
            base = f"http://127.0.0.1:{endpoints[0][2]}"
            login = _post_json(f"{base}/login", {"principal_id": 100})
            assert int(login["actor_id"]) == 1

            for facade, _, _ in endpoints:
                facade.shutdown()
            for t in threads:
                t.join(timeout=5.0)
        finally:
            cluster.stop()


class TestDistributedDeliveryParser:
    """The launcher's delivery-preset flags: absent by default, parsed
    verbatim when set."""

    def test_defaults_keep_the_factory_preset(self) -> None:
        args = _build_parser().parse_args([])
        assert args.delivery_strategy is None
        assert args.tiered_max_gap_ms is None

    def test_strategy_flag_parses(self) -> None:
        args = _build_parser().parse_args(["--delivery-strategy", "tiered"])
        assert args.delivery_strategy == "tiered"

    def test_backstop_flag_parses_milliseconds(self) -> None:
        args = _build_parser().parse_args(["--tiered-max-gap-ms", "5000"])
        assert args.tiered_max_gap_ms == 5000

    def test_flags_compose(self) -> None:
        args = _build_parser().parse_args(
            ["--delivery-strategy", "tiered", "--tiered-max-gap-ms", "5000"]
        )
        assert args.delivery_strategy == "tiered"
        assert args.tiered_max_gap_ms == 5000


class TestStandaloneSignalShutdown:
    """The distributed launcher drains on SIGINT to a clean exit.

    The launcher hosts two ``kith_server_run`` threads; a process-directed
    SIGINT delivered to one of those threads would never run Python's
    handler (they block in C), leaving the main thread's wait parked. The
    launcher blocks the shutdown signals before creating the run threads
    (the mask is inherited), so the interrupt always lands in the main
    thread.
    """

    @staticmethod
    def _spawn_entry_point() -> subprocess.Popen[bytes]:
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            [str(_REPO_ROOT / "python"), str(_REPO_ROOT / "examples")]
        )
        env["KITH_LIB"] = str(_BUILD_DEBUG)
        return subprocess.Popen(
            [sys.executable, "-m", "examples.distributed.server"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(_REPO_ROOT),
            env=env,
        )

    def test_launcher_exits_cleanly_on_sigint(self) -> None:
        proc = self._spawn_entry_point()
        try:
            stdout = proc.stdout
            assert stdout is not None
            # The launcher prints the ports and the authority transition
            # before blocking, so both lines prove the demo cycle completed
            # and the servers are serving.
            ports_line = stdout.readline().decode("utf-8").strip()
            match = re.fullmatch(
                r"distributed: gateway_a=(\d+) control_a=(\d+) "
                r"gateway_b=(\d+) control_b=(\d+)",
                ports_line,
            )
            assert match is not None, f"unexpected handshake line: {ports_line!r}"
            control_url = f"http://127.0.0.1:{int(match.group(2))}"
            authority_line = stdout.readline().decode("utf-8").strip()
            assert authority_line.startswith("cell(1,1):"), authority_line

            # Signal only once the control plane is demonstrably answering.
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    _get_json(f"{control_url}/query_state")
                    break
                except error.HTTPError as exc:
                    # A 404 still proves the HTTP surface serves; any other
                    # status is a real failure, not readiness.
                    assert exc.code == 404, f"control plane answered {exc.code}"
                    exc.close()
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)

            os.kill(proc.pid, int(signal.SIGINT))
            exit_code = proc.wait(timeout=20.0)
            stderr_text = b""
            if proc.stderr is not None:
                stderr_text = proc.stderr.read()
            assert exit_code == 0, (
                f"child exited {exit_code} on SIGINT; stderr:\n"
                f"{stderr_text.decode('utf-8', errors='replace')}"
            )
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            if proc.stdout is not None:
                proc.stdout.close()
            if proc.stderr is not None:
                proc.stderr.close()

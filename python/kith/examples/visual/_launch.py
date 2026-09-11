"""Server-process plumbing for the visual example's scenarios.

The drivers and the one-command path spawn the example server as a child
process (``python -m kith.examples.visual.server``), wait for its
handshake line, and stop it with an escalating signal. The child
resolves its shared libraries through the package's documented discovery
order, so no library pinning rides along; the only environment work is
making ``kith`` importable for a child interpreter when the parent
imported it from a source checkout rather than an install.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import kith


_HANDSHAKE_TIMEOUT_S = 15.0
_STOP_GRACE_S = 8.0


def _child_env() -> dict[str, str]:
    """Return a child environment in which ``-m kith.examples.visual.server``
    resolves.

    A child interpreter resolves imports from its own environment, not
    from the parent's ``sys.path``: an editable or wheel install needs
    nothing, while a source checkout (and a pytest run, which injects the
    repo paths without touching the environment) needs the package's
    parent directory on ``PYTHONPATH``.
    """
    env = dict(os.environ)
    pkg_parent = str(Path(kith.__file__).resolve().parent.parent)
    existing = env.get("PYTHONPATH", "")
    parts = existing.split(os.pathsep) if existing else []
    if pkg_parent not in parts:
        env["PYTHONPATH"] = os.pathsep.join([pkg_parent, *parts])
    return env


def spawn_server(
    *,
    tick_hz: int = 20,
    npcs: int = 40,
    preset: str = "full",
    port: int = 0,
    python_workers: int = 8,
    delivery_workers: int = 0,
    view_max_subjects: int = 0,
    view_refresh_ms: int = 0,
    cache_refresh_ms: int = 0,
    replication_batch_type_id: int = 0,
    tiered_max_gap_ms: int = 1000,
    seed: int = 7,
    env_extra: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[bytes], int, int]:
    """Boot a visual example server; return (proc, gateway port, control port)."""
    cmd = [
        sys.executable,
        "-m",
        "kith.examples.visual.server",
        "--tick-hz",
        str(tick_hz),
        "--npcs",
        str(npcs),
        "--delivery-preset",
        preset,
        "--python-workers",
        str(python_workers),
        "--tiered-max-gap-ms",
        str(tiered_max_gap_ms),
        "--seed",
        str(seed),
    ]
    if port:
        cmd += ["--port", str(port)]
    if delivery_workers:
        cmd += ["--delivery-workers", str(delivery_workers)]
    if view_max_subjects:
        cmd += ["--view-max-subjects", str(view_max_subjects)]
    if view_refresh_ms:
        cmd += ["--view-refresh-ms", str(view_refresh_ms)]
    if cache_refresh_ms:
        cmd += ["--cache-refresh-ms", str(cache_refresh_ms)]
    if replication_batch_type_id:
        cmd += ["--replication-batch-type-id", str(replication_batch_type_id)]
    env = _child_env()
    if env_extra:
        env.update(env_extra)
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        gw, ctl = wait_handshake(proc)
    except Exception:
        # A boot failure leaves a dead child and an open pipe; reap and
        # close here so the caller's error path owns nothing.
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=1.0)
        if proc.stdout is not None:
            proc.stdout.close()
        raise
    return proc, gw, ctl


def wait_handshake(proc: subprocess.Popen[bytes]) -> tuple[int, int]:
    """Wait for the server's handshake line; fail loud on early death.

    The server prints one ``playground:`` line with ``gateway=`` and
    ``control=`` fields the moment its planes are live. A server that
    exits before the handshake carries its captured output into the
    error, so a boot failure diagnoses itself.
    """
    assert proc.stdout is not None
    seen: list[str] = []
    deadline = time.monotonic() + _HANDSHAKE_TIMEOUT_S
    while time.monotonic() < deadline:
        line = proc.stdout.readline().decode("utf-8", "replace")
        if not line:
            raise RuntimeError(
                f"visual server exited early: rc={proc.poll()} output: {''.join(seen)[-2000:]}"
            )
        seen.append(line)
        if line.startswith("playground:"):
            fields = dict(part.split("=", 1) for part in line.strip().split()[1:] if "=" in part)
            return int(fields["gateway"]), int(fields["control"])
    raise RuntimeError(f"visual server handshake timeout; output: {''.join(seen)[-2000:]}")


def stop_server(proc: subprocess.Popen[bytes], *, timeout: float = _STOP_GRACE_S) -> None:
    """Stop the server: SIGINT, escalating to SIGKILL; safe on an exited process."""
    if proc.poll() is None:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=timeout)
    # The handshake reader owns the pipe for its lifetime; leaving it to
    # the collector turns interpreter exit into a finalizer exception.
    if proc.stdout is not None:
        proc.stdout.close()

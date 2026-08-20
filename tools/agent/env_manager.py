"""Docker Compose lifecycle manager for the agentic harness.

Wraps the ``docker compose`` (v2 plugin) command-line as a small, typed
Python surface so scenarios and the orchestrator can bring a cluster up,
scale it, wait for health, and tear it down without shelling out inline.
The manager is synchronous: ``docker compose`` is a subprocess whose
lifecycle is setup/teardown around an async run, not part of the asyncio
loop. The orchestrator calls it before and after the async scenario
phase.

The surface maps one method to one ``docker compose`` subcommand
(``up``/``down``/``restart``/``stop``/``scale``/``logs``/``ps``/``build``/
``exec``) plus structured helpers (``running_services``/``is_running``/
``wait_for_healthy``) that consume ``docker compose ps --format json`` so
status parsing does not depend on the shifting column layout of the text
table.

The default project name is ``kith`` (the framework namespace). No
game-specific identifiers appear here.
"""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Mapping, Sequence
from typing import Final


__all__ = [
    "ComposeError",
    "EnvironmentManager",
]


_DEFAULT_PROJECT: Final[str] = "kith"
_DEFAULT_TIMEOUT_S: Final[float] = 300.0
_HEALTH_INTERVAL_S: Final[float] = 2.0
_HEALTH_TIMEOUT_S: Final[float] = 60.0


class ComposeError(Exception):
    """Raised when a ``docker compose`` invocation exits non-zero."""


class EnvironmentManager:
    """A typed wrapper over ``docker compose`` for cluster lifecycle."""

    def __init__(
        self,
        compose_file: str | None = None,
        *,
        project_name: str = _DEFAULT_PROJECT,
    ) -> None:
        self._compose_file = compose_file
        self._project = project_name
        self._base_cmd: list[str] = ["docker", "compose"]
        if compose_file:
            self._base_cmd.extend(["-f", compose_file])
        self._base_cmd.extend(["-p", project_name])

    @property
    def project_name(self) -> str:
        return self._project

    @property
    def compose_file(self) -> str | None:
        return self._compose_file

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def up(
        self,
        *,
        detach: bool = True,
        scale: Mapping[str, int] | None = None,
        wait: bool = False,
        wait_service: str | None = None,
        wait_timeout_s: float = _HEALTH_TIMEOUT_S,
    ) -> None:
        """Bring the project up, optionally waiting for one service to be healthy.

        ``scale`` maps service name to replica count. When ``wait`` is set,
        poll ``wait_service`` (or the first scaled service) for the
        ``(healthy)`` status rather than sleeping a fixed delay.
        """
        args: list[str] = ["up"]
        if detach:
            args.append("-d")
        for svc, count in (scale or {}).items():
            args.extend(["--scale", f"{svc}={count}"])
        self._run(args, check=True, timeout=_DEFAULT_TIMEOUT_S)
        if wait:
            target = wait_service or _first_scaled(scale)
            if target is not None:
                self.wait_for_healthy(target, timeout_s=wait_timeout_s)

    def down(
        self,
        *,
        remove_volumes: bool = False,
        timeout_s: int | None = None,
    ) -> None:
        """Tear the project down, optionally removing volumes."""
        args: list[str] = ["down"]
        if remove_volumes:
            args.append("-v")
        if timeout_s is not None:
            args.extend(["-t", str(timeout_s)])
        self._run(args, check=False, timeout=_DEFAULT_TIMEOUT_S)

    def restart(self, service: str) -> None:
        """Restart a single service."""
        self._run(["restart", service], check=True, timeout=_DEFAULT_TIMEOUT_S)

    def stop(self, service: str | None = None) -> None:
        """Stop the project, or one service if given."""
        args: list[str] = ["stop"]
        if service:
            args.append(service)
        self._run(args, check=False, timeout=_DEFAULT_TIMEOUT_S)

    def scale(self, service: str, count: int) -> None:
        """Scale a service to ``count`` replicas without recreating."""
        self._run(
            ["up", "-d", "--scale", f"{service}={count}", "--no-recreate", service],
            check=True,
            timeout=_DEFAULT_TIMEOUT_S,
        )

    def build(self, service: str | None = None) -> None:
        """Build images for the project, or one service if given."""
        args: list[str] = ["build"]
        if service:
            args.append(service)
        self._run(args, check=True, timeout=_DEFAULT_TIMEOUT_S)

    def exec(self, service: str, command: Sequence[str]) -> str:
        """Run ``command`` inside ``service`` and return its stdout.

        ``command`` is a pre-split sequence so arguments containing spaces
        are preserved. Uses ``-T`` to disable TTY allocation (non-interactive).
        """
        result = self._run(
            ["exec", "-T", service, *command],
            check=True,
            timeout=None,
        )
        return result.stdout or ""

    # -----------------------------------------------------------------------
    # diagnostics
    # -----------------------------------------------------------------------

    def logs(self, service: str | None = None, *, tail: int = 100) -> str:
        """Return the last ``tail`` log lines, optionally for one service."""
        args: list[str] = ["logs", f"--tail={tail}"]
        if service:
            args.append(service)
        result = self._run(args, check=False, timeout=_DEFAULT_TIMEOUT_S)
        return (result.stdout or "") + (result.stderr or "")

    def ps(self) -> str:
        """Return the raw ``docker compose ps`` text table."""
        result = self._run(["ps"], check=False, timeout=60.0)
        return result.stdout or ""

    def running_services(self) -> list[str]:
        """Return service names whose status indicates they are running."""
        entries = self._ps_json()
        return [
            str(entry["Service"])
            for entry in entries
            if _is_up(str(entry.get("State", entry.get("Status", ""))))
        ]

    def is_running(self, service: str) -> bool:
        """Return whether ``service`` is in a running state."""
        entries = self._ps_json()
        return any(
            str(entry.get("Service")) == service
            and _is_up(str(entry.get("State", entry.get("Status", ""))))
            for entry in entries
        )

    def wait_for_healthy(
        self,
        service: str,
        *,
        timeout_s: float = _HEALTH_TIMEOUT_S,
        interval_s: float = _HEALTH_INTERVAL_S,
    ) -> bool:
        """Poll until ``service`` reports ``(healthy)``, or the deadline passes."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            for entry in self._ps_json():
                if str(entry.get("Service")) != service:
                    continue
                if _is_healthy(str(entry.get("Health", entry.get("Status", "")))):
                    return True
            time.sleep(interval_s)
        return False

    # -----------------------------------------------------------------------
    # internals
    # -----------------------------------------------------------------------

    def _ps_json(self) -> list[dict[str, object]]:
        """Return the parsed ``docker compose ps --format json`` entries."""
        result = self._run(
            ["ps", "--format", "json"],
            check=False,
            timeout=60.0,
        )
        stdout = result.stdout or ""
        if not stdout.strip():
            return []
        entries = _parse_ps_json(stdout)
        return [e for e in entries if isinstance(e, dict)]

    def _run(
        self,
        args: Sequence[str],
        *,
        check: bool,
        timeout: float | None,
    ) -> subprocess.CompletedProcess[str]:
        cmd = self._base_cmd + list(args)
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise ComposeError(
                f"docker compose {' '.join(args)} timed out after {timeout}s"
            ) from exc
        if check and result.returncode != 0:
            raise ComposeError(
                f"docker compose {' '.join(args)} failed "
                f"(exit {result.returncode}): {result.stderr or result.stdout or ''}"
            )
        return result


def _first_scaled(scale: Mapping[str, int] | None) -> str | None:
    if not scale:
        return None
    return next(iter(scale))


def _parse_ps_json(stdout: str) -> list[dict[str, object]]:
    """Parse ``docker compose ps --format json`` output.

    compose v2 emits one JSON object per line. Some versions emit a single
    JSON array. Both forms are accepted; non-JSON lines are skipped.
    """
    stripped = stdout.strip()
    if not stripped:
        return []
    if stripped.startswith("["):
        try:
            decoded = json.loads(stripped)
        except (json.JSONDecodeError, ValueError) as _exc:
            return []
        if isinstance(decoded, list):
            return [item for item in decoded if isinstance(item, dict)]
        return []
    entries: list[dict[str, object]] = []
    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            decoded = json.loads(line)
        except (json.JSONDecodeError, ValueError) as _exc:
            continue
        if isinstance(decoded, dict):
            entries.append(decoded)
    return entries


def _is_up(state: str) -> bool:
    """Return whether a service state string indicates running."""
    if not state:
        return False
    head = state.split(" ", 1)[0]
    return head.lower() in ("running", "up")


def _is_healthy(state: str) -> bool:
    """Return whether a health/state string reports healthy."""
    text = state.lower()
    return "healthy" in text and "unhealthy" not in text

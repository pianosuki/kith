"""Client events, bootstrap specifications and status snapshots."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DecodedFrame:
    """A decoded proto frame, copied for Python-side ownership."""

    type_id: int
    flags: int
    has_correlation: bool
    correlation_id: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class ClientEvent:
    """An event derived from an inbound runtime frame."""

    ts_mono_ns: int
    type_id: int
    correlation_id: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class BootstrapStep:
    """A Python spec for one bootstrap FSM step.

    on_enter returns (type_id, payload_bytes) for the outbound frame, or
    None to wait passively. on_reply receives the decoded inbound frame and
    returns the next step index (== step_count means READY, > step_count
    means FAILED, == current step retries). A None callback auto-advances.
    """

    await_type_id: int
    on_enter: Callable[[], tuple[int, bytes] | None] | None = None
    on_reply: Callable[[DecodedFrame], int] | None = None


@dataclass(frozen=True, slots=True)
class ClientStatus:
    """A snapshot of connection and bootstrap state."""

    connected: bool
    bootstrap_state: int
    bootstrap_step: int
    bootstrap_step_count: int
    rtt_last_ms: int
    reconnect_attempts: int

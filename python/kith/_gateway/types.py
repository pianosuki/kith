"""Gateway types implementation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from kith._generated import gateway as gen_gateway


class SessionType(IntEnum):
    """Session authentication type."""

    SUBSCRIBER = int(gen_gateway.kith_gateway_session_type.KITH_GATEWAY_SESSION_SUBSCRIBER)
    APP = int(gen_gateway.kith_gateway_session_type.KITH_GATEWAY_SESSION_APP)
    SERVICE = int(gen_gateway.kith_gateway_session_type.KITH_GATEWAY_SESSION_SERVICE)


@dataclass(frozen=True)
class SessionInfo:
    """Session metadata."""

    session_id: int
    principal_id: int
    actor_id: int
    type: int


@dataclass(frozen=True)
class CacheStats:
    """Shared cell cache statistics.

    ``cell_count`` counts cells tracked in the shared cache;
    ``subscribed_cell_count`` counts cells subscribed on the gateway's
    fabric subscription. Both tables are fixed-size: recurring
    ``KITH_ENOMEM`` from ``window_add`` is a read-the-gauges-first
    signal, not by itself proof either table is full — a failed add is
    retained and healed by the tick pass's retry (the
    ``kith_gateway_window_retries_pending`` metric reads the queue's
    depth), and the getting-started guide's window capacity section
    carries the sizing contract.
    """

    cell_count: int
    subscribed_cell_count: int
    last_refresh_ms: int


@dataclass(frozen=True)
class ViewSnapshot:
    """A subscriber's composed view set metadata."""

    subscriber_actor_id: int
    built_at_ms: int
    candidate_count: int
    selected_count: int
    tier_selected_count: tuple[int, ...]
    class_selected_count: tuple[int, ...]
    sticky_selected_count: int
    demoted_selected_count: int


@dataclass(frozen=True)
class ViewSubject:
    """One subject in a composed view set."""

    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    input_tick: int
    level: int
    subject_class: int
    sticky: bool
    demoted: bool


@dataclass(frozen=True)
class DeliveryStats:
    """Per-session delivery result."""

    enqueued: int
    dropped: int


@dataclass(frozen=True)
class DeliveryTotals:
    """Cumulative delivery totals folded across every deliver call (both
    the inline tick path and the executor's workers)."""

    enqueued: int
    dropped: int
    events_enqueued: int
    suppressed: int


@dataclass(frozen=True)
class ViewTotals:
    """Cumulative view-composition population totals aggregated at each
    delivery-pass visit of a view-holding session."""

    visits: int
    candidate_total: int
    selected_total: int
    candidate_high_watermark: int
    selected_high_watermark: int


SessionType.__module__ = "kith.gateway"

SessionInfo.__module__ = "kith.gateway"

CacheStats.__module__ = "kith.gateway"

ViewSnapshot.__module__ = "kith.gateway"

ViewSubject.__module__ = "kith.gateway"

DeliveryStats.__module__ = "kith.gateway"

DeliveryTotals.__module__ = "kith.gateway"

ViewTotals.__module__ = "kith.gateway"

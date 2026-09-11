"""The visual client's thread-safe snapshot store.

The network thread folds each replication frame into the store; the
render thread projects it. One lock covers every handoff: last-known
actor states with arrival histories, membership ghosts, the snap-event
log (the rubberband detector), chat, per-second rate counters, link
status, and the server metrics scrape. The snap threshold auto-calibrates
to one tick of the configured max run velocity, so a rendered
displacement jump past it is wire evidence, not an interp artifact.
"""

from __future__ import annotations

import itertools
import json
import math
import threading
import time
from collections import deque
from typing import TextIO

from kith.examples.visual._protocol import (
    MEMBERSHIP_EVENT_CROWD_ENTER,
    MEMBERSHIP_EVENT_CROWD_EXIT,
    MEMBERSHIP_EVENT_ENTER,
    MEMBERSHIP_EVENT_EXIT,
    MEMBERSHIP_EVENT_VANISH,
    MEMBERSHIP_MARKER,
    ActorStateView,
    is_membership_record,
)
from kith.examples.visual._world import q16_to_units


_HISTORY_LEN = 16
_GHOST_TTL_S = 1.2
_SNAP_MARGIN = 1.05  # a jump must clear one tick of max velocity by 5%
_SNAP_SEVERE = 2.1  # past two ticks of max velocity it is a snap, not a gap

# Actor colors: the self actor, session principals, ambient actors, and
# the staleness tint the projection lerps toward.
_SELF_COLOR = (80, 255, 120)
_PLAYER_COLOR = (90, 200, 255)
_AMBIENT_COLOR = (170, 170, 170)
_STALE_COLOR = (255, 70, 60)
_GHOST_COLORS = {
    MEMBERSHIP_EVENT_ENTER: (120, 255, 150),
    MEMBERSHIP_EVENT_EXIT: (255, 170, 60),
    MEMBERSHIP_EVENT_CROWD_ENTER: (150, 200, 255),
    MEMBERSHIP_EVENT_CROWD_EXIT: (150, 200, 255),
    MEMBERSHIP_EVENT_VANISH: (255, 80, 80),
}


class ActorSnap:
    """One actor's last received state plus its arrival history."""

    __slots__ = (
        "history",
        "input_tick",
        "last_ns",
        "product_level",
        "update_seq",
        "vx",
        "vy",
        "x",
        "y",
    )

    def __init__(self, x: float, y: float, now_ns: int) -> None:
        self.x = x
        self.y = y
        self.vx = 0.0
        self.vy = 0.0
        self.update_seq = 0
        self.input_tick = 0
        self.product_level = 0
        self.last_ns = now_ns
        self.history: deque[tuple[int, float, float]] = deque(maxlen=_HISTORY_LEN)


class SnapshotStore:
    """The thread-safe handoff between the network thread and the render
    thread: last-known actor states, membership ghosts, snap events, chat,
    and the counters the HUD reads."""

    def __init__(
        self, *, tick_hz: float, max_speed_units: float, snap_log: TextIO | None = None
    ) -> None:
        self._lock = threading.Lock()
        self._tick_s = 1.0 / tick_hz
        self._stale_s = 3.0 * self._tick_s
        self._snap_threshold_units = _SNAP_MARGIN * max_speed_units * self._tick_s
        self._snap_log = snap_log
        self._actors: dict[int, ActorSnap] = {}
        self._ghosts: dict[int, tuple[int, int, float | None, float | None]] = {}
        self._pending_enter: set[int] = set()
        self._snaps: deque[tuple[int, int, float, bool]] = deque(maxlen=48)
        self._chat: deque[tuple[int, int, str]] = deque(maxlen=8)
        self._jitter_ns: deque[int] = deque(maxlen=128)
        self._sends: deque[tuple[int, int]] = deque(maxlen=64)
        self._last_arrival_ns: int | None = None
        self._self_actor_id = 0
        self._self_last_seq = 0
        # counters
        self._frames_total = 0
        self._records_total = 0
        self._bytes_total = 0
        self._snaps_total = 0
        self._snips_total = 0
        self._memberships_total = 0
        self._self_gaps = 0
        self._drop_total = 0
        self._batches_total = 0
        self._last_batch = 0
        self._max_batch = 0
        self._rate_count = 0
        self._rate_ts: int | None = None
        self._rate = 0.0
        # status + server scrape
        self._connected = False
        self._status_seen = False
        self._rtt_ms = 0
        self._reconnect_attempts = 0
        self._echo_lag_ms: float | None = None
        self._metrics: dict[str, object] | None = None
        self._supp_prev: int | None = None
        self._supp_rate = 0
        self._poll_fails = 0
        # The ambient/player coloring divides session actors from ambient
        # ones by the server-reported ambient count (ambient actors
        # allocate first, so their ids land below every session id).
        self._ambient_ceil = 0

    # ---------------------------------------------------------------------------
    # net thread
    # ---------------------------------------------------------------------------

    def attach_snap_log(self, fh: TextIO) -> None:
        """Attach the per-event snap JSONL stream (net-thread appends only)."""
        with self._lock:
            self._snap_log = fh

    def absorb_records(self, records: list[ActorStateView], now_ns: int) -> None:
        """Fold one replication frame's records into the store."""
        with self._lock:
            self._records_total += len(records)
            self._rate_count += len(records)
            self._batches_total += 1
            self._last_batch = len(records)
            if self._last_batch > self._max_batch:
                self._max_batch = self._last_batch
            if self._rate_ts is None:
                self._rate_ts = now_ns
            elif now_ns - self._rate_ts >= 1_000_000_000:
                span = (now_ns - self._rate_ts) / 1e9
                self._rate = self._rate_count / span
                self._rate_ts = now_ns
                self._rate_count = 0
            if self._last_arrival_ns is not None:
                self._jitter_ns.append(now_ns - self._last_arrival_ns)
            self._last_arrival_ns = now_ns
            for rec in records:
                actor_id = rec.actor_id
                if is_membership_record(rec):
                    self._memberships_total += 1
                    kind = rec.product_level & ~MEMBERSHIP_MARKER
                    if kind == MEMBERSHIP_EVENT_ENTER:
                        self._pending_enter.add(actor_id)
                        self._ghosts[actor_id] = (kind, now_ns, None, None)
                    else:
                        known = self._actors.get(actor_id)
                        gx = known.x if known is not None else None
                        gy = known.y if known is not None else None
                        self._ghosts[actor_id] = (kind, now_ns, gx, gy)
                        # The composer stopped tracking the subject: drop the
                        # stale snapshot so a re-entry starts a fresh track
                        # (no phantom snap against the old position).
                        self._actors.pop(actor_id, None)
                    continue
                xu = q16_to_units(rec.pos_x)
                yu = q16_to_units(rec.pos_y)
                snap = self._actors.get(actor_id)
                if snap is None:
                    snap = ActorSnap(xu, yu, now_ns)
                    self._actors[actor_id] = snap
                    self._ghosts.pop(actor_id, None)
                else:
                    disp = math.hypot(xu - snap.x, yu - snap.y)
                    if disp > self._snap_threshold_units:
                        severe = disp > self._snap_threshold_units * (_SNAP_SEVERE / _SNAP_MARGIN)
                        if severe:
                            self._snaps_total += 1
                        else:
                            self._snips_total += 1
                        self._snaps.append((now_ns, actor_id, disp, severe))
                        if self._snap_log is not None:
                            try:
                                self._snap_log.write(
                                    json.dumps(
                                        {
                                            "wall_s": time.time(),
                                            "actor": actor_id,
                                            "disp_u": round(disp, 3),
                                            "severe": severe,
                                        }
                                    )
                                    + "\n"
                                )
                                self._snap_log.flush()
                            except Exception:
                                pass
                snap.x = xu
                snap.y = yu
                snap.vx = q16_to_units(rec.vel_x)
                snap.vy = q16_to_units(rec.vel_y)
                snap.update_seq = rec.update_seq
                snap.input_tick = rec.input_tick
                snap.product_level = rec.product_level
                snap.last_ns = now_ns
                snap.history.append((now_ns, xu, yu))
                if actor_id in self._pending_enter:
                    self._pending_enter.discard(actor_id)
                    self._ghosts[actor_id] = (MEMBERSHIP_EVENT_ENTER, now_ns, xu, yu)
                if actor_id == self._self_actor_id:
                    if self._self_last_seq and rec.update_seq > self._self_last_seq + 1:
                        self._self_gaps += rec.update_seq - self._self_last_seq - 1
                    self._self_last_seq = rec.update_seq
                    while self._sends and self._sends[0][0] <= rec.input_tick:
                        _sent_tick, sent_ns = self._sends.popleft()
                        self._echo_lag_ms = (now_ns - sent_ns) / 1e6

    def record_chat(self, now_ns: int, actor_id: int, text: str) -> None:
        with self._lock:
            self._chat.append((now_ns, actor_id, text))

    def set_self_actor(self, actor_id: int) -> None:
        with self._lock:
            # A login reply arrives once per connect cycle, and every
            # reconnect in the example lands on a fresh server: a second
            # reply means a new world, whose actors will never deliver
            # membership exits for the previous one's entries.
            if self._actors or self._ghosts or self._self_actor_id or self._sends:
                self._actors.clear()
                self._ghosts.clear()
                self._pending_enter.clear()
                self._snaps.clear()
                self._snaps_total = 0
                self._snips_total = 0
                self._self_last_seq = 0
                self._self_gaps = 0
                self._echo_lag_ms = None
                self._sends.clear()
                self._jitter_ns.clear()
                self._last_arrival_ns = None
            self._self_actor_id = actor_id

    def push_send(self, input_tick: int, now_ns: int) -> None:
        with self._lock:
            self._sends.append((input_tick, now_ns))

    def set_status(self, connected: bool, rtt_ms: int, reconnect_attempts: int) -> None:
        with self._lock:
            self._connected = connected
            self._status_seen = True
            self._rtt_ms = rtt_ms
            self._reconnect_attempts = reconnect_attempts
            self._poll_fails = 0

    def fail_status_poll(self) -> None:
        """A status query raised. Three consecutive failures (~3 s) mean
        the engine's state is unreadable and the link cannot be trusted:
        flip the banner inputs to down rather than freezing on the last
        good snapshot (the scenario-end case, where the engine is stuck
        in a backoff loop whose status raises)."""
        with self._lock:
            self._poll_fails += 1
            if self._poll_fails >= 3:
                self._connected = False
                self._status_seen = True

    def mark_link_up(self) -> None:
        """A login reply arrived on the wire — the link is live even if
        the 1 Hz status poll has not caught up yet (rebind shows the
        world the instant it exists, without waiting a poll)."""
        with self._lock:
            self._connected = True
            self._status_seen = True

    def frame_received(self, size: int) -> None:
        with self._lock:
            self._frames_total += 1
            self._bytes_total += size

    def set_metrics(self, data: dict[str, object] | None) -> None:
        with self._lock:
            self._metrics = data
            # Suppression is latched (it cannot flap at the budget edge),
            # but the latch itself is not exposed; a per-second climb of
            # the cumulative counter is the honest observable of it
            # actively suppressing.
            delivery = data.get("delivery", {}) if data else {}
            total = delivery.get("suppressed") if isinstance(delivery, dict) else None
            if isinstance(total, int):
                self._supp_rate = 0 if self._supp_prev is None else total - self._supp_prev
                self._supp_prev = total
            ambient = data.get("ambient_count") if data else None
            if isinstance(ambient, int):
                self._ambient_ceil = ambient

    def count_drop(self) -> None:
        """Count a submit refused while disconnected (render-thread only)."""
        with self._lock:
            self._drop_total += 1

    def dropped_submits(self) -> int:
        with self._lock:
            return self._drop_total

    # ---------------------------------------------------------------------------
    # render thread
    # ---------------------------------------------------------------------------

    def self_actor_id(self) -> int:
        with self._lock:
            return self._self_actor_id

    def link_state(self) -> tuple[bool, int]:
        """(connected, reconnect_attempts) — the banner's inputs."""
        with self._lock:
            return self._connected, self._reconnect_attempts

    def status_seen(self) -> bool:
        """True once the link monitor has completed a real check — the
        banner must not fire on the pre-scrape boot window."""
        with self._lock:
            return self._status_seen

    def render_view(
        self, now_ns: int, interp: bool, interp_s: float
    ) -> list[tuple[float, float, tuple[int, int, int], float, bool]]:
        """Project every actor into render-space: (x, y, color, radius,
        outline). RAW draws the last received position; INTERP draws the
        position the server had ``interp_s`` ago (interpolated), lerped
        between the two bracketing arrival samples."""
        target = now_ns - int(interp_s * 1e9)
        out: list[tuple[float, float, tuple[int, int, int], float, bool]] = []
        with self._lock:
            # A dead link means nothing on screen is wire truth anymore;
            # stale-red tracks read as a live-but-frozen world, so the
            # whole field degrades to faint hollow outlines.
            link_live = self._connected
            for actor_id, snap in self._actors.items():
                age_s = (now_ns - snap.last_ns) / 1e9
                staleness = min(1.0, age_s / self._stale_s) if self._stale_s > 0 else 0.0
                if not link_live:
                    out.append((snap.x, snap.y, (70, 78, 72), 4.0, True))
                    continue
                if actor_id == self._self_actor_id:
                    color = _SELF_COLOR
                elif actor_id <= self._ambient_ceil:
                    color = _AMBIENT_COLOR
                else:
                    color = _PLAYER_COLOR
                color = _lerp_rgb(color, _STALE_COLOR, staleness * 0.9)
                if interp:
                    x, y = _interp_pos(snap.history, target)
                else:
                    x, y = snap.x, snap.y
                level = snap.product_level & MEMBERSHIP_MARKER
                if level:  # membership-marked leftovers render as tiny dots
                    out.append((x, y, _lerp_rgb(color, (60, 60, 60), 0.5), 2.0, False))
                elif snap.product_level == 1:
                    out.append((x, y, color, 6.0, True))
                elif snap.product_level >= 2:
                    out.append((x, y, color, 2.5, False))
                else:
                    radius = 7.0 if actor_id != self._self_actor_id else 8.0
                    out.append((x, y, color, radius, False))
        return out

    def ghost_view(
        self, now_ns: int
    ) -> list[tuple[float, float, tuple[int, int, int, int], float]]:
        """Membership ghosts still inside their fade window."""
        out = []
        with self._lock:
            expired: list[int] = []
            for actor_id, (kind, ts, gx, gy) in self._ghosts.items():
                age_s = (now_ns - ts) / 1e9
                if age_s > _GHOST_TTL_S:
                    expired.append(actor_id)
                    continue
                if gx is None or gy is None:
                    continue
                alpha = 1.0 - age_s / _GHOST_TTL_S
                base = _GHOST_COLORS.get(kind, (200, 200, 200))
                out.append((gx, gy, _fade(base, alpha), 8.0 + 4.0 * alpha))
            for actor_id in expired:
                del self._ghosts[actor_id]
        return out

    def hud_lines(self, now_ns: int, fps: float) -> list[str]:
        """The HUD's live rows (server scrape + client counters)."""
        with self._lock:
            metrics = self._metrics
            jitter = ""
            if len(self._jitter_ns) >= 8:
                vals = sorted(self._jitter_ns)
                p50 = vals[len(vals) // 2] / 1e6
                jitter = f"  jitter p50 {p50:.1f} max {vals[-1] / 1e6:.1f} ms"
            echo = f"{self._echo_lag_ms:.0f}" if self._echo_lag_ms is not None else "-"
            rows = [
                f"fps {fps:5.1f}  rec/s {self._rate:6.1f}{jitter}",
                f"frames {self._frames_total}  records {self._records_total}"
                f"  bytes {self._bytes_total}",
                f"connected {self._connected}  rtt {self._rtt_ms} ms"
                f"  reconn {self._reconnect_attempts}",
                f"echo lag {echo} ms  self seq-gaps {self._self_gaps}  drops {self._drop_total}",
                f"actors {len(self._actors)}  snaps {self._snaps_total}"
                f" (snips {self._snips_total})  membership {self._memberships_total}",
            ]
            if metrics is not None:
                delivery = metrics.get("delivery", {})
                if not isinstance(delivery, dict):
                    delivery = {}
                rows.append(
                    "srv tick "
                    f"{metrics.get('tick')} hz {metrics.get('tick_hz')}"
                    f"  sessions {metrics.get('sessions')}  actors {metrics.get('actors')}"
                )
                rows.append(
                    "delivery enq "
                    f"{delivery.get('enqueued')}  drop {delivery.get('dropped')}"
                    f"  supp {delivery.get('suppressed')}  events {delivery.get('events_enqueued')}"
                )
                rate = self._supp_rate
                latch = f"suppression ACTIVE +{rate}/s" if rate > 0 else "suppression idle"
                rows.append(f"budget {latch}  (latch = crowd regime hysteresis)")
        return rows

    def snap_lines(self, now_ns: int, count: int = 6) -> list[str]:
        with self._lock:
            recent = list(self._snaps)[-count:]
        return [
            f"{'SNAP' if severe else 'snip'} a{aid} +{disp:.1f}u @ {(ns - now_ns) / 1e9:+.1f}s"
            for ns, aid, disp, severe in recent
        ]

    def chat_lines(self) -> list[str]:
        with self._lock:
            return [f"[{aid}] {text}" for _ts, aid, text in self._chat]

    def snap_threshold_units(self) -> float:
        return self._snap_threshold_units

    def debug_view(self, fps: float, draw_ms: float) -> dict[str, object]:
        """Everything the HUD knows in one locked pass — the HUD
        streamer's payload (it runs on its own daemon thread at 1 Hz;
        draw_ms is the render thread's last frame wall time)."""
        now_ns = time.monotonic_ns()
        with self._lock:
            jitter: list[float] = []
            if len(self._jitter_ns) >= 2:
                vals = sorted(self._jitter_ns)
                jitter = [round(vals[len(vals) // 2] / 1e6, 1), round(vals[-1] / 1e6, 1)]
            snaps = [
                {
                    "age_s": round((ns - now_ns) / 1e9, 1),
                    "actor": aid,
                    "disp_u": round(disp, 2),
                    "severe": severe,
                }
                for ns, aid, disp, severe in self._snaps
            ]
            return {
                "fps": round(fps, 1),
                "draw_ms": round(draw_ms, 1),
                "connected": self._connected,
                "rtt_ms": self._rtt_ms,
                "reconnects": self._reconnect_attempts,
                "frames": self._frames_total,
                "records": self._records_total,
                "bytes": self._bytes_total,
                "rec_s": round(self._rate, 1),
                "batches": self._batches_total,
                "last_batch": self._last_batch,
                "max_batch": self._max_batch,
                "echo_lag_ms": None if self._echo_lag_ms is None else round(self._echo_lag_ms, 1),
                "self_seq_gaps": self._self_gaps,
                "input_drops": self._drop_total,
                "actors": len(self._actors),
                "ghosts": len(self._ghosts),
                "pending_enter": len(self._pending_enter),
                "snaps_total": self._snaps_total,
                "snips_total": self._snips_total,
                "membership": self._memberships_total,
                "jitter_p50_max_ms": jitter,
                "server": self._metrics,
                "snap_events": snaps,
            }

    def raw_positions(self) -> dict[int, tuple[float, float]]:
        """Last received positions, for the self ring (read-only)."""
        with self._lock:
            return {actor_id: (snap.x, snap.y) for actor_id, snap in self._actors.items()}

    def echo_lag_ms(self) -> float | None:
        with self._lock:
            return self._echo_lag_ms

    def snaps_total(self) -> int:
        with self._lock:
            return self._snaps_total

    def snips_total(self) -> int:
        with self._lock:
            return self._snips_total

    def cadence_stats(self, actor_id: int) -> tuple[int, float, float, float, float]:
        """(samples, p50 disp u, max disp u, p50 gap ms, max gap ms) for one
        actor's arrival history — the record-cadence diagnostic."""
        with self._lock:
            snap = self._actors.get(actor_id)
            if snap is None or len(snap.history) < 2:
                return (0, 0.0, 0.0, 0.0, 0.0)
            items = list(snap.history)
            disps = sorted(
                math.hypot(b[1] - a[1], b[2] - a[2]) for a, b in itertools.pairwise(items)
            )
            gaps = sorted((b[0] - a[0]) / 1e6 for a, b in itertools.pairwise(items))
            return (
                len(items),
                disps[len(disps) // 2],
                disps[-1],
                gaps[len(gaps) // 2],
                gaps[-1],
            )


def _lerp_rgb(a: tuple[int, int, int], b: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    return (
        int(a[0] + (b[0] - a[0]) * t),
        int(a[1] + (b[1] - a[1]) * t),
        int(a[2] + (b[2] - a[2]) * t),
    )


def _fade(color: tuple[int, int, int], alpha: float) -> tuple[int, int, int, int]:
    return color[0], color[1], color[2], max(0, min(255, int(255 * alpha)))


def _interp_pos(history: deque[tuple[int, float, float]], target_ns: int) -> tuple[float, float]:
    """Position at ``target_ns`` between bracketing arrival samples (no
    extrapolation: samples older than the target clamp to the newest)."""
    if not history:
        return 0.0, 0.0
    prev = history[0]
    for sample in history:
        if sample[0] > target_ns:
            if prev[0] > target_ns:
                return prev[1], prev[2]
            span = sample[0] - prev[0]
            t = (target_ns - prev[0]) / span if span > 0 else 0.0
            return prev[1] + (sample[1] - prev[1]) * t, prev[2] + (sample[2] - prev[2]) * t
        prev = sample
    return prev[1], prev[2]

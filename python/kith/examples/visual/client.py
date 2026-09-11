"""Open Range visual client: the arcade window over the snapshot store.

The render thread (this module's main thread) draws the world from the
store the network thread fills: the cell grid, the 3x3-cell interest
window around the self actor, per-actor circles colored by role and
staleness, membership ghosts, and the HUD — the measurement core
(fps, records/s, jitter, echo lag, snap events, delivery counters).
WASD moves, Shift runs, Enter chats, Tab toggles RAW (wire truth) and
INTERP (sample interpolation). Boot the network thread from ``_net``
before the window opens and stop it after it closes.
"""

from __future__ import annotations

import argparse
import contextlib
import faulthandler
import json
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

import arcade

from kith import SimInput
from kith._agent.ahc import KithClientError
from kith._agent.server_control import ServerControlClient
from kith.examples.visual import _world
from kith.examples.visual._net import NetClient, NetThread
from kith.examples.visual._store import SnapshotStore


__all__ = ["ClientArgs", "HudStreamer", "PlaygroundWindow", "main", "parse_args"]


# ---------------------------------------------------------------------------
# window constants
# ---------------------------------------------------------------------------

_MAX_SPEED_UNITS = 40.0  # the configured run speed (world units / second)
_HUD_ASSIGN_S = 0.2  # HUD text relayouts are the render loop's expensive path
# Run rides the held-key set, never the event `modifiers` bitmask: X11 reports
# the modifier state *before* each key event, so a shift press self-reports
# shift-up and a shift release self-reports shift-down — event-latched run
# state engages on release and drops on the next direction key.
_RUN_KEYS = frozenset((arcade.key.LSHIFT, arcade.key.RSHIFT))
_SELF_RING = (235, 255, 240)
_GRID_COLOR = (36, 44, 38)
_BORDER_COLOR = (90, 110, 95)
_AOI_FILL = (80, 255, 140, 22)
_AOI_EDGE = (110, 220, 150, 170)
_AOI_CELL_EDGE = (160, 255, 190, 220)
_HUD_COLOR = (220, 224, 220)
_HUD_DIM = (150, 158, 150)
_SNAP_COLOR = (255, 200, 90)
_CHAT_COLOR = (255, 236, 160)
_BANNER_COLOR = (255, 80, 80)
_BG = (14, 16, 14, 255)
_BACKING_LEFT = (10, 12, 10, 170)
_BACKING_RIGHT = (10, 12, 10, 150)


class HudStreamer(threading.Thread):
    """Appends the full store state as one JSON line per second — the
    client's machine-readable HUD record. One fresh file per client run."""

    def __init__(self, store: SnapshotStore, window: PlaygroundWindow, path: Path) -> None:
        super().__init__(name="hud-stream", daemon=True)
        self._store = store
        self._window = window
        self._path = path
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w") as fh:
            while not self._stop.wait(1.0):
                try:
                    fps = self._window._fps
                    draw_ms = self._window._last_draw_ms
                    fh.write(json.dumps(self._store.debug_view(fps, draw_ms)) + "\n")
                    fh.flush()
                except Exception:
                    pass  # telemetry never takes the window down


@dataclass(frozen=True)
class ClientArgs:
    host: str
    port: int
    control_port: int
    principal_id: int
    tick_hz: int
    send_hz: float
    interp_ms: float
    interp_default: bool
    vsync: bool
    size: int
    max_speed_units: float


class PlaygroundWindow(arcade.Window):
    """The renderer + input surface; the network thread runs beside it."""

    def __init__(self, client: NetClient, store: SnapshotStore, args: ClientArgs) -> None:
        super().__init__(
            args.size,
            args.size,
            f"Open Range — principal {args.principal_id} @ {args.host}:{args.port}",
            vsync=args.vsync,
            update_rate=1 / 60,
            resizable=False,
        )
        arcade.set_background_color(_BG)
        self._client = client
        self._store = store
        self._args = args
        self._world_px = float(args.size)
        self._scale = args.size / _world.WORLD_UNITS
        self._interp = args.interp_default
        self._interp_s = args.interp_ms / 1000.0
        self._send_interval = 1.0 / args.send_hz
        self._keys: set[int] = set()
        self._chat_mode = False
        self._chat_buffer = ""
        self._next_input_tick = 1
        self._last_send_ns = 0
        self._fps = 0.0
        self._quit_deadline: float | None = None
        self._font = ("calibri", "arial")
        # Cached HUD labels (arcade's fast path); .value mutates per frame.
        # The help line owns the top row; the live rows start one step down.
        self._hud_texts = [
            arcade.Text(
                "", 12.0, self.height - 24.0 - 18.0 * (i + 1), _HUD_COLOR, 11, font_name=self._font
            )
            for i in range(10)
        ]
        self._help_text = arcade.Text(
            "", 12.0, self.height - 24.0, _HUD_DIM, 11, font_name=self._font
        )
        self._snap_texts = [
            arcade.Text(
                "",
                self.width - 262.0,
                self.height - 24.0 - 15.0 * i,
                _SNAP_COLOR,
                10,
                font_name=self._font,
            )
            for i in range(7)
        ]
        self._chat_texts = [
            arcade.Text("", 12.0, 24.0 + 16.0 * i, _CHAT_COLOR, 11, font_name=self._font)
            for i in range(9)
        ]
        # Last assigned string per HUD row: pyglet rebuilds the whole glyph
        # document on every value change, so rows are re-assigned only when
        # their content actually differs, at a fixed cadence.
        self._hud_assign_ns = 0
        self._help_cache = ""
        self._hud_cache = ["" for _ in self._hud_texts]
        self._snap_cache = ["" for _ in self._snap_texts]
        self._chat_cache = ["" for _ in self._chat_texts]
        self._last_draw_ms = 0.0
        # The loud link banner: scenario end and world death must be
        # unmistakable. Small-cadence reassigned like every HUD row.
        self._banner = arcade.Text(
            "",
            self.width / 2,
            self.height / 2 + 140.0,
            _BANNER_COLOR,
            22,
            font_name=self._font,
            anchor_x="center",
        )
        self._banner_cache = ""

    def _assign(
        self, text: arcade.Text, cache: list[str], index: int, value: str, assign_ok: bool
    ) -> None:
        if not assign_ok or cache[index] == value:
            return
        cache[index] = value
        text.value = value

    # -----------------------------------------------------------------------
    # input
    # -----------------------------------------------------------------------

    def on_key_press(self, key: int, modifiers: int) -> None:
        del modifiers
        self._keys.add(key)
        if self._chat_mode:
            if key == arcade.key.BACKSPACE:
                self._chat_buffer = self._chat_buffer[:-1]
            elif key == arcade.key.ESCAPE:
                self._chat_mode = False
                self._chat_buffer = ""
            elif key in (arcade.key.ENTER, arcade.key.NUM_ENTER):
                text = self._chat_buffer.strip()
                self._chat_mode = False
                self._chat_buffer = ""
                actor_id = self._store.self_actor_id()
                if text and actor_id:
                    try:
                        self._client.submit_chat(actor_id, text)
                    except KithClientError:
                        self._store.count_drop()
            return
        if key == arcade.key.TAB:
            self._interp = not self._interp
        elif key in (arcade.key.ENTER, arcade.key.NUM_ENTER) and self._store.self_actor_id():
            self._chat_mode = True

    def on_key_release(self, key: int, modifiers: int) -> None:
        del modifiers
        self._keys.discard(key)

    def on_deactivate(self) -> None:
        # Focus loss delivers no releases for held keys; without this the
        # next window keeps moving (and running) on stale key state.
        self._keys.clear()

    def _is_running(self) -> bool:
        return bool(self._keys & _RUN_KEYS)

    def on_text(self, text: str) -> None:
        if self._chat_mode and text.isprintable():
            self._chat_buffer += text

    def _move_vector(self) -> tuple[int, int]:
        k = arcade.key
        dx = (1 if k.D in self._keys else 0) - (1 if k.A in self._keys else 0)
        dy = (1 if k.W in self._keys else 0) - (1 if k.S in self._keys else 0)
        mag = max(abs(dx), abs(dy))
        if mag == 0:
            return 0, 0
        return (dx * 32_767) // mag, (dy * 32_767) // mag

    def set_quit_after(self, seconds: float) -> None:
        """Close the window ``seconds`` after boot (headless checks)."""
        self._quit_deadline = time.monotonic() + seconds

    def on_update(self, delta_time: float) -> None:
        if delta_time > 0:
            self._fps = 0.9 * self._fps + 0.1 * (1.0 / delta_time)
        deadline = self._quit_deadline
        if deadline is not None and time.monotonic() >= deadline:
            self.close()
            return
        now_ns = time.monotonic_ns()
        actor_id = self._store.self_actor_id()
        if (
            not self._chat_mode
            and actor_id
            and now_ns - self._last_send_ns >= self._send_interval * 1e9
        ):
            move_x, move_y = self._move_vector()
            inp = SimInput(
                input_tick=self._next_input_tick,
                move_x=move_x,
                move_y=move_y,
                flags=_world.RUN_FLAG if self._is_running() else 0,
            )
            try:
                self._client.submit_input(actor_id, inp)
            except KithClientError:
                # The engine is between sessions (drop/backoff): the input
                # goes nowhere, so keep the tick and retry on a subsequent frame.
                self._store.count_drop()
                return
            self._store.push_send(self._next_input_tick, now_ns)
            self._next_input_tick += 1
            self._last_send_ns = now_ns

    # -----------------------------------------------------------------------
    # rendering
    # -----------------------------------------------------------------------

    def _px(self, xu: float, yu: float) -> tuple[float, float]:
        return xu * self._scale, yu * self._scale

    def on_draw(self) -> None:
        t0 = time.monotonic_ns()
        self.clear()
        now_ns = time.monotonic_ns()
        self._draw_world()
        self._draw_aoi()
        for x, y, color, radius, outline in self._store.render_view(
            now_ns, self._interp, self._interp_s
        ):
            px, py = self._px(x, y)
            if outline:
                arcade.draw_circle_outline(px, py, radius, color, border_width=2)
            else:
                arcade.draw_circle_filled(px, py, radius, color)
        for gx, gy, gcolor, radius in self._store.ghost_view(now_ns):
            px, py = self._px(gx, gy)
            arcade.draw_circle_outline(px, py, radius, gcolor, border_width=2)
        if self._store.self_actor_id():
            self._draw_self_ring()
        self._draw_hud(now_ns)
        self._last_draw_ms = (time.monotonic_ns() - t0) / 1e6

    def _draw_self_ring(self) -> None:
        """A soft ring at the self actor's last received position."""
        actor_id = self._store.self_actor_id()
        position = self._store.raw_positions().get(actor_id)
        if position is None:
            return
        px, py = self._px(*position)
        arcade.draw_circle_outline(px, py, 11.0, _SELF_RING, border_width=1)

    def _draw_world(self) -> None:
        s = self._scale
        for i in range(_world.WORLD_CELLS + 1):
            p = i * _world.CELL_SIZE_UNITS * s
            arcade.draw_line(p, 0, p, self._world_px, _GRID_COLOR)
            arcade.draw_line(0, p, self._world_px, p, _GRID_COLOR)
        arcade.draw_lrbt_rectangle_outline(0, self._world_px, 0, self._world_px, _BORDER_COLOR, 2)

    def _draw_aoi(self) -> None:
        """The 3x3-cell interest window around the self actor's cell:
        cell-aligned, so its edges sit exactly where window membership
        enters and leaves, and the brighter center rect is the cell whose
        boundary crossing re-centers the window."""
        position = self._store.raw_positions().get(self._store.self_actor_id())
        if position is None:
            return
        s = self._scale
        cell = _world.CELL_SIZE_UNITS
        cx = int(position[0] // cell)
        cy = int(position[1] // cell)
        x0 = max(0, cx - 1) * cell * s
        y0 = max(0, cy - 1) * cell * s
        x1 = min(_world.WORLD_CELLS, cx + 2) * cell * s
        y1 = min(_world.WORLD_CELLS, cy + 2) * cell * s
        arcade.draw_lbwh_rectangle_filled(x0, y0, x1 - x0, y1 - y0, _AOI_FILL)
        arcade.draw_lrbt_rectangle_outline(x0, x1, y0, y1, _AOI_EDGE, 1)
        if 0 <= cx < _world.WORLD_CELLS and 0 <= cy < _world.WORLD_CELLS:
            arcade.draw_lrbt_rectangle_outline(
                cx * cell * s,
                (cx + 1) * cell * s,
                cy * cell * s,
                (cy + 1) * cell * s,
                _AOI_CELL_EDGE,
                1,
            )

    def _draw_hud(self, now_ns: int) -> None:
        # Translucent backing panels so the measurement core reads over the
        # world grid (alpha-blended; the grid stays visible underneath).
        arcade.draw_lbwh_rectangle_filled(
            6, self.height - 24.0 - 18.0 * 11 - 6, 470, 18.0 * 11 + 12, _BACKING_LEFT
        )
        arcade.draw_lbwh_rectangle_filled(
            self.width - 268.0,
            self.height - 24.0 - 15.0 * 7 - 4,
            262,
            15.0 * 7 + 10,
            _BACKING_RIGHT,
        )
        assign_ok = time.monotonic_ns() - self._hud_assign_ns >= int(_HUD_ASSIGN_S * 1e9)
        if assign_ok:
            self._hud_assign_ns = time.monotonic_ns()
        mode = f"INTERP {self._interp_s * 1000:.0f} ms" if self._interp else "RAW"
        self._assign(
            self._help_text,
            [self._help_cache],
            0,
            f"[{mode}]  tab toggles  WASD move  shift run  enter chat",
            assign_ok,
        )
        self._help_text.draw()
        lines = self._store.hud_lines(now_ns, self._fps)
        for i, text in enumerate(self._hud_texts):
            self._assign(text, self._hud_cache, i, lines[i] if i < len(lines) else "", assign_ok)
            text.draw()
        snaps = self._store.snap_lines(now_ns)
        threshold = self._store.snap_threshold_units()
        snap_rows = [*snaps, f"snap threshold {threshold:.2f} u (1 tick max)"]
        for i, text in enumerate(self._snap_texts):
            self._assign(
                text, self._snap_cache, i, snap_rows[i] if i < len(snap_rows) else "", assign_ok
            )
            text.draw()
        chat = self._store.chat_lines()
        if self._chat_mode:
            chat = [*chat, f"chat: {self._chat_buffer}_"]
        for i, text in enumerate(self._chat_texts):
            # The typing echo must bypass the cadence gate: a keystroke that
            # waits up to _HUD_ASSIGN_S to appear reads as input lag.
            typing_row = self._chat_mode and i == len(chat) - 1
            self._assign(
                text, self._chat_cache, i, chat[i] if i < len(chat) else "", assign_ok or typing_row
            )
            text.draw()
        connected, reconn = self._store.link_state()
        banner = (
            f"DISCONNECTED — scenario ended or server lost; reconnecting ({reconn})"
            if not connected and self._store.status_seen() and self._store.self_actor_id()
            else ""
        )
        self._assign(self._banner, [self._banner_cache], 0, banner, assign_ok)
        if banner:
            self._banner.draw()


# ---------------------------------------------------------------------------
# entry
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the visual client's knobs."""
    parser = argparse.ArgumentParser(prog="kith-visual client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True, help="gateway port")
    parser.add_argument(
        "--control-port", type=int, default=0, help="control port for /playground/metrics"
    )
    parser.add_argument("--principal", type=int, default=1)
    parser.add_argument(
        "--tick-hz", type=float, default=20.0, help="server tick (HUD staleness/interp defaults)"
    )
    parser.add_argument("--send-hz", type=float, default=20.0, help="input submit rate")
    parser.add_argument(
        "--interp-ms", type=float, default=None, help="interp buffer (default 2 ticks)"
    )
    parser.add_argument("--mode", choices=("interp", "raw"), default="interp")
    parser.add_argument("--no-vsync", action="store_true")
    parser.add_argument("--size", type=int, default=1024, help="window edge px")
    parser.add_argument(
        "--max-speed-units",
        type=float,
        default=_MAX_SPEED_UNITS,
        help="the server's run speed in u/s (snap-threshold calibration)",
    )
    parser.add_argument(
        "--quit-after",
        type=float,
        default=0.0,
        help="close the window after N seconds (headless checks)",
    )
    parser.add_argument(
        "--hud-log",
        default="none",
        help="HUD JSONL stream path (default 'none' — opt-in)",
    )
    parser.add_argument(
        "--snap-log",
        default="none",
        help="per-event snap JSONL path (default 'none' — opt-in)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Boot the network thread, then the arcade window (this thread)."""
    # SIGUSR1 dumps every thread's Python stack to stderr — the frozen
    # window's diagnosis path (works under free-threaded Python where
    # py-spy cannot attach).
    faulthandler.enable()
    with contextlib.suppress(Exception):
        faulthandler.register(signal.SIGUSR1)
    args = parse_args(argv)
    tick_hz = args.tick_hz
    interp_ms = args.interp_ms if args.interp_ms is not None else 2000.0 / tick_hz
    client_args = ClientArgs(
        host=args.host,
        port=args.port,
        control_port=args.control_port,
        principal_id=args.principal,
        tick_hz=args.tick_hz,
        send_hz=args.send_hz,
        interp_ms=interp_ms,
        interp_default=args.mode == "interp",
        vsync=not args.no_vsync,
        size=args.size,
        max_speed_units=args.max_speed_units,
    )
    store = SnapshotStore(tick_hz=tick_hz, max_speed_units=args.max_speed_units)
    snap_fh = _open_log(args.snap_log)
    if snap_fh is not None:
        store.attach_snap_log(snap_fh)
    client = NetClient(store, host=args.host, port=args.port, principal_id=args.principal)
    if args.control_port:
        client.set_control_client(
            ServerControlClient("127.0.0.1", args.control_port, timeout_s=2.0)
        )

    net = NetThread(client)
    net.start()

    window = PlaygroundWindow(client, store, client_args)
    if args.quit_after > 0.0:
        window.set_quit_after(args.quit_after)
    hud_stream: HudStreamer | None = None
    if args.hud_log != "none":
        hud_path = Path(args.hud_log)
        hud_stream = HudStreamer(store, window, hud_path)
        hud_stream.start()
    try:
        arcade.run()
    finally:
        if hud_stream is not None:
            hud_stream.stop()
            hud_stream.join(timeout=2.0)
        net.stop()
        if snap_fh is not None:
            snap_fh.close()


def _open_log(path_arg: str) -> TextIO | None:
    """Open an opt-in JSONL stream path; 'none' (or empty) disables it."""
    if not path_arg or path_arg == "none":
        return None
    path = Path(path_arg)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("w")


if __name__ == "__main__":
    main()

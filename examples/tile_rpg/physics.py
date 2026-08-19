"""tile2d physics wiring for the tile2d extension.

Instantiates the built-in ``tile2d`` sim model with a game-specific
SimModelConfig and loads a behavior grid converted from a
TMX map through tmx_to_text_grid. The
converter emits the text grid the tile2d model's
~kith.SimModel.load_behavior reads; the model then resolves
axis-separated circle-vs-tile collision each step.

The reference game tunes its tile2d config for a 16-pixel tile RPG: a
moderate walk speed, a faster run, brisk accel/decel, a small move
epsilon, and a collision radius of one tile. A game with a different feel
passes its own SimModelConfig to register.

The text grid is written to a temporary file for the model's
``load_behavior`` call (the C entry point reads a path, not a buffer);
the file is unlinked after the load succeeds since the model copies the
grid into memory and never re-reads the file.
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from pathlib import Path

from examples.tile_rpg import zone

from kith import Server, SimModel, SimModelConfig


__all__ = [
    "DEFAULT_CONFIG",
    "register",
]


# Game-tuned tile2d constants in model units (the model converts to Q16.16
# fixed-point at init). A field left at 0 selects the model's built-in
# default; the values here are the reference game's deliberate tuning, not
# the model's defaults, so a reader sees what the game chose.
_DEFAULT_BASE_SPEED: int = 48
_DEFAULT_RUN_SPEED: int = 96
_DEFAULT_ACCEL: int = 128
_DEFAULT_DECEL: int = 160
_DEFAULT_MOVE_EPS: float = 0.25
_DEFAULT_COLLISION_RADIUS: int = 1


DEFAULT_CONFIG: SimModelConfig = SimModelConfig(
    base_speed=_DEFAULT_BASE_SPEED,
    run_speed=_DEFAULT_RUN_SPEED,
    accel=_DEFAULT_ACCEL,
    decel=_DEFAULT_DECEL,
    move_eps=_DEFAULT_MOVE_EPS,
    collision_radius=_DEFAULT_COLLISION_RADIUS,
)


def register(
    server: Server,
    *,
    zone_name: str,
    tmx_path: str | Path,
    config: SimModelConfig | None = None,
) -> tuple[int, SimModel]:
    """Reserve a zone, build a tile2d model, and load a TMX behavior grid.

    Reads the TMX at ``tmx_path``, converts it to a text behavior grid
    through tmx_to_text_grid, writes the
    grid to a temporary file, and calls ``load_behavior`` on a freshly
    instantiated tile2d model created with ``config`` (or
    DEFAULT_CONFIG when ``None``). The temporary file is removed after
    the load since the model retains the grid in memory.

    Args:
        server: The facade the zone and model register on.
        zone_name: Zone name to reserve; must be unique across the
            server's registrations.
        tmx_path: Path to a TMX (Tiled XML) map whose tilesets carry the
            ``solid`` bool property on solid tiles.
        config: Model creation parameters; ``None`` selects
            DEFAULT_CONFIG.

    Returns:
        The reserved zone id and the tile2d model instance.

    Raises:
        zone.TMXError: When the TMX document cannot be parsed or is
            unsupported.
        KithError: On a zone or model registration failure, or when the
            behavior grid cannot be loaded.
    """
    cfg = config if config is not None else DEFAULT_CONFIG
    zone_id = server.register_zone(zone_name)
    model = server.register_sim_model("tile2d", cfg)

    tmx_text = Path(tmx_path).read_text(encoding="utf-8")
    grid_text = zone.tmx_to_text_grid(tmx_text)

    fd, path = tempfile.mkstemp(prefix="kith_tile2d_", suffix=".grid")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(grid_text)
        model.load_behavior(path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)

    return zone_id, model

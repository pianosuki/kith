"""Tests for the tile_rpg tile2d physics wiring.

The pure-Python case checks the default config shape (no build). The
build-gated cases register a tile2d model on a real :class:`kith.Server`
facade through :func:`examples.tile_rpg.physics.register`, load a TMX
behavior grid, and confirm the model steps actors against the loaded
collision grid.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from _build_gate import _BUILD_DEBUG, needs_build
from examples.tile_rpg import physics, zone

from kith import Actor, KithError, Server, ServerStatus, SimInput, SimModelConfig
from kith._bridge import reset


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test."""
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


# ---------------------------------------------------------------------------
# default config shape (no build)
# ---------------------------------------------------------------------------


class TestDefaultConfig:
    def test_default_config_carries_game_tuned_values(self) -> None:
        cfg = physics.DEFAULT_CONFIG
        assert cfg.base_speed == 48
        assert cfg.run_speed == 96
        assert cfg.accel == 128
        assert cfg.decel == 160
        assert cfg.move_eps == 0.25
        assert cfg.collision_radius == 1

    def test_default_config_is_a_sim_model_config(self) -> None:
        # Structural check: the default config is a valid SimModelConfig.
        assert isinstance(physics.DEFAULT_CONFIG, SimModelConfig)


# ---------------------------------------------------------------------------
# minimal TMX fixture for the build-gated cases
# ---------------------------------------------------------------------------


def _wall_tmx(width: int, height: int) -> str:
    """A TMX map with a solid wall along the top row, walkable below."""
    tilesets = (
        '  <tileset firstgid="1" name="behavior" tilewidth="16" tileheight="16">\n'
        '    <tile id="0"><properties>'
        '<property name="solid" type="bool" value="true"/>'
        "</properties></tile>\n"
        "  </tileset>"
    )
    rows = []
    for y in range(height):
        row = ",".join("1" if y == 0 else "0" for _ in range(width))
        rows.append(row)
    csv = ",\n".join(rows)
    layers = (
        f'  <layer id="1" name="behavior" width="{width}" height="{height}">\n'
        f'    <data encoding="csv">\n{csv}\n</data>\n'
        "  </layer>"
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<map version="1.10" orientation="orthogonal" '
        f'width="{width}" height="{height}" tilewidth="16" tileheight="16">\n'
        f"{tilesets}\n{layers}\n</map>\n"
    )


def _write_tmx(tmx_text: str) -> Path:
    fd, path = tempfile.mkstemp(prefix="kith_test_tmx_", suffix=".tmx")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(tmx_text)
    return Path(path)


# ---------------------------------------------------------------------------
# register (build-gated)
# ---------------------------------------------------------------------------


@needs_build
class TestRegister:
    def test_register_reserves_zone_and_loads_grid(self) -> None:
        tmx_path = _write_tmx(_wall_tmx(4, 3))
        try:
            server = Server(topology="embedded")
            try:
                zone_id, model = physics.register(
                    server,
                    zone_name="world",
                    tmx_path=tmx_path,
                )
                assert server.status is ServerStatus.CREATED
                assert zone_id == 1
                # The model steps actors without error against the grid.
                actor = Actor(id=1, pos_x=1 << 16, pos_y=2 << 16, pos_z=0)
                moved = model.apply_input(
                    actor,
                    SimInput(input_tick=1, move_x=32767, move_y=0),
                )
                stepped = model.step([moved], dt_ms=50)[0]
                assert stepped.id == 1
            finally:
                server.close()
        finally:
            tmx_path.unlink(missing_ok=True)

    def test_register_accepts_custom_config(self) -> None:
        tmx_path = _write_tmx(_wall_tmx(2, 2))
        try:
            server = Server(topology="embedded")
            try:
                cfg = SimModelConfig(base_speed=10, run_speed=20)
                _zone_id, _model = physics.register(
                    server,
                    zone_name="world",
                    tmx_path=tmx_path,
                    config=cfg,
                )
                assert server.status is ServerStatus.CREATED
            finally:
                server.close()
        finally:
            tmx_path.unlink(missing_ok=True)

    def test_register_raises_on_duplicate_zone_name(self) -> None:
        tmx_path = _write_tmx(_wall_tmx(2, 2))
        try:
            server = Server(topology="embedded")
            try:
                physics.register(server, zone_name="world", tmx_path=tmx_path)
                with pytest.raises(KithError, match="already registered"):
                    physics.register(server, zone_name="world", tmx_path=tmx_path)
            finally:
                server.close()
        finally:
            tmx_path.unlink(missing_ok=True)

    def test_register_raises_on_invalid_tmx(self) -> None:
        bad_path = _write_tmx("not xml at all")
        try:
            server = Server(topology="embedded")
            try:
                with pytest.raises(zone.TMXError):
                    physics.register(server, zone_name="world", tmx_path=bad_path)
            finally:
                server.close()
        finally:
            bad_path.unlink(missing_ok=True)

    def test_temp_grid_file_is_removed_after_load(self) -> None:
        # The register helper writes a temporary grid file for
        # load_behavior and unlinks it after; the temporary dir does not
        # grow per call.
        tmx_path = _write_tmx(_wall_tmx(2, 2))
        tmp_dir = Path(tempfile.gettempdir())
        try:
            server = Server(topology="embedded")
            try:
                before = list(tmp_dir.glob("kith_tile2d_*.grid"))
                physics.register(server, zone_name="world", tmx_path=tmx_path)
                after = list(tmp_dir.glob("kith_tile2d_*.grid"))
                assert after == before
            finally:
                server.close()
        finally:
            tmx_path.unlink(missing_ok=True)

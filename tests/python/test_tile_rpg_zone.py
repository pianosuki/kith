"""Tests for the tile_rpg TMX to text-grid converter.

The converter is pure Python (stdlib :mod:`xml.etree.ElementTree`) with no
framework dependency, so every case runs without the debug build. The
build-gated physics wiring that drives ``load_behavior`` from this
converter lives in :mod:`tests.python.test_tile_rpg_physics`.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from _build_gate import _BUILD_DEBUG
from examples.tile_rpg import zone

from kith._bridge import reset


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test.

    The zone converter itself loads no framework libraries, but the
    autouse fixture keeps the test isolated from any bridge state a prior
    test may have left behind.
    """
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


# ---------------------------------------------------------------------------
# minimal TMX fixtures
# ---------------------------------------------------------------------------


def _tmx(
    *,
    width: int,
    height: int,
    tilesets: str = "",
    layers: str,
    orientation: str = "orthogonal",
) -> str:
    """Build a small TMX document from layer fragments."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<map version="1.10" orientation="{orientation}" '
        f'width="{width}" height="{height}" tilewidth="16" tileheight="16">\n'
        f"{tilesets}\n"
        f"{layers}\n"
        "</map>\n"
    )


def _tileset_solid(firstgid: int, count: int) -> str:
    """A behavior tileset whose first ``count`` tiles are all solid."""
    tiles = "\n".join(
        f'    <tile id="{i}"><properties>'
        f'<property name="solid" type="bool" value="true"/>'
        f"</properties></tile>"
        for i in range(count)
    )
    return (
        f'  <tileset firstgid="{firstgid}" name="behavior" '
        f'tilewidth="16" tileheight="16">\n'
        f"{tiles}\n"
        "  </tileset>"
    )


def _layer(name: str, width: int, height: int, csv: str) -> str:
    return (
        f'  <layer id="1" name="{name}" width="{width}" height="{height}">\n'
        f'    <data encoding="csv">\n{csv}\n</data>\n'
        "  </layer>"
    )


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------


class TestTmxToTextGrid:
    def test_solid_tiles_become_hash_walkable_become_dot(self) -> None:
        tilesets = _tileset_solid(firstgid=1, count=1)
        # 4x3 grid; gid 1 = solid, 0 = empty.
        csv = "1,1,0,0,\n0,0,1,1,\n0,0,0,0"
        layers = _layer("behavior", 4, 3, csv)
        text = _tmx(width=4, height=3, tilesets=tilesets, layers=layers)
        grid = zone.tmx_to_text_grid(text)
        assert grid == "##..\n..##\n...."

    def test_multiple_layers_merge_additively(self) -> None:
        tilesets = _tileset_solid(firstgid=1, count=1)
        # Layer a solid at (0,0); layer b solid at (2,1); merged grid has
        # both solid.
        csv_a = "1,0,0,0,0,0"
        csv_b = "0,0,0,0,0,1"
        layers = _layer("a", 3, 2, csv_a) + "\n" + _layer("b", 3, 2, csv_b)
        text = _tmx(width=3, height=2, tilesets=tilesets, layers=layers)
        grid = zone.tmx_to_text_grid(text)
        assert grid == "#..\n..#"

    def test_non_solid_tile_is_walkable(self) -> None:
        # A tileset whose one tile is NOT solid; the grid is all walkable.
        tilesets = (
            '  <tileset firstgid="1" name="art" tilewidth="16" tileheight="16">\n'
            '    <tile id="0"><properties>'
            '<property name="solid" type="bool" value="false"/>'
            "</properties></tile>\n"
            "  </tileset>"
        )
        csv = "1,1,1,1"
        layers = _layer("ground", 2, 2, csv)
        text = _tmx(width=2, height=2, tilesets=tilesets, layers=layers)
        grid = zone.tmx_to_text_grid(text)
        assert grid == "..\n.."

    def test_flip_flags_stripped_from_gid(self) -> None:
        # TMX gids use the high bits for horizontal/vertical/diagonal flip.
        # A flipped solid tile (gid 1 with the horizontal-flip bit set,
        # 0x80000001) is still solid.
        tilesets = _tileset_solid(firstgid=1, count=1)
        flipped = str(0x80000001)
        csv = f"{flipped},0,0,0"
        layers = _layer("behavior", 2, 2, csv)
        text = _tmx(width=2, height=2, tilesets=tilesets, layers=layers)
        grid = zone.tmx_to_text_grid(text)
        assert grid == "#.\n.."

    def test_firstgid_offset_resolves_correctly(self) -> None:
        # A tileset starting at firstgid=5 with 2 solid tiles; gid 5 and 6
        # are solid, gid 1 (from a different tileset) is not.
        tilesets = _tileset_solid(firstgid=5, count=2)
        csv = "5,6,1,0"
        layers = _layer("behavior", 2, 2, csv)
        text = _tmx(width=2, height=2, tilesets=tilesets, layers=layers)
        grid = zone.tmx_to_text_grid(text)
        assert grid == "##\n.."

    def test_empty_layer_data_yields_all_walkable(self) -> None:
        tilesets = _tileset_solid(firstgid=1, count=1)
        # All-zero cells; no solid tiles placed.
        csv = "0,0,0,0,0,0"
        layers = _layer("behavior", 3, 2, csv)
        text = _tmx(width=3, height=2, tilesets=tilesets, layers=layers)
        grid = zone.tmx_to_text_grid(text)
        assert grid == "...\n..."


# ---------------------------------------------------------------------------
# error handling
# ---------------------------------------------------------------------------


class TestTmxErrors:
    def test_non_xml_raises_tmx_error(self) -> None:
        with pytest.raises(zone.TMXError):
            zone.tmx_to_text_grid("not xml at all")

    def test_non_map_root_raises_tmx_error(self) -> None:
        with pytest.raises(zone.TMXError, match="expected 'map'"):
            zone.tmx_to_text_grid("<tileset/>")

    def test_non_orthogonal_raises_tmx_error(self) -> None:
        layers = _layer("behavior", 2, 2, "0,0,0,0")
        text = _tmx(
            width=2,
            height=2,
            tilesets="",
            layers=layers,
            orientation="isometric",
        )
        with pytest.raises(zone.TMXError, match="orientation"):
            zone.tmx_to_text_grid(text)

    def test_zero_dimensions_raise_tmx_error(self) -> None:
        layers = _layer("behavior", 0, 0, "")
        text = _tmx(width=0, height=0, tilesets="", layers=layers)
        with pytest.raises(zone.TMXError, match="dimensions"):
            zone.tmx_to_text_grid(text)

    def test_no_tilelayers_raises_tmx_error(self) -> None:
        text = _tmx(width=2, height=2, tilesets="", layers="")
        with pytest.raises(zone.TMXError, match="no tilelayers"):
            zone.tmx_to_text_grid(text)

    def test_base64_encoding_raises_tmx_error(self) -> None:
        tilesets = _tileset_solid(firstgid=1, count=1)
        layers = (
            '  <layer id="1" name="behavior" width="2" height="2">\n'
            '    <data encoding="base64">AAAAAA==</data>\n'
            "  </layer>"
        )
        text = _tmx(width=2, height=2, tilesets=tilesets, layers=layers)
        with pytest.raises(zone.TMXError, match="encoding"):
            zone.tmx_to_text_grid(text)

    def test_cell_count_mismatch_raises_tmx_error(self) -> None:
        tilesets = _tileset_solid(firstgid=1, count=1)
        # Map says 2x2 (4 cells) but the CSV has 3.
        csv = "1,0,0"
        layers = _layer("behavior", 2, 2, csv)
        text = _tmx(width=2, height=2, tilesets=tilesets, layers=layers)
        with pytest.raises(zone.TMXError, match="3 cells"):
            zone.tmx_to_text_grid(text)

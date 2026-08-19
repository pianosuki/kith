"""TMX to text-grid converter for the tile2d extension.

The built-in ``tile2d`` model loads a behavior grid from a text file: one
character per tile, ``#`` for solid, any other character for walkable,
rows separated by ``\\n``. The reference game authors its zone geometry in
Tiled's TMX format and converts it to that text grid at boot, then hands
the file to load_behavior.

This converter is game code, not framework code. The framework's sim
contract is the text-grid format and the ``load_behavior`` entry point,
not TMX. A game that uses a different map editor writes its own converter
against the same text-grid contract; a game that uses Tiled's CSV layer
encoding uses this one.

The converter reads solid-ness from the TMX itself: a tile is solid when
its tileset entry carries a ``solid`` bool property set to ``true``. This
mirrors the behavior-palette authoring flow (a small tileset whose tiles
exist to stamp gameplay metadata into behavior layers), so the TMX is
self-describing and the converter takes no gameplay parameters. All
tilelayers in the map merge additively: a cell is solid when any layer's
gid at that cell resolves to a solid tile. The z-level, one-way, and
steepness metadata a richer game might carry are game-specific extensions
dropped from the reference catalog; the text grid is one bit per cell.

Only the CSV data encoding is supported (Tiled's "CSV" layer encoding
option, the most readable and the format the reference game's fixtures
ship in). Base64 and compressed encodings are game extensions a game adds
by writing its own converter; the framework's text-grid contract is
encoding-agnostic.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET


__all__ = [
    "SOLID_TILE_PROP",
    "TMXError",
    "tmx_to_text_grid",
]


# The tileset tile property name that marks a tile as solid. The reference
# game's behavior tileset uses this name; a game that uses a different
# property name writes its own converter against the same text-grid
# contract (the property name is game vocabulary, not framework vocabulary).
SOLID_TILE_PROP: str = "solid"


class TMXError(ValueError):
    """Raised when a TMX document cannot be parsed or is unsupported."""


def _solid_gids(root: ET.Element) -> set[int]:
    """Collect gids whose tileset tile carries the solid bool property.

    A tile is solid when its tileset entry has a ``solid`` property with
    type ``bool`` and value ``true``. The gid is the tileset's ``firstgid``
    plus the tile's ``id``. Returns the set of solid gids across every
    tileset in the map.
    """
    solid: set[int] = set()
    for tileset in root.findall("tileset"):
        firstgid_str = tileset.get("firstgid", "0")
        firstgid = int(firstgid_str) if firstgid_str else 0
        for tile in tileset.findall("tile"):
            tid_str = tile.get("id", "0")
            tid = int(tid_str) if tid_str else 0
            props = tile.find("properties")
            if props is None:
                continue
            for prop in props.findall("property"):
                if prop.get("name") != SOLID_TILE_PROP:
                    continue
                if prop.get("type", "string") != "bool":
                    continue
                if prop.get("value", "false").lower() != "true":
                    continue
                solid.add(firstgid + tid)
                break
    return solid


def _layer_cells(data: ET.Element, width: int, height: int) -> list[int]:
    """Parse one layer's ``<data>`` element into a flat gid list.

    Only the CSV encoding is supported. TMX gids use the high bits for
    flip flags; the caller masks them off before resolving the tile.
    """
    encoding = data.get("encoding", "csv")
    if encoding != "csv":
        raise TMXError(f"unsupported layer encoding: {encoding!r}; only 'csv' is supported")
    text = (data.text or "").strip()
    if not text:
        return [0] * (width * height)
    tokens = [tok for tok in text.split(",") if tok.strip() != ""]
    cells = [int(tok) for tok in tokens]
    expected = width * height
    if len(cells) != expected:
        raise TMXError(
            f"layer data has {len(cells)} cells, expected {expected} "
            f"(width={width} height={height})"
        )
    return cells


def tmx_to_text_grid(tmx_text: str) -> str:
    """Convert a TMX document to a tile2d text behavior grid.

    Each cell of each tilelayer is resolved against the map's tilesets: a
    cell is ``#`` (solid) when any layer's gid at that cell maps to a
    tile carrying the ``solid`` bool property, and ``.`` (walkable)
    otherwise. Layers merge additively. The result is one row per map
    row, characters joined without separators, rows separated by ``\\n``.

    Args:
        tmx_text: The TMX document as a string (XML, with or without the
            ``<?xml?>`` declaration).

    Returns:
        The text grid the tile2d model's ``load_behavior`` reads.

    Raises:
        TMXError: When the document is not a finite orthogonal TMX map,
            has no tilelayers, uses a non-CSV layer encoding, or has a
            layer whose cell count does not match the map dimensions.
    """
    try:
        root = ET.fromstring(tmx_text)
    except ET.ParseError as exc:
        raise TMXError(f"not a valid TMX document: {exc}") from exc
    if root.tag != "map":
        raise TMXError(f"root element is {root.tag!r}, expected 'map'")
    orientation = root.get("orientation", "orthogonal")
    if orientation != "orthogonal":
        raise TMXError(
            f"unsupported map orientation: {orientation!r}; only 'orthogonal' is supported"
        )
    width = int(root.get("width", "0"))
    height = int(root.get("height", "0"))
    if width <= 0 or height <= 0:
        raise TMXError(f"map dimensions must be positive: width={width} height={height}")

    solid = _solid_gids(root)
    grid = [[False] * width for _ in range(height)]
    found_layer = False
    for layer in root.findall("layer"):
        found_layer = True
        data = layer.find("data")
        if data is None:
            continue
        cells = _layer_cells(data, width, height)
        for i, gid_raw in enumerate(cells):
            gid = gid_raw & 0x1FFFFFFF
            if gid in solid:
                y = i // width
                x = i % width
                grid[y][x] = True
    if not found_layer:
        raise TMXError("no tilelayers in map")

    rows = ["".join("#" if cell else "." for cell in row) for row in grid]
    return "\n".join(rows)

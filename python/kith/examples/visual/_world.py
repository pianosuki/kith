"""Open Range world geometry: the plain, its cells, and the fixed-point
conversions every peer shares.

A seamless 512x512-unit plain cut into 32-unit cells (16x16 cells), a
Chebyshev radius-1 subscription window (3x3 cells, 96 units of visible
reach), and Q16.16 fixed-point positions. Both peers and the scenario
drivers reproduce the server's neighborhood semantics from raw positions
through these constants, so the geometry has one source of truth.
"""

from __future__ import annotations


Q16: int = 1 << 16
WORLD_UNITS: int = 512
CELL_SIZE_UNITS: int = 32
CELL_SHIFT: int = 16 + 5  # 32 Q16.16 units per cell edge
CELL_SIZE_Q16: int = 1 << CELL_SHIFT
CELL_RADIUS: int = 1
WORLD_CELLS: int = WORLD_UNITS // CELL_SIZE_UNITS
WORLD_Q16: int = WORLD_UNITS * Q16

# Run flag on SimInput (bit 0): base 20 u/s vs run 40 u/s under the
# server's movement model.
RUN_FLAG: int = 0x1


def units_to_q16(units: float) -> int:
    """Convert world units to Q16.16 fixed point."""
    return int(units * Q16)


def q16_to_units(q16: int) -> float:
    """Convert Q16.16 fixed point to world units."""
    return q16 / Q16

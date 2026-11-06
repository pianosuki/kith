"""Percentile calculation shared by measurements and rendering."""

from __future__ import annotations

from collections.abc import Sequence


def percentile(values: Sequence[int], pct: int) -> float:
    """Return the ``pct``-th percentile of ``values`` (nearest-rank)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, (pct * len(ordered) + 99) // 100 - 1))
    return float(ordered[idx])

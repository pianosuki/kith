"""Pagination helper for the ``/query_state`` control-plane route.

The control plane allocates one write buffer per connection sized by
``write_buffer_cap`` (default 262144 bytes). A roster serialized as one JSON
response overflows that buffer once the actor count grows past what the
capacity holds, so the ``/query_state`` route exposes
``?offset=<n>&limit=<m>`` to page the roster into responses that fit the
buffer. A caller collecting the full set iterates pages until the returned
slice is shorter than the requested limit.

Without query parameters the route returns the full roster as
``{"actors": [...]}``; this keeps the small-roster integration tests and the
single-actor lookup (``?actor_id=<n>``) unchanged. The paginated envelope adds
``offset``, ``limit``, and ``total`` fields so a caller can detect the end of
the roster without a separate count request.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse


__all__ = [
    "QUERY_STATE_DEFAULT_PAGE_SIZE",
    "paginate_query_state",
]

# A page size chosen so a full page of the largest reference-game actor
# records (pos + vel + input_tick, ~150 bytes each) plus the JSON envelope
# and the HTTP headers fits the default write buffer with margin.
QUERY_STATE_DEFAULT_PAGE_SIZE: int = 32


def paginate_query_state(
    path: str,
    actors: list[dict[str, Any]],
    *,
    key: str = "actors",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a paginated control-plane roster response body.

    Args:
        path: The request path, including the query string.
        actors: The full roster serialized as a list of record dicts.
        key: The field name carrying the roster slice. ``/query_state``
            serves actor records under ``actors``; other routes (such as
            principal→actor bindings) page their own records by naming
            their field here.
        extra: Additional envelope fields merged into every response —
            non-paginated scalars a route reports alongside each slice
            (counter totals, for example).

    Returns:
        ``{key: [...]}`` when no ``offset``/``limit`` params are present
        (the full roster, plus any ``extra`` fields), or
        ``{key: [<slice>], "offset": <n>, "limit": <m>, "total": <T>}``
        plus any ``extra`` fields when both are present.
    """
    query = parse_qs(urlparse(path).query)
    body: dict[str, Any]
    if "offset" not in query or "limit" not in query:
        body = {key: actors}
    else:
        offset = max(int(query["offset"][0]), 0)
        limit = max(int(query["limit"][0]), 0)
        page = actors[offset : offset + limit] if limit > 0 else []
        body = {key: page, "offset": offset, "limit": limit, "total": len(actors)}
    if extra:
        body.update(extra)
    return body

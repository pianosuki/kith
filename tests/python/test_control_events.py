"""Unit tests for the control-plane event wrapper: the correlation id is the
8-byte trailer the wire contract pins, and the wrapper rejects any other
length before it can reach the C copy."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from _build_gate import needs_build

from kith.control import Control
from kith.reactor import Reactor


@pytest.fixture()
def control() -> Generator[Control]:
    """One created (unstarted) control handle: the event bus exists from
    create, so publishes do not need the listener."""
    reactor = Reactor()
    control = Control(reactor, port=0, worker_count=0)
    yield control
    control.close()
    reactor.close()


@needs_build
def test_publish_accepts_exact_correlation_id(control: Control) -> None:
    control.publish_event("evt.ok", b"", correlation_id=bytes(range(8)))


@needs_build
def test_publish_rejects_short_correlation_id(control: Control) -> None:
    with pytest.raises(ValueError, match="8 bytes"):
        control.publish_event("evt.short", b"", correlation_id=b"short")


@needs_build
def test_publish_rejects_long_correlation_id(control: Control) -> None:
    with pytest.raises(ValueError, match="8 bytes"):
        control.publish_event("evt.long", b"", correlation_id=bytes(9))

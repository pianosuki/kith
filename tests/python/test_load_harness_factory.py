"""Tests for the load harness's host-factory argument forwarding.

The host factories are thin wiring layers over the example hosts; these
tests hold each new knob to its forwarding contract without booting a
server.
"""

from __future__ import annotations

import argparse

import pytest
from tools.agent import load_harness


def test_embedded_factory_forwards_native_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_factory(**kwargs: object) -> object:
        captured.update(kwargs)
        return lambda: None

    monkeypatch.setattr("tools.agent.embedded_host.embedded_host_factory", fake_factory)
    load_harness._embedded_factory(native_apply=True)
    assert captured["native_apply"] is True
    load_harness._embedded_factory()
    assert captured["native_apply"] is False


def _parse(argv: list[str]) -> argparse.Namespace:
    return load_harness._build_parser().parse_args(argv)


def test_native_apply_flag_defaults_to_false() -> None:
    args = _parse(["--profile", "dense-1000", "--embedded"])
    assert args.native_apply is False


def test_native_apply_requires_embedded() -> None:
    with pytest.raises(SystemExit):
        load_harness.main(["--profile", "dense-1000", "--native-apply"])

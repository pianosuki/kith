"""Determinism tests for the simulation replay tool.

The replay tool drives the C simulation library through its public ABI
and hashes the live actor set. These tests assert the properties that
make it a useful regression oracle over both record encodings:

  - Determinism: two runs on the same record produce identical hashes.
  - Baseline: the binary fixture's hash sequence matches a pinned golden
    value held INDEPENDENTLY of the fixture's own embedded expectations,
    so a symmetric writer/reader defect cannot pass unnoticed.
  - Parity: the legacy text fixture and the binary fixture carry the same
    logical content and produce the same hash sequence.
  - Modes: verify enforces embedded expectations, and diff/transcode
    behave per docs/guides/replay_format.md.
  - Coverage: a comparison binds only ticks present on both sides — the
    default zero cadence cannot pass a pinned expectation, and verify
    refuses it up front.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from examples._common import replay_format as rf


REPO = Path(__file__).resolve().parents[2]
REPLAY = REPO / "tools" / "replay.py"
TEXT_FIXTURE = REPO / "tools" / "fixtures" / "replay_tile2d.txt"
BINARY_FIXTURE = REPO / "tools" / "fixtures" / "replay_tile2d.krpl"
LIB = (
    Path(os.environ.get("KITH_BUILD_DIR", str(REPO / "build" / "debug"))) / "libkith_sim.so.1"
).resolve()

GOLDEN: dict[int, int] = {
    0: 0x35752879FE13749D,
    5: 0x87CF7485F97C500D,
    10: 0xB4ECC38A95AF09BC,
    15: 0xDB4BACB2D6E6445C,
}


def _lib_args() -> list[str]:
    """Return the --lib argument pinning the locally built library."""
    return ["--lib", str(LIB)] if LIB.is_file() else []


def _run_sim(
    record: Path,
    *extra: str,
    command: str = "",
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run play/verify on ``record`` with the standard tile2d cadence.

    An empty ``command`` exercises the legacy bare invocation, where the
    tool itself defaults the mode to play.
    """
    args = [sys.executable, str(REPLAY)]
    if command:
        args.append(command)
    args += [
        "--input",
        str(record),
        "--model",
        "tile2d",
        "--ticks",
        "20",
        "--hash-every",
        "5",
        *_lib_args(),
        *extra,
    ]
    return subprocess.run(args, capture_output=True, text=True, check=check)


def _run_tool(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run the tool with a raw argument vector (diff/transcode)."""
    return subprocess.run(
        [sys.executable, str(REPLAY), *args],
        capture_output=True,
        text=True,
        check=check,
    )


def _parse_hashes(stdout: str) -> dict[int, int]:
    """Parse 'tick=N hash=HEX' lines into a tick->hash map."""
    hashes: dict[int, int] = {}
    for line in stdout.strip().splitlines():
        tick_part, hash_part = line.split()
        tick = int(tick_part.split("=")[1])
        h = int(hash_part.split("=")[1], 16)
        hashes[tick] = h
    return hashes


def _fixture_document() -> rf.ReplayDocument:
    """Return the parsed committed binary fixture."""
    return rf.read_document_file(BINARY_FIXTURE)


def _require_lib() -> None:
    """Skip the test when the simulation library is not built."""
    if not LIB.is_file():
        pytest.skip(
            "libkith_sim not built (run: cmake --preset debug && cmake --build build/debug)"
        )


def test_replay_is_deterministic_over_the_binary_fixture() -> None:
    """Two consecutive bare-form runs on the binary record are identical."""
    _require_lib()
    first = _run_sim(BINARY_FIXTURE)
    second = _run_sim(BINARY_FIXTURE)
    assert first.stdout == second.stdout


def test_binary_fixture_matches_golden_hash() -> None:
    """The binary record's hashes match the pinned baseline."""
    _require_lib()
    result = _run_sim(BINARY_FIXTURE)
    assert _parse_hashes(result.stdout) == GOLDEN


def test_text_and_binary_fixtures_produce_identical_hashes() -> None:
    """Legacy text and binary encodings of the same content agree."""
    _require_lib()
    from_text = _run_sim(TEXT_FIXTURE)
    from_binary = _run_sim(BINARY_FIXTURE)
    assert _parse_hashes(from_text.stdout) == _parse_hashes(from_binary.stdout)


def test_verify_accepts_embedded_expectations(tmp_path: Path) -> None:
    """Verify passes against embedded records and fails on wrong ones."""
    _require_lib()
    ok = _run_sim(BINARY_FIXTURE, command="verify")
    assert ok.returncode == 0
    assert _parse_hashes(ok.stdout) == GOLDEN

    wrong = replace(
        _fixture_document(),
        expected_hashes=tuple(rf.ExpectedHash(tick, h ^ 1) for tick, h in GOLDEN.items()),
    )
    bad = tmp_path / "tampered.krpl"
    bad.write_bytes(rf.encode_document(wrong))
    failed = _run_sim(bad, command="verify", check=False)
    assert failed.returncode == 1
    assert "!= expected" in failed.stderr


def test_verify_refuses_to_run_without_expectations(tmp_path: Path) -> None:
    """Verify without embedded or CLI-pinned expectations is a usage error."""
    _require_lib()
    stripped = replace(_fixture_document(), expected_hashes=())
    bare = tmp_path / "bare.krpl"
    bare.write_bytes(rf.encode_document(stripped))
    result = _run_sim(bare, command="verify", check=False)
    assert result.returncode == 2
    assert "no expectations" in result.stderr


def test_cli_expectations_override_embedded_ones(tmp_path: Path) -> None:
    """An explicit --expect-file wins over the record's embedded values."""
    _require_lib()
    sidecar = tmp_path / "expect.txt"
    sidecar.write_text(
        "".join(f"tick={tick} hash={h:016x}\n" for tick, h in GOLDEN.items()),
        encoding="utf-8",
    )
    result = _run_sim(BINARY_FIXTURE, "--expect-file", str(sidecar), command="verify")
    assert result.returncode == 0


def test_play_with_a_sidecar_and_default_cadence_fails(tmp_path: Path) -> None:
    """Pinned expectations at the default zero cadence fail, never pass.

    A comparison binds only ticks present on both sides; with nothing
    emitted there is nothing to bind, so the pinned expectations must fail
    loudly instead of exiting 0.
    """
    _require_lib()
    sidecar = tmp_path / "corrupt.txt"
    sidecar.write_text(
        "".join(f"tick={tick} hash={h ^ 1:016x}\n" for tick, h in GOLDEN.items()),
        encoding="utf-8",
    )
    result = _run_tool(
        "play",
        "--input",
        str(BINARY_FIXTURE),
        "--ticks",
        "20",
        "--expect-file",
        str(sidecar),
        *_lib_args(),
        check=False,
    )
    assert result.returncode == 1
    assert "never emitted" in result.stderr


def test_verify_with_the_default_cadence_is_a_usage_error() -> None:
    """Verify refuses a non-positive --hash-every before replaying."""
    _require_lib()
    result = _run_tool("verify", "--input", str(BINARY_FIXTURE), *_lib_args(), check=False)
    assert result.returncode == 2
    assert "--hash-every" in result.stderr


def test_verify_fails_when_a_pinned_tick_is_never_emitted(tmp_path: Path) -> None:
    """An expectation off the emission cadence fails the verification."""
    _require_lib()
    sidecar = tmp_path / "offcadence.txt"
    sidecar.write_text(
        "".join(f"tick={tick} hash={h:016x}\n" for tick, h in GOLDEN.items())
        + "tick=3 hash=0000000000000000\n",
        encoding="utf-8",
    )
    result = _run_tool(
        "verify",
        "--input",
        str(BINARY_FIXTURE),
        "--ticks",
        "20",
        "--hash-every",
        "5",
        "--expect-file",
        str(sidecar),
        *_lib_args(),
        check=False,
    )
    assert result.returncode == 1
    assert "tick 3" in result.stderr
    assert "never emitted" in result.stderr


def test_play_with_a_matching_sidecar_and_cadence_passes(tmp_path: Path) -> None:
    """A sidecar covered by the emission cadence verifies cleanly."""
    _require_lib()
    sidecar = tmp_path / "golden.txt"
    sidecar.write_text(
        "".join(f"tick={tick} hash={h:016x}\n" for tick, h in GOLDEN.items()),
        encoding="utf-8",
    )
    result = _run_tool(
        "play",
        "--input",
        str(BINARY_FIXTURE),
        "--ticks",
        "20",
        "--hash-every",
        "5",
        "--expect-file",
        str(sidecar),
        *_lib_args(),
    )
    assert result.returncode == 0


def test_diff_reports_divergence_between_records(tmp_path: Path) -> None:
    """Diff of a record against itself succeeds; a mutation is reported."""
    base = _fixture_document()
    mutated = replace(
        base,
        ticks=(
            *base.ticks,
            rf.TickRecord(
                6,
                (rf.MoveEvent(actor_id=1, input_tick=1, move_x=1, move_y=0, move_z=0, flags=0),),
            ),
        ),
    )
    other = tmp_path / "mutated.krpl"
    other.write_bytes(rf.encode_document(mutated))

    same = _run_tool("diff", "--input", str(BINARY_FIXTURE), "--input-b", str(BINARY_FIXTURE))
    assert same.returncode == 0
    different = _run_tool(
        "diff", "--input", str(BINARY_FIXTURE), "--input-b", str(other), check=False
    )
    assert different.returncode == 1
    assert "tick 6" in different.stdout


def test_transcode_reproduces_the_fixture_ticks(tmp_path: Path) -> None:
    """Transcoding the text fixture yields the committed ticks (no hashes)."""
    out = tmp_path / "regen.krpl"
    result = _run_tool(
        "transcode",
        "--input",
        str(TEXT_FIXTURE),
        "--output",
        str(out),
    )
    assert result.returncode == 0
    regenerated = rf.read_document_file(out)
    committed = _fixture_document()
    assert regenerated.ticks == committed.ticks
    assert regenerated.expected_hashes == ()
    embedded = {e.tick: e.hash for e in committed.expected_hashes}
    assert embedded and embedded == GOLDEN

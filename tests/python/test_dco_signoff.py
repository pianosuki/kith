"""Unit tests for the commit-msg DCO sign-off auto-append hook."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest
from tools import dco_signoff


_IDENTITY = ("Scott Weer", "scott@example.com")
_TRAILER = "Signed-off-by: Scott Weer <scott@example.com>"


def _git_ok(stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


def _patch_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dco_signoff, "committer_signoff", lambda: _IDENTITY)


def test_strip_scissors_tail_drops_template() -> None:
    text = (
        "chore(tools): bump pin\n"
        "\n"
        "Keeps regenerated bindings formatted identically.\n"
        "\n"
        "# Please enter the commit message for your changes.\n"
        "# ------------------------ >8 ------------------------\n"
        "diff --git a/x b/x\n"
    )
    cleaned = dco_signoff.strip_scissors_tail(text)
    assert cleaned.startswith("chore(tools): bump pin\n")
    assert "Keeps regenerated bindings" in cleaned
    assert ">8" not in cleaned
    assert "diff --git" not in cleaned


def test_strip_scissors_tail_without_marker_returns_text() -> None:
    text = "feat(sim): subject only\n"
    assert dco_signoff.strip_scissors_tail(text) == text


def test_strip_scissors_tail_normalizes_crlf() -> None:
    text = "fix(pool): shrink freelist\r\n\r\nFixes the leak.\r\n"
    assert (
        dco_signoff.strip_scissors_tail(text) == "fix(pool): shrink freelist\n\nFixes the leak.\n"
    )


def test_has_signoff_is_identity_agnostic() -> None:
    assert dco_signoff.has_signoff("x\n\nSigned-off-by: Someone Else <s@e.org>\n")
    assert not dco_signoff.has_signoff("x\n\nReviewed-by: Someone Else <s@e.org>\n")


def test_has_content_ignores_comment_lines(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dco_signoff, "_comment_prefix", lambda: "#")
    assert not dco_signoff.has_content("\n# Please enter the commit message.\n# more\n")
    assert dco_signoff.has_content("feat(sim): subject\n# template below\n")
    assert not dco_signoff.has_content("")


def test_comment_prefix_defaults_to_hash_on_auto(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dco_signoff, "_git", lambda args, input_text=None: _git_ok("auto\n"))
    assert dco_signoff._comment_prefix() == "#"
    monkeypatch.setattr(dco_signoff, "_git", lambda args, input_text=None: _git_ok("//\n"))
    assert dco_signoff._comment_prefix() == "//"


def test_append_trailer_signs_subject_only_message() -> None:
    out = dco_signoff.append_trailer("chore(tools): bump pin\n", *_IDENTITY)
    lines = out.splitlines()
    assert lines[0] == "chore(tools): bump pin"
    assert _TRAILER in lines
    # A subject-only message still gains the trailer on a separated final
    # paragraph: the body is exempt for chore commits, the sign-off is not.
    assert lines.index(_TRAILER) >= 2


def test_append_trailer_joins_existing_block() -> None:
    text = "fix(pool): shrink freelist\n\nFixes the leak.\n\nReviewed-by: R E V <rev@example.org>\n"
    out = dco_signoff.append_trailer(text, *_IDENTITY)
    assert (
        "Reviewed-by: R E V <rev@example.org>\nSigned-off-by: Scott Weer <scott@example.com>" in out
    )


def test_main_appends_and_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_identity(monkeypatch)
    message = tmp_path / "COMMIT_EDITMSG"
    message.write_text("feat(sim): add circle query\n\nCuts the scan to O(buckets).\n")
    assert dco_signoff.main([str(message)]) == 0
    written = message.read_text(encoding="utf-8")
    assert _TRAILER in written
    assert written.count("Signed-off-by:") == 1


def test_main_skips_when_signoff_present(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_identity(monkeypatch)
    original = "feat(sim): add circle query\n\nCuts the scan.\n\nSigned-off-by: Pre Signed <pre@signed.org>\n"
    message = tmp_path / "COMMIT_EDITMSG"
    message.write_text(original)
    assert dco_signoff.main([str(message)]) == 0
    assert message.read_text(encoding="utf-8") == original


def test_main_noop_on_template_only_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_identity(monkeypatch)
    original = (
        "\n"
        "# Please enter the commit message for your changes.\n"
        "# ------------------------ >8 ------------------------\n"
        "diff --git a/x b/x\n"
    )
    message = tmp_path / "COMMIT_EDITMSG"
    message.write_text(original)
    assert dco_signoff.main([str(message)]) == 0
    assert message.read_text(encoding="utf-8") == original


def test_main_fails_closed_when_identity_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=[], returncode=128, stdout="", stderr="no identity")

    monkeypatch.setattr(dco_signoff, "_git", fail)
    message = tmp_path / "COMMIT_EDITMSG"
    message.write_text("feat(sim): add circle query\n\nCuts the scan.\n")
    with pytest.raises(SystemExit) as excinfo:
        dco_signoff.main([str(message)])
    assert excinfo.value.code == 1
    assert "Signed-off-by" not in message.read_text(encoding="utf-8")


def test_main_reports_interpret_trailers_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_identity(monkeypatch)

    def fake_git(
        args: Sequence[str], input_text: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        if args[:2] == ["git", "var"]:
            ident = "Scott Weer <scott@example.com> 1755828000 -0300"
            return _git_ok(ident)
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="boom")

    monkeypatch.setattr(dco_signoff, "_git", fake_git)
    message = tmp_path / "COMMIT_EDITMSG"
    message.write_text("feat(sim): add circle query\n\nCuts the scan.\n")
    with pytest.raises(SystemExit) as excinfo:
        dco_signoff.main([str(message)])
    assert excinfo.value.code == 1
    assert "interpret-trailers failed" in capsys.readouterr().err


def test_committer_signoff_parses_git_var_output(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_args: list[str] = []

    def fake_git(
        args: Sequence[str], input_text: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        seen_args.extend(args)
        return _git_ok("Scott Weer <scott@example.com> 1755828000 -0300")

    monkeypatch.setattr(dco_signoff, "_git", fake_git)
    assert dco_signoff.committer_signoff() == ("Scott Weer", "scott@example.com")
    assert seen_args == ["git", "var", "GIT_COMMITTER_IDENT"]

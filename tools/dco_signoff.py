#!/usr/bin/env python3
"""Append the DCO ``Signed-off-by`` trailer to an in-flight commit message.

Runs on the ``commit-msg`` stage ahead of the conformance checker: the
message file git passes as ``argv[1]`` gains a ``Signed-off-by`` trailer
whenever one is absent, mirroring Gerrit's Change-Id auto-append. The
identity comes from ``git var GIT_COMMITTER_IDENT`` — the same
env-and-config resolution ``git commit --signoff`` uses — so a commit made
with an overridden committer environment is signed off with the identity
git actually records.

Everything from the scissors line onward is dropped before trailer
placement; ordinary ``#`` comment lines are left in place because git's
cleanup pass strips them after this hook runs and ``git
interpret-trailers`` already inserts the trailer ahead of them. A message
that already carries any ``Signed-off-by`` line is written back untouched,
as is a message with no content beyond the editor template.

Commits created with ``--no-verify`` bypass every commit-msg hook,
including this one and the conformance checker's single-message mode;
those commits are caught by the range scan of ``scripts/verify.sh``
locally and by CI's commits stage on push.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


# The scissors marker git writes in the commit template; the dash runs
# flank the ">8" figure regardless of the configured comment character.
_SCISSORS_RE: re.Pattern[str] = re.compile(r"^.*-{2,}\s*>8\s*-{2,}.*$", re.MULTILINE)

# Any existing sign-off is respected: the checker's DCO contract is
# identity-agnostic, and a pre-signed message must not grow a duplicate.
_SIGNOFF_RE: re.Pattern[str] = re.compile(r"^Signed-off-by:\s", re.MULTILINE)

# Committer identity as git records it: "Name <email> timestamp tz".
_IDENT_RE: re.Pattern[str] = re.compile(r"^(.+?)\s+<([^<>]+)>")


def _git(args: list[str], input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    """Run a git command, capturing output without raising on failure."""
    return subprocess.run(args, input=input_text, capture_output=True, text=True, check=False)


def _comment_prefix() -> str:
    """Return the configured commit-template comment prefix ('#' when unset)."""
    proc = _git(["git", "config", "core.commentChar"])
    value = proc.stdout.strip()
    if proc.returncode != 0 or not value or value == "auto":
        return "#"
    return value


def has_content(text: str) -> bool:
    """Return True when a non-comment line survives the template strip.

    The editor template is all comment lines, so a template-only buffer
    reads as empty. The '#' default covers the 'auto' comment character; a
    mis-guessed prefix there only over-signs a buffer that git's cleanup
    pass empties into an aborted commit anyway.
    """
    prefix = _comment_prefix()
    return any(line.strip() and not line.startswith(prefix) for line in text.splitlines())


def strip_scissors_tail(text: str) -> str:
    """Return the message with everything from the scissors line onward removed.

    Line endings are normalized to ``\\n`` so trailer lines written back
    never carry a stray ``\\r`` from a CRLF-edited message file.
    """
    text = text.replace("\r\n", "\n")
    match = _SCISSORS_RE.search(text)
    return text[: match.start()] if match else text


def has_signoff(text: str) -> bool:
    """Return True when the message already contains a ``Signed-off-by`` line."""
    return _SIGNOFF_RE.search(text) is not None


def committer_signoff() -> tuple[str, str]:
    """Return the ``(name, email)`` pair git records for the committer.

    Raises:
        SystemExit: when the identity is unset or unparsable. Failing the
            commit is the honest outcome: git itself refuses to create a
            commit without an identity, and a silent skip here would
            recreate the unsigned-commit gap this hook exists to close.
    """
    proc = _git(["git", "var", "GIT_COMMITTER_IDENT"])
    if proc.returncode != 0:
        detail = proc.stderr.strip() or "git var GIT_COMMITTER_IDENT failed"
        print(f"dco-signoff: cannot resolve committer identity: {detail}", file=sys.stderr)
        raise SystemExit(1)
    match = _IDENT_RE.match(proc.stdout.strip())
    if match is None:
        print(
            f"dco-signoff: cannot parse committer identity: {proc.stdout.strip()!r}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return match.group(1), match.group(2)


def append_trailer(text: str, name: str, email: str) -> str:
    """Return the message with a ``Signed-off-by: Name <email>`` trailer appended.

    Placement is delegated to ``git interpret-trailers`` so an existing
    trailer block gains the sign-off inside itself and a message without
    one gets a properly separated final paragraph.
    """
    proc = _git(
        ["git", "interpret-trailers", "--trailer", f"Signed-off-by={name} <{email}>"],
        input_text=text,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or "non-zero exit"
        print(f"dco-signoff: interpret-trailers failed: {detail}", file=sys.stderr)
        raise SystemExit(1)
    return proc.stdout


def main(argv: list[str]) -> int:
    """Rewrite the commit-message file named by ``argv[0]`` in place."""
    if len(argv) != 1:
        print("usage: dco_signoff.py <message-file>", file=sys.stderr)
        return 2
    path = Path(argv[0])
    cleaned = strip_scissors_tail(path.read_text(encoding="utf-8"))
    if not has_content(cleaned):
        return 0
    if has_signoff(cleaned):
        return 0
    name, email = committer_signoff()
    path.write_text(append_trailer(cleaned, name, email), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Commit message conformance checker for the kith framework.

Validates git commit messages against the project's Conventional Commits
contract, the body-content rule (the body explains WHY, never a bare file
list), DCO (Developer Certificate of Origin) requirement, commit signing,
and the header and body length limits. Designed for continuous integration:
it inspects a range of commits already on a branch and reports any
violations.

Automated commits authored by bots are exempt from the DCO check. Bots are
not human contributors; the DCO is a human attestation, and bot commit
messages follow the host platform's own conventions.

Header contract, matching the conventional-pre-commit commit-msg hook
configured in .pre-commit-config.yaml:

    <type>(<scope>)!: <summary>

  - type must be one of the project type allowlist.
  - scope is mandatory and must be one of the module allowlist.
  - an optional '!' before the colon marks a breaking change.
  - a space and a non-empty summary follow the colon.
  - the full header must not exceed 72 characters (soft limit 50).
  - the summary must be imperative and lowercase with no trailing period.

Body contract (AGENTS.md §5.2.5): the body explains WHY the change was
made, not WHAT changed — the diff already shows WHAT. The body must contain
substantive prose (at minimum, the prose-chars floor below) that states the
motivation and, where the impact is non-obvious, the behavioral difference.
A body that only enumerates the files the commit touches ("Add src/foo.c
and tests/c/test_foo.c.") or the standard build wiring ("Add cmake/lib_x,
src/x/kith_x.map, and wire add_subdirectory ... into CMakeLists.txt.") is a
file manifest, not a body, and is rejected for every type except `docs` and
`chore` (AGENTS.md §5.2.1). Footer trailer lines (Signed-off-by,
Reviewed-by, Fixes, BREAKING CHANGE, ...) are not body prose. The 72-column
body wrap is an editorial convention (AGENTS.md §5.2.5), not a checker gate:
the author and reviewer keep editorial control over line length.

Commit signing: every commit must carry a cryptographic signature (SSH, as
configured). By default the check confirms the signature is present (the
gpgsig header exists), not that it cryptographically validates against a
pinned keyring: identity and authenticity are verified by the hosting
platform (GitHub displays the commit as "Verified" against the account's
registered SSH keys) and enforced by branch protection ("Require signed
commits"). This keeps the signing check hermetic so it never depends on a
per-repo trust file.

When --allowed-signers is passed (pointing at a git allowedSignersFile),
the check upgrades from presence to cryptographic verification: each
commit is validated via 'git verify-commit' against the SSH keys listed
in that file. The file is built in CI from a secret and never committed
to the repo, so no per-repo trust file is tracked (AGENTS.md §5.2.8).

DCO contract: the message body must contain at least one trailer line of
the form 'Signed-off-by: Name <email>'.

Usage (CI range mode):

    check_commit_messages.py --head <sha> [--base <sha>] [--max-commits N]

Usage (single-message mode, for reuse and self-test):

    check_commit_messages.py --message <path-to-message-file>

Usage (internal self-test):

    check_commit_messages.py --self-test

In range mode, when --base is omitted or is unreachable (a force push or a
new branch), every commit reachable from --head is inspected, capped at
--max-commits (default 256) to bound runtime on large first pushes.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


# Conventional Commits type allowlist. Mirrors the conventional-pre-commit
# hook configuration in .pre-commit-config.yaml.
TYPES: tuple[str, ...] = (
    "feat",
    "fix",
    "perf",
    "refactor",
    "test",
    "docs",
    "build",
    "ci",
    "chore",
    "revert",
)

# Scope allowlist. Module granularity only, mirroring the hook configuration
# and the project commit-message standard. Dependency-bot scopes are not
# listed: bot commits are exempted rather than admitted to the human scope
# vocabulary.
SCOPES: tuple[str, ...] = (
    "util",
    "config",
    "logger",
    "metrics",
    "net",
    "proto",
    "reactor",
    "worker",
    "state",
    "db",
    "aoi",
    "sim",
    "fabric",
    "gateway",
    "coord",
    "control",
    "client",
    "server",
    "examples",
    "framework",
    "tools",
)

# Bot-authored commits are exempt. GitHub bots author as e.g.
# 'dependabot[bot] <dependabot[bot]@users.noreply.github.com>'.
BOT_RE: re.Pattern[str] = re.compile(r"\[bot\]")

# Header line: a mandatory type and scope, an optional breaking '!', a
# colon, a space, and a non-empty summary. The full header must not exceed
# MAX_HEADER_LENGTH characters (the conventional-pre-commit hook already
# enforces the format; this adds the length gate).
MAX_HEADER_LENGTH: int = 72
HEADER_RE: re.Pattern[str] = re.compile(
    r"^(?P<type>" + "|".join(TYPES) + r")"
    r"\((?P<scope>" + "|".join(SCOPES) + r")\)"
    r"(?P<break>!)?: .+$"
)

# DCO trailer: 'Signed-off-by: Name <email>'.
DCO_RE: re.Pattern[str] = re.compile(r"^Signed-off-by: .+ <[^<>]+@[^<>]+>\s*$", re.MULTILINE)

# Footer trailers that terminate the message body. A line in this set is a
# git trailer (token: value) and is not body prose.
TRAILER_PREFIXES: tuple[str, ...] = (
    "Signed-off-by:",
    "Reviewed-by:",
    "Tested-by:",
    "Reported-by:",
    "Co-authored-by:",
    "Acked-by:",
    "Fixes:",
    "Closes:",
    "Refs:",
    "DEPRECATED:",
    "BREAKING CHANGE:",
)

# Body presence rule (AGENTS.md §5.2.1): the body is mandatory for all types
# except docs and chore. The keyword is presence, not length: a body holding
# only a footer trailer is a manifest, not a rationale.
BODY_EXEMPT_TYPES: frozenset[str] = frozenset({"docs", "chore"})

# Body substance (AGENTS.md §5.2.5): the body must explain WHY, not re-list
# WHAT the diff already shows. A body that only enumerates the files it
# touches ("Add src/foo.c and tests/c/test_foo.c.") or a build body that
# only names the wiring it performs ("Add cmake/lib_foo.cmake,
# src/foo/kith_foo.map, and wire add_subdirectory(src/foo) into
# CMakeLists.txt.") is a file manifest, not a body, and is rejected.
# Rejection is deliberately conservative: a body qualifies as rational
# prose as soon as it contains at least MIN_MEANINGFUL_WORDS words that are
# not path names or build-wiring vocabulary, so short-but-real sentences
# ("Fix the leak in the pool.") pass. Authorship and style beyond that are
# the reviewer's judgment.
MIN_MEANINGFUL_WORDS: int = 1

# Tokens that make a message read as a file manifest rather than a rationale:
# path names (contain '/', '*' or end in a known source extension) and the
# wiring vocabulary that ties build fragments and version scripts together.
# These are subtracted from the body when measuring its substantive prose.
_PATH_SUFFIXES: tuple[str, ...] = (
    ".c",
    ".h",
    ".py",
    ".cmake",
    ".map",
    ".txt",
    ".json",
    ".yaml",
    ".toml",
    ".conf",
    ".md",
    ".lock",
    ".abi",
    ".yml",
)
_WIRE_WORDS: frozenset[str] = frozenset(
    {
        "a",
        "add",
        "add_subdirectory",
        "an",
        "and",
        "CMakeLists.txt",
        "cmake",
        "for",
        "from",
        "in",
        "into",
        "of",
        "on",
        "the",
        "to",
        "wire",
        "with",
    }
)


def _is_path_token(token: str) -> bool:
    """True when a word is a file path or path-like identifier."""
    if "/" in token or "*" in token or "(" in token or ")" in token:
        return True
    return token.endswith(_PATH_SUFFIXES)


def meaningful_prose_words(lines: list[str]) -> int:
    """Count words of substantive prose in body lines.

    Words that name files (paths, globs, source extensions) and the
    build-wiring vocabulary are manifest noise, not prose; they do not
    count toward the WHY explanation. Everything else -- verbs, objects,
    reasons -- is prose. A count of zero means the whole body was a path
    listing; a count of one or more means the author wrote something.
    """
    count = 0
    for line in lines:
        for word in re.split(r"\s+", line):
            stripped = word.strip(",;:.()[]{}")
            if not stripped:
                continue
            if _is_path_token(stripped):
                continue
            if stripped.lower() in _WIRE_WORDS:
                continue
            count += 1
    return count


# Separator used between the git format placeholders so author name, author
# email, and the raw message can be recovered from a single git invocation.
FIELD_SEP = "\x1f"


@dataclass
class CommitInfo:
    """A single commit's authorship and message for validation.

    Attributes:
        sha: Object name, or '(message)' in single-message mode.
        author_name: Author name (empty in single-message mode).
        author_email: Author email (empty in single-message mode).
        subject: First line of the commit message.
        body: Full raw commit message (subject plus body and trailers).
        signature_status: 'signed' (gpgsig header present, no
            cryptographic verification), 'verified' (gpgsig present and
            git verify-commit passed), 'unverified' (gpgsig present but
            git verify-commit failed), 'unsigned' (no gpgsig header), or
            empty string in single-message mode.
    """

    sha: str
    author_name: str
    author_email: str
    subject: str
    body: str
    signature_status: str = ""


def git(*args: str) -> str:
    """Run a git command and return stdout. Raise on non-zero exit."""
    proc = subprocess.run(["git", *args], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        message = proc.stderr.strip() or f"git {' '.join(args)} failed"
        raise RuntimeError(message)
    return proc.stdout


def verify_commit_signature(sha: str, allowed_signers: Path) -> bool:
    """Cryptographically verify a commit's SSH signature.

    Runs 'git verify-commit' with gpg.ssh.allowedSignersFile pointed at
    the given file, so the signature is validated against the SSH keys
    listed there rather than just checked for presence. Returns True when
    the signature verifies, False otherwise. The -c flags set the config
    inline so the repository's own git config is not modified.
    """
    result = subprocess.run(
        [
            "git",
            "-c",
            "gpg.format=ssh",
            "-c",
            f"gpg.ssh.allowedSignersFile={allowed_signers}",
            "verify-commit",
            sha,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def commit_shas(base: str | None, head: str, max_commits: int) -> list[str]:
    """Return the object names to validate, oldest first.

    When base is given and reachable, returns the commits in base..head.
    When base is omitted or unreachable, returns every commit reachable
    from head, capped at max_commits.
    """
    if base:
        proc = subprocess.run(
            ["git", "rev-list", "--reverse", f"{base}..{head}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return proc.stdout.split()
    out = git("rev-list", "--reverse", "--max-count", str(max_commits), head)
    return out.split()


def commit_info(sha: str, allowed_signers: Path | None = None) -> CommitInfo:
    """Read one commit's author, message, and signature via git.

    Signature detection reads the raw commit object and checks for a
    'gpgsig' header (present when the commit was created with signing
    enabled, e.g. commit.gpgsign). This detects presence independent of
    any trust anchor: git reports %G? as 'U' (untrusted) for an SSH-signed
    commit whenever gpg.ssh.allowedSignersFile is unset, which would make a
    genuinely signed commit look unsigned on a fresh checkout. Presence is
    the hermetic, platform-independent signal; the hosting platform performs
    identity verification and shows the commit as "Verified".

    When allowed_signers is provided, the presence check is upgraded to
    cryptographic verification via 'git verify-commit' with the given
    allowedSignersFile, so the signature is validated against the SSH keys
    listed there rather than just checked for presence.
    """
    out = git("show", "-s", f"--format=%an{FIELD_SEP}%ae{FIELD_SEP}%B", sha)
    parts = out.split(FIELD_SEP, 2)
    if len(parts) < 3:
        raise RuntimeError(f"unexpected git output for {sha}")
    name, email, body = parts
    body = body.rstrip("\n")
    subject = body.split("\n", 1)[0] if body else ""
    raw = git("cat-file", "commit", sha)
    header = raw.split("\n\n", 1)[0]
    has_gpgsig = any(line.startswith("gpgsig ") for line in header.split("\n"))
    if allowed_signers is not None:
        if has_gpgsig and verify_commit_signature(sha, allowed_signers):
            status = "verified"
        elif has_gpgsig:
            status = "unverified"
        else:
            status = "unsigned"
    else:
        status = "signed" if has_gpgsig else "unsigned"
    return CommitInfo(
        sha=sha,
        author_name=name,
        author_email=email,
        subject=subject,
        body=body,
        signature_status=status,
    )


def is_bot(info: CommitInfo) -> bool:
    """True for commits authored by an automation bot."""
    return bool(BOT_RE.search(info.author_name) or BOT_RE.search(info.author_email))


def body_lines(info: CommitInfo) -> list[str]:
    """Return the message's body lines, excluding footer trailers.

    The subject line and the git trailer block (Signed-off-by, Reviewed-by,
    Fixes, BREAKING CHANGE, ...) are not body prose. The body starts at the
    first line after the subject; the trailer block begins at the first
    line that is blank or a trailer prefix and runs to the end.
    """
    lines = info.body.splitlines()
    if len(lines) <= 1:
        return []
    body: list[str] = []
    for line in lines[1:]:
        stripped = line.strip()
        if not stripped:
            body.append("")
            continue
        if any(stripped.startswith(prefix) for prefix in TRAILER_PREFIXES):
            break
        body.append(stripped)
    return body


def commit_type(info: CommitInfo) -> str:
    """Return the conventional-commit type, or empty when unparsable."""
    match = HEADER_RE.match(info.subject)
    return match.group("type") if match else ""


def validate(info: CommitInfo, *, skip_signature: bool = False) -> list[str]:
    """Return violation messages for one commit. Empty means conformant.

    When skip_signature is True (single-message mode), the signature check is
    omitted — only the message format, length, body content, and DCO are
    validated. Author name and email are never validated here; authorship
    policy is orthogonal to message conformance and is a project/org concern,
    not a tool concern.
    """
    if is_bot(info):
        return []
    errors: list[str] = []
    full_header = info.subject
    if not HEADER_RE.match(full_header):
        errors.append(f"header does not match 'type(scope)!: summary': {full_header!r}")
    if len(full_header) > MAX_HEADER_LENGTH:
        errors.append(
            f"header exceeds {MAX_HEADER_LENGTH} chars ({len(full_header)}): {full_header!r}"
        )
    if not skip_signature and info.signature_status in ("unsigned", "unverified"):
        if info.signature_status == "unverified":
            errors.append("commit signature: present but cryptographic verification failed")
        else:
            errors.append("commit signature: unsigned")
    if not DCO_RE.search(info.body):
        errors.append("missing 'Signed-off-by: Name <email>' trailer (DCO)")

    body = body_lines(info)
    ctype = commit_type(info)
    needs_prose = ctype and ctype not in BODY_EXEMPT_TYPES
    if needs_prose and meaningful_prose_words(body) < MIN_MEANINGFUL_WORDS:
        errors.append(
            "body is a file manifest, not a rationale: explain WHY the "
            "change was made in your own words (AGENTS.md §5.2.5)"
        )
    return errors


def run_self_test() -> int:
    """Validate representative messages to prove the checker's own logic.

    Each fixture asserts an expected outcome (violations present or absent)
    so a regression in the checker is caught at build time rather than by a
    commit slipping through.
    """
    fixtures: list[tuple[str, bool]] = [
        # (message, expect_violations)
        (
            "feat(sim): add circle query with grid bucketing\n\n"
            "Scans only the grid buckets intersecting the query circle, cutting\n"
            "the scan from O(actors) to O(buckets-in-circle).\n\n"
            "Signed-off-by: Example Name <email@example.com>\n",
            False,
        ),
        (
            "build(logger): add cmake fragment and version script\n\n"
            "Adds the module to the build and member-hides its internals so\n"
            "-Wmissing-prototypes and the ABI diff stay clean.\n\n"
            "Signed-off-by: Example Name <email@example.com>\n",
            False,
        ),
        ("feat(sim): no body\n\nSigned-off-by: Example Name <email@example.com>\n", True),
        (
            "build(net): wire fragment\n\n"
            "Add cmake/lib_net.cmake, src/net/kith_net.map, and wire\n"
            "add_subdirectory(src/net) into CMakeLists.txt.\n\n"
            "Signed-off-by: Example Name <email@example.com>\n",
            True,
        ),
        (
            "feat(sim): tighten origin check\n\n"
            "Fix the leak.\n\n"
            "Signed-off-by: Example Name <email@example.com>\n",
            False,
        ),
        (
            "docs(framework): add AGENTS.md\n\n"
            "Adds include/kith/foo.h.\n\n"
            "Signed-off-by: Example Name <email@example.com>\n",
            False,
        ),
    ]
    failures = 0
    for message, expect_violations in fixtures:
        lines = message.splitlines()
        info = CommitInfo(
            sha="(self-test)",
            author_name="",
            author_email="",
            subject=lines[0] if lines else "",
            body=message.rstrip("\n"),
            signature_status="",
        )
        violations = validate(info, skip_signature=True)
        got = bool(violations)
        if got != expect_violations:
            failures += 1
            print(
                f"check_commit_messages.py self-test: unexpected result\n"
                f"  message: {lines[0] if lines else ''}\n"
                f"  expected violations: {expect_violations}, got: {got}\n"
                f"  details: {violations}",
                file=sys.stderr,
            )
    if failures:
        print(
            f"self-test FAILED: {failures}/{len(fixtures)} fixture(s) mismatched", file=sys.stderr
        )
        return 1
    print("self-test OK")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Validate commit messages against the project's "
        "Conventional Commits, DCO, signing, and header-length contracts.",
    )
    parser.add_argument(
        "--message",
        metavar="PATH",
        help="validate a single message file instead of a git range",
    )
    parser.add_argument("--head", default="HEAD", help="range head (default HEAD)")
    parser.add_argument("--base", default=None, help="range base; omitted means all reachable")
    parser.add_argument(
        "--max-commits",
        type=int,
        default=256,
        help="cap on commits inspected when base is omitted or unreachable",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run the internal conformance self-test and exit",
    )
    parser.add_argument(
        "--allowed-signers",
        metavar="PATH",
        default=None,
        help="path to a git allowedSignersFile; when set, signatures are "
        "cryptographically verified via git verify-commit instead of "
        "presence-only",
    )
    args = parser.parse_args(argv[1:])

    if args.self_test:
        return run_self_test()

    allowed_signers: Path | None = None
    if args.allowed_signers:
        allowed_signers = Path(args.allowed_signers)
        if not allowed_signers.is_file():
            print(f"error: allowed-signers file not found: {allowed_signers}", file=sys.stderr)
            return 2

    infos: list[CommitInfo] = []
    skip_signature = bool(args.message)
    if args.message:
        text = Path(args.message).read_text(encoding="utf-8")
        lines = text.splitlines()
        subject = lines[0] if lines else ""
        infos.append(
            CommitInfo(
                sha="(message)",
                author_name="",
                author_email="",
                subject=subject,
                body=text.rstrip("\n"),
                signature_status="",
            )
        )
    else:
        shas = commit_shas(args.base, args.head, args.max_commits)
        if not shas:
            print("no commits to validate")
            return 0
        infos = [commit_info(sha, allowed_signers) for sha in shas]

    violations: list[tuple[str, str]] = []
    for info in infos:
        for error in validate(info, skip_signature=skip_signature):
            violations.append((info.sha, error))

    if violations:
        print("Commit message violations detected:")
        for sha, error in violations:
            print(f"  {sha}: {error}")
        print(f"Total violations: {len(violations)}")
        return 1

    print(f"OK: {len(infos)} commit(s) conform.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

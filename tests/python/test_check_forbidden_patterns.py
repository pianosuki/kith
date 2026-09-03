"""Self-test for tools/check_forbidden_patterns.py.

Drives the scanner end-to-end against fixture trees covering the
lexicon scopes: game terms are confined to src/ and include/, the
KITH_FORBIDDEN_PATTERNS_LOCAL identifiers apply to code and to the
.md prose surfaces alike (content and file paths), and the #define
tunable heuristic survives on public headers.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


CHECKER = Path(__file__).resolve().parents[2] / "tools" / "check_forbidden_patterns.py"

LOCAL_RULES = "acme_internal\nacme\n"


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    """Materialize a fixture repo tree under tmp_path and return its root."""
    root = tmp_path / "repo"
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return root


def _run(root: Path, local_rules: str | None, *rels: str) -> subprocess.CompletedProcess[str]:
    """Run the checker against rel paths inside the fixture root, with a
    hermetic environment: the host's KITH_FORBIDDEN_PATTERNS_LOCAL never
    leaks into a fixture run."""
    env = {k: v for k, v in os.environ.items() if k != "KITH_FORBIDDEN_PATTERNS_LOCAL"}
    if local_rules is not None:
        rules = root / "local-rules.txt"
        rules.write_text(local_rules, encoding="utf-8")
        env["KITH_FORBIDDEN_PATTERNS_LOCAL"] = str(rules)
    return subprocess.run(
        [sys.executable, str(CHECKER), *rels],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_game_term_flagged_in_src(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"src/mod.c": "/* the player waits for the quest */\n"})
    proc = _run(root, None, "src/mod.c")
    assert proc.returncode == 1
    assert "forbidden identifier: player" in proc.stdout


def test_game_term_allowed_outside_src_and_include(tmp_path: Path) -> None:
    # The scoping rule: game terms apply to src/ and include/ only.
    # Examples live on game vocabulary by law; the scanner must not flag
    # them.
    root = _repo(tmp_path, {"examples/game/play.py": "count = 4  # the player count\n"})
    proc = _run(root, None, "examples/game/play.py")
    assert proc.returncode == 0


def test_game_term_allowed_in_docs_prose(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"docs/guide.md": "The player sees the lobby.\n"})
    proc = _run(root, None, "docs/guide.md")
    assert proc.returncode == 0


def test_local_identifier_flagged_in_code(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"src/mod.c": "int acme_internal = 0;\n"})
    proc = _run(root, LOCAL_RULES, "src/mod.c")
    assert proc.returncode == 1
    assert "forbidden identifier: acme_internal" in proc.stdout


def test_local_identifier_flagged_in_docs_prose(tmp_path: Path) -> None:
    # The prose surfaces join the scan: the local lexicon covers the .md
    # files too, which is where borrowed vocabulary lands.
    root = _repo(tmp_path, {"docs/note.md": "Handled per the acme process.\n"})
    proc = _run(root, LOCAL_RULES, "docs/note.md")
    assert proc.returncode == 1
    assert "forbidden identifier: acme" in proc.stdout


def test_local_identifier_flagged_in_root_prose(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"README.md": "An acme-flavored entry point.\n"})
    proc = _run(root, LOCAL_RULES, "README.md")
    assert proc.returncode == 1


def test_local_identifier_flagged_in_path(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"src/acme_internal.c": ""})
    proc = _run(root, LOCAL_RULES, "src/acme_internal.c")
    assert proc.returncode == 1
    assert "forbidden identifier in path" in proc.stdout


def test_unset_local_lexicon_scans_clean(tmp_path: Path) -> None:
    # Without the local file the scanner carries the game terms only; a
    # tree with none of them passes, prose surfaces included.
    files = {
        "README.md": "A clean entry point.\n",
        "docs/note.md": "Nothing to flag here.\n",
        "src/mod.c": "int count = 0;\n",
    }
    root = _repo(tmp_path, files)
    proc = _run(root, None, *files)
    assert proc.returncode == 0


def test_define_tunable_flagged_in_public_header(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"include/kith/mod.h": "#define KITH_MAX_RETRIES 3\n"})
    proc = _run(root, None, "include/kith/mod.h")
    assert proc.returncode == 1
    assert "tunable #define in public header" in proc.stdout

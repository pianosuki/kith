"""Markdown formatting gate and semantic-preservation checks."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import yaml


REPO = Path(__file__).resolve().parents[2]


def _run(directory: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "mdformat", *args],
        cwd=directory,
        capture_output=True,
        text=True,
        check=False,
    )


def _fixture(directory: Path, text: str) -> Path:
    (directory / ".mdformat.toml").write_bytes((REPO / ".mdformat.toml").read_bytes())
    document = directory / "sample.md"
    document.write_text(text, encoding="utf-8")
    return document


def test_markdown_gate_rejects_without_rewriting(tmp_path: Path) -> None:
    text = "The selected subjects\nremain\nindividual within the configured budget.\n"
    document = _fixture(tmp_path, text)
    result = _run(tmp_path, "--check", document.name)
    assert result.returncode == 1, result.stdout + result.stderr
    assert document.read_text(encoding="utf-8") == text
    result = _run(tmp_path, document.name)
    assert result.returncode == 0, result.stdout + result.stderr
    assert document.read_text(encoding="utf-8") == (
        "The selected subjects remain individual within the configured budget.\n"
    )
    assert _run(tmp_path, "--check", document.name).returncode == 0


def test_markdown_wrap_preserves_structures_and_is_idempotent(tmp_path: Path) -> None:
    paragraph = " ".join(["The publication interval bounds when state becomes eligible."] * 4)
    code = 'value = "' + "x" * 100 + '"\n'
    html = '<details>\n<summary>Details</summary>\n<p data-id="x">Kept as HTML.</p>\n</details>\n'
    document = _fixture(
        tmp_path,
        paragraph
        + "\n\n```python\n"
        + code
        + "```\n\n"
        + "| Name | Value |\n| --- | --- |\n| cap | `24` |\n\n"
        + "- [x] ~~closed~~\n\n"
        + html,
    )
    result = _run(tmp_path, document.name)
    assert result.returncode == 0, result.stdout + result.stderr
    formatted = document.read_text(encoding="utf-8")
    wrapped = formatted.split("\n\n", 1)[0]
    assert len(wrapped.splitlines()) > 1
    assert max(map(len, wrapped.splitlines())) <= 80
    assert " ".join(wrapped.splitlines()) == paragraph
    assert "```python\n" + code + "```" in formatted
    assert html in formatted
    assert "| cap" in formatted and "`24`" in formatted
    assert "[x] ~~closed~~" in formatted
    assert _run(tmp_path, document.name).returncode == 0
    assert document.read_text(encoding="utf-8") == formatted


def test_markdown_hook_is_pinned_and_verify_only() -> None:
    config = yaml.safe_load((REPO / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    hooks = [hook for repo in config["repos"] for hook in repo["hooks"]]
    hook = next(hook for hook in hooks if hook["id"] == "mdformat")
    assert hook["entry"] == "mdformat --check"
    assert hook["language"] == "python"
    pins = dict(pin.split("==") for pin in hook["additional_dependencies"])
    assert set(pins) == {
        "mdformat",
        "mdformat-gfm",
        "markdown-it-py",
        "mdit-py-plugins",
        "mdurl",
        "wcwidth",
    }
    locked = tomllib.loads((REPO / "uv.lock").read_text(encoding="utf-8"))
    versions = {
        package["name"]: package["version"]
        for package in locked["package"]
        if package["name"] in pins
    }
    assert pins == {name: versions[name] for name in pins}
    for script in ("scripts/provision-ci.sh", "scripts/verify.sh"):
        source = (REPO / script).read_text(encoding="utf-8")
        assert all(f"{name}=={version}" in source for name, version in pins.items())

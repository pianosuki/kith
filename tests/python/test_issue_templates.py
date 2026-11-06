"""GitHub issue forms retain valid metadata and optional contributor prompts."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml


TEMPLATES = Path(__file__).resolve().parents[2] / ".github" / "ISSUE_TEMPLATE"


@pytest.mark.parametrize(
    ("stem", "name", "title", "label", "fields"),
    [
        (
            "bug_report",
            "Bug report",
            "bug: ",
            "bug",
            [
                "Summary",
                "Steps to reproduce",
                "Expected behavior",
                "Actual behavior",
                "Environment",
                "Logs",
            ],
        ),
        (
            "feature_request",
            "Feature request",
            "feature: ",
            "enhancement",
            ["Problem", "Proposed solution", "Alternatives considered", "Scope"],
        ),
    ],
)
def test_issue_forms_are_valid_and_preserve_optional_fields(
    stem: str, name: str, title: str, label: str, fields: list[str]
) -> None:
    assert not (TEMPLATES / f"{stem}.md").exists(), "Markdown metadata is not formatter-safe"
    form = yaml.safe_load((TEMPLATES / f"{stem}.yml").read_text(encoding="utf-8"))
    assert form["name"] == name
    assert isinstance(form["description"], str) and form["description"].strip()
    assert form["title"] == title
    assert form["labels"] == [label]
    assert form["assignees"] == []
    body = form["body"]
    assert isinstance(body, list) and body
    assert [field["attributes"]["label"] for field in body] == fields
    identifiers: set[str] = set()
    for field in body:
        assert field["type"] in {"textarea", "checkboxes"}
        identifier = field["id"]
        assert isinstance(identifier, str) and re.fullmatch(r"[A-Za-z0-9_-]+", identifier)
        assert identifier not in identifiers
        identifiers.add(identifier)
        attributes = field["attributes"]
        assert isinstance(attributes["label"], str) and attributes["label"].strip()
        assert not field.get("validations", {}).get("required", False)
        if field["type"] == "checkboxes":
            options = attributes["options"]
            assert isinstance(options, list) and options
            assert all(
                isinstance(option["label"], str) and option["label"].strip() for option in options
            )
            assert all(not option.get("required", False) for option in options)

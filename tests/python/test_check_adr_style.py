"""Self-test for tools/check_adr_style.py.

Drives the record house-style checker against fixture texts covering the
legal shapes (full record, path pointers, decision-content tokens) and at
least one violation per mechanical rule (filename, title, status, skeleton,
H2 set, trailers, blockquotes, voice, vocabulary, code identifiers, CLI
flags, counts, self-demotion). The committed records are additionally
scanned whole: the corpus must be clean, which pins conformance at every
verification run.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import tools.check_adr_style as checker


_REPO_ROOT = Path(__file__).resolve().parents[2]
_ADR_DIR = _REPO_ROOT / "docs" / "architecture" / "adr"


def _record(status: str = "Accepted", body: str = "") -> str:
    sections = textwrap.dedent(
        """\
        # ADR-0042: A decision about the record shape

        **Status:** {status}

        ## Context

        The framework needs a decision recorded.

        ## Decision

        The decision binds the contract. Worker-pool tasks submit jobs.

        ## Consequences

        Positive — the contract is explicit. Negative — none noted.
        """
    ).format(status=status)
    return sections + textwrap.dedent(body)


def _violations(text: str, number: str = "0042") -> list[tuple[int, str, str]]:
    return checker.check_text(text, number)


def test_accepted_record_conforms() -> None:
    assert _violations(_record()) == []


def test_amended_status_is_flagged() -> None:
    status = "Accepted — amended by ADR-0099 (the passage marker)"
    violations = _violations(_record(status=status))
    assert any("status is not exactly" in description for _, _, description in violations)


def test_folded_status_is_flagged() -> None:
    status = "Folded into ADR-0007 (the surviving record carries the decision)"
    violations = _violations(_record(status=status))
    assert any("status is not exactly" in description for _, _, description in violations)


def test_missing_section_is_flagged() -> None:
    text = _record().replace("## Consequences", "## Results")
    violations = _violations(text)
    descriptions = [description for _, _, description in violations]
    assert "missing `## Consequences` section" in descriptions
    assert any("outside the house skeleton" in item for item in descriptions)


def test_skeleton_order_is_enforced() -> None:
    text = _record()
    swapped = (
        text.replace("## Decision", "@@TMP@@")
        .replace("## Consequences", "## Decision")
        .replace("@@TMP@@", "## Consequences")
    )
    violations = _violations(swapped)
    assert any("out of order" in description for _, _, description in violations)


def test_status_vocabulary_is_enforced() -> None:
    violations = _violations(_record(status="Draft"))
    assert any("status is not exactly" in description for _, _, description in violations)


def test_title_number_mismatch_is_flagged() -> None:
    violations = _violations(_record(), number="0017")
    assert any("differs from filename" in description for _, _, description in violations)


@pytest.mark.parametrize(
    "line",
    [
        "We bind the contract here.",
        "our decision",
        "You configure the pool.",
        "your handler",
        "let's record it",
    ],
)
def test_pronouns_are_flagged(line: str) -> None:
    text = _record().replace("The decision binds the contract.", line)
    violations = _violations(text)
    assert any("pronoun" in description for _, _, description in violations)


def test_inline_code_spans_are_exempt_from_voice_rules() -> None:
    text = _record().replace(
        "The decision binds the contract.",
        "The loop `for us in items` binds the contract.",
    )
    assert _violations(text) == []

    unbackticked = _record().replace(
        "The decision binds the contract.",
        "The loop for us in items binds the contract.",
    )
    assert any("pronoun" in description for _, _, description in _violations(unbackticked))


@pytest.mark.parametrize(
    "line",
    [
        "Per the roadmap, this lands later.",
        "golden-history rewrite reference",
        "the 07i work",
    ],
)
def test_forbidden_vocabulary_is_flagged(line: str) -> None:
    text = _record().replace("The decision binds the contract.", line)
    violations = _violations(text)
    assert any("forbidden vocabulary" in description for _, _, description in violations)


def test_trailing_citation_trailer_is_flagged() -> None:
    text = _record(body="\nRefs ADR-0002, ADR-0007\n")
    violations = _violations(text)
    assert any("citation trailer" in description for _, _, description in violations)


def test_blockquote_is_flagged() -> None:
    text = _record(body="\n> Amended by ADR-0099: the executor case.\n")
    violations = _violations(text)
    assert any("blockquote" in description for _, _, description in violations)


@pytest.mark.parametrize(
    "line",
    [
        "the pool entry point `kith_worker_submit` runs the job",
        "the failure code `KITH_EBUSY` reports a full queue",
        "the flag `KITH_GATEWAY_HANDLER_POOL` selects dispatch",
        "the drop counter `session_ok` reads below the bar",
        "the knob `delivery_worker_count` sizes the executor",
        "the pump calls `PyErr_CheckSignals` once per tick",
        "the interpreter raises `KeyboardInterrupt`",
        "the metric `bootstrap_ms_p95` is thresholded",
        "the run passes `--delivery-workers 2`",
    ],
)
def test_code_identifiers_are_flagged(line: str) -> None:
    text = _record().replace("The decision binds the contract.", line)
    violations = _violations(text)
    assert any(
        "code identifier" in description or "CLI flag" in description
        for _, _, description in violations
    )


@pytest.mark.parametrize(
    "line",
    [
        "the reactor uses io_uring behind the abstraction",
        "the publisher mints `update_seq` per applied input",
        "the gate reads `move_missing_ratio_certified`",
        "the companion `move_missing_ratio` stays reported",
        "the build asserts FLT_EVAL_METHOD is 0",
        "the build pins `-ffp-contract=off`",
        "the spans generalize into OpenTelemetry",
        "the layout is owned by `include/kith/util/rng.h`",
        "the thresholds live in `docs/guides/scaling_checklist.md`",
        "the headers live in `src/` and `include/`",
        "the presets are `full`, `delta`, and `tiered`",
        "the trade-off is the one `struct iovec` makes",
    ],
)
def test_decision_content_tokens_are_allowed(line: str) -> None:
    text = _record().replace("The decision binds the contract.", line)
    assert _violations(text) == []


@pytest.mark.parametrize(
    "line",
    [
        "certified at the distributed-2000 point",
        "the gate runs 2000 distributed actors",
        "measured at 1000 sessions per instance",
        "the cohort holds 500 clients",
        "the pool ran at 16 workers",
        "apply cost measured at 8 threads",
    ],
)
def test_counts_are_flagged(line: str) -> None:
    text = _record().replace("The decision binds the contract.", line)
    violations = _violations(text)
    assert any("count" in description for _, _, description in violations)


def test_operating_point_language_is_allowed() -> None:
    text = _record().replace(
        "The decision binds the contract.",
        "At the fidelity gate's operating point the demand exceeds the budget.",
    )
    assert _violations(text) == []


@pytest.mark.parametrize(
    "line",
    [
        "this is an additive component under ADR-0027",
        "the new flag is additive per ADR-0027",
    ],
)
def test_self_demotion_is_flagged(line: str) -> None:
    text = _record().replace("The decision binds the contract.", line)
    violations = _violations(text)
    assert any("self-demotion" in description for _, _, description in violations)


@pytest.mark.parametrize(
    "line",
    [
        "reserved slots absorb additive fields",
        "the addition of any new module or additive component requires a record",
        "it conforms to the founding set, including the additive-components rule (ADR-0027)",
    ],
)
def test_additive_contract_vocabulary_is_allowed(line: str) -> None:
    text = _record().replace("The decision binds the contract.", line)
    assert _violations(text) == []


def test_filename_shape_is_enforced(tmp_path: Path) -> None:
    good = tmp_path / "0042-record-shape.md"
    good.write_text(_record(), encoding="utf-8")
    assert checker.check_file(good) == []

    bad = tmp_path / "42-Record-Shape.md"
    bad.write_text(_record(), encoding="utf-8")
    violations = checker.check_file(bad)
    assert any("filename" in description for _, _, description in violations)


def test_committed_corpus_is_clean() -> None:
    files = sorted(_ADR_DIR.glob("*.md"))
    assert files, "architecture record directory is empty"
    failures: list[str] = []
    for path in files:
        for line_no, matched, description in checker.check_file(path):
            failures.append(f"{path.name}:{line_no}: {description}: '{matched}'")
    assert failures == [], "\n".join(failures)

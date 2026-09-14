# Commit message canon and examples

`AGENTS.md` §5.2 carries the law: the enforced structure, the type and
scope allowlists, and the mandatory rules for summaries, bodies, and
footers. This guide carries the material the law file does not need to
repeat: the full annotated examples, the formatting canon this
repository follows, and the mechanics of reverts and breaking changes.

## Sources

- Conventional Commits 1.0.0: <https://www.conventionalcommits.org/>
- Angular Commit Message Guidelines:
  <https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit>
- Tim Pope, "A Note About Git Commit Messages":
  <https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html>

The enforcement layer maps to the law: `conventional-pre-commit`
validates the header on the `commit-msg` stage;
`tools/check_commit_messages.py` enforces body presence and rejects
file-manifest bodies; the `dco-signoff` hook appends the sign-off when
the message lacks one.

## Why the 72-column body

`git log` does not wrap commit messages; with the default pager the
paragraphs run off the screen. `git format-patch` turns commits into
emails, where 72 columns leaves room for nested reply markers. The
50-character summary target keeps the header readable everywhere it is
shown truncated: one-line logs, interactive rebase, shortlog, reflogs,
merge-commit messages, and hosting UIs.

## The imperative mood

"add", not "added" or "adds". The summary completes the sentence "when
applied, this commit will ...": `feat(aoi): add circle query` reads as
"when applied, this commit will add a circle query". The convention
matches the messages `git merge` and `git revert` generate.

## Annotated examples

Good — a feature whose body carries the motivation and the measured
difference:

    feat(aoi): add circle query with grid bucketing

    Implements the circle query by scanning only grid buckets whose AABB
    intersects the query circle, reducing the per-query scan from O(actors)
    to O(buckets-in-circle + actors-in-those-buckets).

    Benchmark on the 1000-actor fixture: p50 12us -> p50 8us.

    Signed-off-by: Example Name <email@example.com>

Good — a breaking change with the migration path:

    feat(sim)!: redesign model registration ABI

    Replace the fixed function-pointer struct with a versioned vtable keyed
    by abi_version, allowing models compiled against v1 to load under v1.x
    runtimes.

    BREAKING CHANGE: sim_register_model now takes const sim_model_vtable *
    instead of a fixed struct. Existing models must add abi_version and
    reserved[8] fields. See docs/guides/writing_c_sim_models.md for the
    migration.

    Signed-off-by: Example Name <email@example.com>

Short bodies pass; the author controls pacing and wrapping:

    fix(worker): bound the shutdown drain by the queue depth

    A pool wider than the queue could block shutdown past the join
    timeout.

    Signed-off-by: Example Name <email@example.com>

Bad — capitalized, period, past tense, no type or scope:

    Added AOI circle query.

Bad — a file manifest in the body (the diff already shows the paths;
AGENTS.md §5.2.5):

    feat(sim): add circle query with grid bucketing

    Add include/kith/sim/sim.h, src/sim/query.c, and tests/c/test_sim_query.c.

Bad — aspirational (AGENTS.md §1.2):

    feat(aoi): add circle query

    Will eventually support ellipses too.

Bad — several concerns in one commit:

    feat(framework): add aoi circle query and fix reactor leak and update README

Split into `feat(aoi): ...`, `fix(reactor): ...`, `docs(framework): ...`.

Bad — a plan reference in place of the technical change (AGENTS.md
§5.2.13):

    feat(aoi): implement phase 3 task 2

Describe the actual technical change instead.

## Reverts

    revert: <original header line>

    This reverts commit <full 64-char SHA>.

    <reason>

The SHA is the full 64-character hash, not an abbreviation, and the
body carries a clear reason for the revert.

## Breaking changes

Prefer the `BREAKING CHANGE:` footer when migration instructions are
needed: the summary line, a blank line, then the description and the
migration path. The `!` form (`feat(sim)!: redesign model registration
ABI`) fits when a one-line flag is enough. Both count as breaking for
versioning; the footer form is preferred because it carries the
migration.

## Bullet formatting

Bullets use `-` followed by a single space, with a hanging indent on
continuation lines:

    - First bullet, wrapped to 72 columns with a
      hanging indent on continuation lines.
    - Second bullet.

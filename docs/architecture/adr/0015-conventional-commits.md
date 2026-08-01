# ADR-0015: Conventional Commits with Angular interpretation and Tim Pope formatting

**Status:** Accepted

## Context

The project's initial development and ongoing maintenance produce hundreds of
commits. Without an enforced convention, history drifts: inconsistent types,
invented scopes, past-tense summaries, missing bodies, aspirational language.
A drifted history is not navigable — targeted history search fails, changelog
generation fails, and long-term maintenance suffers.

## Decision

The project adopts Conventional Commits 1.0.0 as the spec, the Angular Commit
Message Guidelines as the interpretation (mandatory body except
`docs`/`chore`, type+scope allowlists, an imperative-present-lowercase-
no-period summary, and a body that explains WHY), and Tim Pope's formatting
canon (subject ≤50 soft / ≤72 hard, body wrapped at 72, blank line after the
subject, hanging-indent bullets). Enforcement is via a commit-message hook with
explicit type and scope allowlists, plus DCO sign-off. Commits are scoped per
module. The full style guide is defined in the repository's contributor
documentation.

The full SHA is used in revert commits. Commits are signed with SSH keys rather
than GPG, reusing existing developer keys and avoiding GPG's operational
burden, with signing enforced repository-wide.

## Consequences

Positive — machine-parseable history, automatic CHANGELOG, and a durable and
navigable log. Negative — commit overhead per change, mitigated by the atomic
one-concern-per-commit rule producing small, focused commits. The
convention is locked at v1.0.0; changing it requires a new ADR superseding this
one.

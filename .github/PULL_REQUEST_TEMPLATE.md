## Summary

<!-- One or two sentences on what this change does. -->

## Motivation

<!-- Why this change is needed. Reference an issue with "Refs #N" if one
exists. -->

## Changes

<!-- Notable changes as a bulleted list. -->

-

## Verification

- [ ] `scripts/verify.sh` is all-green (the single gate; see §5.0a in
      AGENTS.md)

## Checklist

- [ ] Commit messages follow Conventional Commits (`type(scope): summary`)
- [ ] Every commit is DCO signed off (`Signed-off-by:`) and SSH-signed
- [ ] One logical change per commit; no plan, phase, roadmap, or task
      references in commits or comments
- [ ] New public C types are opaque; new public functions have Doxygen
      comments (`@param`, `@return`, `@thread_safety`, `@ownership`)
- [ ] New behavior is covered by a test

See [`AGENTS.md`](../AGENTS.md) for the coding standards and review checklist
and [`CONTRIBUTING.md`](../CONTRIBUTING.md) for the contributor workflow.

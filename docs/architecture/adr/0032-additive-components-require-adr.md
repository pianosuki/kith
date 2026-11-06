# ADR-0032: Additive components require an ADR and follow the founding set

**Status:** Accepted

## Context

The framework will grow after release through contributions and new components.
Without a guardrail, a well-intentioned but ad-hoc addition can silently violate
a founding decision (plane invariants, ABI stability, the deterministic
simulation contract, or the error contract) and erode the architecture over
time.

## Decision

The status of every ADR is locked at v1.0.0. Any change to a locked convention,
and the addition of any new module or additive component, requires a new (or a
superseding) ADR before it lands. A new library module must conform to the
founding set — the five-plane architecture (ADR-0001), the deterministic
simulation contract (ADR-0014), the error/allocator contract (ADR-0018), and
hardening (ADR-0017) — and is subject to the same verification gate as all
existing modules.

This record holds the set's last number: a new record takes the number ahead of
it, and this record renumbers to the new last number.

**Amendment (released decision evolution):** the lock above preserves released
artifacts and forbids silent decision rewrites. Working-branch revisions of an
existing decision may use an explicitly identified contract-only amendment that
names the affected rule, its replacement or qualification, and the reason. A
replacement of the whole decision uses a superseding record; new components
still require their own records. Implementation references belong outside the
records so decisions remain independent of source organization. Published record
numbers are immutable: the renumbering rule above applied only before the first
release. Subsequent records append unused numbers. This preserves stable
citations while making bounded contract revisions explicit.

## Consequences

Positive — the architecture cannot be silently eroded; every growth decision is
explicit, reviewable, and consistent with what exists. Negative — an extra
up-front documentation step before adding a component, acceptable for the
durability it provides.

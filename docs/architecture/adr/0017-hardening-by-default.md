# ADR-0017: Every library is hardened by default and verified

**Status:** Accepted

## Context

A released library that is not memory-safe hardened is a liability for every
application linking it. Hardening must be the default, not an opt-in flag, and
it must be verified or it will silently regress.

## Decision

Every kith library and executable is built by default with: position-independent
code, full RELRO, stack canaries, control-flow integrity and stack-clash
protection, a non-executable stack, no RPATH, and fortified source code, with
symbol visibility hidden by default (ADR-0008). The build emits a hardening
report and CI fails if any artifact drops below the required hardening set.

**Amendment (wheel distribution):** installed Python wheels use relative
origin-based library search paths within the package to resolve their bundled
libraries. The release repair tool may emit RPATH for transitive dependencies;
absolute build paths and writable external search directories remain forbidden.
The wheel packaging contract is documented in `docs/guides/releases.md`.

## Consequences

Positive — every release is hardened by construction and a regression is
caught by CI rather than by a downstream incident. Negative — build-time
overhead and marginally larger binaries, acceptable for the security posture.

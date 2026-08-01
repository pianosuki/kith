# ADR-0012: Generate Python ctypes bindings from C headers

**Status:** Accepted

## Context

Hand-mirrored ctypes bindings drift silently from headers. In an earlier
monolithic, actor-centered MMO server, a manually maintained ctypes
structure file drifted and caused memory corruption.

## Decision

Python ctypes bindings are generated from the framework's public C headers by
a generator, and a drift check fails CI if the generated bindings diverge from
those headers. The runtime stays pure ctypes with no C++ toolchain.

## Consequences

Positive — no drift; a CI check guarantees binding/header consistency.
Negative — the generator and its drift check are framework tooling to
maintain.

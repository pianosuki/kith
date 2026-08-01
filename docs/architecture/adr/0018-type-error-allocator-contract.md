# ADR-0018: A single C contract for types, errors, and allocators

**Status:** Accepted

## Context

Each library with its own opinion about error reporting and ownership
would be unusable. Every public function must report outcome consistently
and return memory through one rule so callers can rely on a single contract.

## Decision

All kith C APIs share one contract, defined in the framework's shared public
header: a single typed error enum with an explicit success code and distinct
error codes; functions return an error status and write results through
out-parameters (never global state), with the sign applied through one
return helper so negative outcomes never scatter a bare negation; and
memory is allocated through a per-object allocator seam. Every allocating
public create takes an optional allocator parameter before its
out-parameter; passing NULL selects the default implementation (the libc
heap). A created handle stores the allocator it was created with and routes
every allocation it performs over its lifetime — including teardown —
through that one instance: the allocator that creates a resource destroys
it. Every module implements against this contract, so callers handle
errors and ownership identically across all libraries.

## Consequences

Positive — uniform API feel, predictable ownership, and no singleton or
global allocator; a caller-supplied allocator can account for or fail the
allocations of any single object without touching the rest. Negative — a
little verbosity from typed out-parameters and the allocator plumbing, and
one indirection per allocation through the stored instance, accepted for
consistency and correctness.

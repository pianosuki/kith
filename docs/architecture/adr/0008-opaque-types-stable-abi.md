# ADR-0008: Opaque types, size-versioned structs, value-type DTOs, and a locked, verifiable ABI

**Status:** Accepted

## Context

ABI stability requires that binary consumers remain compatible across
releases. Two things make that possible: never exposing the layout of types
whose representation may evolve, and freezing each library's exported symbol
surface. Some public types are intentionally exposed-layout: small,
fixed-shape DTOs callers own on the stack and pass by pointer (decoded frame
views, log/metric label pairs, cell locators, query descriptors, route
entries). Treating these as opaque would add accessor overhead and heap
churn for no evolution benefit, since the field set itself is the contract
(the same trade-off `struct iovec` makes). The ABI contract must therefore
distinguish opaque types from exposed-layout DTOs by rule, not by
convention.

## Decision

Every public type belongs to one of three categories.

### Opaque handles

A type whose representation may evolve is opaque: an incomplete struct type
exposed to callers only as a handle, its definition living in `src/`, never
in `include/`. Callers interact only through functions. This is the default
for any type with internal state, resources, or non-trivial invariants.

### Size-versioned structs

Creation parameters, configuration, vtables, and status snapshots carry an
explicit size and an ABI version so the runtime can reject an undersized or
incompatible-generation struct. The size is checked first (it catches a
caller compiled against a smaller struct), then the ABI version (it catches
a generation mismatch). Reserved slots absorb new additive fields without
breaking layout. The exact layout convention — field order, the width of the
size and version fields, the reserved-slot shape — is owned by the
framework's shared public header.

### Value-type DTOs

A type may be exposed-layout without size-versioning only when it meets all
four criteria:

1. **Fixed layout.** The field set, types, order, and alignment are part of
   the public contract and do not change across releases. Padding bytes are
   documented.
2. **No internal or opaque handles as fields.** Every field is a scalar, a
   fixed-shape value type, or a borrowed pointer whose lifetime is
   documented and never exceeds the call. A field that hides an internal
   implementation handle disqualifies the type.
3. **No evolution-coupled inline arrays without size-versioning.** An inline
   array whose length may change across releases must ride a type that
   carries its own size and ABI version (the category above), or the array
   must be replaced by a pointer + length pair. Documented padding arrays
   and arrays whose length is a named public invariant are fixed-shape
   fields under criterion 1, not evolution-coupled arrays.
4. **Documented as a stable DTO.** The doc comment states the type is an
   exposed-layout value type and records the lifetime of every borrowed
   pointer field.

The category is chosen per type by the module that owns it, and the same
conceptual role may land in different categories in different modules: one
module's status snapshot can be size-versioned while another's is a frozen
value-type DTO. The type itself states the evolution contract — the size
and version fields, or the value-type designation and the frozen field
set — and that statement is what a consumer reads.

Value-type DTOs are passed by pointer (never by value) and are stack- or
caller-owned; the framework never retains them past the call. They exist for
the same reason `struct iovec` does: zero-copy, no heap, no accessor
overhead, with the field set itself as the contract.

### Symbol surface and ABI baseline

Every shared library has a generated version script exporting only
publicly-visible symbols. Each module commits a checked-in ABI baseline
produced by tooling alongside the module's tests, and CI compares each
built library against its baseline, failing on any unintended change to
the exported surface. Baselines are regenerated deliberately, riding the
source change that alters the surface (the repository's generated-files
rule), each update verified by the ABI diff against the prior baseline.

## Consequences

Callers keep zero-copy, stack-owned DTOs where they earn their keep; the
ABI stays evolvable for everything else; and the rule is mechanically
checkable — a checker reads the three categories and the four value-type
criteria from this ADR. Costs are accessor verbosity for opaque types and
an extra judgment call when introducing a public type (which category?).
Both are accepted in exchange for a stable, auditable ABI. A regression
that adds, removes, or renames an exported symbol fails CI, protecting
every downstream consumer.

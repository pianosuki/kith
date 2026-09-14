# External code vetting protocol

When a module has an equivalent in an external reference codebase (a
library being replaced, a reference implementation), the code is
studied before reimplementation, then written fresh against this
framework's contracts — never copied, never translated line-by-line.
`AGENTS.md` §9 carries the mandate; this guide carries the
five-dimension checklist a module passes before it is considered
complete. The dimensions go beyond what mechanical checkers catch:
they are the qualitative judgment that guards against inheriting
outdated choices, confusing naming, inconsistent idioms, and hidden
conceptual coupling.

## 1. Naming consistency

- Every function follows `module_verb_noun`.
- Every type follows `module_thing_t`; every internal struct
  `module_thing`.
- Every file follows `module_submodule.c` / `module_submodule.h`.
- The create/destroy pair is consistent: `foo_create` / `foo_destroy`,
  never `foo_new` / `foo_free` or `foo_init` / `foo_cleanup`.
- No names that made sense in the external reference's context but
  confuse in this generic framework. A name whose only justification
  is "that's what the reference called it" is renamed.

## 2. Architectural conformance

- Public types are opaque, or are value-type DTOs meeting the
  criteria in `AGENTS.md` §2.2.
- All size-versioned public structs carry `size` + `abi_version` +
  `void *reserved[8]` (§2.2).
- No global singletons; dependencies are injected.
- Plane invariants are respected (`docs/architecture/planes.md`); the
  plane checker passes.
- The module uses the same patterns, idioms, and structure as the
  other modules. A module that feels ported is not done.

## 3. Implementation quality

- C23 features used where appropriate: `<stdckdint.h>` for
  overflow-safe math, `memset_explicit` for secrets,
  `<stdatomic.h>` for lock-free queues, `alignas(64)` for hot
  structs (§2.1).
- Function size within limits (§2.6): 80 lines, 60 statements, 12
  branches, 8 parameters.
- Error handling consistent (§2.4): `int` return, 0 = success,
  negative = error; `[[nodiscard]]` on error-returning public
  functions.
- Memory ownership documented in every doc comment (`@ownership`).
- No algorithmic complexity that cannot scale to the framework's
  target load (§4.4). An O(n²) shape that suited a smaller reference
  is redesigned (O(n log n) or better).

## 4. Forbidden anti-patterns

Checked by review and by the mechanical checkers:

- No global config singleton or equivalent.
- No logging macros that bypass the logger API.
- No `#define`d tunable constants in public headers (use config
  structs).
- No per-connection AOI push (use cell-stream publish; see
  `docs/architecture/planes.md`).
- No hand-mirrored ctypes bindings (use generated bindings; see
  `tools/check_ctypes_drift.py`).
- No reactor-centric dispatch (use the registration table; see
  `docs/architecture/`).
- No references to any external reference project, by name, path,
  file name, or identifier, anywhere in code, comments, or
  documentation.
- No game-specific conceptual coupling disguised with generic names
  (a renamed concept that still carries game-specific semantics).
  Game concepts belong in `examples/`, not in `src/` or `include/`.

## 5. Cross-module consistency

- Error handling matches the other modules (same return convention,
  same error-code range, same `[[nodiscard]]` usage).
- Logging pattern matches (structured fields, same key names, same
  level semantics).
- Config struct shape matches (`size` + `abi_version` + fields +
  `reserved[8]`).
- Test naming matches (§4.3).
- Doc comment style matches (§2.9 tags; §3.6 sections).
- Same C23 idiom choices: if module A uses `<stdatomic.h>` for a
  purpose, module B does not use pthreads for the same purpose.
- If the module feels different from the others, identify why and
  align it.

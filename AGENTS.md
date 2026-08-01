# AGENTS.md

This file defines the development philosophy, coding standards, and workflow
rules for this repository. All code — whether written by humans or AI agents —
must conform to these rules. Automated tooling enforces most of them; manual
review enforces the rest.

## Contents

1. Philosophy — communication, voice, and the comment style system (§1.7)
2. C code standards (§2)
3. Python code standards (§3)
4. Testing — tiers, naming, and the scaling gates (§4)
5. Git workflow — push law, the verification gate, commit messages (§5)
6. Dependencies (§6)
7. Documentation — API reference, records, guides (§7)
8. Review checklist (§8)
9. External code vetting (§9)

## 1. Philosophy

### 1.1 Code is communication
Code is read more often than it is written. Optimize for the reader. If a
construct requires a comment to be understood, first try renaming or
restructuring. Comments are the last resort.

### 1.2 No aspirational language
Comments and commit messages describe what the code does, not what it
will do. No "TODO", "FIXME", "should eventually", "in the future". If
something is incomplete, either complete it or do not commit it.

The ban targets promises about the change's own future: future-tense
"will"/"eventually" or scheduling notes ("a later task"). Present-tense
descriptions of a current condition or absence stay allowed ("returns
EAGAIN when the ring is empty", "the backend is not implemented"), and
messages may quote a checker's own vocabulary when documenting the
tokens it detects.

### 1.3 No personal voice
Comments are impersonal. No "I", "we", "you", "let's". No authorship markers,
no names, no dates, no initials. The codebase belongs to no one and everyone.

### 1.4 No game-specific terms in core
The core framework is generic. Terms like "quest", "npc", "monster", "loot" do
not appear in `src/` or `include/`. Game-specific vocabulary lives only in
`examples/` and user code.

### 1.5 Tooling over convention
If a rule can be enforced by a tool, it is enforced by a tool. Conventions that
require human memory will be violated. The pre-commit hooks are the source of
truth. If a rule is not enforced, it does not exist.

### 1.6 Plane invariants are non-negotiable
The runtime is organized into planes (Sim, Fabric, Gateway, Coord, Control),
each with a data-flow ownership contract in `docs/architecture/planes.md`. The
plane checker enforces these. A change that violates a plane invariant is
rejected regardless of whether it makes a local test pass. The plane
boundaries are what keeps the framework at MMO scale; weakening them
reintroduces the N² ceiling.

### 1.7 Comments
This section is the style system for every comment and docstring in the
repository, and the only one: no separate style guide exists. §2.9 (C doc
comments) and §3.6 (Python docstrings) fix the formats; this section
governs content and voice everywhere: C, Python, shell, CMake, and config
files. Shell and CMake comments follow the taxonomy by analogy; no
dedicated format section exists for them.
`tools/check_comments.py` is the mechanical floor for `.c`/`.h`/`.py`
comments (excluding `tools/fixtures/`) and for the `#` comments of
YAML and CMake files; shell scripts stay review-enforced, where
heredocs and quoting make mechanical extraction unsound. Review
enforces the rest.

**The bar.** A comment earns its place by adding what the code and its
names cannot: the invariant, the constraint, the reason. If a construct
needs a comment to be understood, first rename or restructure (§1.1).
The codebase reads as if written by one engineer: the same judgment
about when a comment is worth its line, the same register everywhere,
pacing that follows the material — terse where the code is
self-evident, longer where the constraint is subtle.

**Types.** Three types, each with a narrow job. Use the smallest that
fits.

**Inline** — a single line above or trailing the statement it clarifies.
States the why: the invariant preserved, the constraint honored, the
reason the non-obvious shape is correct. Never restates the operation.
Implementation comments are declarative; `should`, `would`, and `could`
belong to contract prose in doc comments, not to narration (the checker
enforces that split).

**Structural** — a section divider in a file large enough that sections
aid navigation. Sparing: a real boundary earns a divider, a short file
earns none. Rule lines sandwich a title line; one divider form per file;
fixed width within a file. The title is a lowercase noun phrase — a
label, not a sentence, with no trailing period; embedded identifiers and
initialisms keep their casing (`replay record FIFO`, `HTTP helpers`). A
block may carry body prose between the title and the closing rule when a
section's contract needs more than the title says.

C block form:

    /*---------------------------------------------------------------------------
     * shared helpers
     *-------------------------------------------------------------------------*/

C line-comment form (one C form per file, not both):

    //---------------------------------------------------------------------------
    // shared helpers
    //---------------------------------------------------------------------------

Python:

    # ---------------------------------------------------------------------------
    # public enums
    # ---------------------------------------------------------------------------

**Header (file-level)** — the top-of-file block stating what the file
contains and the role it plays in its module. One to a few lines. Public
headers carry the Doxygen module block (§2.9); private headers and `.c`
files a `/* ... */` block naming contents and siblings; Python modules
the module docstring (§3.5). No license block: Apache-2.0 lives in
`LICENSE`, not in every file.

**Content bans.** Comments support the code that exists. They never:

- **Narrate history.** No fixes, patches, regressions, discoveries, or
  prior behavior: no "previously", "used to", "no longer", "workaround",
  "bug", "it turned out", "was found that". The repository reads as one
  continuous, correct body of work, because the only state a reader has
  is the current one. (§1.2 is the future-facing counterpart.)
- **Argue with unshipped alternatives.** No weighing of rejected
  options, no defense of the design against what it does not do. State
  the shipped contract. A comparison with an absent behavior earns its
  place only when a competent reader would otherwise assume that
  behavior and misread its absence as a defect — and then it is one
  clause, not a narrative. A rejected design alternative never meets
  that bar.
- **Cite decision records.** No `ADR-NNNN` in code comments or
  docstrings: code under `src/`, `include/`, `python/`, `examples/`, and
  `tests/` must be self-supporting, and the same rule governs `tools/`
  and `scripts/` except where the file's subject is the records
  themselves. Restate the binding fact in the comment; the record keeps
  the decision's why in `docs/architecture/adr/`. Where a pointer is
  genuinely load-bearing, point at the doc path, never the record
  number. Records may be cited from `docs/**`, this file, the checkers
  that validate them, and commit footers (`Refs ADR-NNNN`, §5.2.6).
- **Reference plans or tickets.** No phases, tasks, workstreams, or
  issue numbers (§5.2.13; the checker's ticket ban).
- **Carry noise.** Restated code, language tutorials, commented-out
  code, authorship markers (§1.3).

**Voice.** Impersonal (§1.3), plain register, prose where prose flows.
The recurring patterns to reject, each with its fix:

| Pattern | Reads as | Fix |
|---|---|---|
| Throat-clearing | "Note that...", "It's worth noting..." | Delete the opener; state the thing |
| Restated code | `i++` /* increment i */ | Delete; rename or restructure (§1.1) |
| What-narration | "First check X, then allocate Y" | Write the invariant the sequence preserves |
| History narration | "previously", "no longer", "workaround" | Describe the shipped behavior only |
| Rejected-alternative essay | "X was considered but Y is better" | State the contract; one clause only if the absence would read as a defect |
| Qualification stacking | "generally, but not always, typically" | One hedge or none; name the real exception in a follow-on clause |
| Formal register | "utilize", "in order to", "prior to" | Use, to, before |
| First person | "we iterate", "let's look" | Declarative: "The loop iterates..." (§1.3) |
| Listicle fragmentation | Bullets where two sentences of prose flow | Prose, unless the items are genuinely parallel |
| False precision | "up to 10x faster" without a benchmark | The measured number, or the qualitative claim without the multiplier |

The table governs docs, guides, and README prose as well (§7.3), with
four further patterns that matter mostly there: transition-phrase
scaffolding ("Furthermore", "Moreover"), symmetry-seeking structure,
the summary-within-the-summary, and empty openers and closers.

**Test comments.** Tests may explain more: the scenario's intent, why a
fixture is arranged as it is, the invariant under attack. Multiple
sentences are normal. They obey every other rule above — impersonal, no
history, no alternatives, no record citations — and stay consistent
with each other: an intent statement sits above the test it describes,
not scattered through the body. This covers `tests/` and the agentic
scenarios in `tools/agent/scenarios/`. `tools/fixtures/` sits outside
the checker because fixture files exist to carry comment-like payloads;
real test code gets no such exemption.

## 2. C Code Standards

### 2.1 Language Standard
- C23 (`-std=c23`). No GNU extensions unless explicitly justified and guarded
  by `__has_extension` or feature-test macros.
- Use the C23 features where they fit: `nullptr`, `bool`/`true`/`false`
  (no `<stdbool.h>`), `static_assert(expr, "msg")`, `[[nodiscard]]`,
  `[[maybe_unused]]`, `[[noreturn]]`, `[[fallthrough]]`,
  `[[unreachable]]`, `auto` (complex compound types only), `#elifdef`,
  digit separators (`1'000'000`), and binary literals — with
  `alignas(64)` cache-line alignment for hot per-connection structs and
  lock-free queue nodes.
- Use the standard facilities where they fit: `<stdckdint.h>` for
  checked integer arithmetic (overflow-safe length math in the protocol
  path), `<stdbit.h>` for bit/endianness/counting utilities,
  `memset_explicit` for clearing secrets without compiler elision —
  only where the binary's install floor is not pinned by the glibc
  that introduced the symbol; in wheel-bound code the volatile-store
  loop keeps the floor host-independent — `<stdatomic.h>` for portable
  atomics (reactor lock-free queues, refcounted gateway cache
  entries), and `thread_local` for per-thread reactor state.
- Do not use `_BitInt` in public ABI (width is implementation-defined).
- Do not use `#pragma once` in installed public headers (use traditional include
  guards). `#pragma once` is acceptable in private/internal headers.

### 2.2 Public API Rules
- All public types are opaque: `typedef struct foo *foo_t;`. Never expose
  struct layout in `include/` for a type whose representation may evolve. A
  type may be exposed-layout without size-versioning only as a value-type DTO
  meeting the ADR-0008 criteria: (a) fixed layout, (b) no internal or opaque
  handles as fields, (c) no inline array whose length may evolve without
  per-instance `size`/`abi_version` (documented padding and arrays bounded by
  named public invariants are fixed-shape fields), (d) documented as a stable
  DTO.
- Every public struct that carries parameters, configuration, vtables, or
  status snapshots is size-versioned: begins with `uint32_t size` and
  `uint32_t abi_version`, followed by fields, followed by `void *reserved[8]`.
- Creation structs are named `*_params_t` (the arguments to a module's
  `*_create` call) and sub-constructor tuning bundles are named `*_config_t`
  (the argument bundle of a vtable constructor or a strategy selector, such
  as a sim model init or a delivery strategy). Both are consumed at
  construction. Retention is a per-struct documented fact, never a name
  inference: the struct's own doc comment states whether the callee keeps
  the struct (or an image of it) after the call or borrows it for the call
  only. A handle-creation struct is always `*_params_t`, never
  `*_config_t`.
- Bounds in config fields name their enforcement object. `_cap` and
  `_capacity` are one scheme, two spellings: the size of a fixed
  allocation (byte buffer, frame queue, event ring, pre-allocated
  task-node pool) or a single operation's work budget
  (`out_drain_cap` caps one write call); the unit (bytes, frames,
  records, nodes) is stated in the field's doc. `_max` bounds a
  quantity that would otherwise grow or accumulate: simultaneous
  connections or sessions, tracked file descriptors, payload size, a
  buffer's grow ceiling, reconnect attempts, backoff ceilings, time
  gaps. `_min` is its floor (`min_connections`,
  `split_min_dwell_ms`). `_threshold` is a level that triggers an
  action (`split_threshold`/`merge_threshold`), not a storage bound.
  `_budget` is a time bound in microseconds
  (`delivery_wait_budget_us`). Paired `min_x`/`max_x` fields bound a
  spatial extent. `cap` as a buffer-length function parameter
  (`char *buf, size_t cap`) is the snprintf idiom, not a field
  scheme.
- Dimension and count fields state exact quantities, never limits.
  `_size` is a dimension fixed at create (a handler table's slot
  count, a cell's edge length). `_count` is a literal count: a config
  dimension (array length, hash bucket count, worker pool size) or a
  status count. `_high_water`/`_low_water` pairs are byte thresholds
  at which net queue flow pauses, rejects, resumes, or drains. Status
  snapshots name live gauges and accumulators with state suffixes
  (`_current`, `_total`) and observed peaks or extrema as
  `_high_watermark` or `_min`/`_max` (`rtt_min_ms`/`rtt_max_ms`);
  whether a `_min`/`_max` field is a config bound or an observation
  follows from the struct it lives in (params/config vs status).
- Accessors drop the `get_` prefix: `foo_count`, `foo_level`, not
  `foo_get_count`. The bare noun reads as the property being read. Predicates
  keep a verb (`foo_has`, `foo_exists`, `foo_enabled`) because they answer a
  yes/no question rather than return a value.
- Ancillary public types use the full module name, matching the primary
  handle: `kith_logger_level_t` (not `kith_log_level_t`),
  `kith_metrics_label_t` (not `kith_metric_label_t`),
  `kith_proto_flag_t` (not `kith_msg_flag_t`). The module prefix is the
  directory/library name, not an abbreviation of it.
- Every public function is marked `KITH_API` (visibility default).
- Every internal function is marked `KITH_LOCAL` (visibility hidden) or
  declared `static`.
- Public functions pass structs by pointer, never by value.
- Public enums use a fixed underlying type: `enum foo : unsigned int { ... };`
  (prevents ABI-breaking width changes).
- Public functions return `int` (0 = success, negative = error code) or return a
  handle (`foo_t *`, NULL on failure). Never return a bare bool for operations
  that can fail for multiple reasons.
- Error codes are defined in `include/kith/types.h` as an enum with a fixed
  underlying type.
- Public function signatures evolve additively (new functions) or via a major
  ABI bump. A signature change is always ABI-breaking; size-versioning handles
  struct evolution, not signature evolution.

### 2.3 Memory Ownership
- The allocator that creates a resource destroys it. If a function returns a
  pointer, the doc comment says who owns it: `@ownership caller` or
  `@ownership callee`.
- Public API functions that allocate take an optional `const kith_allocator_t *`
  parameter before the out-parameter. If NULL, the default allocator
  (malloc/free) is used. Each handle stores the allocator it was created with
  and routes every allocation it performs over its lifetime — including
  teardown — through that one instance.
- Never use `alloca` or variable-length arrays in public API paths.
- Pair every `create`/`init` with a `destroy`/`shutdown`. Names are consistent:
  `foo_create` / `foo_destroy`, never `foo_new` / `foo_free` or `foo_init` /
  `foo_cleanup` (pick one pair and use it everywhere).

### 2.4 Error Handling
- Return `int` error codes. 0 is success. Negative values are errors.
- Error codes are small negative integers defined in the public error enum.
- Constructor failures are reported through the `int` return and the `out_`
  handle: `0` stores the new handle, a negative code stores `NULL`. The
  negative codes a function can return are enumerated in its `@return` doc —
  that enumeration is the error contract. The public API has no last-error
  query and no errno-based error reporting.
- Never ignore a return value from a function marked `[[nodiscard]]`.
- Use `[[nodiscard]]` on all public functions that return error codes or
  allocated resources.
- Out-parameters (for returning additional values) are last, named with `out_`
  prefix: `foo_get_value(foo_t *, int *out_value)`.
- The error enum is extensible: games register custom error codes in a
  game-specific range (10000+).

### 2.5 Thread Safety
- Every public function's doc comment includes `@thread_safety` indicating
  whether it is safe to call concurrently: `@thread_safety safe`,
  `@thread_safety unsafe`, or `@thread_safety safe-if (condition)`.
- Public types that are internally synchronized are marked in their doc comment.
  Types that are not synchronized must be protected by the caller.
- The reactor hot path never blocks on Python and never runs game or
  handler logic inline (ADR-0004). Python handlers are dispatched on a worker
  pool: size 1 under the GIL, size N under free-threaded Python. The pool
  size is a config tunable. One bounded exception: a server run loop on the
  interpreter's main thread may register a synchronous poll observer that
  re-enters Python once per tick solely to pump pending signals; it never
  carries game logic (ADR-0004).

### 2.6 Function Size
- Maximum 80 lines, 60 statements, 12 branches (enforced by clang-tidy
  `readability-function-size`).
- If a function exceeds these, extract a helper. Helpers are `static` with a
  descriptive name (no `_` prefix convention).
- No function has more than 8 parameters. If more are needed, pass a config
  struct.

### 2.7 Include Hygiene
- Public headers use traditional include guards: `#ifndef KITH_MODULE_H` /
  `#define KITH_MODULE_H` / `#endif`. Private headers may use `#pragma once`.
- Cross-module includes use public entry headers only: `#include "kith/net.h"`,
  not `#include "kith/net/internal/conn.h"`.
- Include order: C standard, system/third-party, project (enforced by
  clang-format `IncludeBlocks: Regroup`).
- No circular includes. `misc-header-include-cycle` enforces this.
- System includes are restricted to a whitelist
  (`portability-restrict-system-includes`).

### 2.8 File Structure
Every C source and header file follows the same top-to-bottom shape so a
reader builds navigation intuition across modules.

**Headers (`.h`):**
1. Include guard — traditional `#ifndef KITH_MODULE_H` / `#define` /
   `#endif` for public headers; `#pragma once` is acceptable for private
   headers (§2.7).
2. Includes, grouped per §2.7 (C standard → system/third-party → project);
   `clang-format` enforces the grouping.
3. File-level header comment (§1.7) stating what the module exposes.
4. Forward declarations and `typedef`s (opaque handle typedefs first, then
   ancillary public types).
5. Struct/union/enum definitions — only value-type DTOs expose layout
   (§2.2); opaque types stay forward-declared.
6. Function declarations, in the order they appear in the module's
   narrative (create/destroy/lifecycle first, then accessors, then
   operations).

**Source files (`.c`):**
1. File-level header comment (§1.7) stating what the file contains and how
   it relates to its siblings.
2. Includes, grouped per §2.7.
3. File-scope macros and named constants.
4. Forward declarations and `typedef`s (including `static` function
   prototypes when a helper is used before its definition).
5. Internal struct/union/enum definitions.
6. `static` helper functions, defined before use or forward-declared above.
7. Public (`KITH_API`) and module-local (`KITH_LOCAL`) functions, in the
   order matching the public header.

C requires a function to be declared before use, so helpers precede the
functions that call them; do not forward-declare a static merely to invert
that order. A blank line separates each section; a section with no members
is omitted rather than left as an empty placeholder.

### 2.9 Doc Comments
Every public C function, struct, enum, and typedef has a `/** ... */`
Doxygen block. The block opens with `/**`, continues each line with ` * `,
and closes with ` */`. Content and voice follow §1.7; this section fixes
only the format.

**Module block.** Every public header joins its library's Doxygen
group. The library's primary header carries

    /**
     * @defgroup kith_<library> <Title>
     * @{
     */

after the file-level header comment (§1.7) and any borrowed forward
declarations, and the closing

    /** @} */

precedes the include guard's `#endif`. A secondary header of a
multi-header library joins the same group with a titleless
`@addtogroup kith_<library> @{` block instead of defining a second
group; exactly one header defines each group name. A nested group
declares `@ingroup kith_<group>` inside its own `@defgroup` block.

- **Summary** — the first line, one sentence, imperative or declarative,
  ending with a period. States what the symbol is or does at a glance.
- **Body** (when needed) — the expanded contract: invariants, the
  lifecycle the function participates in, the relationship to borrowed
  handles. The body either continues on the summary line or follows a
  blank line.
- **`@param name description`** — one per parameter, in declaration order.
  Continuation lines align under the description column. Reference a
  parameter inline with `@p name`.
- **`@return`** — the return value and its meaning. For `int`-returning
  functions, enumerate the negative error codes.
- **`@thread_safety`** — required on every public function (§2.5): `safe`,
  `unsafe`, or `safe-if (condition)`.
- **`@ownership`** — required when the function returns or transfers a
  pointer: `@ownership caller` or `@ownership callee` (§2.3).
- **`@note` / `@warning`** — optional, for a non-obvious constraint or a
  hazard the caller must avoid.

Tag order: summary → body → `@param` → `@return` → `@thread_safety` →
`@ownership` → `@note`/`@warning`. A function with no parameters or no
meaningful return omits the tag rather than writing an empty one.
`tools/check_public_api.py` fails a public function whose doc comment is
missing `@ownership` or `@thread_safety`.

## 3. Python Code Standards

### 3.1 Style
- `ruff` is the formatter and linter. Configuration in `pyproject.toml`.
- Line length: 100 (matching C).
- Type hints are required on all public functions. `mypy --strict` runs in CI.
- No `Any` type without a `# type: ignore[reason]` comment explaining why.

### 3.2 Module Structure
- The `kith` package is the public API. Internal modules are prefixed with `_`
  (`kith._bridge`, `kith._generated`).
- Public API functions never expose ctypes types directly. They wrap them in
  Python types and convert at the boundary.
- Every public Python function has a docstring (Google style).

### 3.3 Error Handling
- C error codes are translated to Python exceptions at the boundary.
- The exception hierarchy: `KithError` (base), `KithConfigError`,
  `KithNetworkError`, `KithProtocolError`, `KithStateError`,
  `KithNotFoundError`, `KithResponseOverflowError`.
- Never let a C error code propagate as a raw integer to user code.

### 3.4 Free-threaded Python
- The package is tested under both standard and free-threaded (`t`) builds.
- Public Python API is thread-safe by default; documented exceptions carry
  `@thread_safety unsafe`.
- The handler worker pool size is a config tunable (`python_worker_count`).

### 3.5 File Structure
Every Python module follows the same top-to-bottom shape. The order is
enforced by review; `ruff` enforces import grouping and sorting within the
import block.

1. Module docstring (the file-level header, §1.7): what the module exposes
   and the role it plays.
2. `from __future__ import annotations`.
3. Standard library imports, sorted.
4. Third-party imports, sorted.
5. `kith` imports (own package), sorted.
6. `__all__` when the module is part of the public API; private (`_`-prefixed)
   modules omit it.
7. Module-level constants and type aliases.
8. Classes — public first, then private/`_`-prefixed.
9. Functions — public first, then private/`_`-prefixed.
10. `if __name__ == "__main__":` block, entry-point modules only.

A blank line separates each section; a section with no members is omitted.

### 3.6 Docstrings
Every public Python function, method, and class has a Google-style
docstring. The summary is the first line; sections follow, each introduced
by its header word followed by a colon. Content and voice follow §1.7;
this section fixes only the format.

- **Summary** — one sentence, ending with a period.
- **Body** (when needed) — a blank line after the summary, then the
  expanded contract.
- **`Args:`** — one `name: description` line per parameter; a description
  spanning lines indents under the name. Omit when the function takes no
  parameters.
- **`Returns:`** — the return value and its meaning. Omit when the function
  returns `None` and that is obvious from the signature.
- **`Raises:`** — `ExcType: when...` per exception. Omit when the function
  raises nothing beyond pass-through.
- **`Thread safety:`** — present when the function has a non-obvious
  concurrency contract; the default (thread-safe, §3.4) needs no section.
- **`Ownership:`** — present when the function returns a resource with an
  ownership contract: one the caller must close or release, or a borrowed
  view that must not be closed.

Section order: summary → body → `Args:` → `Returns:` → `Raises:` →
`Thread safety:` → `Ownership:`. A section with nothing to say is omitted.

## 4. Testing

### 4.1 Test Tiers

| Tier | Scope | Runs when | Framework |
|---|---|---|---|
| Unit (C) | Single function/module | Every commit | custom harness (standalone ctest executables) |
| Unit (Python) | Single function/class | Every commit | pytest |
| Integration | Multi-module interaction | Every commit | pytest + C test binary |
| Smoke | Build + bootstrap + shutdown | Every commit | load-harness smoke profiles + integration suite; release-day wheel smoke (scripts/wheel_smoke.py) |
| ABI | Symbol/struct layout diff | Every build (gate runs it unconditionally) | abidiff |
| Agentic | Closed-loop scenario (setup→act→assert→diagnose→report) | Every commit | tools/agent (AHC + scenarios) |
| Replay | Deterministic sim replay/hash regression | Every commit (sim changes) | tools/replay.py |
| Performance | Latency/throughput regression | Manual runs before each release on a dedicated reference host whose envelope is recorded (docs/guides/scaling_checklist.md) | custom benchmark + perf |
| Stress | Scaling-gate canary on hosted runners; the certified gates run on a dedicated reference host whose envelope is recorded (docs/guides/scaling_checklist.md) | Weekly (canary); manual certified runs before each release | stress harness |
| Fuzz | Protocol decoder, control HTTP parser | Weekly | libFuzzer |

### 4.2 Coverage
- C core: ≥ 85% line coverage (lcov / llvm-cov).
- Python framework: ≥ 90% line coverage (coverage.py).
- Coverage is reported but not gated (gating encourages gaming the
  metric). Critical paths have explicit tests.

### 4.3 Test Naming
- C: `test_<module>_<scenario>.c` in `tests/c/`.
- Python: `test_<module>[_<scenario>].py` in `tests/python/`.
- Integration: `test_<feature>_<scenario>.py` in `tests/integration/`.
- Agentic: scenario modules in `tools/agent/scenarios/`, driven through
  `tools/agent/scenario.py`; their tests live as `test_scenario*.py` in
  `tests/python/` and `test_scenarios_*.py` in `tests/integration/`.

### 4.4 Scaling Gates (numeric, enforced)

Before claiming the N² ceiling is broken, the framework must pass the
two-tier scaling gates (ADR-0020):

- **1000 dense actors (embedded, stress gate)**: `session_ok >= 99%`,
  `selected_clients >= 95%`, `bootstrap_ms_p95 < 3000`, no crash or
  corruption. The delivery-fidelity metrics
  (`move_missing_ratio_certified`, `mm_event`, `continuity_flicker`) are
  reported, not thresholded — the embedded single-cell pileup is the
  adversarial case the embedded topology is not the fidelity claim for.
- **2000 distributed actors (fidelity gate)**: the full thresholds per
  instance — `session_ok >= 99%`, `selected_clients >= 95%`,
  `move_missing_ratio_certified < 10%`, `bootstrap_ms_p95 < 3000`, no
  continuity flicker — plus no single instance the fixed publish
  bottleneck under normal spread. The gate runs under free-threaded
  Python (`python3.14t`, GIL disabled) on the native movement-apply
  path — the certified configuration, the only one under which the
  `move_missing_ratio_certified < 10%` bar is reachable. A GIL run (or
  a Python-apply run) is a regression baseline, not a gating
  configuration.

Reaching 900 green actors is not sufficient, and a passing dense-1000
stress gate alone does not claim the N² ceiling is broken; the
distributed-2000 fidelity gate is the topology that claim is made for.

The per-operation budgets, the per-tick capacity model, the measured
apply-path regime ladder, and the certified invocation shape live in
`docs/architecture/performance_budgets.md` and
`docs/guides/scaling_checklist.md`.

## 5. Git Workflow

### 5.0 Push Authorization

Automated tooling must never push to the remote without explicit human approval.
Commits are staged, formatted, tested, and verified locally by tools (including
AI agents), but `git push` is always executed by a human. No exceptions — not
for trivial fixes, not for hotfixes, not for post-review cleanup.

This rule exists because pushes are irreversible and externally visible. A
machine cannot judge whether the commit author is ready for the change to
leave the local repo. The human owns the decision to publish.

### 5.0a The verification gate (single source of truth)

`scripts/verify.sh` is the single gate: it runs lint (pre-commit, license
compliance, mypy) and build (configure, compile, ctest, pytest, check-all,
the ABI diff, checksec) in one invocation, so a change cannot pass one
check and fail another. The gate is verify-only: it never rewrites files.
Run `scripts/verify.sh` (or `scripts/verify.sh <stage>`) and get all-green
before **every** commit. When a gate reports a formatting violation, fix
it with `cmake --build --target format` (or `scripts/format.sh`), then
re-run the gate. `pre-commit` alone is insufficient (it omits clang-tidy,
mypy-on-push, license compliance, the build, and the ABI/hardening
checks).

### 5.1 Branch Strategy
- `main` is always releasable. The maintainer commits directly to
  `main`, gated by §5.0a per commit; no merge commits on `main`.
- External contributions land as squash-merged PRs from
  `feat/<short-description>` / `fix/<short-description>` branches.

### 5.2 Commit Messages

Follows Conventional Commits 1.0.0, interpreted through the Angular
Commit Message Guidelines and Tim Pope's formatting canon. The full
annotated examples, the canon notes, and the revert and breaking-change
mechanics live in `docs/guides/commit_messages.md`. Enforcement is via
the `conventional-pre-commit` hook on the `commit-msg` stage; what the
hook enforces is law, what this section describes is the rule.

#### 5.2.1 Structure

    <type>(<scope>): <summary>
                      <- blank line (mandatory if body or footer follows)
    <body>             <- wrapped at 72 chars; paragraphs separated by blank lines
                      <- blank line (mandatory if footer follows)
    <footer>

The header is mandatory. The body is mandatory for all types except
`docs` and `chore`. The footer is optional.

#### 5.2.2 Type (mandatory)

| Type | Use for | SemVer impact |
|---|---|---|
| `feat` | A new feature | minor |
| `fix` | A bug fix | patch |
| `perf` | A code change that improves performance | patch |
| `refactor` | A code change that neither fixes a bug nor adds a feature | none |
| `test` | Adding missing tests or correcting existing tests | none |
| `docs` | Documentation only changes | none |
| `build` | Build system, dependencies, CMake, toolchain | none |
| `ci` | CI configuration, workflows, runners | none |
| `chore` | Repo maintenance not covered above | none |
| `revert` | Reverting a prior commit (§5.2.7) | varies |

`style` is deliberately excluded: formatting is applied via
`cmake --build --target format` (or `scripts/format.sh`) before
committing and never forms a separate commit.

#### 5.2.3 Scope (mandatory; `--force-scope` enforced by the hook)

The scope is the module or area affected, matching the repository
layout. The allowlist:

`util`, `config`, `logger`, `metrics`, `net`, `proto`, `reactor`, `worker`,
`state`, `db`, `aoi`, `sim`, `fabric`, `gateway`, `coord`, `control`,
`client`, `server`, `examples`, `framework`, `tools`

- `framework` — cross-cutting changes spanning multiple modules or
  repo-wide.
- `worker` — the handler-dispatch worker pool (`src/worker/`).
- `examples` — the example programs and shared helpers under
  `examples/`.
- `tools` — the `tools/` checkers and generators.
- Empty scope is not permitted. For subsystem-specific changes use the
  parent module scope, not a sub-scope (`refactor(gateway): ...`, not
  `refactor(gateway-view): ...`), so the allowlist stays stable as
  subsystems evolve.

#### 5.2.4 Summary

- Imperative, present tense: "add", not "added" or "adds".
- Lowercase first letter, no trailing period, soft limit 50 characters
  (hard 72, so the header fits one line in `git log --oneline` and in
  GitHub's truncated views).
- Describes WHAT changed, not WHY (the body covers WHY), and never its
  position in a plan or workstream (§5.2.13).

Good: `feat(sim): register movement models via vtable`
Bad:  `Added AOI circle query.` (capitalized, period, past tense, no type/scope)

#### 5.2.5 Body

The body explains WHY the change was made, not WHAT — the diff already
shows WHAT. It must contain at least one paragraph of substantive prose
stating the motivation and, when the impact is non-obvious, the
behavioral difference for a caller or reader.
`tools/check_commit_messages.py` rejects a body that is a file manifest.

- Imperative, present tense, wrapped at 72 characters, paragraphs
  separated by blank lines.
- Bullets use `-` with a hanging indent.
- No aspirational language (§1.2 applies to commit messages too).
- No plan, phase, or roadmap references (§5.2.13).

#### 5.2.6 Footer

Footers follow the git trailer convention: a token, `:` or `#`, a
space, then a value; tokens use `-` for internal spaces.

- `BREAKING CHANGE: <summary>` plus migration detail, or `!` after the
  type/scope (`feat(sim)!: ...`).
- `Fixes #<n>` / `Refs #<n>` for tracker references; `Refs ADR-NNNN` for
  records (ADRs are repo documentation).
- `Signed-off-by: <name> <email>` — the DCO sign-off, always the last
  footer line (§5.2.8).

#### 5.2.7 Revert commits

    revert: <original header line>

    This reverts commit <full 64-char SHA>.

    <reason for reverting>

The SHA is the full 64-character hash, not an abbreviation, and the
body must contain a clear reason for the revert.

#### 5.2.8 DCO (Developer Certificate of Origin) and Commit Signing

Every commit is signed off (DCO) and cryptographically signed (SSH, not
GPG). The sign-off certifies the Developer Certificate of Origin
(https://developercertificate.org), including the right to submit the
work under the project's Apache-2.0 license. Sign-off is passed
explicitly on every commit (`git commit -s`): git has no setting that
enables `--signoff` by default (gitfaq(7)). The signing configuration (`gpg.format ssh`, the
Ed25519 signing key, `commit.gpgsign true`) is required of every clone. An
unsigned commit does not land; GitHub verifies SSH-signed commits
against the key registered to the author's account.

#### 5.2.9 Atomic commits

One logical change per commit; split whenever a description needs "and
also". A commit's diff should be reviewable in under 10 minutes
(roughly ≤500 lines excluding generated files); larger changes land as
a commit series, each buildable and passing tests.

#### 5.2.10 Generated files

Generated artifacts (ctypes bindings in `python/kith/_generated/`, ABI
snapshots in `tests/abi/`, `.map` version scripts) are committed in the
same commit as the source change that generated them — never as a
follow-up "regenerate" commit. `check_ctypes_drift.py` fails the commit
if they diverge.

#### 5.2.11 Buildability

The repository builds and all tests pass after every commit. If a
change is too large to land as a series of buildable commits, it
belongs on a feature branch.

#### 5.2.12 Examples

The full annotated example set — features, breaking changes, reverts,
and the bad-pattern catalog — lives in
`docs/guides/commit_messages.md`.

#### 5.2.13 No plan, phase, or roadmap references

Commit messages and code comments describe the change objectively and
independently. They never reference plans, phases, roadmaps, sprints,
workstreams, task numbers, or any external tracking document. If the
body references a deliverable or architectural goal, that goal must be
defined in the repository's own documentation (an ADR, an architecture
doc, a guide) — `Refs ADR-NNNN` is acceptable; `Refs Phase 1 Task 3` is
not. The git history reads as a coherent, linear development by someone
who knew what to build and why, in order.

### 5.3 Pull Requests
- Maintainer commits: direct to `main` per §5.1. External contributions:
  one PR per feature or fix, atomic.
- PR description template: what changed, why, how to test, breaking changes.
- Review required.
- Squash-merge to `main`.

## 6. Dependencies

### 6.1 External Dependencies
| Dependency | Purpose | Acceptable? |
|---|---|---|
| `pthread` | Threading | Yes (system) |
| `liburing` | io_uring async I/O (Linux) | Yes (system, pkg-config) |
| `hiredis` | Redis client | Yes (system, pkg-config) |
| `libpq` | Postgres client | Yes (system, pkg-config) |

- No header-only vendored libraries unless trivial (≤ 500 lines, like `jsmn`).
- No C++ dependencies. The C core is pure C23.
- Python dependencies are listed in `pyproject.toml` and pinned via `uv.lock`.
- New dependencies require an ADR.

### 6.2 Version Pinning
- C system deps: versioned external libraries resolve via `pkg_check_modules`
  in the `cmake/lib_*.cmake` module files (the CMake-bundled Threads port
  resolves via `find_package`); minimum versions stated where one is enforced
  (e.g. `liburing>=2.7`).
- Python deps: exact versions in `uv.lock` (`exclude-newer` for reproducibility).
- Pre-commit hook revs pinned to specific tags.

## 7. Documentation

### 7.1 API Reference
- Every public C function and struct has a Doxygen doc comment following
  the format in §2.9 (required `@param`, `@return`, `@thread_safety`,
  `@ownership` tags).
- API reference generated by Doxygen → published as static HTML (GitHub Pages).

### 7.2 Architecture Decision Records
- Every significant architectural decision has an ADR in
  `docs/architecture/adr/NNNN-title.md`. ADRs are numbered sequentially
  without gaps; the additive-gate record always holds the set's last
  number — a new record takes the number ahead of it, and the gate record
  renumbers to the new last number — and an assigned number never changes
  otherwise. Cross-references cite the number inline;
  trailing citation lists are not house style. Code comments and
  docstrings never cite records (§1.7); the citation surfaces are
  `docs/**`, this file, the checkers that validate the records, and
  commit footers.
- Record numbers are immutable from v1.0.0: the set evolves only by
  supersession or explicit amendment — never silently.
- A record states the decision and its rationale at the contract level:
  what is bound, and why. A record body carries no code identifiers, no
  CLI flag spellings, and no client/actor/session/instance counts: a
  contract's concrete surface is owned by its public header, which the
  record cites by docs path when the pointer earns its place.
  Implementation detail lives in headers, guides, and the architecture
  docs, where it evolves without touching the record. Tokens that are
  themselves the decision — a counter's name, a gate metric's name, an
  algorithm's name — stay, as do wire sizes and the shipped default
  budget.
- Record statuses: `Accepted` — binding as written; every founding
  record reads exactly this. `Superseded by ADR-NNNN` —
  post-v1.0.0 only: a successor decision replaces the record, which
  remains for history. Amendments are inline conditional statements in
  prose — the governing passage names the record that owns the case —
  never status markers.
- House style: the same skeleton in every record — `# ADR-NNNN: Title`,
  `**Status:** Accepted`, `## Context`, `## Decision`, `## Consequences`
  — impersonal voice, consequences stating positive and negative
  effects, no plan or workstream vocabulary.
  `tools/check_adr_style.py` enforces the mechanical parts; comparable
  length across records is review-enforced — condense wording, never
  binding content.

### 7.3 Guides
- `docs/guides/getting_started.md` — quickstart.
- `docs/guides/commit_messages.md` — commit message canon and examples.
- `docs/guides/writing_extensions.md` — Python extension authoring.
- `docs/guides/writing_c_sim_models.md` — C sim model authoring.
- `docs/guides/scaling_checklist.md` — running and interpreting the scaling gates.
- `docs/guides/external_code_vetting.md` — the external code vetting protocol.
- `docs/guides/agentic_headless_client.md` — agentic testing end-to-end.
- `docs/guides/replay_format.md` — the replay record format reference.
- `docs/guides/wire_protocol.md` — the v1 wire format for hand-written
  peers: frame header, correlation trailer, the replication record, and
  the delivery cadence.
- `docs/guides/publish_choreography.md` — the per-tick publish
  choreography: the artifact-to-fabric pipeline, the step/publish/bump
  contract, and the dirty-cell flush.
- `docs/guides/operations.md` — the operator's runbook: first-boot
  delivery checks, capacity and failure-class reading, and recovery
  recipes per plane.

## 8. Review Checklist
Before requesting review, verify:
- [ ] `scripts/verify.sh` is all-green (the single gate: lint + build +
      ABI diff + checksec; see §5.0a). This subsumes the items
      below, which are listed for traceability.
- [ ] Formatters and linters pass: clang-format, clang-tidy, ruff check
      and format, mypy --strict.
- [ ] Structural checkers pass: module layers, runtime planes, ctypes
      drift, comment philosophy (§1.7), forbidden patterns, public API
      contract, layout consistency.
- [ ] New public C functions carry §2.9 doc comments; new public Python
      functions and methods carry §3.6 docstrings.
- [ ] New public types are opaque, or are value-type DTOs meeting the
      criteria in §2.2.
- [ ] No game-specific terms in `src/` or `include/`; no aspirational
      language, TODOs, or authorship markers; comments follow §1.7.
- [ ] New C files follow §2.8; new Python modules follow §3.5; tests
      added for new behavior, named per §4.3.
- [ ] Conventional commit message format; DCO signed (§5.2).
- [ ] If touching `include/`: ABI diff reviewed, `SOVERSION` bumped if
      breaking.
- [ ] If porting logic from an external reference: the vetting protocol
      (§9) is complete.

## 9. External Code Vetting

When a module has an equivalent in an external reference codebase (a
library being replaced, a reference implementation), the code is studied
before reimplementation, then written fresh against this framework's
contracts — never copied, never translated line-by-line. Before the
module is considered complete, it passes the five-dimension vetting
protocol in `docs/guides/external_code_vetting.md`: naming consistency,
architectural conformance, implementation quality, forbidden
anti-patterns, and cross-module consistency. The dimensions go beyond
what mechanical checkers catch; a module that feels ported is not done.

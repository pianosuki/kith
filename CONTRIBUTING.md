# Contributing to kith

This document covers the practical mechanics of getting a change landed.
The full coding standards, philosophy, and review checklist live in
[`AGENTS.md`](AGENTS.md) — that file
is the single source of truth for how code is written and reviewed in this
repository. Read it before opening a change.

kith has a single maintainer: changes are reviewed and merged by that one
person, and issue triage follows the same capacity.

## 1. Prerequisites

| Tool | Version |
|---|---|
| Clang | 22+ |
| CMake | 4.4+ |
| Ninja | 1.13+ |
| Python | 3.14+ |
| uv | 0.5+ |
| pre-commit | 3.0+ |

The C core targets C23 and Linux only (io_uring reactor). Other platforms
are not supported; on them the reactor creation path reports KITH_ENOSYS.

## 2. Initial setup

Clone, configure git, and bootstrap the development environment:

```sh
git clone <repo-url> kith
cd kith
uv sync
pre-commit install --hook-type pre-commit --hook-type commit-msg --hook-type pre-push
```

Commit signing is SSH (not GPG). Configure the identity and the signing
key that git commits with:

```sh
git config user.name "Your Name"
git config user.email "you@example.com"
git config commit.gpgsign true
git config gpg.format ssh
git config user.signingkey ~/.ssh/id_ed25519.pub
```

The DCO sign-off
line is added by the `dco-signoff` commit-msg hook that the pre-commit step
installs. An unsigned commit does not land.

## 3. Building, testing, and the verification gate

`scripts/verify.sh` is the single gate: it runs exactly what CI runs and is
the same script CI invokes, so a change that passes locally passes CI. The
gate is **verify-only** — it never rewrites files. Run it before every commit
and before every push:

```sh
# Run every gate: lint (pre-commit verify + mypy), commits (conventional +
# DCO + signing), build (configure, compile, ctest, check-all, ABI diff,
# checksec), and free-threaded (the Python suite under python3.14t).
./scripts/verify.sh

# Or run a single stage:
./scripts/verify.sh lint     # pre-commit (verify-only) + mypy
./scripts/verify.sh build    # configure, build, ctest, check-all, ABI, checksec
./scripts/verify.sh commits  # conventional + DCO + signing on main..HEAD
./scripts/verify.sh free-threaded  # the Python suite under python3.14t
```

When a gate reports a formatting violation, fix it with the single format
command, then re-run the gate:

```sh
# Apply every formatter in place (clang-format, ruff, trivial
# whitespace/line-ending/BOM fixers). Idempotent; never fails.
cmake --build --target format
# or, without a configured build directory:
./scripts/format.sh
```

The individual commands the gate wraps are shown below for reference; prefer
`scripts/verify.sh`:

```sh
# Configure + build (Ninja, debug preset).
cmake --preset debug
cmake --build build/debug

# Run the C test suite.
ctest --test-dir build/debug --output-on-failure

# Run the full pre-commit suite across the tree (verify-only: reports
# violations, never fixes).
pre-commit run --all-files
```

The build carries a small set of CMake knobs, settable as cache
variables at configure time and preset-driven in practice:
`CONTROL_PLANE_ENABLED` (default `ON`; `OFF` leaves the control-plane
library out of the build entirely), `KITH_USE_MOLD` (default `OFF`;
links with the mold linker when it is installed),
`KITH_ENABLE_FORTIFY` (on in optimized presets, off in the debug,
coverage, and sanitizer presets where unoptimized code cannot use it),
and the `KITH_ENABLE_ASAN` / `KITH_ENABLE_UBSAN` / `KITH_ENABLE_TSAN` /
`KITH_ENABLE_COVERAGE` / `KITH_ENABLE_FUZZ` family (all `OFF`; the
`asan`, `asan-ubsan`, `tsan`, `coverage`, and `fuzz` presets each turn
on their own). Knobs combine freely with a preset:

```sh
cmake --preset release -DKITH_USE_MOLD=ON
```

The repository must build and `ctest` must pass after every commit. There are
no "broken window" commits; if a change is too large to land as a series of
buildable commits, it goes on a feature branch.

### Running the database integration tests

`tests/integration/test_postgres_store.py` runs against a live Postgres
shaped like the CI service container: role `kith`, password `kith`,
database `kith_example`. Without a usable one the tests skip cleanly
locally; in CI they never skip. Two supported ways to get the service:

- **Dev Containers / Codespaces:** reopen the repository in the dev
  container — its compose stack starts Postgres reachable as host `db`
  and sets the `KITH_PG_*` variables automatically. Nothing else to do.
- **Plain Docker:** run `./scripts/dev-postgres.sh`, then paste the
  `KITH_PG_*` exports it prints into the shell before invoking pytest.
  The exports are required: without them the client defaults to an empty
  password and authentication fails.

If a Postgres instance already runs on 5432, put the dev instance
elsewhere: `KITH_DEV_PG_PORT=55432 ./scripts/dev-postgres.sh`.

## 4. Code style

- **C:** `clang-format` (config in `.clang-format`) and `clang-tidy` (config in
  `.clang-tidy`) are the source of truth. C23, 4-space indent, 100-column
  limit. See `AGENTS.md` §2 for the full C standard (public API rules, memory
  ownership, error handling, thread safety, function size, include hygiene).
- **Python:** `ruff` (format + lint) and `mypy --strict`. Config in
  `pyproject.toml`. 100-column limit. See `AGENTS.md` §3.
- **Comments and commit messages:** no aspirational language ("TODO", "will
  eventually"), no personal voice ("I", "we"), no authorship markers, no
  references to external plans, phases, roadmaps, or task numbers. See
  `AGENTS.md` §1 and §5.2.13.

Formatting is never applied automatically by the gate. When a check reports a
violation, run `cmake --build --target format` (or `./scripts/format.sh`) to
apply every formatter in place, then re-run `./scripts/verify.sh`. A
formatting change that accompanies a functional change is part of that
change's commit, never a standalone `style` commit.

The checkers in `tools/` enforce the architectural invariants (module layers,
runtime planes, public API surface, comment philosophy, forbidden patterns,
ctypes binding drift). They run as pre-commit hooks (verify-only) and as
`check-*` build targets; see `AGENTS.md` §8 for the full review checklist.

## 5. Commit messages

Follow Conventional Commits 1.0.0 with the Angular interpretation and Tim Pope
formatting, enforced by the `conventional-pre-commit` hook on `commit-msg`.
Body content (present when the type requires it, never a bare file list) is
enforced by `tools/check_commit_messages.py`. The full guide is in `AGENTS.md`
§5.2; the essentials:

```
type(scope): imperative summary ≤50 chars

Body wrapped at 72 columns, explaining WHY (the diff shows WHAT): the
motivation and, where the impact is non-obvious, the before/after
behavior. A body that only re-lists the files it touches is rejected.

Signed-off-by: Example Name <email@example.com>
```

- **Type** is mandatory and must be on the allowlist (`feat`, `fix`, `perf`,
  `refactor`, `test`, `docs`, `build`, `ci`, `chore`, `revert`).
- **Scope** is mandatory (`--force-scope`): a module name from the allowlist in
  `AGENTS.md` §5.2.3, or `framework` for cross-cutting changes, or `tools` for
  `tools/` changes, or `examples` for the example programs under `examples/`.
- **Body** is mandatory for every type except `docs` and `chore`, and must
  contain substantive prose — a file manifest is not a body (AGENTS.md
  §5.2.5).
- **DCO sign-off** is mandatory on every commit. The sign-off certifies the
  [Developer Certificate of Origin](https://developercertificate.org),
  including the right to submit the work under the project's Apache-2.0
  license. Use `git commit --signoff`.
- **Signing** is mandatory (SSH). An unsigned commit is rejected.
- **Atomic:** one logical change per commit. A change that adds a feature AND
  fixes an unrelated bug AND reformats a neighbor is three commits.
- **No plan/phase/roadmap/task references** in the summary, body, or code
  comments. Describe the technical change objectively and independently.

## 6. Branch model

The maintainer commits directly to `main` (AGENTS.md §5.1); `main` is
always releasable and carries no merge commits. External contributions
land through feature branches (`feat/<desc>`, `fix/<desc>`) as
squash-merge pull requests with required review.

## 7. Generated files

Generated artifacts (ctypes bindings in `python/kith/_generated/`, ABI
snapshots in `tests/abi/`, `.map` version scripts) are committed in the same
commit as the source change that produced them — never in a follow-up
"regenerate" commit. `tools/check_ctypes_drift.py` fails the commit if the
committed bindings diverge from a fresh generation.

## 8. Adding a dependency

New C or Python dependencies require an Architecture Decision Record in
`docs/architecture/adr/` explaining why the dependency is needed and why no
existing option suffices. See `AGENTS.md` §6.1 for the allowed system
dependencies and §7.2 for the ADR format.

## 9. Reporting issues

Public issues are for bugs and feature requests, not security vulnerabilities.
For security-sensitive reports, follow [`SECURITY.md`](SECURITY.md) and report
privately — do not open a public issue.

## 10. Code of conduct

Participation in this project is governed by the
[Contributor Covenant 2.1](CODE_OF_CONDUCT.md). By contributing you agree to
uphold it.

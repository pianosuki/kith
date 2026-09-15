# Security Policy

## Supported versions

kith 1.0.0 is the first stable release. Security fixes apply to the latest
minor release line; there are no backport branches.

| Version | Supported |
|---|---|
| latest minor release (currently `v1.0.0`) | security fixes |

## Reporting a vulnerability

**Report security vulnerabilities privately — do not open a public issue.**

Email: <pianosuki@protonmail.com>

Please include:

* A description of the issue and its potential impact.
* The exact commit SHA (or version) affected.
* Reproduction steps, a proof-of-concept, or a crash trace.
* Any suggested fix or mitigation.

The report receives an acknowledgment within **5 business days**,
including a preliminary assessment and an estimated timeline for a fix or
advisory. If the report is accepted, a coordinated disclosure date is
agreed with the reporter; reporters are credited in the published advisory
unless anonymity is requested.

## Scope

In scope:

* The C core library and its public API surface (`include/`, `src/`).
* The Python bridge and generated ctypes bindings.
* The reactor, protocol path, and network handling (memory safety, protocol
  decoding, connection and session handling).
* Denial-of-service from untrusted network input.
* Build-system issues that lead to insecure compilation (e.g. hardening flags
  being dropped).

Out of scope:

* Authentication and authorization. The framework classifies session
  identity types and carries sessions; it verifies no credentials. Identity
  binding and permission checks are the game's layer (the example wire
  catalog's principal exchange is the shape).
* Vulnerabilities in dependencies (report upstream) — a heads-up is still
  appreciated so the pin can be bumped promptly.
* Issues that require an attacker to already control the host running the
  server (trusted-host threat model).
* Theoretical timing side-channels without a demonstrated remote exploit.

The control plane serves unauthenticated HTTP on loopback by default and
passes route parameters through to game handlers; rebinding it away from
loopback opens a second ingress whose consequences
`docs/architecture/threat_model.md` documents rather than hardens.

The human-readable log sink renders caller-supplied message and field values
verbatim (no newline or ANSI-escape handling; lines truncate silently at 4096
bytes), so a game that logs attacker-derived strings through the fields API
can have its log lines forged or spoofed. The structured JSONL sink escapes
values by construction and is the safe path for attacker-derived data; the
fields API's own `@warning` states the same contract.

## Disclosure

Coordinated disclosure: the fix is developed privately, an advisory is
prepared, and both are published together once a release containing the fix
is available. A CVE is
requested when the issue meets the threshold for a formal advisory.

## Hardening expectations

Production builds are expected to be compiled with the hardening flags defined
in `cmake/hardening.cmake` (full RELRO, stack protector, `-D_FORTIFY_SOURCE=3`,
PIE, and CFI where the toolchain supports it). Secrets in memory are cleared
with `memset_explicit` (not `memset`, which compilers elide). A drop in
hardening caused by a code or build change is treated as a security defect.

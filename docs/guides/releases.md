# Releases

The CMake project version owns the distribution version. Update it together
with the version header, regenerate the ctypes bindings, and add the matching
changelog heading. A compatible runtime or packaging correction increments the
patch version. An additive public feature increments the minor version; a
breaking public API or ABI change requires a major version. Dependency updates
alone do not create a new public feature.

## Native artifacts

`uv build` produces an sdist and a host-platform wheel. The wheel contains
the framework libraries and resolves their native dependencies from the build
host's system ABI. A local Linux wheel is suitable for that host and compatible
deployments; its tag does not promise cross-distribution compatibility.

The release workflow repairs the wheel with auditwheel against the
`manylinux_2_38_x86_64` policy. Repair bundles non-policy dependencies and gives
them private names and package-relative search paths. Framework library names
remain stable for the ctypes bridge. Repair must fail when the selected
toolchain or a dependency requires symbols newer than the policy permits.
Changing the filename or checking only the framework's glibc symbol versions
does not establish compatibility.

Only the repaired wheel enters the release checksums, SBOM, attestations and
registry upload. The artifact carries redistribution notices for the native
dependency closure. The package maintainer owns security updates to these
bundled libraries; upgrading a system package does not update a wheel's private
copy. Source builds continue to use system libraries and their security updates.

The portable wheel uses a release-only PostgreSQL client built from a
SHA-256-pinned upstream archive in a private prefix. TLS through OpenSSL,
password/SCRAM and certificate authentication remain available. LDAP lookup
and GSSAPI authentication are excluded from this wheel configuration; source
installs retain the host libpq's authentication capabilities. Applications
requiring those optional mechanisms use a source build against their configured
system library. This restriction concerns the packaged libpq configuration;
the public KITH SQL and database parameter interfaces remain unchanged.
The 1.0.0 Ubuntu wheel linked the host's full libpq, whose optional
LDAP/GSSAPI facilities could therefore be available. The portable candidate
does not preserve those indirect authentication capabilities; source installation
is the compatibility path for applications requiring them.

The portable Redis client is hiredis 1.5.0, built from a SHA-256-pinned
upstream archive. It includes the current parser overflow/depth and Linux
nonblocking connection fixes. The upstream optional TLS peer-verification API
change does not affect KITH, which uses the non-TLS hiredis async API.
Native source builds continue to exercise the host libraries; the portable
artifact owns its independently pinned native clients.

The notice collector permits only the reviewed libpq, OpenSSL, hiredis and
liburing SONAMEs. An unexpected graft fails the build. Review both its license
and complete redistribution requirements before changing that allowlist;
retaining notices alone does not satisfy every license's source obligations.
The wheel includes actual Ubuntu package copyright files, common-license texts
and the upstream PostgreSQL copyright from the verified archive, with package
versions and source provenance in its manifest and SBOM.

## Verification and publication

Run `scripts/verify.sh` before every commit. Run `scripts/ci-local.sh` against
the final candidate to reproduce the push jobs in Ubuntu containers, including
their provisioning and PostgreSQL service. Check the compiler matrix as well
as standard and free-threaded Python. Scheduled sanitizer, fuzz, coverage and
stress jobs have separate commands in their workflows. Release performance
certification follows `docs/guides/scaling_checklist.md` on the reference host.
Retain the exact tree identity, environment and complete logs with each result.

Build and install the sdist in a clean environment with `uv`, then run
`scripts/wheel_smoke.py` outside the source checkout. Install the repaired wheel
in clean standard and free-threaded environments and run the same smoke. Verify
that the bridge selects the installed package's libraries, reports the package
version, boots a listener and shuts down cleanly. Repeat the wheel smoke on a
different supported distribution without installing the native build dependencies.
Run affected downstream tests with the installed candidate in an isolated
environment; preserve the downstream's inherited environment.

Pushes and publication are human-owned. Before a tag push, review the complete
verification results and the exact commit range. The tag must match the project
version. A tag-triggered run creates a draft GitHub release and subsequently
uploads to PyPI automatically: the draft is not an approval barrier for PyPI.
The registry upload is irreversible. A workflow dispatch exercises build and
smoke jobs without tagging or publishing. Inspect the repaired wheel, notices,
SBOM, checksums and attestations before authorizing the tag push; review the
draft separately before publishing the GitHub release. Never replace an existing
published tag or version to repair an artifact.

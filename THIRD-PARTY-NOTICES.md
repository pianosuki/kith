# Third-party notices

The framework links three direct third-party C libraries. Source builds and
raw host wheels resolve these libraries from the host. Portable release wheels
bundle their non-policy dependency closure, including transitive dependencies,
through auditwheel. The exact bundled objects, hashes, dependency edges and
build-package versions appear in the release's CycloneDX SBOM.

Portable wheels retain each bundled Ubuntu package's complete copyright file,
its referenced common-license texts, and a manifest under the distribution's
`.dist-info/licenses/native/` directory. The source-built liburing dependency
uses the MIT text below; the release-only PostgreSQL client retains the
`COPYRIGHT` file from its verified upstream source archive. The release-only
hiredis 1.5.0 client retains its verified upstream `COPYING` file. Missing dependency ownership or redistribution notices
fail the release build. These artifact-specific notices cover transitive
libraries; the table below describes the framework's direct dependencies.
Verbatim direct-dependency license texts also live in
[docs/third-party/](docs/third-party/).

| Library | License | Verdict against Apache-2.0 |
|---|---|---|
| [liburing](https://github.com/axboe/liburing) (io_uring userspace library) | MIT. The kernel uAPI header it ships (`liburing/io_uring.h`) carries `(GPL-2.0 WITH Linux-syscall-note) OR MIT`; the syscall-note exception permits userspace use through the stable syscall interface, and this project consumes the header under its MIT arm | Permissive; retaining the copyright and license text below satisfies it |
| [libpq](https://www.postgresql.org) (PostgreSQL client library) | PostgreSQL license | Permissive; retaining the copyright and license text below satisfies it |
| [hiredis](https://github.com/redis/hiredis) (Redis client library) | BSD-3-Clause | Permissive; retaining the copyright and license text below satisfies it |

Each of these licenses requires retaining its copyright notice and license
text with redistributed copies. The direct texts under `docs/third-party/`
travel with source distributions; portable wheels carry native notices inside
the installed distribution, alongside the license manifest. A published SBOM
describes composition and does not replace those notices.

The portable wheel's PostgreSQL client supports TLS through OpenSSL and the
password/SCRAM and certificate authentication available in that build. Its
release-only configuration excludes LDAP lookup and GSSAPI authentication,
which prevents grafting unrelated authentication-library dependency trees.
The reviewed native closure is limited to libpq, OpenSSL's libssl/libcrypto,
hiredis and liburing; an additional SONAME fails notice collection until its
license and redistribution requirements are reviewed. Source builds use the
host libpq and retain that host build's authentication capabilities.

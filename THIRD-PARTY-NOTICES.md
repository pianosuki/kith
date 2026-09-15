# Third-party notices

The framework links three third-party C libraries. The release wheel bundles
only the framework's own libraries: these dependencies come from the host
system at runtime (wheel path) or build from source (source path), so no
third-party code is redistributed inside the wheel. The CycloneDX SBOM
attached to each release draft records the machine-readable composition.
Verbatim license texts live in [docs/third-party/](docs/third-party/).

| Library | License | Verdict against Apache-2.0 |
|---|---|---|
| [liburing](https://github.com/axboe/liburing) (io_uring userspace library) | MIT. The kernel uAPI header it ships (`liburing/io_uring.h`) carries `(GPL-2.0 WITH Linux-syscall-note) OR MIT`; the syscall-note exception permits userspace use through the stable syscall interface, and this project consumes the header under its MIT arm | Permissive; retaining the copyright and license text below satisfies it |
| [libpq](https://www.postgresql.org) (PostgreSQL client library) | PostgreSQL license | Permissive; retaining the copyright and license text below satisfies it |
| [hiredis](https://github.com/redis/hiredis) (Redis client library) | BSD-3-Clause | Permissive; retaining the copyright and license text below satisfies it |

Each of these licenses requires retaining its copyright notice and license
text with copies of the work: the texts under `docs/third-party/` discharge
that for source and sdist distribution, and the SBOM carries the composition
for the binary wheel.

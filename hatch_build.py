"""Hatch build hook that compiles and bundles the kith C shared libraries.

When ``uv build --wheel`` (or ``pip wheel``) builds a wheel,
this hook configures CMake, builds every library the bundle ships with a
release configuration, and force-includes the SONAME-form libraries from the
CMake build tree into the wheel under ``kith/_libs/`` so the ctypes bridge
(``python/kith/_bridge.py``) finds them via its ``_WHEEL_SUBDIRS`` discovery
on ``import kith``. The result is a binary wheel carrying the framework's
own shared libraries: installing it lands those libraries next to the
Python package, and no build-from-source step is required of the installing
user. The system libraries they link (liburing, libpq, libhiredis) come
from the host at runtime.

The hook is inert for sdist builds: an sdist ships the source tree and the
CMake build files so a user with a C toolchain can build from source, but the
sdist itself carries no compiled artifacts.

Editable installs are also inert: ``uv sync`` and ``pip install -e`` build an
editable wheel through the same wheel target, and the hook exits immediately
for them. A development checkout loads the C libraries from an in-tree preset
build directory at import time (the bridge's discovery), so syncing the
Python environment never compiles C code.

The wheel build resolves its C compiler itself instead of leaving the choice
to CMake's default probe: the ``CC`` environment variable wins when set;
otherwise Clang is used when a major version of at least the framework
minimum is on PATH; otherwise the hook falls back to ``cc`` or ``gcc``.
LTO is enabled only for clang-family compilers: GCC emits GIMPLE-bitcode
objects under LTO and the lld link selected by cmake/framework.cmake cannot
read them, so every version-script symbol assignment fails at link time.
Non-clang wheel builds therefore configure with IPO off, giving up link-time
optimization so the link is guaranteed to succeed.

The C build is skippable: set ``KITH_SKIP_C_BUILD=1`` in the environment and
the hook becomes a no-op. That keeps a pure-Python ``uv build --wheel`` usable
for development wheel snapshots that load the C libraries from an in-tree
build directory (the bridge's discovery walks the preset build directories
when no wheel-relative libraries are present).

The libraries are bundled from the build tree rather than through
``cmake --install`` for runpath reasons: cmake/framework.cmake builds with
``CMAKE_BUILD_RPATH_USE_ORIGIN`` ON, so every built library carries
``RUNPATH $ORIGIN`` and resolves its ``DT_NEEDED`` siblings in the same flat
directory — exactly the layout of ``kith/_libs/``. An install relinks for the
``<prefix>/lib`` consumer layout (``$ORIGIN/../lib``), which cannot resolve
inside a wheel, and a cache-level ``-DCMAKE_INSTALL_RPATH`` override from the
hook is shadowed by the normal-variable set in framework.cmake. The build
tree's own runpath is the one that matches the bundle.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


# The SONAME-form shared libraries the bridge expects to discover under
# kith/_libs/. The bundle ships every library the framework builds, so the
# tuple grows as each module's shared library lands.
_REQUIRED_LIBS: tuple[str, ...] = (
    "util",
    "config",
    "logger",
    "metrics",
    "proto",
    "net",
    "reactor",
    "worker",
    "state",
    "db",
)

# Where the bundled libraries land inside the wheel archive (and thus under
# the installed package). The ctypes bridge searches this name among its
# _WHEEL_SUBDIRS discovery list, and the $ORIGIN runpath makes every bundled
# library resolve its DT_NEEDED siblings within this same directory.
_BUNDLE_DIR = "kith/_libs"

# Minimum Clang major version the framework accepts (cmake/framework.cmake
# fatals below this). The hook only *prefers* Clang when it meets the floor;
# an older Clang on PATH is skipped so the fallback compiler still produces a
# working wheel instead of a configure-time fatal error.
_CLANG_FLOOR = 22


def _shared_suffix() -> str:
    """Return the platform shared-object suffix (without SONAME version)."""
    return ".dylib" if sys.platform == "darwin" else ".so"


def _soname_filename(module: str) -> str:
    """Return the SONAME-form filename the bridge searches for the module."""
    suffix = _shared_suffix()
    if sys.platform == "darwin":
        return f"libkith_{module}{suffix}"
    return f"libkith_{module}{suffix}.1"


def _run(cmd: list[str], cwd: Path) -> None:
    """Invoke a command, raising RuntimeError with full context on failure."""
    try:
        subprocess.run(cmd, cwd=str(cwd), check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"wheel build hook: command failed (exit {exc.returncode}): {' '.join(cmd)} (cwd={cwd})"
        ) from exc


def _resolve_wheel_dir(env_var: str, default: Path, root: Path) -> Path:
    """Resolve a wheel build/install directory from an env var.

    Returns ``default`` when the variable is absent or strips to empty.
    The branch tests the env-var string, not the wrapped path:
    ``Path("")`` is a truthy object and resolves to the current working
    directory, and the caller removes the returned directory with
    ``shutil.rmtree`` when it exists.

    The candidate is refused when it equals the repo root, lives inside
    it, or is an ancestor of it: rmtree on any of those would delete
    source the build is currently reading.
    """
    raw = os.environ.get(env_var, "").strip()
    if not raw:
        return default
    candidate = Path(raw).resolve()
    if candidate == root or _contains(candidate, root) or _contains(root, candidate):
        raise RuntimeError(
            f"wheel build hook: refusing to use {candidate} from {env_var}; "
            f"the path is the repo root, contains the repo root, or is "
            f"contained by the repo root ({root}). Pick a scratch directory "
            f"outside the source tree."
        )
    return candidate


def _contains(outer: Path, inner: Path) -> bool:
    """True if ``inner`` is equal to or lives inside ``outer``."""
    try:
        inner.relative_to(outer)
        return True
    except ValueError:
        return False


def _compiler_version_output(compiler: str) -> str:
    """Return the compiler's ``--version`` output, or "" if it cannot run."""
    try:
        proc = subprocess.run([compiler, "--version"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as _exc:
        return ""
    return proc.stdout if proc.returncode == 0 else ""


def _is_clang(compiler: str) -> bool:
    """True if the compiler identifies as clang-family in its version output."""
    return "clang" in _compiler_version_output(compiler).lower()


def _clang_major(compiler: str) -> int | None:
    """Return the compiler's Clang major version, or None if not clang."""
    match = re.search(r"clang version (\d+)", _compiler_version_output(compiler), re.IGNORECASE)
    return int(match.group(1)) if match else None


def _select_c_compiler() -> tuple[str | None, bool]:
    """Resolve the wheel build's C compiler and whether LTO may be enabled.

    Returns ``(compiler, lto_allowed)``. ``CC`` wins when set; otherwise a
    Clang meeting the framework floor is preferred; otherwise the default
    ``cc``/``gcc`` is used. The exact binary returned here is what the hook
    passes to CMake, so the LTO decision is made about the compiler that
    actually builds. ``lto_allowed`` is false for every non-clang selection:
    GCC LTO objects carry GIMPLE bitcode that the lld link forced by
    cmake/framework.cmake cannot read, which fails the link at the version
    script. A compiler that cannot be probed is treated as non-clang.
    """
    explicit = os.environ.get("CC", "").strip()
    if explicit:
        return explicit, _is_clang(explicit)
    clang = shutil.which("clang")
    if clang and (_clang_major(clang) or 0) >= _CLANG_FLOOR:
        return clang, True
    fallback = shutil.which("cc") or shutil.which("gcc")
    if fallback:
        return fallback, _is_clang(fallback)
    return None, False


class BundleCLibsHook(BuildHookInterface):  # type: ignore[misc]
    """Build hook that compiles the C shared libraries into the wheel."""

    def initialize(self, version: str, build_data: dict[str, object]) -> None:
        # The hook only fires for wheel builds. An sdist ships source and the
        # CMake build files; no compiled artifacts belong in the sdist.
        if self.target_name != "wheel":
            return

        # Editable wheels (uv sync, pip install -e) are development
        # checkouts: the bridge discovers an in-tree build directory at
        # import time, so syncing must not compile C code.
        if version == "editable":
            self.app.display_info(
                "kith wheel build hook: editable install, C bundling skipped "
                "(import kith discovers an in-tree build directory)"
            )
            return

        if os.environ.get("KITH_SKIP_C_BUILD") == "1":
            self.app.display_info("kith wheel build hook: KITH_SKIP_C_BUILD=1, skipping")
            return
        root = Path(self.root).resolve()
        build_dir = _resolve_wheel_dir(
            "KITH_WHEEL_BUILD_DIR", root / "build" / "_wheel_cmake", root
        )

        # Rebuild from a clean slate so a prior failed build does not ship
        # stale or partial artifacts into the next wheel.
        if build_dir.exists():
            shutil.rmtree(build_dir)
        build_dir.mkdir(parents=True, exist_ok=True)

        compiler, lto_allowed = _select_c_compiler()
        if compiler is None:
            self.app.display_info(
                "kith wheel build hook: no C compiler on PATH; "
                "letting CMake probe and report its own error"
            )
        else:
            detail = "" if lto_allowed or "CC" in os.environ else " (set CC to override)"
            lto_note = "" if lto_allowed else "; LTO disabled (non-clang toolchain)"
            self.app.display_info(f"kith wheel build hook: C compiler {compiler}{lto_note}{detail}")

        self.app.display_info(f"kith wheel build hook: configuring CMake at {build_dir}")

        # Release + IPO per the bundle contract; the compiler is pinned to
        # the probed binary so the LTO decision above describes what actually
        # builds. IPO stays off for non-clang toolchains because their LTO
        # objects cannot be linked by lld. No install-prefix or install-rpath
        # configuration: the libraries are bundled from this build tree,
        # whose runpath cmake/framework.cmake already shapes for flat
        # sibling resolution.
        configure_cmd = [
            "cmake",
            "-S",
            str(root),
            "-B",
            str(build_dir),
            "-G",
            "Ninja",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=" + ("ON" if lto_allowed else "OFF"),
        ]
        if compiler:
            configure_cmd.append("-DCMAKE_C_COMPILER=" + compiler)
        _run(configure_cmd, cwd=root)

        self.app.display_info("kith wheel build hook: building C shared libraries")

        # Every library the bundle installs has an install(TARGETS) rule, so
        # the build must produce all of them: the bridge loads each library
        # directly.
        targets = [f"kith_{module}" for module in _REQUIRED_LIBS]
        _run(["cmake", "--build", str(build_dir), "--target", *targets], cwd=root)

        self.app.display_info(f"kith wheel build hook: bundling C libraries from {build_dir}")

        # The build-tree artifacts already carry the runpath a flat bundle
        # needs ($ORIGIN, from CMAKE_BUILD_RPATH_USE_ORIGIN), and each module
        # exposes its SONAME-form name as a symlink here.
        force_include: dict[str, str] = build_data.setdefault("force_include", {})  # type: ignore[assignment]
        bundled: list[str] = []
        for module in _REQUIRED_LIBS:
            soname = _soname_filename(module)
            src = build_dir / soname
            if not src.is_file():
                raise RuntimeError(f"wheel build hook: required library not built at {src}")
            archive_path = f"{_BUNDLE_DIR}/{soname}"
            force_include[str(src)] = archive_path
            bundled.append(archive_path)

        if not bundled:
            raise RuntimeError("wheel build hook: no libraries were bundled")

        self.app.display_info(
            f"kith wheel build hook: bundled {len(bundled)} libraries into {_BUNDLE_DIR}/"
        )

        # A wheel carrying platform binaries must not ship the pure-Python
        # default tag: pip installs such a wheel on any operating system,
        # and the failure waits for import. manylinux_2_38 is the lowest
        # PyPI-acceptable tag (bare linux_x86_64 is rejected at upload), its
        # glibc floor is the documented install floor, and pip enforces it
        # at install time, falling back to the sdist below it. py3-none
        # keeps the artifact interpreter-agnostic.
        build_data["pure_python"] = False
        build_data["tag"] = "py3-none-manylinux_2_38_x86_64"

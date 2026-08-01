"""ctypes loader for the kith C shared libraries.

The bridge is the single place where the Python package touches ``dlopen``.
It locates the framework shared libraries, loads them with :mod:`ctypes`,
verifies that the runtime ABI generation matches the generation this loader
was written against, and hands out per-module ``CDLL`` handles for the
generated bindings and the public ``Server`` facade to attach
``argtypes``/``restype`` to.

Discovery walks candidate directories in priority order and loads from the
first directory that contains the framework libraries:

  1. ``$KITH_LIB`` — explicit directory (or file) on the environment.
  2. ``$KITH_BUILD_DIR`` — explicit build directory (when ``$KITH_LIB`` is
     unset), so a build context whose source root differs from the
     development tree keeps its libraries discoverable.
  3. Wheel-relative — alongside this module, or in its ``lib``/``_libs``
     subdirectories (where the binary wheel bundles the ``.so`` files).
  4. Build directories — ``build/<preset>`` under the repository root, for
     in-tree development (``debug``, ``release``, ``asan``).
  5. System library directories — let ``dlopen`` resolve the SONAMEs
     (``libkith_util.so.1``) against the default linker search path, for
     an installed-from-source layout that puts the libraries in
     ``/usr/lib`` or a directory on ``/etc/ld.so.conf``.

Two load granularities are supported:

  * **monolith** — a single ``libkith.so`` (or ``libkith.dylib``) that
    re-exports every module's ``KITH_API`` symbols through one combined
    version script. Every per-module handle aliases this one ``CDLL``.
  * **individual** — the per-module ``libkith_<module>.so`` files. Each
    module is loaded by full path; ``libkith_server`` links its plane
    dependencies via ``RUNPATH=$ORIGIN``, and loading a module by path
    lets ``dlopen`` resolve those dependencies automatically. A module
    not present (the optional control plane, disabled at build time via
    ``CONTROL_PLANE_ENABLED=OFF``) is skipped silently.

The loader is a process-wide singleton guarded by a :class:`threading.Lock`
so concurrent threads under a free-threaded interpreter (``python3.14t``,
PEP 703) do not double-load. :func:`load` is the entry point; :func:`reset`
exists for tests that vary ``$KITH_LIB``.
"""

from __future__ import annotations

import atexit
import ctypes
import os
import sys
import threading
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from kith._generated.version import KITH_ABI_VERSION
from kith.exceptions import ABIVersionError, BridgeLoadError


# Size-versioned struct ABI generation this loader was written against. The
# runtime reports the same value via kith_version_abi(); a mismatch means the
# loaded C libraries belong to a different generation than the Python package
# and struct layouts cannot be trusted. The value is sourced from the
# generated bindings (kith._generated.version.KITH_ABI_VERSION), which read it
# from include/kith/version.h at generation time — so the loader and the
# headers share one definition.
EXPECTED_ABI_VERSION: Final[int] = KITH_ABI_VERSION


# Every per-module shared library, in dependency order (leaf utilities first,
# composition root last). The control plane is optional: a build with
# CONTROL_PLANE_ENABLED=OFF produces no libkith_control and the loader skips it.
_REQUIRED_LIBS: Final[tuple[str, ...]] = (
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
    "aoi",
    "sim",
    "fabric",
    "gateway",
    "coord",
    "client",
    "server",
)
_OPTIONAL_LIBS: Final[tuple[str, ...]] = ("control",)
_ALL_LIBS: Final[tuple[str, ...]] = (*_REQUIRED_LIBS, *_OPTIONAL_LIBS)

# Build-tree presets searched in priority order for in-tree development.
_BUILD_PRESETS: Final[tuple[str, ...]] = ("debug", "release", "asan")

# Subdirectories searched relative to this module's install location for a
# wheel that bundles the .so files alongside the package.
_WHEEL_SUBDIRS: Final[tuple[str, ...]] = ("", "lib", "_libs")


@dataclass(frozen=True)
class Bridge:
    """A loaded set of kith shared libraries.

    Attributes:
        mode: ``"monolith"`` when one library re-exports every module's
            symbols, or ``"individual"`` when each module is a separate
            ``CDLL``.
        root: Directory the libraries were loaded from, or ``None`` when
            loaded from the system linker search path (no single owning
            directory).
        abi_version: The runtime ABI generation reported by
            ``kith_version_abi``.
        version_string: The framework release version string reported by
            ``kith_version_string``.
    """

    mode: str
    root: Path | None
    abi_version: int
    version_string: str
    _handles: dict[str, ctypes.CDLL]

    def lib(self, name: str) -> ctypes.CDLL:
        """Return the ``CDLL`` handle for module ``name``.

        In monolith mode every known module name aliases the single
        library handle. In individual mode each name maps to the
        corresponding ``libkith_<name>`` handle loaded at :func:`load`
        time. Raises :class:`BridgeLoadError` for an unknown or absent
        (optional, not built) module name.
        """
        try:
            return self._handles[name]
        except KeyError:
            raise BridgeLoadError(f"no loaded kith library named {name!r}") from None

    def server(self) -> ctypes.CDLL:
        """Convenience accessor for the composition-root library handle."""
        return self.lib("server")

    def lib_names(self) -> tuple[str, ...]:
        """Return the names of every loaded module handle."""
        return tuple(self._handles)


# ---------------------------------------------------------------------------
# path discovery
# ---------------------------------------------------------------------------


def _lib_suffix() -> str:
    """Return the unversioned shared-object suffix for the host platform."""
    # Platform-aware suffix/SONAME selection stays as pure string mechanics
    # (unit-tested, no support promise); the load guard in _do_load fires
    # first on any non-Linux host.
    return ".dylib" if sys.platform == "darwin" else ".so"


def _find_lib(root: Path, name: str) -> Path | None:
    """Return the first path under ``root`` naming the module's library.

    On Linux both the development symlink (``libkith_<name>.so``) and the
    SONAME form (``libkith_<name>.so.1``) are accepted; on macOS only the
    ``.dylib`` form (the install_name carries the version).
    """
    suffix = _lib_suffix()
    candidates: list[Path]
    if sys.platform == "darwin":
        candidates = [root / f"libkith_{name}{suffix}"]
    else:
        candidates = [
            root / f"libkith_{name}{suffix}",
            root / f"libkith_{name}{suffix}.1",
        ]
    for cand in candidates:
        if cand.is_file():
            return cand
    return None


def _find_monolith(root: Path) -> Path | None:
    """Return the path to a true-monolith ``libkith`` under ``root``, or None.

    A monolith is a single library (no module suffix in its basename) that
    re-exports every module's symbols through one combined version script. It
    is distinct from ``libkith_server``, the composition-root library, whose
    version script exports only the ``kith_server_*`` surface.
    """
    suffix = _lib_suffix()
    candidates: list[Path]
    if sys.platform == "darwin":
        candidates = [root / f"libkith{suffix}"]
    else:
        candidates = [
            root / f"libkith{suffix}",
            root / f"libkith{suffix}.1",
        ]
    for cand in candidates:
        if cand.is_file():
            return cand
    return None


def _candidate_dirs() -> list[Path]:
    """Build the fall-back discovery search path (env handled separately).

    When ``$KITH_LIB`` is unset, directories are tried in documented
    priority order: the ``$KITH_BUILD_DIR`` override (when set, so a
    build context whose source root differs from the development tree — a
    bind-mounted container recording /work paths in its CMake cache —
    keeps its libraries discoverable), then wheel-relative locations, then
    the in-tree preset build directories.
    """
    dirs: list[Path] = []

    build_dir = os.environ.get("KITH_BUILD_DIR")
    if build_dir:
        dirs.append(_resolve_env_dir(build_dir))

    pkg_dir = Path(__file__).resolve().parent
    for sub in _WHEEL_SUBDIRS:
        dirs.append(pkg_dir / sub if sub else pkg_dir)

    # python/kith/_bridge.py -> kith -> python -> repository root.
    repo_root = pkg_dir.parents[1]
    for preset in _BUILD_PRESETS:
        dirs.append(repo_root / "build" / preset)

    return dirs


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------


def _cdll(path: Path) -> ctypes.CDLL:
    """Open a shared library by absolute path, wrapping failures in BridgeLoadError."""
    try:
        return ctypes.CDLL(str(path))
    except OSError as exc:
        raise BridgeLoadError(f"failed to load {path}: {exc}") from exc


def _configure_version_symbols(lib: ctypes.CDLL) -> None:
    """Declare argtypes/restype for the version-introspection symbols.

    Raises :class:`BridgeLoadError` when the symbols are absent, which
    indicates a library that is not a kith framework library.
    """
    for sym in ("kith_version_abi", "kith_version_string"):
        if not hasattr(lib, sym):
            raise BridgeLoadError(f"symbol {sym!r} not found; not a kith library")
    lib.kith_version_abi.restype = ctypes.c_uint
    lib.kith_version_abi.argtypes = []
    lib.kith_version_string.restype = ctypes.c_char_p
    lib.kith_version_string.argtypes = []


def _read_abi(lib: ctypes.CDLL) -> int:
    """Return the runtime ABI generation, after verifying it matches expected."""
    runtime = int(lib.kith_version_abi())
    if runtime != EXPECTED_ABI_VERSION:
        raise ABIVersionError(
            f"runtime ABI generation {runtime} != expected {EXPECTED_ABI_VERSION}; "
            "the loaded C libraries belong to a different generation than the "
            "Python package"
        )
    return runtime


def _read_version_string(lib: ctypes.CDLL) -> str:
    """Return the framework release version string."""
    raw = lib.kith_version_string()
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


def _load_monolith(path: Path) -> Bridge:
    """Load a single re-exporting monolith and alias every module name to it."""
    lib = _cdll(path)
    _configure_version_symbols(lib)
    abi = _read_abi(lib)
    version = _read_version_string(lib)
    handles = dict.fromkeys(_ALL_LIBS, lib)
    return Bridge(
        mode="monolith",
        root=path.parent,
        abi_version=abi,
        version_string=version,
        _handles=handles,
    )


def _load_individual(root: Path) -> Bridge:
    """Load every required per-module library under ``root`` plus optional ones."""
    handles: dict[str, ctypes.CDLL] = {}

    util_path = _find_lib(root, "util")
    if util_path is None:
        raise BridgeLoadError(f"libkith_util not found in {root}")
    util = _cdll(util_path)
    _configure_version_symbols(util)
    abi = _read_abi(util)
    version = _read_version_string(util)
    handles["util"] = util

    for name in _REQUIRED_LIBS:
        if name == "util":
            continue
        path = _find_lib(root, name)
        if path is None:
            raise BridgeLoadError(f"libkith_{name} not found in {root}")
        handles[name] = _cdll(path)

    for name in _OPTIONAL_LIBS:
        path = _find_lib(root, name)
        if path is not None:
            handles[name] = _cdll(path)

    return Bridge(
        mode="individual",
        root=root,
        abi_version=abi,
        version_string=version,
        _handles=handles,
    )


def _soname(name: str) -> str:
    """Return the SONAME dlopen accepts for system resolution of module ``name``."""
    if sys.platform == "darwin":
        return f"libkith_{name}.dylib"
    return f"libkith_{name}.so.1"


def _load_by_soname(name: str) -> ctypes.CDLL | None:
    """Try to load a module from the default linker search path; None if absent."""
    try:
        return ctypes.CDLL(_soname(name))
    except OSError:
        return None


def _try_system_load() -> Bridge | None:
    """Attempt a system-path load after every directory candidate failed.

    Returns a Bridge when ``libkith_util`` resolves on the default linker
    search path, or ``None`` when it does not (so the caller can raise a
    discovery error that names the last directory tried).
    """
    util = _load_by_soname("util")
    if util is None:
        return None
    _configure_version_symbols(util)
    abi = _read_abi(util)
    version = _read_version_string(util)
    handles: dict[str, ctypes.CDLL] = {"util": util}
    for name in _REQUIRED_LIBS:
        if name == "util":
            continue
        h = _load_by_soname(name)
        if h is None:
            raise BridgeLoadError(
                f"system libkith_{name} ({_soname(name)}) not found after "
                "libkith_util resolved; partial install"
            )
        handles[name] = h
    for name in _OPTIONAL_LIBS:
        h = _load_by_soname(name)
        if h is not None:
            handles[name] = h
    return Bridge(
        mode="individual",
        root=None,
        abi_version=abi,
        version_string=version,
        _handles=handles,
    )


def _resolve_env_dir(env: str) -> Path:
    """Resolve a ``$KITH_LIB`` value to an absolute directory path."""
    p = Path(env)
    return p if p.is_absolute() else (Path.cwd() / p).resolve()


def _do_load() -> Bridge:
    """Walk the discovery order and return the first successfully loaded Bridge.

    When ``$KITH_LIB`` is set it is treated as the authoritative directory:
    a missing directory or one without kith libraries raises immediately
    rather than silently falling through, so an explicit override that
    points at the wrong place is reported instead of masked by a subsequent
    candidate. When unset, wheel-relative, build-tree, and system
    directories are tried in order.

    Loading raises on any non-Linux host: the reactor's io_uring backend
    is Linux-only, so no loadable library exists there and a clear error
    beats the loader's "library not found" failure after path sniffing.
    """
    if sys.platform != "linux":
        raise BridgeLoadError(
            "kith supports Linux only: the reactor's io_uring backend is "
            f"Linux-specific, and this interpreter reports "
            f"sys.platform={sys.platform!r}. See CONTRIBUTING.md."
        )
    env = os.environ.get("KITH_LIB")
    if env:
        root = _resolve_env_dir(env)
        if not root.is_dir():
            raise BridgeLoadError(f"KITH_LIB={env!r} is not a directory: {root}")
        mono = _find_monolith(root)
        if mono is not None:
            return _load_monolith(mono)
        if _find_lib(root, "util") is not None:
            return _load_individual(root)
        raise BridgeLoadError(f"KITH_LIB={env!r} ({root}) contains no kith shared libraries")

    for root in _candidate_dirs():
        if not root.is_dir():
            continue
        mono = _find_monolith(root)
        if mono is not None:
            return _load_monolith(mono)
        if _find_lib(root, "util") is not None:
            return _load_individual(root)

    system = _try_system_load()
    if system is not None:
        return system

    raise BridgeLoadError(
        "could not locate the kith shared libraries. Set KITH_LIB to the "
        "directory containing libkith_server.so, or build via "
        "'cmake --preset debug && cmake --build build/debug'."
    )


# ---------------------------------------------------------------------------
# interpreter exit gate
# ---------------------------------------------------------------------------


_exit_gate_registered = False


def _arm_python_exit_gate() -> None:
    """Set the C-side interpreter exit gate.

    CPython invokes atexit handlers while the interpreter is still fully
    intact and still servicing threads; the runtime marks itself finalizing
    only afterwards. Every foreign-thread callback entry after this hook is
    refused by the C dispatch sites, so the process exits instead of
    crashing inside the tearing-down interpreter.
    """
    try:
        gate_set = load().lib("util").kith_python_finalizing_set
        gate_set.argtypes = [ctypes.c_int]
        gate_set(1)
    except Exception:
        # The gate is a crash backstop for foreign threads; an exit hook
        # must never break the interpreter's own shutdown.
        pass


def _register_exit_gate() -> None:
    """Register the exit gate hook once per process."""
    global _exit_gate_registered
    if _exit_gate_registered:
        return
    _exit_gate_registered = True
    atexit.register(_arm_python_exit_gate)


# ---------------------------------------------------------------------------
# handler-exception guards
# ---------------------------------------------------------------------------


def count_handler_exception() -> None:
    """Count one handler exception in the interpreter-scoped util counter.

    The count call is itself guarded: a failure here must not raise out of
    an exception handler, where it would escape to ctypes' unraisable
    print — the noise the guards exist to contain.
    """
    try:
        note = load().lib("util").kith_python_note_handler_exception
        note.argtypes = []
        note.restype = None
        note()
    except Exception:
        pass


def handler_exception_reporter(kind: str) -> Callable[[Exception], None]:
    """Return a per-registration exception reporter for a Python handler.

    The reporter counts every exception in the interpreter-scoped counter
    and prints the first exception's traceback to stderr under a one-line
    header naming the handler seam. Subsequent exceptions count silently: the
    counter is the rate signal, the first traceback is the diagnostic
    sample, and the handler owns any richer failure logging.
    """
    printed = False

    def _report(exc: Exception) -> None:
        nonlocal printed
        count_handler_exception()
        if printed:
            return
        printed = True
        # Two workers may hold the flag unset at once; the worst case is a
        # second sample traceback, which is harmless.
        print(
            f"kith: {kind} raised; further exceptions from this handler are counted, not printed",
            file=sys.stderr,
        )
        traceback.print_exc()

    return _report


def response_overflow_reporter() -> Callable[[str, int, int], None]:
    """Return a per-registration oversize-response reporter for a control route.

    The reporter prints the first oversize response per registration to
    stderr — the route, the attempted size, and the write-buffer capacity —
    and stays silent afterwards: the overflow counter is the rate signal and
    the canonical rejection body carries the per-request detail. An oversize
    response is a sizing condition, not a handler exception, so no traceback
    prints.
    """
    printed = False

    def _note(route: str, attempted: int, cap: int) -> None:
        nonlocal printed
        if printed:
            return
        # Two workers may hold the flag unset at once; the worst case is a
        # second message, which is harmless.
        printed = True
        print(
            f"kith: control route {route} response of {attempted} bytes exceeded the "
            f"{cap}-byte write buffer; further oversize responses count in "
            f"kith_control_response_overflow_total",
            file=sys.stderr,
        )

    return _note


def guarded_handler(handler: Callable[..., None], kind: str) -> Callable[..., None]:
    """Wrap a pool-dispatched Python handler so its exceptions stay contained.

    The wrapped callable catches ``Exception`` from the handler, reports it
    through :func:`handler_exception_reporter`, and returns normally — the
    C dispatch task sees a completed callback either way, so one failing
    invocation never blocks the pool or the dispatching plane.
    ``BaseException`` (KeyboardInterrupt, SystemExit) still escapes to
    ctypes' unraisable print: the guards cover the failures a handler can
    raise in normal operation, not interpreter control flow.

    Thread safety:
        Safe. The count is an atomic add; two workers racing the
        first-print flag print at most a second sample traceback.
    """
    report = handler_exception_reporter(kind)

    def _guarded(*args: object) -> None:
        try:
            handler(*args)
        except Exception as exc:
            report(exc)

    return _guarded


# ---------------------------------------------------------------------------
# singleton
# ---------------------------------------------------------------------------


_lock = threading.Lock()
_bridge: Bridge | None = None


def load(*, force: bool = False) -> Bridge:
    """Return the process-wide loaded :class:`Bridge`, loading on first use.

    The first call walks the discovery order and caches the result;
    subsequent calls return the cached bridge. Pass ``force=True`` to ignore
    the cache
    and reload (used by tests that vary ``$KITH_LIB``); the reload replaces
    the cached singleton.

    The singleton is guarded by a lock so concurrent threads under a
    free-threaded interpreter do not double-load. The first successful load
    also registers the interpreter exit gate (see
    :func:`_arm_python_exit_gate`): the C dispatch sites refuse
    python-bound callbacks once the interpreter is exiting, so a process
    that dies with a live server exits cleanly instead of crashing inside
    the interpreter's finalization.
    """
    global _bridge
    if not force and _bridge is not None:
        return _bridge
    with _lock:
        if not force and _bridge is not None:
            return _bridge
        _bridge = _do_load()
        _register_exit_gate()
        return _bridge


def reset() -> None:
    """Drop the cached singleton (for tests that change discovery inputs)."""
    global _bridge
    with _lock:
        _bridge = None

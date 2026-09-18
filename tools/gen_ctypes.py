#!/usr/bin/env python3
"""Generate Python ctypes bindings from kith public headers.

Parses every public header under ``include/kith/`` with libclang and emits
``ctypes.Structure`` subclasses, ``IntEnum`` enumerators, function-pointer
``CFUNCTYPE`` aliases, macro constants, and a per-module ``configure(lib)``
that attaches ``argtypes``/``restype`` to every ``KITH_API`` function, into
``python/kith/_generated/``. The generated files are checked in; the drift
checker (``tools/check_ctypes_drift.py``) regenerates into a temporary
directory and fails the commit if the checked-in bindings diverge.

This eliminates the hand-mirror drift class (silent memory corruption from a
missed struct-field update) while keeping the runtime pure-ctypes: no C++
toolchain, no nanobind, no import-time code generation.

A ``KITH_API`` function is a ``FUNCTION_DECL`` whose declarator carries a
``visibility("default")`` attribute (the macro expands to
``__attribute__((visibility("default")))``). Internal functions
(``static``, ``KITH_LOCAL``) carry ``visibility("hidden")`` or none and are
skipped. Opaque struct typedefs (``typedef struct foo foo_t;``) emit an
empty ``ctypes.Structure`` shell — the layout stays hidden by contract.
Exposed-layout structs (``struct foo { ... };``) emit ``_fields_`` read
verbatim from the AST, so a header field change regenerates the binding and
the drift gate fails if the regeneration was not checked in.

Usage:
    gen_ctypes.py [--include-root <dir>] [--out <dir>]

    --include-root  public headers root (default: include)
    --out           output directory for generated bindings
                    (default: python/kith/_generated)
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from clang.cindex import Cursor, CursorKind, Index, TranslationUnit, Type, TypeKind


_GENERATED_BY = "tools/gen_ctypes.py"

# Headers that define the public C surface but carry only macro plumbing
# (no types, functions, or constants worth mirroring). Skipped entirely.
_SKIP_HEADERS: frozenset[str] = frozenset({"kith/api.h"})

# C type spelling -> ctypes attribute name. Applied to canonical builtin
# types and stdint typedefs. Enum canonical types map to c_uint32 (the
# headers fix every public enum to ``unsigned int``).
_BUILTIN_MAP: dict[str, str] = {
    "void": "None",
    "int": "ctypes.c_int",
    "unsigned int": "ctypes.c_uint",
    "char": "ctypes.c_char",
    "bool": "ctypes.c_bool",
    "float": "ctypes.c_float",
    "double": "ctypes.c_double",
    "unsigned char": "ctypes.c_uint8",
    "signed char": "ctypes.c_int8",
    "short": "ctypes.c_int16",
    "unsigned short": "ctypes.c_uint16",
    "long": "ctypes.c_long",
    "unsigned long": "ctypes.c_ulong",
    "long long": "ctypes.c_int64",
    "unsigned long long": "ctypes.c_uint64",
    "size_t": "ctypes.c_size_t",
    "ssize_t": "ctypes.c_ssize_t",
    "uint8_t": "ctypes.c_uint8",
    "uint16_t": "ctypes.c_uint16",
    "uint32_t": "ctypes.c_uint32",
    "uint64_t": "ctypes.c_uint64",
    "int8_t": "ctypes.c_int8",
    "int16_t": "ctypes.c_int16",
    "int32_t": "ctypes.c_int32",
    "int64_t": "ctypes.c_int64",
}


@dataclass
class EnumDecl:
    name: str
    typedef_name: str | None
    constants: list[tuple[str, int]]


@dataclass
class StructDecl:
    name: str
    typedef_name: str | None
    fields: list[tuple[str, str]]  # (field_name, ctypes_expr)


@dataclass
class FuncPtrTypedef:
    name: str
    restype: str
    argtypes: list[str]


@dataclass
class FunctionDecl:
    name: str
    restype: str
    argtypes: list[str]


@dataclass
class MacroConst:
    name: str
    value: int | str


@dataclass
class HeaderDecls:
    module: str
    enums: list[EnumDecl] = field(default_factory=list)
    structs: list[StructDecl] = field(default_factory=list)
    funcptr_typedefs: list[FuncPtrTypedef] = field(default_factory=list)
    functions: list[FunctionDecl] = field(default_factory=list)
    macros: list[MacroConst] = field(default_factory=list)
    # struct/typedef names declared in this header (for cross-module import).
    exported_names: set[str] = field(default_factory=set)


def _system_include_args() -> list[str]:
    """Return the toolchain's system include directory arguments."""
    out = subprocess.run(
        ["clang", "-E", "-v", "-xc", "/dev/null", "-o", "/dev/null"],
        capture_output=True,
        text=True,
        check=False,
    )
    args: list[str] = []
    capture = False
    for line in out.stderr.splitlines():
        if "search starts here" in line:
            capture = True
            continue
        if "End of search" in line:
            capture = False
            continue
        if capture and line.strip():
            args.append(f"-I{line.strip()}")
    return args


def _in_file(cursor: Cursor, header: Path) -> bool:
    loc = cursor.location
    if loc is None or loc.file is None:
        return False
    try:
        return bool(Path(loc.file.name).resolve() == header.resolve())
    except (OSError, ValueError) as _exc:
        del _exc
        return bool(loc.file.name == str(header))


def _is_api_function(cursor: Cursor) -> bool:
    """Return True when a FUNCTION_DECL carries ``visibility("default")``."""
    return any(child.kind == CursorKind.VISIBILITY_ATTR for child in cursor.get_children())


def _strip_int_suffix(tokens: str) -> str:
    """Strip C integer suffixes (u/U/l/L) so Python ``eval`` accepts the value."""
    return re.sub(r"\b(\d+)[uUlL]+\b", r"\1", tokens)


def _eval_macro_int(token_spellings: list[str]) -> int | None:
    """Evaluate an object-like integer macro to a Python int, or None if unsafe."""
    expr = " ".join(token_spellings)
    expr = _strip_int_suffix(expr)
    # Reject anything that is not digits, arithmetic operators, parens, spaces.
    if not re.fullmatch(r"[\d\s+\-*/()<<>>]*", expr):
        return None
    try:
        return cast(int, eval(expr, {"__builtins__": {}}, {}))
    except (SyntaxError, NameError, ValueError, ZeroDivisionError) as _exc:
        del _exc
        return None


def _collect_macros(tu: TranslationUnit, header: Path) -> list[MacroConst]:
    macros: list[MacroConst] = []
    for cursor in tu.cursor.get_children():
        if cursor.kind != CursorKind.MACRO_DEFINITION:
            continue
        if not _in_file(cursor, header):
            continue
        name = cursor.spelling
        if not name.startswith("KITH_"):
            continue
        # Skip include guards (name ends with _H and equals the guard macro).
        if name.endswith("_H") and name.replace("_", "").isupper():
            continue
        tokens = [t.spelling for t in cursor.get_tokens()]
        # First token is the macro name; the rest is the expansion.
        expansion = tokens[1:]
        if not expansion:
            continue
        joined = "".join(expansion)
        # String literal?
        if joined.startswith('"') and joined.endswith('"'):
            macros.append(MacroConst(name, joined))
            continue
        value = _eval_macro_int(expansion)
        if value is not None:
            macros.append(MacroConst(name, value))
    return macros


@dataclass
class _TypeRegistry:
    """Maps a public type name to the module that declares it.

    The first module to register a type owns it: a module that only
    redeclares an opaque handle (a forward ``typedef struct foo foo_t;`` so
    its header is self-contained) does not take ownership. The owning module
    emits the ``ctypes.Structure`` class; every other module imports the name.
    """

    name_to_module: dict[str, str] = field(default_factory=dict)
    # Function-pointer typedef names -> the module that defines them.
    funcptr_names: set[str] = field(default_factory=set)
    # Struct class names (the struct tag AND the typedef alias).
    struct_names: set[str] = field(default_factory=set)
    # Enum class names (the enum tag AND the typedef alias).
    enum_names: set[str] = field(default_factory=set)

    def register(self, module: str, decls: HeaderDecls) -> None:
        for s in decls.structs:
            self.struct_names.add(s.name)
            if s.typedef_name:
                self.struct_names.add(s.typedef_name)
            # First declarer wins: the module that first registers a struct
            # owns it, so a forward-declaration in another header does not
            # steal ownership.
            self.name_to_module.setdefault(s.name, module)
            if s.typedef_name:
                self.name_to_module.setdefault(s.typedef_name, module)
        for fp in decls.funcptr_typedefs:
            self.funcptr_names.add(fp.name)
            self.name_to_module.setdefault(fp.name, module)
        for e in decls.enums:
            self.enum_names.add(e.name)
            if e.typedef_name:
                self.enum_names.add(e.typedef_name)
            self.name_to_module.setdefault(e.name, module)
            if e.typedef_name:
                self.name_to_module.setdefault(e.typedef_name, module)


class _TypeMapper:
    """Maps a libclang ``Type`` to a ctypes Python expression string.

    Records the public type names referenced by the mapping (for cross-module
    import emission) in ``referenced_names``.
    """

    def __init__(self, registry: _TypeRegistry, current_module: str) -> None:
        self._registry = registry
        self._current_module = current_module
        self.referenced_names: set[str] = set()
        # Inline CFUNCTYPE declarations accumulated for anonymous
        # function-pointer struct fields.
        self.inline_funcptrs: list[tuple[str, str]] = []

    def map(self, t: Type, *, for_arg: bool = False) -> str:
        kind = getattr(t, "kind", None)
        if kind == TypeKind.POINTER:
            return self._map_pointer(t, for_arg=for_arg)
        if kind == TypeKind.CONSTANTARRAY or kind == TypeKind.INCOMPLETEARRAY:
            return self._map_array(t, for_arg=for_arg)
        if kind == TypeKind.TYPEDEF:
            return self._map_typedef(t)
        if kind == TypeKind.ENUM:
            return "ctypes.c_uint32"
        if kind == TypeKind.RECORD:
            return self._map_record(t)
        if kind == TypeKind.FUNCTIONPROTO:
            # Bare function prototype (no typedef) — emit inline CFUNCTYPE.
            return self._cfunc_expr(t)
        if kind == TypeKind.VOID:
            return "None"
        # Builtin: map by spelling.
        spelling = getattr(t, "spelling", "")
        if spelling in _BUILTIN_MAP:
            return _BUILTIN_MAP[spelling]
        # Canonical fallback for elaborated types (e.g. ``enum foo``).
        canonical = t.get_canonical()
        ckind = canonical.kind
        if ckind == TypeKind.ENUM:
            return "ctypes.c_uint32"
        if ckind == TypeKind.RECORD:
            return self._map_record(canonical)
        if ckind in _BUILTIN_MAP:
            return _BUILTIN_MAP[ckind]
        return "ctypes.c_void_p"

    def _map_pointer(self, t: Type, *, for_arg: bool) -> str:
        return self._map_pointee(t.get_pointee(), for_arg=for_arg)

    def _map_pointee(self, pointee: Type, *, for_arg: bool) -> str:
        """Map a pointee type: the target of a pointer or a decayed array."""
        canonical = pointee.get_canonical()
        if canonical.kind == TypeKind.VOID:
            return "ctypes.c_void_p"
        if canonical.kind in (TypeKind.CHAR_S, TypeKind.CHAR_U):
            # ``char *`` -> c_char_p (NUL-terminated string convention).
            return "ctypes.c_char_p"
        if canonical.kind == TypeKind.FUNCTIONPROTO:
            # If the pointee is reached via a typedef name, the typedef's
            # CFUNCTYPE alias is emitted by _map_typedef. A bare function
            # pointer (no typedef) emits an inline CFUNCTYPE.
            return self._cfunc_expr(canonical)
        inner = self.map(pointee, for_arg=for_arg)
        if inner == "None":
            return "ctypes.c_void_p"
        return f"ctypes.POINTER({inner})"

    def _map_array(self, t: Type, *, for_arg: bool) -> str:
        elt = t.element_type
        if for_arg:
            # Array parameters decay to pointers: the element type maps as
            # the decayed pointer's pointee (char buf[17] -> c_char_p).
            return self._map_pointee(elt, for_arg=True)
        try:
            n = t.get_array_size()
        except Exception:
            n = 0
        inner = self.map(elt, for_arg=False)
        return f"{inner} * {n}"

    def _map_typedef(self, t: Type) -> str:
        name = str(t.spelling)
        # Strip a leading ``const `` qualifier so the emitted name is a bare
        # Python identifier (``const kith_foo_t`` is not valid Python).
        if name.startswith("const "):
            name = name[len("const ") :]
        # Prefer the declared typedef spelling for stdint types so ``uint16_t``
        # maps to ``c_uint16`` (not the canonical ``unsigned short``, which has
        # no entry and falls back to ``c_void_p``).
        if name in _BUILTIN_MAP:
            return _BUILTIN_MAP[name]
        canonical = t.get_canonical()
        if canonical.kind == TypeKind.ENUM:
            return "ctypes.c_uint32"
        if canonical.kind == TypeKind.RECORD:
            # Reference the struct class by name (cross-module import added
            # when the name is declared in a different module).
            self.referenced_names.add(name)
            return name
        if canonical.kind == TypeKind.POINTER:
            pointee = canonical.get_pointee()
            if pointee.kind == TypeKind.FUNCTIONPROTO:
                # Named function-pointer typedef — reference the CFUNCTYPE alias.
                self.referenced_names.add(name)
                return name
            # Pointer to a typed thing.
            return self._map_pointer(canonical, for_arg=False)
        spelling = str(canonical.spelling)
        if spelling in _BUILTIN_MAP:
            return _BUILTIN_MAP[spelling]
        return "ctypes.c_void_p"

    def _map_record(self, t: Type) -> str:
        decl = t.get_declaration()
        name = str(decl.spelling or t.spelling)
        if name:
            self.referenced_names.add(name)
            return name
        return "ctypes.c_void_p"

    def _cfunc_expr(self, proto: Type) -> str:
        """Return a CFUNCTYPE(...) expression for a function proto type."""
        result = proto.get_result()
        restype = self.map(result, for_arg=False)
        args = [self.map(a, for_arg=True) for a in proto.argument_types()]
        joined = ", ".join([restype, *args])
        return f"ctypes.CFUNCTYPE({joined})"


def _collect_decls(
    tu: TranslationUnit, header: Path, module: str, registry: _TypeRegistry
) -> HeaderDecls:
    decls = HeaderDecls(module=module)
    for cursor in tu.cursor.walk_preorder():
        if not _in_file(cursor, header):
            continue
        kind = cursor.kind
        if kind == CursorKind.STRUCT_DECL and cursor.spelling:
            decl = _collect_struct(cursor, module, registry)
            if decl is not None:
                decls.structs.append(decl)
        elif kind == CursorKind.ENUM_DECL and cursor.spelling:
            decls.enums.append(_collect_enum(cursor))
        elif kind == CursorKind.TYPEDEF_DECL and cursor.spelling:
            _attach_typedef(cursor, decls)
        elif kind == CursorKind.FUNCTION_DECL and _is_api_function(cursor):
            if cursor.is_definition():
                continue
            decls.functions.append(_collect_function(cursor, module, registry))
    return decls


def _collect_struct(cursor: Cursor, module: str, registry: _TypeRegistry) -> StructDecl | None:
    name = str(cursor.spelling)
    if not name:
        return None
    mapper = _TypeMapper(registry, module)
    fields: list[tuple[str, str]] = []
    for child in cursor.get_children():
        if child.kind != CursorKind.FIELD_DECL:
            continue
        expr = mapper.map(child.type, for_arg=False)
        fields.append((str(child.spelling), expr))
    return StructDecl(name=name, typedef_name=None, fields=fields)


def _collect_enum(cursor: Cursor) -> EnumDecl:
    name = str(cursor.spelling)
    constants: list[tuple[str, int]] = []
    for child in cursor.get_children():
        if child.kind == CursorKind.ENUM_CONSTANT_DECL:
            constants.append((str(child.spelling), int(child.enum_value)))
    return EnumDecl(name=name, typedef_name=None, constants=constants)


def _attach_typedef(cursor: Cursor, decls: HeaderDecls) -> None:
    """Link a typedef to the struct/enum/funcptr it names."""
    name = str(cursor.spelling)
    underlying = cursor.underlying_typedef_type
    canonical = underlying.get_canonical()
    if canonical.kind == TypeKind.RECORD:
        struct_decl = canonical.get_declaration()
        tag = str(struct_decl.spelling)
        for s in decls.structs:
            if s.name == tag:
                s.typedef_name = name
                return
        # Typedef of an opaque struct (no definition in this header).
        # Register an opaque shell.
        decls.structs.append(StructDecl(name=tag, typedef_name=name, fields=[]))
    elif canonical.kind == TypeKind.ENUM:
        enum_decl = canonical.get_declaration()
        tag = str(enum_decl.spelling)
        for e in decls.enums:
            if e.name == tag:
                e.typedef_name = name
                return
    elif canonical.kind == TypeKind.POINTER:
        pointee = canonical.get_pointee()
        if pointee.kind == TypeKind.FUNCTIONPROTO:
            restype = _map_type_simple(pointee.get_result())
            argtypes = [_map_type_simple(a) for a in pointee.argument_types()]
            decls.funcptr_typedefs.append(
                FuncPtrTypedef(name=name, restype=restype, argtypes=argtypes)
            )


def _map_type_simple(t: Type) -> str:
    """Standalone type mapper for function-pointer typedef signatures.

    Uses the shared _TypeMapper against a throwaway registry so cross-module
    references resolve to bare names (resolved at emit time).
    """
    mapper = _TypeMapper(_TypeRegistry(), "")
    return mapper.map(t, for_arg=True)


def _collect_function(cursor: Cursor, module: str, registry: _TypeRegistry) -> FunctionDecl:
    mapper = _TypeMapper(registry, module)
    restype = mapper.map(cursor.result_type, for_arg=False)
    argtypes: list[str] = []
    for param in cursor.get_arguments():
        argtypes.append(mapper.map(param.type, for_arg=True))
    # The mapper's referenced_names are folded into the emit pass via a
    # fresh scan of the emitted expressions, so function signatures do not
    # need a separate cross-module import record here.
    return FunctionDecl(name=str(cursor.spelling), restype=restype, argtypes=argtypes)


# ---------------------------------------------------------------------------
# emission
# ---------------------------------------------------------------------------


_HEADER_COMMENT = (
    "# Auto-generated by {gen} from:\n"
    "{rel_lines}"
    "# Regenerate via `python3 {gen}`. Do not edit by hand.\n"
)


def _section_divider(title: str) -> list[str]:
    """Emit a three-line structural divider (AGENTS.md 1.7)."""
    rule = "# " + "-" * 75
    return [rule, f"# {title}", rule]


def _emit_module(
    module: str,
    decls: HeaderDecls,
    registry: _TypeRegistry,
    include_root: Path,
) -> str:
    """Return the Python source for one generated module file."""
    headers = _headers_for_module(module, include_root)
    rels: list[str] = []
    for header in headers:
        try:
            rels.append(str(header.resolve().relative_to(include_root.resolve())))
        except ValueError:
            rels.append(str(header))
    lines: list[str] = []
    rel_lines = "".join(f"#   {rel}\n" for rel in rels)
    lines.append(_HEADER_COMMENT.format(gen=_GENERATED_BY, rel_lines=rel_lines))
    lines.append("")
    lines.append("from __future__ import annotations")
    lines.append("")
    lines.append("import ctypes")
    if decls.enums:
        lines.append("from enum import IntEnum")
    lines.append("")

    # Collect cross-module imports: any referenced name declared in a
    # different module. Re-scan struct fields and function argtypes for
    # referenced type names via a token scan over the emitted expressions.
    referenced: dict[str, str] = {}  # name -> module

    def _record_refs(expr: str) -> None:
        for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr):
            if token in ("ctypes", "POINTER", "CFUNCTYPE", "None", "IntEnum"):
                continue
            if (
                token in registry.struct_names
                or token in registry.funcptr_names
                or token in registry.enum_names
            ):
                owner = registry.name_to_module.get(token)
                if owner and owner != module:
                    referenced[token] = owner

    for s in decls.structs:
        for _, expr in s.fields:
            _record_refs(expr)
    for fp in decls.funcptr_typedefs:
        _record_refs(fp.restype)
        for a in fp.argtypes:
            _record_refs(a)
    for fn in decls.functions:
        _record_refs(fn.restype)
        for a in fn.argtypes:
            _record_refs(a)

    if referenced:
        by_mod: dict[str, list[str]] = {}
        for name, mod in referenced.items():
            by_mod.setdefault(mod, []).append(name)
        for mod in sorted(by_mod):
            names = sorted(by_mod[mod])
            lines.append(f"from kith._generated.{mod} import {', '.join(names)}")
        lines.append("")

    # Opaque struct shells + exposed-layout structs. A struct declared in
    # another module (a forward ``typedef struct foo foo_t;`` this header
    # repeats so it is self-contained) is not re-emitted here: the name is
    # imported from its owning module via the cross-module import block above.
    for s in decls.structs:
        owner = registry.name_to_module.get(s.name, module)
        if owner != module:
            continue
        lines.append(f"class {s.name}(ctypes.Structure):")
        lines.append("    pass")
        lines.append("")
        if s.typedef_name and s.typedef_name != s.name:
            lines.append(f"{s.typedef_name} = {s.name}")
            lines.append("")

    # Enums.
    for e in decls.enums:
        owner = registry.name_to_module.get(e.name, module)
        if owner != module:
            continue
        lines.append(f"class {e.name}(IntEnum):")
        if not e.constants:
            lines.append("    pass")
        else:
            for cname, cval in e.constants:
                lines.append(f"    {cname} = {cval}")
        lines.append("")
        if e.typedef_name and e.typedef_name != e.name:
            lines.append(f"{e.typedef_name} = {e.name}")
            lines.append("")

    # Function-pointer typedefs.
    for fp in decls.funcptr_typedefs:
        owner = registry.name_to_module.get(fp.name, module)
        if owner != module:
            continue
        args = ", ".join([fp.restype, *fp.argtypes])
        lines.append(f"{fp.name} = ctypes.CFUNCTYPE({args})")
        lines.append("")

    # Struct field layouts (assigned after all shells + imports are defined).
    for s in decls.structs:
        owner = registry.name_to_module.get(s.name, module)
        if owner != module or not s.fields:
            continue
        lines.append(f"{s.name}._fields_ = [")
        for fname, fexpr in s.fields:
            lines.append(f'    ("{fname}", {fexpr}),')
        lines.append("]")
        lines.append("")

    # Macro constants.
    if decls.macros:
        lines.extend(_section_divider("macro constants"))
        for m in decls.macros:
            if isinstance(m.value, str):
                lines.append(f"{m.name} = {m.value}")
            else:
                lines.append(f"{m.name} = {m.value}")
        lines.append("")

    # Function signature table + configure().
    if decls.functions:
        lines.extend(_section_divider("function signatures"))
        lines.append("_FUNCTIONS: list[tuple[str, object, list[object]]] = [")
        for fn in decls.functions:
            args = ", ".join(fn.argtypes) if fn.argtypes else ""
            lines.append(f'    ("{fn.name}", {fn.restype}, [{args}]),')
        lines.append("]")
        lines.append("")
        lines.append("def configure(lib: ctypes.CDLL) -> None:")
        lines.append('    """Attach argtypes/restype to every KITH_API symbol on `lib`."""')
        lines.append("    for name, restype, argtypes in _FUNCTIONS:")
        lines.append("        fn = getattr(lib, name)")
        lines.append("        fn.restype = restype")
        lines.append("        fn.argtypes = argtypes")
        lines.append("")
    else:
        lines.append("def configure(lib: ctypes.CDLL) -> None:")
        lines.append('    """No KITH_API functions in this module."""')
        lines.append("    _ = lib")
        lines.append("")

    return "\n".join(lines)


_MODULE_ORDER: tuple[str, ...] = (
    "types",
    "version",
    "util",
    "config",
    "logger",
    "metrics",
    "proto",
    "sim",
    "aoi",
    "fabric",
    "net",
    "reactor",
    "worker",
    "state",
    "db",
    "control",
    "coord",
    "gateway",
    "client",
    "server",
)

# The control plane library is optional (CONTROL_PLANE_ENABLED=OFF omits it).
_OPTIONAL_MODULES: frozenset[str] = frozenset({"control"})


def _headers_for_module(module: str, include_root: Path) -> list[Path]:
    # Module headers live under include/kith/<module>/*.h (a module may span
    # several public headers, mirroring gen_symbols.py's discovery); the
    # cross-cutting types.h and version.h live at include/kith/<module>.h.
    nested = sorted((include_root / "kith" / module).rglob("*.h"))
    if nested:
        return nested
    top = include_root / "kith" / f"{module}.h"
    return [top] if top.is_file() else []


def _emit_init(modules: list[str], decls_by_module: dict[str, HeaderDecls]) -> str:
    # The __init__ imports exactly the modules configure() drives; a module
    # without KITH_API functions (types, version) has no library to attach.
    configured = [
        m for m in modules if (decls := decls_by_module.get(m)) is not None and decls.functions
    ]
    lines: list[str] = []
    lines.append("# Auto-generated by tools/gen_ctypes.py. Do not edit by hand.")
    lines.append("")
    lines.append("from __future__ import annotations")
    lines.append("")
    lines.append("import contextlib")
    lines.append("import ctypes")
    lines.append("from typing import Protocol")
    lines.append("")
    for m in configured:
        lines.append(f"from . import {m}")
    lines.append("")
    lines.append("")
    lines.append("class _LibProvider(Protocol):")
    lines.append("    def lib(self, name: str) -> ctypes.CDLL: ...")
    lines.append("")
    lines.append("")
    lines.append("def configure(bridge: _LibProvider) -> None:")
    lines.append('    """Attach argtypes/restype to every loaded module\'s KITH_API surface."""')
    for m in configured:
        if m in _OPTIONAL_MODULES:
            lines.append("    with contextlib.suppress(OSError):")
            lines.append(f'        {m}.configure(bridge.lib("{m}"))')
        else:
            lines.append(f'    {m}.configure(bridge.lib("{m}"))')
    lines.append("")
    return "\n".join(lines)


def _run_ruff(out_dir: Path) -> None:
    """Format and lint-fix generated files so checked-in output matches the drift check."""
    for argv in (
        ["ruff", "format", str(out_dir)],
        ["ruff", "check", "--fix", str(out_dir)],
    ):
        try:
            subprocess.run(argv, check=True, capture_output=True, text=True)
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            print(f"warning: {' '.join(argv)} unavailable ({exc})", file=sys.stderr)


def _collect_module_decls(
    index: Index,
    module: str,
    headers: list[Path],
    parse_args: list[str],
    registry: _TypeRegistry,
) -> HeaderDecls | None:
    """Parse every public header of @p module into one merged declaration set.

    The type registry is deliberately not touched here; callers register the
    merged set once so ownership follows the first module seen, as before.
    """
    decls = HeaderDecls(module=module)
    for header in headers:
        tu = index.parse(
            str(header),
            args=parse_args,
            options=TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD,
        )
        for diag in tu.diagnostics:
            if diag.severity >= 3:
                print(f"warning: {header}: {diag.spelling}", file=sys.stderr)
        part = _collect_decls(tu, header, module, registry)
        decls.enums.extend(part.enums)
        decls.structs.extend(part.structs)
        decls.funcptr_typedefs.extend(part.funcptr_typedefs)
        decls.functions.extend(part.functions)
        decls.macros.extend(_collect_macros(tu, header))
        decls.exported_names |= part.exported_names
    if not (
        decls.enums or decls.structs or decls.funcptr_typedefs or decls.functions or decls.macros
    ):
        return None
    return decls


def generate(include_root: Path, out_dir: Path) -> list[str]:
    """Generate bindings into ``out_dir``; return the list of written module names."""
    kith_root = include_root / "kith"
    if not kith_root.is_dir():
        raise FileNotFoundError(f"public headers not found: {kith_root}")

    sysinc = _system_include_args()
    parse_args = ["-std=c23", f"-I{include_root}", *sysinc]
    index = Index.create()

    # Pass 1: parse every header, collect declarations, build the type registry.
    all_decls: dict[str, HeaderDecls] = {}
    registry = _TypeRegistry()
    for module in _MODULE_ORDER:
        headers = _headers_for_module(module, include_root)
        if not headers:
            continue
        decls = _collect_module_decls(index, module, headers, parse_args, registry)
        if decls is None:
            continue
        registry.register(module, decls)
        all_decls[module] = decls

    # Pass 2: re-collect struct fields and function argtypes now that the
    # registry is complete (cross-module references resolve correctly).
    for module, _decls in all_decls.items():
        refreshed = _collect_module_decls(
            index, module, _headers_for_module(module, include_root), parse_args, registry
        )
        if refreshed is None:
            continue
        all_decls[module] = refreshed

    # Pass 3: emit.
    out_dir.mkdir(parents=True, exist_ok=True)
    modules: list[str] = []
    for module in _MODULE_ORDER:
        if module not in all_decls:
            continue
        source = _emit_module(module, all_decls[module], registry, include_root)
        (out_dir / f"{module}.py").write_text(source, encoding="utf-8")
        modules.append(module)

    (out_dir / "__init__.py").write_text(_emit_init(modules, all_decls), encoding="utf-8")

    _run_ruff(out_dir)
    return modules


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Generate Python ctypes bindings from public headers."
    )
    parser.add_argument(
        "--include-root",
        type=Path,
        default=Path("include"),
        help="public headers root (default: include)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("python/kith/_generated"),
        help="output directory for generated bindings",
    )
    args = parser.parse_args(argv[1:])

    try:
        modules = generate(args.include_root, args.out)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(f"wrote {len(modules)} modules to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

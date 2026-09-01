#!/usr/bin/env python3
"""Structural public-API contract enforcer for the kith framework.

Parses every public header under ``include/kith/`` with libclang and verifies
the public C surface against the framework's contracts (the public API
rules in the repository's own AGENTS.md):

  - size-versioned structs (``*_params_t`` / ``*_config_t`` / ``*_vtable_t``,
    plus any struct carrying ``uint32_t size`` + ``uint32_t abi_version``) begin
    with ``uint32_t size``, carry ``uint32_t abi_version``, and end with
    ``void *reserved[8]`` (not ``uint64_t reserved[4]``);
  - every ``KITH_API`` function documents ``@thread_safety`` in its doc
    comment, plus ``@ownership`` when it returns a pointer or transfers one
    through a pointer-to-pointer out-parameter;
  - every ``KITH_API`` function parameter carries a matching ``@param`` line
    (a parameter the doc block omits, or a ``@param`` naming a parameter the
    signature does not take, is flagged);
  - every opaque-handle typedef follows ``kith_<module>_*`` / ``kith_<module>_*_t``;
  - no public struct field is documented "do not access directly";
  - every size-versioned creation struct is named ``*_params_t`` or ``*_config_t``
    (vtables and status snapshots carry their own suffix);
  - the zero enumerator of a public enum is written ``= 0u`` (a bare ``= 0`` is
    flagged);
  - exposed-layout value-type DTOs meet the value-type criteria: no
    field hides an internal/opaque handle, and the struct is documented as a
    value-type DTO.

A deviation is reported as tracked debt when its signature appears in
``tools/public_api_baseline.txt``; a deviation absent from the baseline fails
the gate. Regenerate the baseline with ``--update-baseline`` only when
deliberately retiring a known deviation — the list shrinks over time, never
grows. The baseline records the known deviations, keeping each rule's
adoption independent of the repairs it implies.

Usage:
    check_public_api.py [--include-root <dir>] [--baseline <file>]
                        [--update-baseline]

The committed pre-commit hook runs with no arguments (defaults).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from clang.cindex import Cursor, CursorKind, Index, TranslationUnit, Type, TypeKind


HERE = Path(__file__).parent
BASELINE_PATH = HERE / "public_api_baseline.txt"

# Suffixes that force the size-versioned shape check by name. A struct named
# ``*_params`` / ``*_config`` / ``*_vtable`` / ``*_allocator`` is size-versioned
# by contract even when a malformed instance omits ``size``/``abi_version``. A
# ``*_status`` snapshot is size-versioned only when it actually carries
# ``size`` + ``abi_version`` (by shape): a ``*_status`` struct without them is
# an exposed-layout value-type DTO, not a size-versioned snapshot.
SHAPE_FORCED_SUFFIXES: tuple[str, ...] = ("_params", "_config", "_vtable", "_allocator")

# Suffixes exempt from the creation-name rule. Creation structs are
# ``*_params`` / ``*_config``; vtables, status snapshots, event records, and
# allocator contracts carry their own suffix and are not creation structs. An
# event record is a size-versioned DTO when it carries a fixed inline payload
# array (a fixed inline payload array); it is not a creation struct. An
# allocator contract is a size-versioned ops bundle with its own user-data
# slot, distinct from the pure-ops vtable shape.
CREATION_EXEMPT_SUFFIXES: tuple[str, ...] = (
    "_params",
    "_config",
    "_vtable",
    "_status",
    "_event",
    "_allocator",
)


@dataclass(frozen=True)
class Violation:
    """One contract deviation.

    Attributes:
        header_rel: path relative to the include root (e.g. ``kith/sim/sim.h``).
        rule_id: short stable rule identifier (part of the signature).
        symbol: the struct tag, typedef name, function name, or enum tag the
            deviation attaches to (stable across edits; never a line number).
        detail: human-readable explanation (not part of the signature).
        line: declaration line (1-based), for reporting only.
    """

    header_rel: str
    rule_id: str
    symbol: str
    detail: str
    line: int

    @property
    def signature(self) -> str:
        return f"{self.header_rel}|{self.rule_id}|{self.symbol}"


# ---------------------------------------------------------------------------
# libclang helpers (mirroring tools/gen_ctypes.py)
# ---------------------------------------------------------------------------


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
    except (OSError, ValueError) as exc:
        del exc
        return bool(loc.file.name == str(header))


def _is_api_function(cursor: Cursor) -> bool:
    """Return True when a FUNCTION_DECL carries ``visibility("default")``."""
    return any(child.kind == CursorKind.VISIBILITY_ATTR for child in cursor.get_children())


def _returns_or_transfers_pointer(cursor: Cursor) -> bool:
    """True when the function returns a pointer or takes a pointer-to-pointer
    out-parameter — the surfaces whose doc comment must state ownership.

    Resolution is canonical, so a typedef'd handle return counts as a
    pointer return and a borrowed single-pointer parameter does not count
    as a transfer."""
    if cursor.result_type.get_canonical().kind == TypeKind.POINTER:
        return True
    return any(
        arg.get_canonical().kind == TypeKind.POINTER
        and arg.get_canonical().get_pointee().kind == TypeKind.POINTER
        for arg in cursor.type.argument_types()
    )


def _is_uint32(field_type: Type) -> bool:
    """True for ``uint32_t`` (and its canonical ``unsigned int``)."""
    return bool(field_type.get_canonical().spelling == "unsigned int")


def _is_void_ptr_array_n(field_type: Type, count: int) -> bool:
    """True for ``void *<name>[count]``."""
    if field_type.kind != TypeKind.CONSTANTARRAY:
        return False
    el = field_type.element_type
    if el.kind != TypeKind.POINTER:
        return False
    if el.get_pointee().kind != TypeKind.VOID:
        return False
    try:
        return bool(field_type.get_array_size() == count)
    except Exception as exc:
        del exc
        return False


def _pointee_record_tag(pointee: Type) -> str:
    """Return the struct tag a pointer-to-record pointee names, else "".

    Handles typedef'd (``kith_foo_t *``), elaborated (``struct foo *``), and
    bare record pointees; returns "" for non-record pointees.
    """
    if pointee.kind in (TypeKind.TYPEDEF, TypeKind.ELABORATED):
        canonical = pointee.get_canonical()
        if canonical.kind == TypeKind.RECORD:
            return str(canonical.get_declaration().spelling)
    elif pointee.kind == TypeKind.RECORD:
        return str(pointee.get_declaration().spelling)
    return ""


def _field_type_spells_handle(t: Type, exposed_tags: set[str]) -> bool:
    """True when a field type is a pointer to an opaque handle struct.

    A pointer to an exposed-layout value type (e.g. a borrowed header array)
    is allowed by criterion (b); only a pointer to a struct that has no public
    definition (an opaque handle) disqualifies the value-type DTO.
    """
    if t.kind != TypeKind.POINTER:
        return False
    tag = _pointee_record_tag(t.get_pointee())
    if not tag:
        return False
    return tag not in exposed_tags


# ---------------------------------------------------------------------------
# doc-comment inspection
# ---------------------------------------------------------------------------


_DOC_LINE_PREFIX = re.compile(r"^\s*\* ?")


def _normalize_doc(text: str) -> str:
    """Collapse a Doxygen comment to a single space-separated string.

    Doxygen wraps prose across ``*``-prefixed continuation lines, so a phrase
    like "value type" can appear as "value\\n * type" in the raw comment.
    Stripping the per-line ``*`` markers and joining on spaces lets a substring
    search match phrases that the wrapping split apart.
    """
    t = text
    if t.startswith("/**"):
        t = t[3:]
    if t.endswith("*/"):
        t = t[:-2]
    lines = [_DOC_LINE_PREFIX.sub("", ln) for ln in t.splitlines()]
    return " ".join(lines)


def _doc_comment(cursor: Cursor) -> str:
    rc = cursor.raw_comment
    return rc if rc else ""


def _doc_has(cursor: Cursor, needle: str) -> bool:
    return needle.lower() in _normalize_doc(_doc_comment(cursor)).lower()


def _doc_param_names(cursor: Cursor) -> set[str]:
    """The parameter names the doc comment documents with ``@param``."""
    return set(re.findall(r"@param\s+(\w+)", _normalize_doc(_doc_comment(cursor))))


def _signature_param_names(cursor: Cursor) -> set[str]:
    """The declaration's parameter names, as spelled in the signature."""
    return {str(arg.spelling) for arg in cursor.get_arguments()}


# ---------------------------------------------------------------------------
# per-struct analysis
# ---------------------------------------------------------------------------


@dataclass
class StructFields:
    tag: str
    fields: list[tuple[str, Type, Cursor]]  # (name, type, field_cursor)
    is_definition: bool
    cursor: Cursor


def _collect_struct(cursor: Cursor) -> StructFields:
    fields: list[tuple[str, Type, Cursor]] = []
    for child in cursor.get_children():
        if child.kind == CursorKind.FIELD_DECL:
            fields.append((str(child.spelling), child.type, child))
    return StructFields(
        tag=str(cursor.spelling),
        fields=fields,
        is_definition=bool(cursor.is_definition()),
        cursor=cursor,
    )


def _has_size_abi_version(s: StructFields) -> bool:
    """True when the struct carries the size-versioned shape (size + abi_version)."""
    names = {name for name, _, _ in s.fields}
    if "size" not in names or "abi_version" not in names:
        return False
    for name, ftype, _ in s.fields:
        if name in ("size", "abi_version") and not _is_uint32(ftype):
            return False
    return True


def _is_size_versioned(s: StructFields) -> bool:
    if _has_size_abi_version(s):
        return True
    return s.tag.endswith(SHAPE_FORCED_SUFFIXES)


def _check_size_versioned_shape(s: StructFields, header_rel: str, line: int) -> list[Violation]:
    out: list[Violation] = []
    if not s.fields:
        return out
    first_name, first_type, _ = s.fields[0]
    if first_name != "size" or not _is_uint32(first_type):
        out.append(
            Violation(
                header_rel,
                "struct_size_first",
                s.tag,
                "size-versioned struct must begin with `uint32_t size;`",
                line,
            )
        )
    if not any(name == "abi_version" and _is_uint32(ft) for name, ft, _ in s.fields):
        out.append(
            Violation(
                header_rel,
                "struct_abi_version",
                s.tag,
                "size-versioned struct must carry `uint32_t abi_version;`",
                line,
            )
        )
    reserved = [(name, ft) for name, ft, _ in s.fields if name == "reserved"]
    if not reserved or not _is_void_ptr_array_n(reserved[0][1], 8):
        out.append(
            Violation(
                header_rel,
                "struct_reserved",
                s.tag,
                "size-versioned struct must end with `void *reserved[8];` "
                "(not `uint64_t reserved[4]` or missing)",
                line,
            )
        )
    return out


def _check_creation_name(s: StructFields, header_rel: str, line: int) -> list[Violation]:
    if not _has_size_abi_version(s):
        return []
    if s.tag.endswith(CREATION_EXEMPT_SUFFIXES):
        return []
    return [
        Violation(
            header_rel,
            "creation_struct_name",
            s.tag,
            "a size-versioned creation struct must be named `*_params_t` or "
            "`*_config_t` (vtables and status snapshots carry their own suffix)",
            line,
        )
    ]


def _check_value_type(
    s: StructFields, header_rel: str, line: int, exposed_tags: set[str]
) -> list[Violation]:
    out: list[Violation] = []
    if _is_size_versioned(s):
        return out
    if not _doc_has(s.cursor, "value type"):
        out.append(
            Violation(
                header_rel,
                "value_type_doc",
                s.tag,
                "exposed-layout struct not documented as a value-type DTO "
                "(doc comment must state it is a value type)",
                line,
            )
        )
    for name, ftype, fcursor in s.fields:
        if _doc_has(fcursor, "do not access"):
            out.append(
                Violation(
                    header_rel,
                    "value_type_handle_field",
                    f"{s.tag}.{name}",
                    "value-type DTO field hides an internal handle "
                    "(documented 'do not access directly')",
                    line,
                )
            )
            continue
        if _field_type_spells_handle(ftype, exposed_tags):
            out.append(
                Violation(
                    header_rel,
                    "value_type_handle_field",
                    f"{s.tag}.{name}",
                    "value-type DTO field is an opaque-handle pointer "
                    "(criterion (b): no internal/opaque handles as fields)",
                    line,
                )
            )
    return out


# The struct cursor is carried on StructFields; no global stash is needed.


def _check_field_do_not_access(s: StructFields, header_rel: str, line: int) -> list[Violation]:
    out: list[Violation] = []
    for name, _, fcursor in s.fields:
        if _doc_has(fcursor, "do not access"):
            out.append(
                Violation(
                    header_rel,
                    "field_do_not_access",
                    f"{s.tag}.{name}",
                    "public struct field documented 'do not access directly'; "
                    "make the type opaque instead",
                    line,
                )
            )
    return out


# ---------------------------------------------------------------------------
# enum and function checks
# ---------------------------------------------------------------------------


_ENUM_ZERO_BARE = re.compile(r"0[uUlL]*")


def _enum_zero_is_bare(constant_cursor: Cursor) -> bool:
    """True for an enumerator with value 0 written as a bare ``= 0`` (no u/U)."""
    if int(constant_cursor.enum_value) != 0:
        return False
    toks = [t.spelling for t in constant_cursor.get_tokens()]
    for i, tok in enumerate(toks):
        if tok == "=" and i + 1 < len(toks):
            val = toks[i + 1]
            return bool(_ENUM_ZERO_BARE.fullmatch(val) and not re.search(r"[uU]", val))
    return False


_TYPEDEF_NAME_RE = re.compile(r"^kith_[a-z0-9]+(?:_[a-z0-9]+)*_t$")
_TYPEDEF_TAG_RE = re.compile(r"^kith_[a-z0-9]+(?:_[a-z0-9]+)*$")


def _check_handle_typedef(cursor: Cursor, header_rel: str, line: int) -> list[Violation]:
    underlying = cursor.underlying_typedef_type
    if underlying is None:
        return []
    canonical = underlying.get_canonical()
    if canonical.kind != TypeKind.RECORD:
        return []
    tag = str(canonical.get_declaration().spelling)
    name = str(cursor.spelling)
    out: list[Violation] = []
    if not _TYPEDEF_NAME_RE.match(name) or not _TYPEDEF_TAG_RE.match(tag) or name != f"{tag}_t":
        out.append(
            Violation(
                header_rel,
                "handle_typedef_name",
                name,
                f"opaque-handle typedef must follow "
                f"`typedef struct kith_<module>_* kith_<module>_*_t;` "
                f"(got `typedef struct {tag} {name};`)",
                line,
            )
        )
    return out


# ---------------------------------------------------------------------------
# header scanning
# ---------------------------------------------------------------------------


def _public_headers(include_root: Path) -> list[Path]:
    kith = include_root / "kith"
    if not kith.is_dir():
        return []
    return sorted(p for p in kith.rglob("*.h") if p.is_file())


def _scan_header(
    header: Path,
    include_root: Path,
    index: Index,
    parse_args: list[str],
    exposed_tags: set[str],
) -> list[Violation]:
    header_rel = header.resolve().relative_to(include_root.resolve()).as_posix()
    tu = index.parse(
        str(header),
        args=parse_args,
        options=TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD,
    )
    for diag in tu.diagnostics:
        if diag.severity >= 3:
            print(f"warning: {header_rel}: {diag.spelling}", file=sys.stderr)

    violations: list[Violation] = []
    seen_typedefs: set[str] = set()

    for cursor in tu.cursor.walk_preorder():
        if not _in_file(cursor, header):
            continue
        kind = cursor.kind
        line = cursor.location.line if cursor.location else 0

        if kind == CursorKind.STRUCT_DECL and cursor.spelling:
            s = _collect_struct(cursor)
            if not s.is_definition:
                continue
            if _is_size_versioned(s):
                violations.extend(_check_size_versioned_shape(s, header_rel, line))
                violations.extend(_check_creation_name(s, header_rel, line))
            violations.extend(_check_value_type(s, header_rel, line, exposed_tags))
            violations.extend(_check_field_do_not_access(s, header_rel, line))

        elif kind == CursorKind.TYPEDEF_DECL and cursor.spelling:
            name = str(cursor.spelling)
            if name in seen_typedefs:
                continue
            seen_typedefs.add(name)
            violations.extend(_check_handle_typedef(cursor, header_rel, line))

        elif kind == CursorKind.ENUM_DECL and cursor.spelling:
            for child in cursor.get_children():
                if child.kind != CursorKind.ENUM_CONSTANT_DECL:
                    continue
                if _enum_zero_is_bare(child):
                    violations.append(
                        Violation(
                            header_rel,
                            "enum_zero_suffix",
                            f"{cursor.spelling}.{child.spelling}",
                            "zero enumerator must be written `= 0u` (bare `= 0` flagged)",
                            child.location.line if child.location else line,
                        )
                    )

        elif kind == CursorKind.FUNCTION_DECL and _is_api_function(cursor):
            if cursor.is_definition():
                continue
            name = str(cursor.spelling)
            if _returns_or_transfers_pointer(cursor) and not _doc_has(cursor, "@ownership"):
                violations.append(
                    Violation(
                        header_rel,
                        "func_ownership",
                        name,
                        "KITH_API function doc comment missing `@ownership`",
                        line,
                    )
                )
            if not _doc_has(cursor, "@thread_safety"):
                violations.append(
                    Violation(
                        header_rel,
                        "func_thread_safety",
                        name,
                        "KITH_API function doc comment missing `@thread_safety`",
                        line,
                    )
                )
            params = _signature_param_names(cursor)
            documented = _doc_param_names(cursor)
            if params and not params <= documented:
                missing = ", ".join(sorted(params - documented))
                violations.append(
                    Violation(
                        header_rel,
                        "func_param_docs",
                        name,
                        f"KITH_API function doc comment missing `@param` for: {missing}",
                        line,
                    )
                )
            elif not params and documented:
                phantom = ", ".join(sorted(documented))
                violations.append(
                    Violation(
                        header_rel,
                        "func_param_docs",
                        name,
                        f"KITH_API function documents parameters it does not take: {phantom}",
                        line,
                    )
                )
    return violations


def _collect_exposed_tags(
    headers: list[Path], include_root: Path, index: Index, parse_args: list[str]
) -> set[str]:
    """Pass 1: tags of structs that have a public definition (exposed layout).

    A pointer field whose pointee tag is absent from this set points to an
    opaque handle (a forward declaration with no public definition); a pointer
    to an exposed value type (present in the set) is allowed by criterion (b).
    """
    tags: set[str] = set()
    for header in headers:
        tu = index.parse(
            str(header),
            args=parse_args,
            options=TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD,
        )
        for cursor in tu.cursor.walk_preorder():
            if (
                cursor.kind == CursorKind.STRUCT_DECL
                and cursor.spelling
                and cursor.is_definition()
                and _in_file(cursor, header)
            ):
                tags.add(str(cursor.spelling))
    return tags


def scan_public_api(include_root: Path) -> list[Violation]:
    """Scan every public header under ``include_root/kith/`` and return violations."""
    headers = _public_headers(include_root)
    if not headers:
        return []
    parse_args = ["-std=c23", f"-I{include_root}", *_system_include_args()]
    index = Index.create()
    exposed_tags = _collect_exposed_tags(headers, include_root, index, parse_args)
    out: list[Violation] = []
    for header in headers:
        out.extend(_scan_header(header, include_root, index, parse_args, exposed_tags))
    return out


# ---------------------------------------------------------------------------
# baseline
# ---------------------------------------------------------------------------


def load_baseline(path: Path) -> set[str]:
    """Load baseline signatures (one per non-comment line)."""
    if not path.exists():
        return set()
    sigs: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        sigs.add(s)
    return sigs


def write_baseline(path: Path, violations: list[Violation]) -> None:
    sigs = sorted({v.signature for v in violations})
    lines = [
        "# Known public-API contract deviations tracked for retirement.",
        "# One signature per line: <header_rel>|<rule_id>|<symbol>.",
        "# A deviation listed here is reported as tracked debt and does not",
        "# fail the gate; a deviation absent from this file fails. Regenerate",
        "# with `python3 tools/check_public_api.py --update-baseline` only when",
        "# deliberately retiring a known deviation — the list shrinks over",
        "# time, never grows. Stale entries (no matching current deviation)",
        "# are reported by the checker as a prompt to remove them.",
        "",
        *sigs,
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _format(v: Violation, kind: str) -> str:
    return f"  [{kind}] {v.header_rel}:{v.line} {v.rule_id} {v.symbol}\n      {v.detail}"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Structural public-API contract enforcer (libclang)."
    )
    parser.add_argument(
        "--include-root",
        type=Path,
        default=Path("include"),
        help="public headers root (default: include)",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=BASELINE_PATH,
        help="baseline file of known deviations (default: tools/public_api_baseline.txt)",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="rewrite the baseline with the current deviation set and exit 0",
    )
    args = parser.parse_args(argv[1:])

    include_root = args.include_root.resolve()
    if not (include_root / "kith").is_dir():
        print(f"error: no public headers under {include_root}/kith/", file=sys.stderr)
        return 2

    violations = scan_public_api(include_root)

    if args.update_baseline:
        write_baseline(args.baseline, violations)
        print(
            f"wrote {len({v.signature for v in violations})} known deviations to {args.baseline}",
            file=sys.stderr,
        )
        return 0

    baseline = load_baseline(args.baseline)
    debt: list[Violation] = []
    fails: list[Violation] = []
    for v in violations:
        (debt if v.signature in baseline else fails).append(v)

    if debt:
        print("Tracked public-API deviations (allowed temporarily):")
        for v in sorted(debt, key=lambda x: x.signature):
            print(_format(v, "debt"))

    if fails:
        print("Public-API contract violations detected:")
        for v in sorted(fails, key=lambda x: (x.header_rel, x.line, x.rule_id)):
            print(_format(v, "FAIL"))
        print(f"Total new violations: {len(fails)}")
        return 1

    # Stale baseline entries: signatures the current headers do not declare.
    current = {v.signature for v in violations}
    stale = sorted(baseline - current)
    if stale:
        print("Stale baseline entries (deviation fixed; remove from baseline):")
        for sig in stale:
            print(f"  {sig}")

    if debt:
        print(f"OK: no new violations; tracked deviations: {len(debt)}.")
    else:
        print("OK: no public-API contract violations detected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

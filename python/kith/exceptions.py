"""Exception hierarchy for the kith framework: the ``kith_error``
translation family and the bridge loader's load-time family.

C functions report outcomes as small negative ``kith_error`` codes. The
boundary translates every negative return value into a Python exception so
user code never sees a raw integer. The hierarchy mirrors the error families
documented for the framework so callers can narrow a handler to one family
(``except KithNetworkError``) without inspecting individual codes.

Families:

- :class:`KithConfigError` — configuration source construction and typed
  accessors (a value that cannot be parsed or falls outside its range, a
  configuration file that cannot be read).
- :class:`KithNetworkError` — connection and transport failures (reset,
  timed out, peer shutdown).
- :class:`KithProtocolError` — wire-protocol decode and encode failures
  (a truncated frame, an unknown type id, a payload that exceeds the
  negotiated maximum).
- :class:`KithStateError` — lifecycle and ownership violations (an
  operation attempted in the wrong state, on a busy resource, or without
  permission).
- :class:`KithNotFoundError` — a named resource a lookup was asked for is
  absent.
- :class:`KithResponseOverflowError` — a control-plane response exceeded the
  connection's write buffer (a sizing condition the route boundary answers
  with the canonical counted rejection, not a handler exception).

Codes whose family is determined by the call site rather than the code
itself (``KITH_EINVAL``, ``KITH_EIO``, ``KITH_ENOMEM``, ``KITH_EABIVER``,
``KITH_ESIZE``, ``KITH_ERANGE``, ``KITH_EFAULT``, ``KITH_EOVERFLOW``,
``KITH_EUSER``) are raised as the base :class:`KithError` unless the
boundary call site names a narrower family through ``exc=``. Codes with a
single natural family are mapped automatically so the common case needs no
extra argument.

The bridge loader raises one family outside the ``kith_error`` translation:
:class:`BridgeError` (with :class:`BridgeLoadError` and
:class:`ABIVersionError`) when the shared libraries cannot be located or
their ABI generation differs from the package's. Load failures carry no C
outcome code, so the family rides :class:`RuntimeError` rather than
:class:`KithError`; :class:`BridgeLoadError` also subclasses
:class:`OSError` so ``except OSError`` handlers — the natural framing
around ``dlopen`` failures — catch it too.
"""

from __future__ import annotations

from typing import Final

from kith._generated import types as gen_types


__all__ = [
    "ABIVersionError",
    "BridgeError",
    "BridgeLoadError",
    "KithConfigError",
    "KithError",
    "KithNetworkError",
    "KithNotFoundError",
    "KithProtocolError",
    "KithResponseOverflowError",
    "KithStateError",
]


_UnknownCode = gen_types.kith_error


class KithError(Exception):
    """Base class for errors reported by the kith framework.

    Wraps a ``kith_error`` outcome code reported by a C function. The
    ``code`` attribute is the matching enumerator; the
    exception message is ``"<code.name>: <context>"`` (or just
    ``"<code.name>"`` when no context is supplied).

    Attributes:
        code: The ``kith_error`` enumerator matching the negative C return
            value. User-registered codes at and above ``KITH_EUSER`` are not
            enumerators in the public enum and are reported as
            ``KITH_EUSER`` so the category is preserved rather than dropped.
    """

    code: gen_types.kith_error

    def __init__(self, code: gen_types.kith_error, context: str = "") -> None:
        self.code = code
        text = f"{code.name}: {context}" if context else code.name
        super().__init__(text)


class KithConfigError(KithError):
    """A configuration source could not be built or a value is invalid."""


class KithNetworkError(KithError):
    """A connection or transport operation failed."""


class KithProtocolError(KithError):
    """A wire-protocol frame could not be encoded or decoded."""


class KithStateError(KithError):
    """An operation was attempted in the wrong lifecycle state."""


class KithNotFoundError(KithError):
    """A named resource a lookup was asked for is absent."""


class KithResponseOverflowError(KithError):
    """A control-plane response exceeded the connection's write buffer.

    The write buffer is fixed storage sized at create; an oversize response
    is a legal, operator-facing sizing condition, not a code defect. The
    route trampoline answers it with the canonical counted rejection instead
    of the handler-exception path.

    Attributes:
        attempted: The attempted body size in bytes when the failing call
            was ``Response.body()``, or ``None`` for the status and header
            builders, which announce no size.
    """

    attempted: int | None

    def __init__(
        self, code: gen_types.kith_error, context: str = "", attempted: int | None = None
    ) -> None:
        super().__init__(code, context)
        self.attempted = attempted


class BridgeError(RuntimeError):
    """Base class for bridge loading failures."""


class BridgeLoadError(BridgeError, OSError):
    """A shared library could not be located or loaded.

    Inherits :class:`OSError` so ``except OSError`` handlers (the natural
    framing around ``dlopen`` failures) catch bridge load failures too.
    """


class ABIVersionError(BridgeError):
    """Runtime ABI generation differs from the loader's expected generation."""


# Codes whose family is fixed regardless of which call site reported them.
# Codes absent from this table are ambiguous (their meaning depends on the
# call site) and are raised as the base KithError unless the boundary passes
# a narrower ``exc=`` to :func:`check_error`.
_FAMILY_BY_CODE: Final[dict[gen_types.kith_error, type[KithError]]] = {
    gen_types.kith_error.KITH_ENOENT: KithNotFoundError,
    gen_types.kith_error.KITH_EEXIST: KithStateError,
    gen_types.kith_error.KITH_EBUSY: KithStateError,
    gen_types.kith_error.KITH_EAGAIN: KithStateError,
    gen_types.kith_error.KITH_EPERM: KithStateError,
    gen_types.kith_error.KITH_ESTATE: KithStateError,
    gen_types.kith_error.KITH_EPROTO: KithProtocolError,
    gen_types.kith_error.KITH_ECONNRESET: KithNetworkError,
    gen_types.kith_error.KITH_ESHUTDOWN: KithNetworkError,
    gen_types.kith_error.KITH_ETIMEDOUT: KithNetworkError,
}


def code_for(rc: int) -> gen_types.kith_error:
    """Translate a negative C return value into its ``kith_error`` enumerator.

    User-registered codes at and above ``KITH_EUSER`` are not enumerators in
    the public enum; an unknown magnitude is reported as ``KITH_EUSER`` so the
    category is preserved rather than dropped.

    Args:
        rc: A negative C return value; its magnitude selects the
            enumerator.
    """
    magnitude = -rc
    try:
        return gen_types.kith_error(magnitude)
    except ValueError:
        return gen_types.kith_error.KITH_EUSER


def exception_for(code: gen_types.kith_error) -> type[KithError]:
    """Return the default exception class for an error code.

    Ambiguous codes (those absent from the family table) map to the base
    :class:`KithError`; the boundary call site overrides the choice by
    passing ``exc=`` to :func:`check_error`.

    Args:
        code: The ``kith_error`` code to classify.
    """
    return _FAMILY_BY_CODE.get(code, KithError)


def check_error(rc: int, context: str, *, exc: type[KithError] | None = None) -> None:
    """Raise a :class:`KithError` (or a subclass) when ``rc`` is negative.

    Args:
        rc: The C return value. Non-negative values are success and return
            without raising.
        context: A short string identifying the call site, folded into the
            exception message.
        exc: The exception class to raise. When ``None``, the class is chosen
            from :func:`exception_for` for codes with a fixed family, or the
            base :class:`KithError` for ambiguous codes. A boundary that knows
            the family (a config accessor, a network call) passes the narrower
            class so callers can catch it specifically.
    """
    if rc >= 0:
        return
    code = code_for(rc)
    cls = exc if exc is not None else exception_for(code)
    raise cls(code, context)

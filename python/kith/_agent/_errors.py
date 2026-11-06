"""Native client return-code translation."""

from __future__ import annotations

from collections.abc import Mapping


ERROR_NAMES: Mapping[int, str] = {
    1: "EINVAL",
    2: "ENOMEM",
    3: "ENOSYS",
    4: "ENOENT",
    5: "EEXIST",
    6: "EBUSY",
    7: "EAGAIN",
    8: "EPERM",
    9: "ERANGE",
    10: "EOVERFLOW",
    11: "EFAULT",
    12: "EIO",
    13: "ETIMEDOUT",
    14: "ECONNRESET",
    15: "ESHUTDOWN",
    16: "ESTATE",
    17: "EPROTO",
    18: "ESIZE",
    19: "EABIVER",
}


class KithClientError(Exception):
    """Raised when a kith client or proto C call returns a negative code."""

    def __init__(self, code: int, context: str = "") -> None:
        self.code = code
        self.context = context
        name = ERROR_NAMES.get(code, f"E{code}")
        msg = f"kith {name} ({code})"
        if context:
            msg = f"{msg}: {context}"
        super().__init__(msg)


def check(code: int, context: str = "") -> int:
    """Translate a C return: success passes through, negative raises."""
    if code < 0:
        raise KithClientError(-code, context)
    return code

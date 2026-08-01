#ifndef KITH_UTIL_UTIL_H
#define KITH_UTIL_UTIL_H

#include <stdint.h>

#include "kith/api.h"
#include "kith/version.h"

/**
 * @defgroup kith_util Utility foundations
 * @{
 */

/**
 * Runtime query of the framework release version string.
 *
 * Returns the same dotted form the KITH_VERSION_STRING macro expands to,
 * as a pointer to static storage, so callers that resolve the version at
 * runtime (e.g. a Python bridge or a control-plane introspection handler)
 * share a single stable definition with the compile-time macro.
 *
 * @return  Pointer to a static, NUL-terminated version string. Never NULL.
 * @thread_safety safe — the storage is immutable for the process lifetime.
 * @ownership callee — the returned pointer aliases static storage and must
 *          not be freed by the caller.
 */
KITH_API const char *kith_version_string(void);

/**
 * Runtime query of the current size-versioned struct ABI generation.
 *
 * Returns the value the KITH_ABI_VERSION macro expands to. Callers store
 * this in the abi_version field of a size-versioned config struct so the
 * framework can reject structs built against an incompatible generation.
 * Exposing it as a function lets a foreign-language bridge or a
 * separately-compiled plugin read the runtime ABI generation without
 * matching the macro at compile time.
 *
 * @return  The current ABI generation (a small positive unsigned value).
 * @thread_safety safe — the value is a compile-time constant.
 * @ownership caller — no ownership transfer; returns a value.
 */
KITH_API unsigned int kith_version_abi(void);

/**
 * Refuses or permits entry into the interpreter for its hosted callbacks.
 *
 * The Python bridge arms the gate (1) from an interpreter exit hook — a
 * phase that runs while the runtime still services foreign threads — and
 * never clears it again during process lifetime. While set, every
 * interpreter-hosted callback dispatch (the callback flags named
 * ``*_PYTHON`` across the server, gateway, and control modules) must be
 * refused: a foreign C thread that enters a finalizing interpreter has no
 * graceful path there. CPython parks threads it registered itself; a
 * thread that arrives through a foreign callback trampoline hits the
 * interpreter's fatal paths instead.
 *
 * The store is a plain state gate, not a synchronizer: a thread that
 * passes the check still serializes against the interpreter's own lock
 * before any entry, where the runtime's finalization guards apply.
 *
 * @param finalizing  1 to refuse interpreter-hosted callback entry, 0 to
 *                    allow it.
 * @thread_safety safe — release-store; readers synchronize with acquire.
 * @ownership caller — no ownership transfer; @p finalizing is a value.
 */
KITH_API void kith_python_finalizing_set(int finalizing);

/**
 * Reports whether interpreter-hosted callback entry is refused.
 *
 * @return  1 while the gate is set, 0 otherwise.
 * @thread_safety safe — acquire-load, synchronized with
 *          @ref kith_python_finalizing_set.
 * @ownership caller — no ownership transfer; returns a value.
 */
KITH_API int kith_python_finalizing(void);

/**
 * Counts one exception raised by an interpreter-hosted callback.
 *
 * The Python bridge calls this from the ctypes trampolines that guard the
 * registered tick, message, session-destroyed, and control-route handlers:
 * a handler's exception is caught at the trampoline boundary, printed to
 * stderr once per registration, and counted here so the failure rate is
 * observable without stderr traffic. The count is process-global by
 * design — the exception originates in the interpreter, not in any plane
 * handle, the same scope as @ref kith_python_finalizing.
 *
 * @thread_safety safe — relaxed atomic add.
 * @ownership caller — no ownership transfer.
 */
KITH_API void kith_python_note_handler_exception(void);

/**
 * Reports the running count of interpreter-hosted callback exceptions.
 *
 * The count accumulates for the process lifetime and never resets. The
 * server's tick pass records the delta into the metrics registry under
 * ``kith_python_handler_exceptions_total``.
 *
 * @return  The monotonic exception count since process start.
 * @thread_safety safe — relaxed atomic load.
 * @ownership caller — no ownership transfer; returns a value.
 */
KITH_API uint64_t kith_python_handler_exceptions(void);

/** @} */

#endif /* KITH_UTIL_UTIL_H */

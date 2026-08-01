/* Version accessors, the interpreter exit gate, and the interpreter
 * handler-exception counter. The public contract is
 * include/kith/util/util.h. */

#include "kith/util/util.h"

#include <stdatomic.h>
#include <stdint.h>

// 0 = interpreter-hosted callbacks may be entered, 1 = refuse entry. Static
// storage zero-initializes the atomic; the bridge sets 1 exactly once, in
// the interpreter exit hook.
static atomic_uint python_finalizing;

const char *kith_version_string(void)
{
    return KITH_VERSION_STRING;
}

unsigned int kith_version_abi(void)
{
    return KITH_ABI_VERSION;
}

void kith_python_finalizing_set(int finalizing)
{
    atomic_store_explicit(&python_finalizing, finalizing != 0 ? 1u : 0u, memory_order_release);
}

int kith_python_finalizing(void)
{
    return atomic_load_explicit(&python_finalizing, memory_order_acquire) != 0u;
}

// Exceptions raised by interpreter-hosted callbacks, counted by the Python
// bridge's handler guards. Static storage zero-initializes the atomic; the
// count never resets within a process.
static _Atomic uint64_t handler_exceptions;

void kith_python_note_handler_exception(void)
{
    atomic_fetch_add_explicit(&handler_exceptions, 1u, memory_order_relaxed);
}

uint64_t kith_python_handler_exceptions(void)
{
    return atomic_load_explicit(&handler_exceptions, memory_order_relaxed);
}

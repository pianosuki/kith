#ifndef KITH_TYPES_H
#define KITH_TYPES_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "kith/version.h"

/**
 * @defgroup kith_types Core types and error codes
 * @{
 */

/**
 * Error outcome codes for public functions. Functions return int: 0 on
 * success, the negation of a kith_error value on failure. Enumerators are
 * small positive magnitudes so the underlying width stays fixed at unsigned
 * int and the sign is applied at the return site. Values at and above
 * KITH_EUSER are reserved for game-specific codes registered by applications.
 */
enum kith_error : unsigned int
{
    KITH_OK = 0u,
    KITH_EINVAL = 1,
    KITH_ENOMEM = 2,
    KITH_ENOSYS = 3,
    KITH_ENOENT = 4,
    KITH_EEXIST = 5,
    KITH_EBUSY = 6,
    KITH_EAGAIN = 7,
    KITH_EPERM = 8,
    KITH_ERANGE = 9,
    KITH_EOVERFLOW = 10,
    KITH_EFAULT = 11,
    KITH_EIO = 12,
    KITH_ETIMEDOUT = 13,
    KITH_ECONNRESET = 14,
    KITH_ESHUTDOWN = 15,
    KITH_ESTATE = 16,
    KITH_EPROTO = 17,
    KITH_ESIZE = 18,
    KITH_EABIVER = 19,
    KITH_EUSER = 10000,
};

/** Alias of enum kith_error. */
typedef enum kith_error kith_error_t;

/**
 * Negate a kith_error enumerator into the int return value a public function
 * reports on failure (0 is success). The enum carries unsigned magnitudes so
 * its underlying width is fixed; the sign is applied here so return statements
 * stay sign-conversion-clean under -Wconversion. Use this for every negative
 * return rather than a bare unary minus on the enumerator.
 */
static inline int kith_error_return(kith_error_t code)
{
    return -(int)code;
}

/**
 * Memory allocation operations. Each receives the owning allocator's
 * @c user_data as its first argument, so one C function can serve several
 * allocator instances with distinct state. The four operations mirror the
 * libc primitives one for one: @c alloc has malloc semantics, @c alloc_zero
 * has calloc semantics (it rejects a @c count * @p size product that would
 * overflow), @c realloc has realloc semantics, and @c free has free
 * semantics (a NULL pointer is a no-op).
 */
typedef void *(*kith_alloc_fn)(void *ctx, size_t size);

/** Zero-initialized allocation with calloc semantics; see kith_alloc_fn. */
typedef void *(*kith_alloc_zero_fn)(void *ctx, size_t count, size_t size);

/** Resizing with realloc semantics; see kith_alloc_fn. */
typedef void *(*kith_realloc_fn)(void *ctx, void *ptr, size_t size);

/** Release with free semantics; see kith_alloc_fn. */
typedef void (*kith_free_fn)(void *ctx, void *ptr);

/**
 * Per-object memory allocator: the vtable every allocating kith create call
 * routes its allocations and frees through.
 *
 * Size-versioned like the params structs: callers set @c size to
 * sizeof(kith_allocator_t) and @c abi_version to KITH_ABI_VERSION at their
 * compile time; create calls reject an incompatible generation, an
 * undersized size, or any NULL operation with -KITH_EABIVER, -KITH_ESIZE,
 * and -KITH_EINVAL respectively. Passing NULL in an @c alloc parameter
 * selects kith_allocator_default.
 *
 * Each handle stores the allocator it was created with and routes every
 * allocation it performs over its lifetime — including teardown — through
 * that one instance: the allocator that creates a resource destroys it.
 * Mixing allocators across one object's lifetime is therefore impossible
 * by construction.
 *
 * @warning The allocator is borrowed, not copied: it must outlive every
 *          handle created against it. Freeing or overwriting the allocator
 *          (or the state its @c user_data points at) while such a handle
 *          exists, including during that handle's teardown, is a
 *          use-after-free.
 */
struct kith_allocator
{
    /** sizeof(kith_allocator_t) at the caller's compile time. */
    uint32_t size;

    /** KITH_ABI_VERSION at the caller's compile time. */
    uint32_t abi_version;

    /** Passed as the first argument to every operation. */
    void *user_data;

    /** malloc-semantics allocation; required. */
    kith_alloc_fn alloc;

    /** calloc-semantics allocation; required. */
    kith_alloc_zero_fn alloc_zero;

    /** realloc-semantics resizing; required. */
    kith_realloc_fn realloc;

    /** free-semantics release; required. */
    kith_free_fn free;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_allocator. */
typedef struct kith_allocator kith_allocator_t;

static inline void *kith_default_alloc(void *ctx, size_t size)
{
    (void)ctx;
    return malloc(size);
}

static inline void *kith_default_alloc_zero(void *ctx, size_t count, size_t size)
{
    (void)ctx;
    return calloc(count, size);
}

static inline void *kith_default_realloc(void *ctx, void *ptr, size_t size)
{
    (void)ctx;
    return realloc(ptr, size);
}

static inline void kith_default_free(void *ctx, void *ptr)
{
    (void)ctx;
    free(ptr);
}

/**
 * Report the default allocator: the instance backed by malloc, calloc,
 * realloc, and free. Create calls apply it wherever a caller passes NULL,
 * and callers building a wrapping allocator (counting, failing, logging)
 * can compose over it directly.
 */
static inline const kith_allocator_t *kith_allocator_default(void)
{
    static const kith_allocator_t instance = {
        .size = sizeof(kith_allocator_t),
        .abi_version = KITH_ABI_VERSION,
        .user_data = nullptr,
        .alloc = kith_default_alloc,
        .alloc_zero = kith_default_alloc_zero,
        .realloc = kith_default_realloc,
        .free = kith_default_free,
        .reserved = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    };
    return &instance;
}

/**
 * Validate a caller-supplied allocator against this build's contract:
 * generation, size, and the presence of all four operations. Returns
 * KITH_OK for a usable allocator, otherwise the error enumerator (not yet
 * negated) the create call must report. NULL needs no validation — create
 * calls treat it as kith_allocator_default directly.
 */
static inline kith_error_t kith_allocator_check(const kith_allocator_t *alloc)
{
    if (alloc->abi_version != KITH_ABI_VERSION)
    {
        return KITH_EABIVER;
    }
    if (alloc->size < sizeof(kith_allocator_t))
    {
        return KITH_ESIZE;
    }
    if (alloc->alloc == nullptr || alloc->alloc_zero == nullptr || alloc->realloc == nullptr ||
        alloc->free == nullptr)
    {
        return KITH_EINVAL;
    }
    return KITH_OK;
}

/**
 * Allocate @p size bytes through @p alloc (the default allocator when
 * NULL). Module internals route every allocation through these helpers so
 * a handle's allocations and frees share one allocator instance.
 */
static inline void *kith_alloc(const kith_allocator_t *alloc, size_t size)
{
    if (alloc == nullptr)
    {
        alloc = kith_allocator_default();
    }
    return alloc->alloc(alloc->user_data, size);
}

/** Allocate @p count zero-initialized elements of @p size bytes. */
static inline void *kith_alloc_zero(const kith_allocator_t *alloc, size_t count, size_t size)
{
    if (alloc == nullptr)
    {
        alloc = kith_allocator_default();
    }
    return alloc->alloc_zero(alloc->user_data, count, size);
}

/** Resize @p ptr to @p size bytes (realloc semantics). */
static inline void *kith_realloc(const kith_allocator_t *alloc, void *ptr, size_t size)
{
    if (alloc == nullptr)
    {
        alloc = kith_allocator_default();
    }
    return alloc->realloc(alloc->user_data, ptr, size);
}

/** Release @p ptr through @p alloc. A NULL pointer is a no-op that never
 * reaches the operation. */
static inline void kith_free(const kith_allocator_t *alloc, void *ptr)
{
    if (ptr == nullptr)
    {
        return;
    }
    if (alloc == nullptr)
    {
        alloc = kith_allocator_default();
    }
    alloc->free(alloc->user_data, ptr);
}

/**
 * Duplicate @p s into storage allocated through @p alloc (the default
 * allocator when NULL). The copy is released through kith_free with the same
 * allocator instance. @p s must not be NULL; NULL is returned only when the
 * allocation fails.
 */
static inline char *kith_strdup(const kith_allocator_t *alloc, const char *s)
{
    const size_t len = strlen(s);
    char *copy = kith_alloc(alloc, len + 1);
    if (copy != nullptr)
    {
        memcpy(copy, s, len + 1);
    }
    return copy;
}

/** @} */

#endif /* KITH_TYPES_H */

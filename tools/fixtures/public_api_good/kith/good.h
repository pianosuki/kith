#ifndef KITH_FIXTURE_GOOD_H
#define KITH_FIXTURE_GOOD_H

/*
 * Fixture for tools/check_public_api.py: a header that conforms to every
 * enforced rule. The self-test (tests/python/test_check_public_api.py) scans
 * this header and asserts zero violations. Lives under tools/fixtures/ so the
 * production checker (which scans include/) never sees it.
 */

#include <stdint.h>

#define KITH_API __attribute__((visibility("default")))

/** Opaque good handle. @ownership callee. */
typedef struct kith_good_handle kith_good_handle_t;

/**
 * Good creation parameters. Size-versioned: begins with uint32_t size and
 * uint32_t abi_version, ends with void *reserved[8].
 */
struct kith_good_params
{
    uint32_t size;
    uint32_t abi_version;
    uint32_t count;
    void *reserved[8];
};

typedef struct kith_good_params kith_good_params_t;

/**
 * A good exposed-layout value type. This is a value type: fixed layout,
 * borrowed pointers documented, no internal or opaque handles as fields.
 */
struct kith_good_point
{
    int32_t x;
    int32_t y;
};

typedef struct kith_good_point kith_good_point_t;

/** Good enum: the zero enumerator is written `= 0u`. */
enum kith_good_flag : unsigned int
{
    KITH_GOOD_FLAG_NONE = 0u,
    KITH_GOOD_FLAG_A = 1u,
};

typedef enum kith_good_flag kith_good_flag_t;

/**
 * Good public function.
 *
 * @param h Handle. Non-NULL.
 * @param p Point. Borrowed for the call only.
 * @return 0 on success, negative on failure.
 * @thread_safety safe
 * @ownership caller — @p h and @p p are borrowed for the call only.
 */
KITH_API int kith_good_do(kith_good_handle_t *h, const kith_good_point_t *p);

/**
 * Return the number of points registered with the handle.
 *
 * Value-returning: no pointer is returned or transferred, so the doc
 * carries no ownership tag.
 *
 * @param h Handle to query.
 * @return The registered point count; 0 when none is registered.
 * @thread_safety safe
 */
KITH_API int kith_good_point_count(kith_good_handle_t *h);

#endif /* KITH_FIXTURE_GOOD_H */

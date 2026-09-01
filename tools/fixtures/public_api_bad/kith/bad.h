#ifndef KITH_FIXTURE_BAD_H
#define KITH_FIXTURE_BAD_H

/*
 * Fixture for tools/check_public_api.py: a header that violates every enforced
 * rule. The self-test scans this header and asserts at least one violation per
 * rule. Lives under tools/fixtures/ so the production checker (which scans
 * include/) never sees it.
 */

#include <stdint.h>

#define KITH_API __attribute__((visibility("default")))

/** Opaque handle used as a value-type field below (violates criterion (b)). */
typedef struct kith_bad_opaque kith_bad_opaque_t;

/** A legitimately-named handle used only as a function parameter. */
typedef struct kith_bad_handle kith_bad_handle_t;

/** Misnamed handle typedef (not kith_<module>_*_t). */
typedef struct kith_bad_thing my_handle_t;

/**
 * Bad vtable: first field is abi_version (not size) and reserved is
 * uint64_t[4] (not void *[8]).
 */
struct kith_bad_vtable
{
    uint32_t abi_version;
    int (*step)(void);
    uint64_t reserved[4];
};

typedef struct kith_bad_vtable kith_bad_vtable_t;

/**
 * Bad creation struct: carries size + abi_version but is not named
 * *_params_t / *_config_t / *_vtable_t / *_status_t.
 */
struct kith_bad_widget
{
    uint32_t size;
    uint32_t abi_version;
    void *reserved[8];
};

typedef struct kith_bad_widget kith_bad_widget_t;

/**
 * Bad exposed-layout struct with an opaque-handle field and a do-not-access
 * field. The doc omits the value-type designation.
 */
struct kith_bad_dto
{
    int32_t a;
    kith_bad_opaque_t *handle_field;
    /** Internal handle. Do not access directly. */
    void *secret;
};

typedef struct kith_bad_dto kith_bad_dto_t;

/** Bad enum: the zero enumerator is a bare `= 0`. */
enum kith_bad_flag : unsigned int
{
    KITH_BAD_FLAG_NONE = 0,
    KITH_BAD_FLAG_A = 1u,
};

typedef enum kith_bad_flag kith_bad_flag_t;

/**
 * Bad public function: the doc omits the ownership and thread-safety tags
 * despite returning a pointer.
 *
 * @return The handle on success.
 */
KITH_API kith_bad_handle_t *kith_bad_do(kith_bad_handle_t *h);

#endif /* KITH_FIXTURE_BAD_H */

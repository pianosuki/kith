#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/aoi/aoi.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "aoi create: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// A NULL out slot is rejected with EINVAL. NULL params selects all defaults;
// the handle builds and reports zero objects. destroy(NULL) is a no-op.
static int test_create_arg_validation(void)
{
    int failures = 0;
    CHECK(kith_aoi_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);
    CHECK(a != nullptr);
    CHECK(kith_aoi_size(a) == 0u);

    kith_aoi_destroy(a);
    kith_aoi_destroy(nullptr);
    return failures;
}

// Params are size-versioned: an incompatible abi_version or an undersized
// size is rejected. A zero cell_size and bucket_count select the defaults
// and build a working handle.
static int test_create_params_validation(void)
{
    int failures = 0;
    kith_aoi_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.cell_size = 0;
    params.bucket_count = 0u;

    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(&params, nullptr, &a) == 0);
    CHECK(a != nullptr);
    CHECK(kith_aoi_size(a) == 0u);
    kith_aoi_destroy(a);

    // Wrong abi_version.
    params.abi_version = KITH_ABI_VERSION
    +1u;
    a = nullptr;
    CHECK(kith_aoi_create(&params, nullptr, &a) == kith_error_return(KITH_EABIVER));
    CHECK(a == nullptr);

    // Undersized size.
    params.abi_version = KITH_ABI_VERSION;
    params.size = sizeof(params) - 1u;
    a = nullptr;
    CHECK(kith_aoi_create(&params, nullptr, &a) == kith_error_return(KITH_ESIZE));
    CHECK(a == nullptr);
    return failures;
}

// A negative cell_size is rejected (zero selects the default; only a
// non-zero value must be positive). A custom positive cell_size buckets
// positions on its own grid.
static int test_create_negative_cell_size(void)
{
    int failures = 0;
    kith_aoi_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;

    params.cell_size = -1;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(&params, nullptr, &a) == kith_error_return(KITH_EINVAL));
    CHECK(a == nullptr);

    // A 4-world-unit cell grid buckets a position at 5.0 into cell 1.
    params.cell_size = (int64_t)4 << KITH_AOI_FIX_SHIFT;
    CHECK(kith_aoi_create(&params, nullptr, &a) == 0);
    CHECK(a != nullptr);

    kith_aoi_object_t obj = {0};
    obj.id = 1u;
    obj.pos_x = (int64_t)5 << KITH_AOI_FIX_SHIFT;
    obj.pos_y = 0;
    obj.pos_z = 0;
    obj.radius = 0;
    CHECK(kith_aoi_insert(a, &obj) == 0);
    CHECK(kith_aoi_size(a) == 1u);

    kith_aoi_object_t out = {0};
    CHECK(kith_aoi_lookup(a, 1u, &out) == 0);
    CHECK(out.pos_x == (int64_t)5 << KITH_AOI_FIX_SHIFT);

    kith_aoi_destroy(a);
    return failures;
}

// A small bucket count is rounded up to a power of two with a floor, so a
// handle with a tiny bucket count still indexes objects (the open-addressed
// tables rely on power-of-two masking).
static int test_create_small_bucket_count(void)
{
    int failures = 0;
    kith_aoi_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.bucket_count = 1u;

    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(&params, nullptr, &a) == 0);
    CHECK(a != nullptr);

    for (uint64_t i = 1u; i <= 8u; ++i)
    {
        kith_aoi_object_t obj = {0};
        obj.id = i;
        obj.pos_x = (int64_t)i << KITH_AOI_FIX_SHIFT;
        CHECK(kith_aoi_insert(a, &obj) == 0);
    }
    CHECK(kith_aoi_size(a) == 8u);

    kith_aoi_destroy(a);
    return failures;
}

// Every public entry point rejects a NULL handle (and NULL sub-argument where
// the function takes one) with EINVAL. size on NULL returns 0.
static int test_public_arg_validation(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t obj = {0};
    obj.id = 1u;
    obj.pos_x = 0;
    obj.pos_y = 0;
    obj.pos_z = 0;
    obj.radius = 0;

    CHECK(kith_aoi_insert(nullptr, &obj) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_insert(a, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_update(nullptr, &obj) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_update(a, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_remove(nullptr, 1u) == kith_error_return(KITH_EINVAL));

    kith_aoi_object_t out = {0};
    CHECK(kith_aoi_lookup(nullptr, 1u, &out) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_lookup(a, 1u, nullptr) == kith_error_return(KITH_EINVAL));

    kith_aoi_sphere_t sphere = {0};
    kith_aoi_box_t box = {0};
    CHECK(kith_aoi_query_sphere(nullptr, &sphere, nullptr, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_query_sphere(a, nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_query_sphere(a, &sphere, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_query_box(nullptr, &box, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_query_box(a, nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_query_box(a, &box, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_aoi_size(nullptr) == 0u);

    kith_aoi_destroy(a);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_arg_validation();
    rc |= test_create_params_validation();
    rc |= test_create_negative_cell_size();
    rc |= test_create_small_bucket_count();
    rc |= test_public_arg_validation();
    if (rc != 0)
    {
        (void)fprintf(stderr, "aoi create tests FAILED\n");
    }
    return rc;
}

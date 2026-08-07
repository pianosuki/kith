/* Bucket-domain and capacity contracts of the AOI index: the create-time
 * bucket-count ceiling (KITH_AOI_MAX_BUCKET_COUNT) on both sides of the
 * boundary, the power-of-two rounding ladder up to that ceiling, and the
 * full-index error class — capacity exhaustion reports -KITH_EBUSY, not
 * -KITH_ENOMEM, and the object and cell indexes differ in what a removal
 * frees. Compiles aoi.c directly because the validation and
 * bucket-resolution helpers are hidden from the shared library, and the
 * ceiling's accept side cannot be reached through the public create
 * without allocating 2^31 buckets. */

#include <stdint.h>
#include <stdio.h>

#include "aoi/aoi_internal.h"
#include "kith/aoi/aoi.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "aoi buckets: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static kith_aoi_params_t params_with_bucket_count(uint32_t bucket_count)
{
    kith_aoi_params_t params;
    params.size = (uint32_t)sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.cell_size = 0;
    params.bucket_count = bucket_count;
    params.pad = 0u;
    for (size_t i = 0; i < 8; ++i)
    {
        params.reserved[i] = nullptr;
    }
    return params;
}

static int test_validate_bucket_ceiling(void)
{
    int failures = 0;
    kith_error_t err = KITH_OK;

    /* Exactly at the ceiling the request is well-formed: the power-of-two
     * rounding stops on the boundary value itself. */
    kith_aoi_params_t at = params_with_bucket_count(KITH_AOI_MAX_BUCKET_COUNT);
    CHECK(aoi_params_validate(&at, &err));
    CHECK(err == KITH_OK);

    kith_aoi_params_t above = params_with_bucket_count(KITH_AOI_MAX_BUCKET_COUNT + 1u);
    CHECK(!aoi_params_validate(&above, &err));
    CHECK(err == KITH_EINVAL);

    kith_aoi_params_t max = params_with_bucket_count(UINT32_MAX);
    CHECK(!aoi_params_validate(&max, &err));
    CHECK(err == KITH_EINVAL);

    kith_aoi_params_t zero = params_with_bucket_count(0u);
    CHECK(aoi_params_validate(&zero, &err));
    CHECK(err == KITH_OK);

    return failures;
}

static int test_resolve_buckets_ladder(void)
{
    int failures = 0;

    CHECK(aoi_resolve_buckets(0u) == 16u);
    CHECK(aoi_resolve_buckets(1u) == 16u);
    CHECK(aoi_resolve_buckets(15u) == 16u);
    CHECK(aoi_resolve_buckets(16u) == 16u);
    CHECK(aoi_resolve_buckets(17u) == 32u);
    CHECK(aoi_resolve_buckets(4095u) == 4096u);
    CHECK(aoi_resolve_buckets(1u << 30) == 1u << 30);
    CHECK(aoi_resolve_buckets(KITH_AOI_MAX_BUCKET_COUNT) == KITH_AOI_MAX_BUCKET_COUNT);

    return failures;
}

static int test_create_rejects_above_ceiling(void)
{
    int failures = 0;
    kith_aoi_t *aoi = nullptr;

    kith_aoi_params_t above = params_with_bucket_count(KITH_AOI_MAX_BUCKET_COUNT + 1u);
    CHECK(kith_aoi_create(&above, nullptr, &aoi) == -(int)KITH_EINVAL);
    CHECK(aoi == nullptr);

    kith_aoi_params_t max = params_with_bucket_count(UINT32_MAX);
    CHECK(kith_aoi_create(&max, nullptr, &aoi) == -(int)KITH_EINVAL);
    CHECK(aoi == nullptr);

    /* The default path (no params) still builds and tears down. */
    CHECK(kith_aoi_create(nullptr, nullptr, &aoi) == 0);
    CHECK(aoi != nullptr);
    kith_aoi_destroy(aoi);

    return failures;
}

static kith_aoi_object_t object_at(uint64_t id, uint32_t cell_index)
{
    kith_aoi_object_t obj;
    obj.id = id;
    obj.pos_x = (int64_t)cell_index * (int64_t)KITH_AOI_DEFAULT_CELL_SIZE;
    obj.pos_y = 0;
    obj.pos_z = 0;
    obj.radius = 0;
    obj.user_ptr = nullptr;
    return obj;
}

static int test_capacity_error_class(void)
{
    int failures = 0;
    kith_aoi_t *aoi = nullptr;

    kith_aoi_params_t params = params_with_bucket_count(16u);
    CHECK(kith_aoi_create(&params, nullptr, &aoi) == 0);

    /* Fill all 16 object slots, each object in its own cell, so both
     * indexes reach capacity together. */
    for (uint32_t i = 0u; i < 16u; ++i)
    {
        kith_aoi_object_t obj = object_at(i + 1u, i + 1u);
        CHECK(kith_aoi_insert(aoi, &obj) == 0);
    }

    /* The 17th insert hits the full object index: capacity, not
     * allocation. */
    kith_aoi_object_t extra = object_at(99u, 99u);
    CHECK(kith_aoi_insert(aoi, &extra) == -(int)KITH_EBUSY);

    /* Removing an object frees its slot but not its cell: a new id in a
     * new cell still cannot enter. */
    CHECK(kith_aoi_remove(aoi, 1u) == 0);
    kith_aoi_object_t roam = object_at(100u, 40u);
    CHECK(kith_aoi_insert(aoi, &roam) == -(int)KITH_EBUSY);

    /* The recycled object slot accepts an id whose cell already exists. */
    kith_aoi_object_t reinsert = object_at(100u, 2u);
    CHECK(kith_aoi_insert(aoi, &reinsert) == 0);

    /* A move into a new cell reports the same capacity class and leaves
     * the object in its old cell. */
    kith_aoi_object_t moved = object_at(100u, 50u);
    CHECK(kith_aoi_update(aoi, &moved) == -(int)KITH_EBUSY);
    kith_aoi_object_t read;
    CHECK(kith_aoi_lookup(aoi, 100u, &read) == 0);
    CHECK(read.pos_x == (int64_t)2 * (int64_t)KITH_AOI_DEFAULT_CELL_SIZE);

    kith_aoi_destroy(aoi);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_validate_bucket_ceiling();
    failures += test_resolve_buckets_ladder();
    failures += test_create_rejects_above_ceiling();
    failures += test_capacity_error_class();
    if (failures)
    {
        (void)fprintf(stderr, "aoi buckets: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

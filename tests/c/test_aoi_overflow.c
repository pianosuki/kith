/* Overflow-boundary behavior of the AOI distance predicates. Positions and
 * radii are Q16.16 raw values; the squared comparison is exact even when the
 * squared distance or the squared radius exceeds the int64 range, per-axis
 * separation excludes an object before any squaring, and region arithmetic
 * saturates for geometry whose reach exceeds the coordinate span. The cell
 * size is raised so wide-radius regions cover a handful of cells. */

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/aoi/aoi.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "aoi overflow: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static kith_aoi_object_t make_raw_obj(uint64_t id, int64_t x, int64_t y, int64_t z, int64_t radius)
{
    kith_aoi_object_t o = {0};
    o.id = id;
    o.pos_x = x;
    o.pos_y = y;
    o.pos_z = z;
    o.radius = radius;
    return o;
}

struct collector
{
    uint64_t ids[64];
    size_t n;
};

static bool collect_visit(const kith_aoi_object_t *obj, void *user)
{
    struct collector *c = user;
    if (c->n < 64u)
    {
        c->ids[c->n] = obj->id;
    }
    c->n += 1u;
    return true;
}

static bool collector_has(const struct collector *c, uint64_t id)
{
    for (size_t i = 0u; i < c->n && i < 64u; ++i)
    {
        if (c->ids[i] == id)
        {
            return true;
        }
    }
    return false;
}

static kith_aoi_t *make_wide_index(int64_t cell_size)
{
    kith_aoi_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.cell_size = cell_size;
    kith_aoi_t *aoi = nullptr;
    if (kith_aoi_create(&params, nullptr, &aoi) != 0)
    {
        return nullptr;
    }
    return aoi;
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// A squared radius beyond 2^63 must not flip the decision: the object sits
// plainly inside the sphere, and a wrapping squared radius turns the
// comparison negative and excludes it.
static int test_sphere_radius_wrap_excludes(void)
{
    int failures = 0;
    kith_aoi_t *aoi = make_wide_index(1LL << 40);
    CHECK(aoi != nullptr);

    kith_aoi_object_t obj = make_raw_obj(1u, 1'000'000'000, 0, 0, 0);
    CHECK(kith_aoi_insert(aoi, &obj) == 0);

    kith_aoi_sphere_t s = {0};
    s.cx = 0;
    s.cy = 0;
    s.cz = 0;
    s.radius = 3'050'000'000;
    struct collector c = {0};
    CHECK(kith_aoi_query_sphere(aoi, &s, collect_visit, &c) == 0);
    CHECK(c.n == 1u);
    CHECK(collector_has(&c, 1u));

    kith_aoi_destroy(aoi);
    return failures;
}

// Three axes each inside the radius can still sum past 2^63: a wrapping
// squared distance turns negative and includes an object that lies outside.
static int test_sphere_distance_wrap_includes(void)
{
    int failures = 0;
    kith_aoi_t *aoi = make_wide_index(1LL << 40);
    CHECK(aoi != nullptr);

    kith_aoi_object_t obj = make_raw_obj(2u, 1'760'000'000, 1'760'000'000, 1'760'000'000, 0);
    CHECK(kith_aoi_insert(aoi, &obj) == 0);

    kith_aoi_sphere_t s = {0};
    s.cx = 0;
    s.cy = 0;
    s.cz = 0;
    s.radius = 3'030'000'000;
    struct collector c = {0};
    CHECK(kith_aoi_query_sphere(aoi, &s, collect_visit, &c) == 0);
    CHECK(c.n == 0u);

    kith_aoi_destroy(aoi);
    return failures;
}

// With every per-axis separation inside the radius, the squared comparison
// runs entirely beyond the int64 range and still orders the two sides
// exactly: just outside for one radius, inside for the larger one.
static int test_sphere_sum_overflow_boundary(void)
{
    int failures = 0;
    kith_aoi_t *aoi = make_wide_index(1LL << 40);
    CHECK(aoi != nullptr);

    kith_aoi_object_t obj = make_raw_obj(3u, 2'000'000'000, 2'000'000'000, 2'000'000'000, 0);
    CHECK(kith_aoi_insert(aoi, &obj) == 0);

    kith_aoi_sphere_t s = {0};
    s.cx = 0;
    s.cy = 0;
    s.cz = 0;
    s.radius = 3'400'000'000;
    struct collector c = {0};
    CHECK(kith_aoi_query_sphere(aoi, &s, collect_visit, &c) == 0);
    CHECK(c.n == 0u);

    s.radius = 3'500'000'000;
    struct collector c2 = {0};
    CHECK(kith_aoi_query_sphere(aoi, &s, collect_visit, &c2) == 0);
    CHECK(c2.n == 1u);
    CHECK(collector_has(&c2, 3u));

    kith_aoi_destroy(aoi);
    return failures;
}

// A separation that does not fit int64 on one axis (positions 2^62 apart)
// is decided exactly: the nearer radius excludes, and a combined radius of
// 2^63 — itself unrepresentable — includes at the exact d2 == r2 boundary.
// The region endpoints are derived beyond int64 so the object's cell stays
// inside the scanned region.
static int test_sphere_sub_overflow_delta(void)
{
    int failures = 0;
    // The saturated reach spans thousands of cells at the 2^40 cell size;
    // a 2^62 cell keeps the region at a handful of cells while the object
    // positions stay representable.
    kith_aoi_t *aoi = make_wide_index(1LL << 62);
    CHECK(aoi != nullptr);

    kith_aoi_object_t far_obj = make_raw_obj(4u, 1LL << 62, 0, 0, 0);
    CHECK(kith_aoi_insert(aoi, &far_obj) == 0);
    kith_aoi_object_t big_obj = make_raw_obj(5u, 1LL << 62, 0, 0, 1LL << 62);
    CHECK(kith_aoi_insert(aoi, &big_obj) == 0);

    kith_aoi_sphere_t s = {0};
    s.cx = -(1LL << 62);
    s.cy = 0;
    s.cz = 0;
    s.radius = 1LL << 62;
    struct collector c = {0};
    CHECK(kith_aoi_query_sphere(aoi, &s, collect_visit, &c) == 0);
    CHECK(c.n == 1u);
    CHECK(collector_has(&c, 5u));
    CHECK(!collector_has(&c, 4u));

    kith_aoi_destroy(aoi);
    return failures;
}

// The box predicate's clamp distance and squared radius run through the
// same exact comparison: an object just past a box face is included even
// though its squared radius exceeds 2^63, and an object far beyond the box
// is excluded by a squared distance far beyond any 64-bit width.
static int test_box_exact_compare(void)
{
    int failures = 0;
    kith_aoi_t *aoi = make_wide_index(1LL << 40);
    CHECK(aoi != nullptr);

    kith_aoi_object_t near_obj =
        make_raw_obj(6u, 2'000'000'000, 2'000'000'000, 2'000'000'000, 3'500'000'000);
    CHECK(kith_aoi_insert(aoi, &near_obj) == 0);
    kith_aoi_object_t far_obj =
        make_raw_obj(7u, -(9LL << 40), -(9LL << 40), -(9LL << 40), 3'500'000'000);
    CHECK(kith_aoi_insert(aoi, &far_obj) == 0);

    kith_aoi_box_t box = {0};
    box.min_x = 1'000'000'000;
    box.min_y = 1'000'000'000;
    box.min_z = 1'000'000'000;
    box.max_x = 1'500'000'000;
    box.max_y = 1'500'000'000;
    box.max_z = 1'500'000'000;
    struct collector c = {0};
    CHECK(kith_aoi_query_box(aoi, &box, collect_visit, &c) == 0);
    CHECK(c.n == 1u);
    CHECK(collector_has(&c, 6u));
    CHECK(!collector_has(&c, 7u));

    box.min_x = -(8LL << 40);
    box.min_y = -(8LL << 40);
    box.min_z = -(8LL << 40);
    box.max_x = -(6LL << 40);
    box.max_y = -(6LL << 40);
    box.max_z = -(6LL << 40);
    struct collector c2 = {0};
    CHECK(kith_aoi_query_box(aoi, &box, collect_visit, &c2) == 0);
    CHECK(c2.n == 0u);

    kith_aoi_destroy(aoi);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_sphere_radius_wrap_excludes();
    rc |= test_sphere_distance_wrap_includes();
    rc |= test_sphere_sum_overflow_boundary();
    rc |= test_sphere_sub_overflow_delta();
    rc |= test_box_exact_compare();

    if (rc != 0)
    {
        (void)fprintf(stderr, "aoi overflow tests FAILED\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

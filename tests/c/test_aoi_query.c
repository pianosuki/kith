#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/aoi/aoi.h"
#include "kith/types.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "aoi query: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static kith_aoi_object_t make_obj(uint64_t id, int64_t x, int64_t y, int64_t z, int64_t r)
{
    kith_aoi_object_t o = {0};
    o.id = id;
    o.pos_x = x << KITH_AOI_FIX_SHIFT;
    o.pos_y = y << KITH_AOI_FIX_SHIFT;
    o.pos_z = z << KITH_AOI_FIX_SHIFT;
    o.radius = r << KITH_AOI_FIX_SHIFT;
    return o;
}

struct collector
{
    uint64_t ids[64];
    size_t n;
    // Stop after this many visits (0 means never stop).
    size_t stop_at;
};

static bool collect_visit(const kith_aoi_object_t *obj, void *user)
{
    struct collector *c = user;
    if (c->n < 64u)
    {
        c->ids[c->n] = obj->id;
    }
    c->n += 1u;
    return c->stop_at == 0u || c->n < c->stop_at;
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

// A sphere query visits the in-region object whose center is inside the
// query sphere, and skips a far object whose center is outside the query
// region entirely.
static int test_query_sphere_in_and_out(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t near = make_obj(1u, 8, 0, 0, 0);
    kith_aoi_object_t far = make_obj(2u, 100, 0, 0, 0);
    CHECK(kith_aoi_insert(a, &near) == 0);
    CHECK(kith_aoi_insert(a, &far) == 0);

    kith_aoi_sphere_t s = {0};
    s.cx = (int64_t)8 << KITH_AOI_FIX_SHIFT;
    s.cy = 0;
    s.cz = 0;
    s.radius = (int64_t)2 << KITH_AOI_FIX_SHIFT;

    struct collector c = {0};
    CHECK(kith_aoi_query_sphere(a, &s, collect_visit, &c) == 0);
    CHECK(c.n == 1u);
    CHECK(collector_has(&c, 1u));
    CHECK(!collector_has(&c, 2u));

    kith_aoi_destroy(a);
    return failures;
}

// An object whose center sits in a neighboring cell but whose bounding
// sphere reaches into the query region is still visited: the index expands
// the scanned region by the largest stored radius so a center-bucketed
// object is never missed. A zero-radius object in the same neighboring
// cell is scanned but filtered out by the predicate.
static int test_query_sphere_radius_leak(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t in_cell0 = make_obj(1u, 8, 0, 0, 0);
    kith_aoi_object_t leaker = make_obj(2u, 20, 0, 0, 13);
    kith_aoi_object_t neighbour = make_obj(3u, 24, 0, 0, 0);
    CHECK(kith_aoi_insert(a, &in_cell0) == 0);
    CHECK(kith_aoi_insert(a, &leaker) == 0);
    CHECK(kith_aoi_insert(a, &neighbour) == 0);

    kith_aoi_sphere_t s = {0};
    s.cx = (int64_t)8 << KITH_AOI_FIX_SHIFT;
    s.cy = 0;
    s.cz = 0;
    s.radius = (int64_t)2 << KITH_AOI_FIX_SHIFT;

    struct collector c = {0};
    CHECK(kith_aoi_query_sphere(a, &s, collect_visit, &c) == 0);
    // The un-expanded query AABB covers only cell 0; the leaker's center is
    // in cell 1, so it is found only because the region was expanded by the
    // stored max radius. The zero-radius neighbor in cell 1 is scanned but
    // rejected by the predicate.
    CHECK(c.n == 2u);
    CHECK(collector_has(&c, 1u));
    CHECK(collector_has(&c, 2u));
    CHECK(!collector_has(&c, 3u));

    kith_aoi_destroy(a);
    return failures;
}

// A sphere query whose region contains no indexed objects visits nothing.
static int test_query_sphere_out_of_region(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t far = make_obj(1u, 160, 0, 0, 0);
    CHECK(kith_aoi_insert(a, &far) == 0);

    kith_aoi_sphere_t s = {0};
    s.cx = (int64_t)8 << KITH_AOI_FIX_SHIFT;
    s.cy = 0;
    s.cz = 0;
    s.radius = (int64_t)2 << KITH_AOI_FIX_SHIFT;

    struct collector c = {0};
    CHECK(kith_aoi_query_sphere(a, &s, collect_visit, &c) == 0);
    CHECK(c.n == 0u);

    kith_aoi_destroy(a);
    return failures;
}

// A box query visits in-region objects and the radius-leaking object whose
// center is in a neighboring cell, and skips an out-of-region object.
static int test_query_box_in_out_and_leak(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t in_cell0 = make_obj(1u, 8, 0, 0, 0);
    kith_aoi_object_t leaker = make_obj(2u, 20, 0, 0, 13);
    kith_aoi_object_t neighbour = make_obj(3u, 24, 0, 0, 0);
    kith_aoi_object_t far = make_obj(4u, 160, 0, 0, 0);
    CHECK(kith_aoi_insert(a, &in_cell0) == 0);
    CHECK(kith_aoi_insert(a, &leaker) == 0);
    CHECK(kith_aoi_insert(a, &neighbour) == 0);
    CHECK(kith_aoi_insert(a, &far) == 0);

    kith_aoi_box_t b = {0};
    b.min_x = (int64_t)6 << KITH_AOI_FIX_SHIFT;
    b.max_x = (int64_t)10 << KITH_AOI_FIX_SHIFT;
    b.min_y = 0;
    b.max_y = 0;
    b.min_z = 0;
    b.max_z = 0;

    struct collector c = {0};
    CHECK(kith_aoi_query_box(a, &b, collect_visit, &c) == 0);
    CHECK(c.n == 2u);
    CHECK(collector_has(&c, 1u));
    CHECK(collector_has(&c, 2u));
    CHECK(!collector_has(&c, 3u));
    CHECK(!collector_has(&c, 4u));

    kith_aoi_destroy(a);
    return failures;
}

// When the visitor returns false, the query stops early and returns 0. The
// visitor is called exactly once before stopping (the within-cell order is
// unspecified, so only the count is asserted).
static int test_query_early_stop(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    for (uint64_t i = 1u; i <= 3u; ++i)
    {
        kith_aoi_object_t o = make_obj(i, 8, 8, 8, 0);
        CHECK(kith_aoi_insert(a, &o) == 0);
    }

    kith_aoi_sphere_t s = {0};
    s.cx = (int64_t)8 << KITH_AOI_FIX_SHIFT;
    s.cy = (int64_t)8 << KITH_AOI_FIX_SHIFT;
    s.cz = (int64_t)8 << KITH_AOI_FIX_SHIFT;
    s.radius = (int64_t)2 << KITH_AOI_FIX_SHIFT;

    struct collector c = {0};
    c.stop_at = 1u;
    CHECK(kith_aoi_query_sphere(a, &s, collect_visit, &c) == 0);
    CHECK(c.n == 1u);

    // Same contract for the box query.
    struct collector cb = {0};
    cb.stop_at = 1u;
    kith_aoi_box_t b = {0};
    b.min_x = (int64_t)6 << KITH_AOI_FIX_SHIFT;
    b.max_x = (int64_t)10 << KITH_AOI_FIX_SHIFT;
    b.min_y = (int64_t)6 << KITH_AOI_FIX_SHIFT;
    b.max_y = (int64_t)10 << KITH_AOI_FIX_SHIFT;
    b.min_z = (int64_t)6 << KITH_AOI_FIX_SHIFT;
    b.max_z = (int64_t)10 << KITH_AOI_FIX_SHIFT;
    CHECK(kith_aoi_query_box(a, &b, collect_visit, &cb) == 0);
    CHECK(cb.n == 1u);

    kith_aoi_destroy(a);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_query_sphere_in_and_out();
    rc |= test_query_sphere_radius_leak();
    rc |= test_query_sphere_out_of_region();
    rc |= test_query_box_in_out_and_leak();
    rc |= test_query_early_stop();
    if (rc != 0)
    {
        (void)fprintf(stderr, "aoi query tests FAILED\n");
    }
    return rc;
}

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
        (void)fprintf(stderr, "aoi index: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

#define CELL_SIZE ((int64_t)16 << KITH_AOI_FIX_SHIFT)

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

struct visit_count
{
    size_t n;
};

static bool count_visit(const kith_aoi_object_t *obj, void *user)
{
    (void)obj;
    struct visit_count *c = user;
    c->n += 1u;
    return true;
}

// Probe a single cell with a box whose bounds match the cell's world extent.
// Objects whose center falls in the cell match (distance to closest box point
// is zero); objects in other cells do not. The query region is expanded by
// the index's max-radius bound, but the predicate still filters to the box,
// so the count reflects only objects whose center is in the cell.
static size_t count_in_cell(kith_aoi_t *a, int32_t cx, int32_t cy, int32_t cz)
{
    kith_aoi_box_t box = {0};
    box.min_x = (int64_t)cx * CELL_SIZE;
    box.max_x = (int64_t)(cx + 1) * CELL_SIZE;
    box.min_y = (int64_t)cy * CELL_SIZE;
    box.max_y = (int64_t)(cy + 1) * CELL_SIZE;
    box.min_z = (int64_t)cz * CELL_SIZE;
    box.max_z = (int64_t)(cz + 1) * CELL_SIZE;
    struct visit_count c = {0};
    (void)kith_aoi_query_box(a, &box, count_visit, &c);
    return c.n;
}

// Inserting the same id twice is rejected with EEXIST and leaves the count
// unchanged. Distinct ids accumulate.
static int test_insert_and_eexist(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t o1 = make_obj(1u, 8, 8, 8, 0);
    CHECK(kith_aoi_insert(a, &o1) == 0);
    CHECK(kith_aoi_size(a) == 1u);

    CHECK(kith_aoi_insert(a, &o1) == kith_error_return(KITH_EEXIST));
    CHECK(kith_aoi_size(a) == 1u);

    kith_aoi_object_t o2 = make_obj(2u, 24, 8, 8, 0);
    CHECK(kith_aoi_insert(a, &o2) == 0);
    CHECK(kith_aoi_size(a) == 2u);

    kith_aoi_destroy(a);
    return failures;
}

// Updating an identifier that is not indexed returns ENOENT.
static int test_update_enoent(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t o = make_obj(99u, 8, 8, 8, 0);
    CHECK(kith_aoi_update(a, &o) == kith_error_return(KITH_ENOENT));
    CHECK(kith_aoi_size(a) == 0u);

    kith_aoi_destroy(a);
    return failures;
}

// Removing an indexed object detaches it from its cell: a query over that
// cell does not visit it. Removal is idempotent: a second removal (and a
// removal of an identifier that was never inserted) returns 0. The lookup
// of a removed id returns ENOENT.
static int test_remove_idempotent_and_removes_from_cell(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t o = make_obj(1u, 8, 8, 8, 0);
    CHECK(kith_aoi_insert(a, &o) == 0);
    CHECK(count_in_cell(a, 0, 0, 0) == 1u);

    CHECK(kith_aoi_remove(a, 1u) == 0);
    CHECK(kith_aoi_size(a) == 0u);
    CHECK(count_in_cell(a, 0, 0, 0) == 0u);

    // Idempotent: removing the same id again, and removing an unknown id,
    // both succeed.
    CHECK(kith_aoi_remove(a, 1u) == 0);
    CHECK(kith_aoi_remove(a, 999u) == 0);

    kith_aoi_object_t out = {0};
    CHECK(kith_aoi_lookup(a, 1u, &out) == kith_error_return(KITH_ENOENT));

    kith_aoi_destroy(a);
    return failures;
}

// Lookup of a missing id returns ENOENT. A successful lookup copies the
// stored fields (position, radius, user pointer).
static int test_lookup_enoent_and_fields(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    kith_aoi_object_t out = {0};
    CHECK(kith_aoi_lookup(a, 7u, &out) == kith_error_return(KITH_ENOENT));

    kith_aoi_object_t o = make_obj(7u, 24, 0, 0, 3);
    o.user_ptr = (void *)0xdeadbeefu;
    CHECK(kith_aoi_insert(a, &o) == 0);

    CHECK(kith_aoi_lookup(a, 7u, &out) == 0);
    CHECK(out.id == 7u);
    CHECK(out.pos_x == (int64_t)24 << KITH_AOI_FIX_SHIFT);
    CHECK(out.radius == (int64_t)3 << KITH_AOI_FIX_SHIFT);
    CHECK(out.user_ptr == (void *)0xdeadbeefu);

    kith_aoi_destroy(a);
    return failures;
}

// A move across a cell boundary re-buckets the object: queries over the
// old cell stop returning it and queries over the new cell return it. A
// move within the same cell leaves the object in that cell and updates its
// stored position. lookup reflects the new position in both cases.
static int test_rebucket_on_move(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);

    // Cross-cell move: cell (0,0,0) -> cell (1,0,0).
    kith_aoi_object_t o = make_obj(1u, 8, 8, 8, 0);
    CHECK(kith_aoi_insert(a, &o) == 0);
    CHECK(count_in_cell(a, 0, 0, 0) == 1u);
    CHECK(count_in_cell(a, 1, 0, 0) == 0u);

    kith_aoi_object_t moved = make_obj(1u, 24, 8, 8, 0);
    CHECK(kith_aoi_update(a, &moved) == 0);
    CHECK(count_in_cell(a, 0, 0, 0) == 0u);
    CHECK(count_in_cell(a, 1, 0, 0) == 1u);

    kith_aoi_object_t out = {0};
    CHECK(kith_aoi_lookup(a, 1u, &out) == 0);
    CHECK(out.pos_x == (int64_t)24 << KITH_AOI_FIX_SHIFT);

    // Same-cell move: (3,3,3) -> (12,12,12), both in cell (0,0,0). The object
    // stays in the cell and the stored position updates.
    kith_aoi_object_t o2 = make_obj(2u, 3, 3, 3, 0);
    CHECK(kith_aoi_insert(a, &o2) == 0);
    kith_aoi_object_t same = make_obj(2u, 12, 12, 12, 0);
    CHECK(kith_aoi_update(a, &same) == 0);
    CHECK(count_in_cell(a, 0, 0, 0) == 1u);
    CHECK(kith_aoi_lookup(a, 2u, &out) == 0);
    CHECK(out.pos_x == (int64_t)12 << KITH_AOI_FIX_SHIFT);

    kith_aoi_destroy(a);
    return failures;
}

// size tracks the live object count across insert and remove, and does not
// move on a rejected insert or an idempotent remove.
static int test_size(void)
{
    int failures = 0;
    kith_aoi_t *a = nullptr;
    CHECK(kith_aoi_create(nullptr, nullptr, &a) == 0);
    CHECK(kith_aoi_size(a) == 0u);

    for (uint64_t i = 1u; i <= 5u; ++i)
    {
        kith_aoi_object_t o = make_obj(i, 8, 8, 8, 0);
        CHECK(kith_aoi_insert(a, &o) == 0);
    }
    CHECK(kith_aoi_size(a) == 5u);

    kith_aoi_object_t dup = make_obj(1u, 8, 8, 8, 0);
    CHECK(kith_aoi_insert(a, &dup) == kith_error_return(KITH_EEXIST));
    CHECK(kith_aoi_size(a) == 5u);

    CHECK(kith_aoi_remove(a, 3u) == 0);
    CHECK(kith_aoi_size(a) == 4u);
    CHECK(kith_aoi_remove(a, 3u) == 0);
    CHECK(kith_aoi_size(a) == 4u);

    kith_aoi_destroy(a);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_insert_and_eexist();
    rc |= test_update_enoent();
    rc |= test_remove_idempotent_and_removes_from_cell();
    rc |= test_lookup_enoent_and_fields();
    rc |= test_rebucket_on_move();
    rc |= test_size();
    if (rc != 0)
    {
        (void)fprintf(stderr, "aoi index tests FAILED\n");
    }
    return rc;
}

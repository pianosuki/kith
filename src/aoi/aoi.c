/* Single translation unit of the AOI module: object-id and cell-coordinate
 * open-addressed indexes, insert/update/remove, and sphere, box, and cell
 * queries. The shared structures live in aoi_internal.h; the public contract
 * is include/kith/aoi/aoi.h. */

#include <stdckdint.h>
#include <string.h>

#include "aoi/aoi_internal.h"
#include "kith/types.h"
#include "kith/version.h"
#include "util/wide_int.h"

/*---------------------------------------------------------------------------
 * shared helpers
 *-------------------------------------------------------------------------*/

static size_t aoi_mask(size_t buckets)
{
    return buckets - 1u;
}

static uint32_t aoi_next_pow2(uint32_t v)
{
    uint32_t r = 1u;
    while (r < v)
    {
        r <<= 1u;
    }
    return r;
}

KITH_LOCAL uint32_t aoi_resolve_buckets(uint32_t requested)
{
    uint32_t b = aoi_next_pow2(requested);
    if (b < 16u)
    {
        b = 16u;
    }
    return b;
}

static uint64_t aoi_obj_hash(uint64_t id)
{
    uint64_t k = id;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

static uint64_t aoi_cell_hash(int32_t cx, int32_t cy, int32_t cz)
{
    uint64_t k = 0;
    k = k * 131u + (uint64_t)(uint32_t)cx;
    k = k * 131u + (uint64_t)(uint32_t)cy;
    k = k * 131u + (uint64_t)(uint32_t)cz;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    return k;
}

static int64_t aoi_clamp(int64_t v, int64_t lo, int64_t hi)
{
    if (v < lo)
    {
        return lo;
    }
    if (v > hi)
    {
        return hi;
    }
    return v;
}

/*---------------------------------------------------------------------------
 * params / defaults
 *-------------------------------------------------------------------------*/

KITH_LOCAL bool aoi_params_validate(const kith_aoi_params_t *params, kith_error_t *out_err)
{
    if (params->size < sizeof(*params))
    {
        *out_err = KITH_ESIZE;
        return false;
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        *out_err = KITH_EABIVER;
        return false;
    }
    if (params->cell_size < 0)
    {
        *out_err = KITH_EINVAL;
        return false;
    }
    if (params->bucket_count > KITH_AOI_MAX_BUCKET_COUNT)
    {
        *out_err = KITH_EINVAL;
        return false;
    }
    *out_err = KITH_OK;
    return true;
}

static void aoi_resolve_params(kith_aoi_params_t *out)
{
    if (out->cell_size == 0)
    {
        out->cell_size = (int64_t)KITH_AOI_DEFAULT_CELL_SIZE;
    }
    if (out->bucket_count == 0u)
    {
        out->bucket_count = KITH_AOI_DEFAULT_BUCKET_COUNT;
    }
}

/*---------------------------------------------------------------------------
 * object-id index
 *-------------------------------------------------------------------------*/

static struct aoi_obj_node *aoi_obj_find(const kith_aoi_t *a, uint64_t id)
{
    size_t mask = aoi_mask(a->obj_buckets);
    size_t i = (size_t)aoi_obj_hash(id) & mask;
    for (size_t probe = 0u; probe < a->obj_buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct aoi_obj_node *n = &a->objs[idx];
        if (!n->used)
        {
            return nullptr;
        }
        if (n->deleted)
        {
            continue;
        }
        if (n->obj.id == id)
        {
            return n;
        }
    }
    return nullptr;
}

/** Locate a free or tombstone slot for a new identifier. Returns NULL when
 *  the table is full of live entries; the caller pre-checks for an existing
 *  identifier so a live match is treated as unavailable. */
static struct aoi_obj_node *aoi_obj_slot_for_insert(kith_aoi_t *a, uint64_t id)
{
    size_t mask = aoi_mask(a->obj_buckets);
    size_t i = (size_t)aoi_obj_hash(id) & mask;
    struct aoi_obj_node *first_tomb = nullptr;
    for (size_t probe = 0u; probe < a->obj_buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct aoi_obj_node *n = &a->objs[idx];
        if (!n->used)
        {
            return first_tomb ? first_tomb : n;
        }
        if (n->deleted)
        {
            if (!first_tomb)
            {
                first_tomb = n;
            }
            continue;
        }
    }
    return first_tomb;
}

/*---------------------------------------------------------------------------
 * cell index
 *-------------------------------------------------------------------------*/

static struct aoi_cell_node *aoi_cell_find(const kith_aoi_t *a, int32_t cx, int32_t cy, int32_t cz)
{
    size_t mask = aoi_mask(a->cell_buckets);
    size_t i = (size_t)aoi_cell_hash(cx, cy, cz) & mask;
    for (size_t probe = 0u; probe < a->cell_buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct aoi_cell_node *c = &a->cells[idx];
        if (!c->used)
        {
            return nullptr;
        }
        if (c->cell_x == cx && c->cell_y == cy && c->cell_z == cz)
        {
            return c;
        }
    }
    return nullptr;
}

static struct aoi_cell_node *
aoi_cell_find_or_create(kith_aoi_t *a, int32_t cx, int32_t cy, int32_t cz)
{
    size_t mask = aoi_mask(a->cell_buckets);
    size_t i = (size_t)aoi_cell_hash(cx, cy, cz) & mask;
    for (size_t probe = 0u; probe < a->cell_buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct aoi_cell_node *c = &a->cells[idx];
        if (!c->used)
        {
            c->used = true;
            c->cell_x = cx;
            c->cell_y = cy;
            c->cell_z = cz;
            c->members = nullptr;
            c->member_count = 0u;
            c->member_cap = 0u;
            return c;
        }
        if (c->cell_x == cx && c->cell_y == cy && c->cell_z == cz)
        {
            return c;
        }
    }
    return nullptr;
}

static int aoi_cell_grow(kith_aoi_t *a, struct aoi_cell_node *cell)
{
    size_t cap = cell->member_cap == 0u ? 4u : cell->member_cap * 2u;
    size_t *arr = kith_realloc(a->allocator, cell->members, cap * sizeof(*arr));
    if (!arr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    cell->members = arr;
    cell->member_cap = cap;
    return 0;
}

static int aoi_cell_attach(kith_aoi_t *a, struct aoi_cell_node *cell, size_t slot)
{
    if (cell->member_count == cell->member_cap)
    {
        int rc = aoi_cell_grow(a, cell);
        if (rc != 0)
        {
            return rc;
        }
    }
    cell->members[cell->member_count] = slot;
    cell->member_count += 1u;
    return 0;
}

/** Remove @p slot from its cell's member array in O(1) by swapping with the
 *  last member and fixing the moved object's back-pointer. */
static void aoi_cell_detach(kith_aoi_t *a, size_t slot)
{
    struct aoi_obj_node *obj = &a->objs[slot];
    struct aoi_cell_node *cell = aoi_cell_find(a, obj->cell_x, obj->cell_y, obj->cell_z);
    if (!cell || cell->member_count == 0u)
    {
        return;
    }
    size_t i = obj->cell_pos;
    if (i >= cell->member_count || cell->members[i] != slot)
    {
        return;
    }
    cell->member_count -= 1u;
    if (i != cell->member_count)
    {
        cell->members[i] = cell->members[cell->member_count];
        a->objs[cell->members[i]].cell_pos = i;
    }
}

/*---------------------------------------------------------------------------
 * query predicates and regions
 *-------------------------------------------------------------------------*/

// Wide arithmetic for the distance predicates: the difference of two int64
// positions and the sum of two int64 radii fit in 128 bits with room to
// spare, and the squared comparison below is exact in that width.

/** Decide whether a per-axis offset triple lies within a radius. Each axis
 *  is excluded against the radius before any squaring, and the squares
 *  accumulate under the invariant sum <= radius², so the decision is exact
 *  for every representable position and radius and no intermediate exceeds
 *  128 bits. */
static bool aoi_dist_within(kith_i128_t dx, kith_i128_t dy, kith_i128_t dz, kith_i128_t radius)
{
    kith_u128_t ar = radius < 0 ? (kith_u128_t)-radius : (kith_u128_t)radius;
    kith_u128_t r2 = ar * ar;
    kith_u128_t sum = 0;
    const kith_i128_t axes[3] = {dx, dy, dz};
    for (int i = 0; i < 3; i++)
    {
        kith_i128_t d = axes[i];
        kith_u128_t ad = d < 0 ? (kith_u128_t)-d : (kith_u128_t)d;
        if (ad > ar)
        {
            return false;
        }
        kith_u128_t sq = ad * ad;
        if (sq > r2 - sum)
        {
            return false;
        }
        sum += sq;
    }
    return true;
}

static bool aoi_pred_sphere(const kith_aoi_object_t *o, const void *query)
{
    const kith_aoi_sphere_t *s = query;
    kith_i128_t dx = (kith_i128_t)o->pos_x - (kith_i128_t)s->cx;
    kith_i128_t dy = (kith_i128_t)o->pos_y - (kith_i128_t)s->cy;
    kith_i128_t dz = (kith_i128_t)o->pos_z - (kith_i128_t)s->cz;
    kith_i128_t radius = (kith_i128_t)s->radius + (kith_i128_t)o->radius;
    return aoi_dist_within(dx, dy, dz, radius);
}

static bool aoi_pred_box(const kith_aoi_object_t *o, const void *query)
{
    const kith_aoi_box_t *b = query;
    kith_i128_t dx = (kith_i128_t)o->pos_x - (kith_i128_t)aoi_clamp(o->pos_x, b->min_x, b->max_x);
    kith_i128_t dy = (kith_i128_t)o->pos_y - (kith_i128_t)aoi_clamp(o->pos_y, b->min_y, b->max_y);
    kith_i128_t dz = (kith_i128_t)o->pos_z - (kith_i128_t)aoi_clamp(o->pos_z, b->min_z, b->max_z);
    return aoi_dist_within(dx, dy, dz, (kith_i128_t)o->radius);
}

// Region endpoints are derived in 128-bit arithmetic: the reach (the query
// radius plus the largest stored radius) and the center shift both exceed
// int64 for geometry at the coordinate span. The endpoint is floored onto
// the cell grid exactly, then clamped to the int32 cell index space, so an
// object whose sphere reaches into the region is never lost to endpoint
// overflow.
static int32_t aoi_region_cell(kith_i128_t endpoint, int64_t cell_size)
{
    kith_i128_t q = endpoint / (kith_i128_t)cell_size;
    kith_i128_t r = endpoint % (kith_i128_t)cell_size;
    if (r < 0)
    {
        q -= 1;
    }
    if (q > INT32_MAX)
    {
        return INT32_MAX;
    }
    if (q < INT32_MIN)
    {
        return INT32_MIN;
    }
    return (int32_t)q;
}

static void
aoi_region_sphere(const kith_aoi_t *a, const kith_aoi_sphere_t *s, int32_t *lo, int32_t *hi)
{
    kith_i128_t reach = (kith_i128_t)s->radius + (kith_i128_t)a->max_radius;
    lo[0] = aoi_region_cell((kith_i128_t)s->cx - reach, a->cell_size);
    lo[1] = aoi_region_cell((kith_i128_t)s->cy - reach, a->cell_size);
    lo[2] = aoi_region_cell((kith_i128_t)s->cz - reach, a->cell_size);
    hi[0] = aoi_region_cell((kith_i128_t)s->cx + reach, a->cell_size);
    hi[1] = aoi_region_cell((kith_i128_t)s->cy + reach, a->cell_size);
    hi[2] = aoi_region_cell((kith_i128_t)s->cz + reach, a->cell_size);
}

static void aoi_region_box(const kith_aoi_t *a, const kith_aoi_box_t *b, int32_t *lo, int32_t *hi)
{
    kith_i128_t m = (kith_i128_t)a->max_radius;
    lo[0] = aoi_region_cell((kith_i128_t)b->min_x - m, a->cell_size);
    lo[1] = aoi_region_cell((kith_i128_t)b->min_y - m, a->cell_size);
    lo[2] = aoi_region_cell((kith_i128_t)b->min_z - m, a->cell_size);
    hi[0] = aoi_region_cell((kith_i128_t)b->max_x + m, a->cell_size);
    hi[1] = aoi_region_cell((kith_i128_t)b->max_y + m, a->cell_size);
    hi[2] = aoi_region_cell((kith_i128_t)b->max_z + m, a->cell_size);
}

typedef bool (*aoi_pred_fn)(const kith_aoi_object_t *obj, const void *query);

static int aoi_visit_region(const kith_aoi_t *a,
                            const int32_t *lo,
                            const int32_t *hi,
                            aoi_pred_fn pred,
                            const void *query,
                            kith_aoi_visit_fn visit,
                            void *user)
{
    for (int32_t cz = lo[2]; cz <= hi[2]; ++cz)
    {
        for (int32_t cy = lo[1]; cy <= hi[1]; ++cy)
        {
            for (int32_t cx = lo[0]; cx <= hi[0]; ++cx)
            {
                const struct aoi_cell_node *cell = aoi_cell_find(a, cx, cy, cz);
                if (!cell)
                {
                    continue;
                }
                for (size_t i = 0u; i < cell->member_count; ++i)
                {
                    const kith_aoi_object_t *o = &a->objs[cell->members[i]].obj;
                    if (!pred(o, query))
                    {
                        continue;
                    }
                    if (!visit(o, user))
                    {
                        return 0;
                    }
                }
            }
        }
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * handle lifecycle
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_aoi_create(const kith_aoi_params_t *params,
                                           const kith_allocator_t *alloc,
                                           kith_aoi_t **out_aoi)
{
    if (!out_aoi)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_aoi = nullptr;

    kith_aoi_params_t resolved;
    if (params)
    {
        kith_error_t err = KITH_OK;
        if (!aoi_params_validate(params, &err))
        {
            return kith_error_return(err);
        }
        resolved = *params;
    }
    else
    {
        memset(&resolved, 0, sizeof(resolved));
        resolved.size = sizeof(resolved);
        resolved.abi_version = KITH_ABI_VERSION;
    }
    aoi_resolve_params(&resolved);

    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    kith_aoi_t *a = kith_alloc_zero(allocator, 1, sizeof(*a));
    if (!a)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    a->allocator = allocator;
    size_t buckets = (size_t)aoi_resolve_buckets(resolved.bucket_count);
    a->objs = kith_alloc_zero(allocator, buckets, sizeof(*a->objs));
    if (!a->objs)
    {
        kith_free(allocator, a);
        return kith_error_return(KITH_ENOMEM);
    }
    a->cells = kith_alloc_zero(allocator, buckets, sizeof(*a->cells));
    if (!a->cells)
    {
        kith_free(allocator, a->objs);
        kith_free(allocator, a);
        return kith_error_return(KITH_ENOMEM);
    }
    a->obj_buckets = buckets;
    a->cell_buckets = buckets;
    a->cell_size = resolved.cell_size;
    a->max_radius = 0;
    a->obj_count = 0u;
    *out_aoi = a;
    return 0;
}

KITH_API void kith_aoi_destroy(kith_aoi_t *a)
{
    if (!a)
    {
        return;
    }
    for (size_t i = 0u; i < a->cell_buckets; ++i)
    {
        kith_free(a->allocator, a->cells[i].members);
    }
    kith_free(a->allocator, a->cells);
    kith_free(a->allocator, a->objs);
    kith_free(a->allocator, a);
}

/*---------------------------------------------------------------------------
 * insert / update / remove
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_aoi_insert(kith_aoi_t *a, const kith_aoi_object_t *obj)
{
    if (!a || !obj)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (aoi_obj_find(a, obj->id))
    {
        return kith_error_return(KITH_EEXIST);
    }
    size_t total = 0u;
    if (ckd_add(&total, a->obj_count, (size_t)1u))
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    struct aoi_obj_node *slot = aoi_obj_slot_for_insert(a, obj->id);
    if (!slot)
    {
        return kith_error_return(KITH_EBUSY);
    }
    int32_t cx = aoi_cell_of(obj->pos_x, a->cell_size);
    int32_t cy = aoi_cell_of(obj->pos_y, a->cell_size);
    int32_t cz = aoi_cell_of(obj->pos_z, a->cell_size);
    struct aoi_cell_node *cell = aoi_cell_find_or_create(a, cx, cy, cz);
    if (!cell)
    {
        return kith_error_return(KITH_EBUSY);
    }
    size_t slot_index = (size_t)(slot - a->objs);
    int rc = aoi_cell_attach(a, cell, slot_index);
    if (rc != 0)
    {
        return rc;
    }
    slot->obj = *obj;
    slot->cell_x = cx;
    slot->cell_y = cy;
    slot->cell_z = cz;
    slot->cell_pos = cell->member_count - 1u;
    slot->used = true;
    slot->deleted = false;
    a->obj_count = total;
    if (obj->radius > a->max_radius)
    {
        a->max_radius = obj->radius;
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_aoi_update(kith_aoi_t *a, const kith_aoi_object_t *obj)
{
    if (!a || !obj)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct aoi_obj_node *slot = aoi_obj_find(a, obj->id);
    if (!slot)
    {
        return kith_error_return(KITH_ENOENT);
    }
    int32_t ncx = aoi_cell_of(obj->pos_x, a->cell_size);
    int32_t ncy = aoi_cell_of(obj->pos_y, a->cell_size);
    int32_t ncz = aoi_cell_of(obj->pos_z, a->cell_size);
    if (ncx != slot->cell_x || ncy != slot->cell_y || ncz != slot->cell_z)
    {
        size_t slot_index = (size_t)(slot - a->objs);
        struct aoi_cell_node *new_cell = aoi_cell_find_or_create(a, ncx, ncy, ncz);
        if (!new_cell)
        {
            return kith_error_return(KITH_EBUSY);
        }
        int rc = aoi_cell_attach(a, new_cell, slot_index);
        if (rc != 0)
        {
            return rc;
        }
        aoi_cell_detach(a, slot_index);
        slot->cell_x = ncx;
        slot->cell_y = ncy;
        slot->cell_z = ncz;
        slot->cell_pos = new_cell->member_count - 1u;
    }
    slot->obj = *obj;
    if (obj->radius > a->max_radius)
    {
        a->max_radius = obj->radius;
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_aoi_remove(kith_aoi_t *a, uint64_t id)
{
    if (!a)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct aoi_obj_node *slot = aoi_obj_find(a, id);
    if (!slot)
    {
        return 0;
    }
    size_t slot_index = (size_t)(slot - a->objs);
    aoi_cell_detach(a, slot_index);
    slot->deleted = true;
    if (a->obj_count > 0u)
    {
        a->obj_count -= 1u;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * lookup
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int
kith_aoi_lookup(const kith_aoi_t *a, uint64_t id, kith_aoi_object_t *out_obj)
{
    if (!a || !out_obj)
    {
        return kith_error_return(KITH_EINVAL);
    }
    const struct aoi_obj_node *slot = aoi_obj_find(a, id);
    if (!slot)
    {
        return kith_error_return(KITH_ENOENT);
    }
    *out_obj = slot->obj;
    return 0;
}

/*---------------------------------------------------------------------------
 * queries
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_aoi_query_sphere(const kith_aoi_t *a,
                                                 const kith_aoi_sphere_t *sphere,
                                                 kith_aoi_visit_fn visit,
                                                 void *user)
{
    if (!a || !sphere || !visit)
    {
        return kith_error_return(KITH_EINVAL);
    }
    int32_t lo[3];
    int32_t hi[3];
    aoi_region_sphere(a, sphere, lo, hi);
    return aoi_visit_region(a, lo, hi, aoi_pred_sphere, sphere, visit, user);
}

[[nodiscard]] KITH_API int kith_aoi_query_box(const kith_aoi_t *a,
                                              const kith_aoi_box_t *box,
                                              kith_aoi_visit_fn visit,
                                              void *user)
{
    if (!a || !box || !visit)
    {
        return kith_error_return(KITH_EINVAL);
    }
    int32_t lo[3];
    int32_t hi[3];
    aoi_region_box(a, box, lo, hi);
    return aoi_visit_region(a, lo, hi, aoi_pred_box, box, visit, user);
}

/*---------------------------------------------------------------------------
 * size
 *-------------------------------------------------------------------------*/

KITH_API size_t kith_aoi_size(const kith_aoi_t *a)
{
    return a ? a->obj_count : 0u;
}

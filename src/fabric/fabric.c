/* Single translation unit of the fabric module: sharded cell store keyed by
 * (zone, cell, lod), immutable publish-product attach, subscription cursors,
 * and overlap-window handoff. The shared structures live in fabric_internal.h;
 * the public contract is include/kith/fabric/fabric.h. */

#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>

#include "fabric/fabric_internal.h"
#include "kith/types.h"
#include "kith/version.h"
#include "util/wide_int.h"

/*---------------------------------------------------------------------------
 * shared helpers
 *-------------------------------------------------------------------------*/

static size_t fabric_mask(size_t buckets)
{
    return buckets - 1u;
}

static uint32_t fabric_next_pow2(uint32_t v)
{
    uint32_t r = 1u;
    while (r < v)
    {
        r <<= 1u;
    }
    return r;
}

static size_t fabric_shard_for(uint32_t zone)
{
    return (size_t)(zone % KITH_FABRIC_CELL_SHARDS);
}

static uint32_t fabric_resolve_buckets(uint32_t requested)
{
    uint32_t b = fabric_next_pow2(requested);
    if (b < 16u)
    {
        b = 16u;
    }
    return b;
}

/*---------------------------------------------------------------------------
 * params / defaults
 *-------------------------------------------------------------------------*/

static void fabric_resolve_params(kith_fabric_params_t *out)
{
    if (out->cell_bucket_count == 0u)
    {
        out->cell_bucket_count = KITH_FABRIC_DEFAULT_BUCKET_COUNT;
    }
}

static bool fabric_params_validate(const kith_fabric_params_t *params, kith_error_t *out_err)
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
    *out_err = KITH_OK;
    return true;
}

/*---------------------------------------------------------------------------
 * cell store
 *-------------------------------------------------------------------------*/

/** Grow trigger threshold. A shard grows when adding one more cell would
 *  push the live-plus-tombstone load above 3/4 of capacity, keeping the
 *  open-addressed probe chains short. The denominator is a power of two so
 *  the compiler reduces the comparison to a shift. */
#define FABRIC_SHARD_LOAD_NUM 3u
#define FABRIC_SHARD_LOAD_DEN 4u

static const struct fabric_cell_node *fabric_cell_lookup(
    const struct fabric_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    size_t mask = fabric_mask(sh->buckets);
    size_t i = (size_t)fabric_cell_mix(zone, cx, cy, cz, lod) & mask;
    for (size_t probe = 0u; probe < sh->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        const struct fabric_cell_node *c = &sh->cells[idx];
        if (!c->used)
        {
            return nullptr;
        }
        if (c->deleted)
        {
            continue;
        }
        if (c->zone == zone && c->cell_x == cx && c->cell_y == cy && c->cell_z == cz &&
            c->lod == lod)
        {
            return c;
        }
    }
    return nullptr;
}

static struct fabric_cell_node *fabric_cell_find(
    struct fabric_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    return (struct fabric_cell_node *)fabric_cell_lookup(sh, zone, cx, cy, cz, lod);
}

static struct fabric_cell_node *fabric_cell_create(
    struct fabric_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    size_t mask = fabric_mask(sh->buckets);
    size_t i = (size_t)fabric_cell_mix(zone, cx, cy, cz, lod) & mask;
    struct fabric_cell_node *first_tomb = nullptr;
    for (size_t probe = 0u; probe < sh->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct fabric_cell_node *c = &sh->cells[idx];
        if (!c->used)
        {
            struct fabric_cell_node *target = first_tomb ? first_tomb : c;
            target->used = true;
            target->deleted = false;
            target->zone = zone;
            target->cell_x = cx;
            target->cell_y = cy;
            target->cell_z = cz;
            target->lod = lod;
            target->authority_epoch = 0u;
            target->publish_seq = 0u;
            target->actor_count = 0u;
            sh->count += 1u;
            if (first_tomb)
            {
                sh->tombstones -= 1u;
            }
            return target;
        }
        if (c->deleted)
        {
            if (!first_tomb)
            {
                first_tomb = c;
            }
            continue;
        }
    }
    // The insert path grows the shard at the load threshold, so the probe
    // always finds a slot; reaching this line means the caller bypassed it.
    return nullptr;
}

/** Double the shard's table and rehash the live cells into the new capacity.
 *  Tombstones drop on rehash (their slots become fresh). The allocation is
 *  staged so a failure leaves the shard at its prior capacity. */
static int fabric_shard_grow(struct fabric_shard *sh)
{
    size_t new_buckets;
    if (ckd_mul(&new_buckets, sh->buckets, (size_t)2u) || new_buckets <= sh->buckets)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    struct fabric_cell_node *grown = kith_alloc_zero(sh->allocator, new_buckets, sizeof(*grown));
    if (!grown)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    size_t mask = new_buckets - 1u;
    size_t live = 0u;
    // The live count sits below the trigger threshold on the old capacity,
    // so every cell finds a slot without exhausting the probe.
    for (size_t i = 0u; i < sh->buckets; ++i)
    {
        const struct fabric_cell_node *c = &sh->cells[i];
        if (!c->used || c->deleted)
        {
            continue;
        }
        size_t slot =
            (size_t)fabric_cell_mix(c->zone, c->cell_x, c->cell_y, c->cell_z, c->lod) & mask;
        while (grown[slot].used)
        {
            slot = (slot + 1u) & mask;
        }
        grown[slot] = *c;
        live += 1u;
    }
    kith_free(sh->allocator, sh->cells);
    sh->cells = grown;
    sh->buckets = new_buckets;
    sh->count = live;
    sh->tombstones = 0u;
    return 0;
}

/** Grow the shard before a publish that adds a cell, so the insert path
 *  never probes a table with no free slot. One doubling always clears the
 *  threshold: the live-plus-tombstone load is at most the old capacity, and
 *  the rehash drops the tombstone share. */
static int fabric_shard_ensure_publish_capacity(struct fabric_shard *sh)
{
    size_t load = sh->count + sh->tombstones + 1u;
    size_t threshold = (sh->buckets * FABRIC_SHARD_LOAD_NUM) / FABRIC_SHARD_LOAD_DEN;
    if (load <= threshold)
    {
        return 0;
    }
    return fabric_shard_grow(sh);
}

static int fabric_shard_init(struct fabric_shard *sh, size_t buckets, const kith_allocator_t *alloc)
{
    sh->allocator = alloc;
    sh->cells = kith_alloc_zero(alloc, buckets, sizeof(*sh->cells));
    if (!sh->cells)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    sh->buckets = buckets;
    sh->count = 0u;
    sh->tombstones = 0u;
    return 0;
}

static void fabric_shard_fini(struct fabric_shard *sh)
{
    kith_free(sh->allocator, sh->cells);
    memset(sh, 0, sizeof(*sh));
}

/*---------------------------------------------------------------------------
 * sim artifact query (borrowed handle)
 *-------------------------------------------------------------------------*/

// Copy attempts for one cell snapshot. The first attempt covers the cold
// scratch (count probe, then copy); the remainder absorb publishes landing
// between a copy and its preceding probe. Exhaustion reports -KITH_ERANGE
// with the required count rather than dropping rows silently.
enum
{
    FABRIC_SNAPSHOT_COPY_ATTEMPTS = 3
};

static void fabric_sim_key(const kith_fabric_cell_key_t *key, kith_sim_artifact_key_t *sk)
{
    memset(sk, 0, sizeof(*sk));
    sk->zone = key->zone;
    sk->cell_x = key->cell_x;
    sk->cell_y = key->cell_y;
    sk->cell_z = key->cell_z;
    sk->lod = key->lod;
}

static uint32_t fabric_query_actor_count(
    kith_sim_t *sim, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    kith_sim_artifact_key_t sk;
    memset(&sk, 0, sizeof(sk));
    sk.zone = zone;
    sk.cell_x = cx;
    sk.cell_y = cy;
    sk.cell_z = cz;
    sk.lod = lod;
    kith_sim_cell_product_t prod;
    if (kith_sim_cell_product(sim, &sk, &prod) != 0)
    {
        return 0u;
    }
    return prod.actor_count;
}

/*---------------------------------------------------------------------------
 * fabric handle lifecycle
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_fabric_create(const kith_fabric_params_t *params,
                                              kith_sim_t *sim,
                                              const kith_allocator_t *alloc,
                                              kith_fabric_t **out_fabric)
{
    if (!out_fabric)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_fabric = nullptr;
    if (!sim)
    {
        return kith_error_return(KITH_EINVAL);
    }

    kith_fabric_params_t resolved;
    if (params)
    {
        kith_error_t err = KITH_OK;
        if (!fabric_params_validate(params, &err))
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
    fabric_resolve_params(&resolved);

    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    kith_fabric_t *f = kith_alloc_zero(allocator, 1, sizeof(*f));
    if (!f)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    f->allocator = allocator;
    if (pthread_mutex_init(&f->lock, nullptr) != 0)
    {
        kith_free(allocator, f);
        return kith_error_return(KITH_ENOMEM);
    }
    if (pthread_mutex_init(&f->snapshot_lock, nullptr) != 0)
    {
        pthread_mutex_destroy(&f->lock);
        kith_free(allocator, f);
        return kith_error_return(KITH_ENOMEM);
    }
    f->sim = sim;

    size_t buckets = (size_t)fabric_resolve_buckets(resolved.cell_bucket_count);
    for (size_t i = 0u; i < KITH_FABRIC_CELL_SHARDS; ++i)
    {
        int rc = fabric_shard_init(&f->shards[i], buckets, allocator);
        if (rc != 0)
        {
            for (size_t j = 0u; j < i; ++j)
            {
                fabric_shard_fini(&f->shards[j]);
            }
            pthread_mutex_destroy(&f->snapshot_lock);
            pthread_mutex_destroy(&f->lock);
            kith_free(allocator, f);
            return rc;
        }
    }
    *out_fabric = f;
    return 0;
}

KITH_API void kith_fabric_destroy(kith_fabric_t *f)
{
    if (!f)
    {
        return;
    }
    for (size_t i = 0u; i < KITH_FABRIC_CELL_SHARDS; ++i)
    {
        fabric_shard_fini(&f->shards[i]);
    }
    for (size_t i = 0u; i < f->sub_count; ++i)
    {
        f->subs[i]->fabric = nullptr;
        pthread_mutex_destroy(&f->subs[i]->lock);
        kith_free(f->subs[i]->allocator, f->subs[i]->nodes);
        kith_free(f->subs[i]->allocator, f->subs[i]);
    }
    kith_free(f->allocator, f->subs);
    kith_free(f->allocator, f->snapshot_scratch);
    pthread_mutex_destroy(&f->snapshot_lock);
    pthread_mutex_destroy(&f->lock);
    kith_free(f->allocator, f);
}

/*---------------------------------------------------------------------------
 * subscription table
 *-------------------------------------------------------------------------*/

static int fabric_sub_table_add(kith_fabric_t *f, kith_fabric_subscription_t *sub)
{
    /* Caller holds the fabric lock. */
    if (f->sub_count == f->sub_cap)
    {
        size_t cap = f->sub_cap == 0u ? 8u : f->sub_cap * 2u;
        kith_fabric_subscription_t **arr = kith_realloc(f->allocator, f->subs, cap * sizeof(*arr));
        if (!arr)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        f->subs = arr;
        f->sub_cap = cap;
    }
    f->subs[f->sub_count] = sub;
    f->sub_count += 1u;
    return 0;
}

static void fabric_sub_table_remove(kith_fabric_t *f, kith_fabric_subscription_t *sub)
{
    /* Caller holds the fabric lock. */
    for (size_t i = 0u; i < f->sub_count; ++i)
    {
        if (f->subs[i] == sub)
        {
            f->subs[i] = f->subs[f->sub_count - 1u];
            f->sub_count -= 1u;
            return;
        }
    }
}

/*---------------------------------------------------------------------------
 * fanout
 *-------------------------------------------------------------------------*/

static void
fabric_fanout(kith_fabric_t *f, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    /* Caller holds the fabric lock. Each subscription's interest set is
     * guarded by its own lock, acquired here per subscription. */
    for (size_t i = 0u; i < f->sub_count; ++i)
    {
        kith_fabric_subscription_t *sub = f->subs[i];
        pthread_mutex_lock(&sub->lock);
        size_t mask = fabric_mask(sub->buckets);
        size_t bi = (size_t)fabric_cell_mix(zone, cx, cy, cz, lod) & mask;
        for (size_t probe = 0u; probe < sub->buckets; ++probe)
        {
            size_t idx = (bi + probe) & mask;
            struct fabric_sub_node *n = &sub->nodes[idx];
            if (!n->used)
            {
                break;
            }
            if (n->deleted)
            {
                continue;
            }
            if (n->key.zone == zone && n->key.cell_x == cx && n->key.cell_y == cy &&
                n->key.cell_z == cz && n->key.lod == lod)
            {
                n->pending = true;
                break;
            }
        }
        pthread_mutex_unlock(&sub->lock);
    }
}

/*---------------------------------------------------------------------------
 * publish
 *-------------------------------------------------------------------------*/

static int
fabric_publish_existing(struct fabric_cell_node *node, uint32_t authority_epoch, uint64_t *out_seq)
{
    node->authority_epoch = authority_epoch;
    node->publish_seq += 1u;
    if (out_seq)
    {
        *out_seq = node->publish_seq;
    }
    return 0;
}

static int fabric_publish_insert(kith_fabric_t *f,
                                 struct fabric_shard *sh,
                                 const kith_fabric_cell_key_t *key,
                                 uint32_t authority_epoch,
                                 uint32_t actor_count,
                                 uint64_t *out_seq)
{
    int rc = fabric_shard_ensure_publish_capacity(sh);
    if (rc != 0)
    {
        return rc;
    }
    struct fabric_cell_node *node =
        fabric_cell_create(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (!node)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    node->authority_epoch = authority_epoch;
    node->publish_seq = 1u;
    node->actor_count = actor_count;
    uint64_t total = 0u;
    if (ckd_add(&total, f->total_count, 1u))
    {
        node->deleted = true;
        sh->count -= 1u;
        return kith_error_return(KITH_EOVERFLOW);
    }
    f->total_count = total;
    if (out_seq)
    {
        *out_seq = node->publish_seq;
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_fabric_publish(kith_fabric_t *f,
                                               const kith_fabric_cell_key_t *key,
                                               uint32_t authority_epoch,
                                               uint64_t *out_seq)
{
    if (!f || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&f->lock);
    struct fabric_shard *sh = &f->shards[fabric_shard_for(key->zone)];
    struct fabric_cell_node *node =
        fabric_cell_find(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (node)
    {
        if (authority_epoch < node->authority_epoch)
        {
            pthread_mutex_unlock(&f->lock);
            return kith_error_return(KITH_EPERM);
        }
        node->actor_count = fabric_query_actor_count(
            f->sim, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
        int rc = fabric_publish_existing(node, authority_epoch, out_seq);
        if (rc != 0)
        {
            pthread_mutex_unlock(&f->lock);
            return rc;
        }
    }
    else
    {
        uint32_t actors = fabric_query_actor_count(
            f->sim, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
        int rc = fabric_publish_insert(f, sh, key, authority_epoch, actors, out_seq);
        if (rc != 0)
        {
            pthread_mutex_unlock(&f->lock);
            return rc;
        }
    }
    fabric_fanout(f, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    // Count every successful publish as a relaxed atomic on the publishing
    // thread (a worker under free-threaded Python). The composition root
    // records the per-tick delta as kith_fabric_publishes_total; this is
    // the publishes/sec term of the per-tick capacity
    // model.
    atomic_fetch_add_explicit(&f->publish_total, 1u, memory_order_relaxed);
    pthread_mutex_unlock(&f->lock);
    return 0;
}

/*---------------------------------------------------------------------------
 * remove
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_fabric_remove_cell(kith_fabric_t *f,
                                                   const kith_fabric_cell_key_t *key)
{
    if (!f || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&f->lock);
    struct fabric_shard *sh = &f->shards[fabric_shard_for(key->zone)];
    struct fabric_cell_node *node =
        fabric_cell_find(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (node)
    {
        node->deleted = true;
        sh->tombstones += 1u;
        if (sh->count > 0u)
        {
            sh->count -= 1u;
        }
        if (f->total_count > 0u)
        {
            f->total_count -= 1u;
        }
    }
    pthread_mutex_unlock(&f->lock);
    return 0;
}

[[nodiscard]] KITH_API int kith_fabric_remove_zone(kith_fabric_t *f, uint32_t zone)
{
    if (!f)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&f->lock);
    struct fabric_shard *sh = &f->shards[fabric_shard_for(zone)];
    for (size_t i = 0u; i < sh->buckets; ++i)
    {
        struct fabric_cell_node *c = &sh->cells[i];
        if (!c->used || c->deleted || c->zone != zone)
        {
            continue;
        }
        c->deleted = true;
        sh->tombstones += 1u;
        if (sh->count > 0u)
        {
            sh->count -= 1u;
        }
        if (f->total_count > 0u)
        {
            f->total_count -= 1u;
        }
    }
    pthread_mutex_unlock(&f->lock);
    return 0;
}

/*---------------------------------------------------------------------------
 * product header fill / compare
 *-------------------------------------------------------------------------*/

static void fabric_fill_product(kith_fabric_cell_product_t *p, const struct fabric_cell_node *c)
{
    p->key.zone = c->zone;
    p->key.cell_x = c->cell_x;
    p->key.cell_y = c->cell_y;
    p->key.cell_z = c->cell_z;
    p->key.lod = c->lod;
    p->key.pad[0] = 0u;
    p->key.pad[1] = 0u;
    p->key.pad[2] = 0u;
    p->authority_epoch = c->authority_epoch;
    p->publish_seq = c->publish_seq;
    p->product_level = KITH_FABRIC_LEVEL_FULL;
    p->actor_count = c->actor_count;
}

static int fabric_prod_cmp_seq_desc(const void *a, const void *b)
{
    const kith_fabric_cell_product_t *pa = a;
    const kith_fabric_cell_product_t *pb = b;
    if (pa->publish_seq < pb->publish_seq)
    {
        return 1;
    }
    if (pa->publish_seq > pb->publish_seq)
    {
        return -1;
    }
    // Equal publish_seq orders by ascending cell key: each cell's sequence
    // starts at 1, so cross-cell ties are real, and the tie-break keeps the
    // drained order specified across libc qsort implementations (the C
    // standard leaves equal-element order unspecified).
    if (pa->key.zone != pb->key.zone)
    {
        return pa->key.zone < pb->key.zone ? -1 : 1;
    }
    if (pa->key.cell_x != pb->key.cell_x)
    {
        return pa->key.cell_x < pb->key.cell_x ? -1 : 1;
    }
    if (pa->key.cell_y != pb->key.cell_y)
    {
        return pa->key.cell_y < pb->key.cell_y ? -1 : 1;
    }
    if (pa->key.cell_z != pb->key.cell_z)
    {
        return pa->key.cell_z < pb->key.cell_z ? -1 : 1;
    }
    if (pa->key.lod != pb->key.lod)
    {
        return pa->key.lod < pb->key.lod ? -1 : 1;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * snapshots
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_fabric_cell_product(kith_fabric_t *f,
                                                    const kith_fabric_cell_key_t *key,
                                                    kith_fabric_cell_product_t *out_product)
{
    if (!f || !key || !out_product)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&f->lock);
    const struct fabric_shard *sh = &f->shards[fabric_shard_for(key->zone)];
    const struct fabric_cell_node *c =
        fabric_cell_lookup(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (!c)
    {
        pthread_mutex_unlock(&f->lock);
        return kith_error_return(KITH_ENOENT);
    }
    fabric_fill_product(out_product, c);
    pthread_mutex_unlock(&f->lock);
    return 0;
}

[[nodiscard]] KITH_API int kith_fabric_snapshot_zone_cells(kith_fabric_t *f,
                                                           uint32_t zone,
                                                           uint8_t lod,
                                                           kith_fabric_cell_product_t *out,
                                                           size_t max,
                                                           size_t *out_count)
{
    if (!f)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&f->lock);
    const struct fabric_shard *sh = &f->shards[fabric_shard_for(zone)];
    size_t total = 0u;
    size_t copied = 0u;
    for (size_t i = 0u; i < sh->buckets; ++i)
    {
        const struct fabric_cell_node *c = &sh->cells[i];
        if (!c->used || c->deleted || c->zone != zone || c->lod != lod)
        {
            continue;
        }
        if (out != nullptr && copied < max)
        {
            fabric_fill_product(&out[copied], c);
            copied += 1u;
        }
        total += 1u;
    }
    pthread_mutex_unlock(&f->lock);
    if (out_count)
    {
        *out_count = out != nullptr ? copied : total;
    }
    return 0;
}

KITH_API uint64_t kith_fabric_product_count(const kith_fabric_t *f)
{
    if (!f)
    {
        return 0u;
    }
    pthread_mutex_lock(&((kith_fabric_t *)f)->lock);
    uint64_t n = f->total_count;
    pthread_mutex_unlock(&((kith_fabric_t *)f)->lock);
    return n;
}

KITH_API uint64_t kith_fabric_publish_total(const kith_fabric_t *f)
{
    if (!f)
    {
        return 0u;
    }
    return atomic_load_explicit(&((kith_fabric_t *)f)->publish_total, memory_order_relaxed);
}

/*---------------------------------------------------------------------------
 * tiered snapshot (queries borrowed sim, renders at a product level)
 *-------------------------------------------------------------------------*/

static void fabric_fill_artifact_full(kith_fabric_artifact_t *dst, const kith_sim_artifact_t *src)
{
    dst->actor_id = src->actor_id;
    dst->pos_x = src->pos_x;
    dst->pos_y = src->pos_y;
    dst->pos_z = src->pos_z;
    dst->vel_x = src->vel_x;
    dst->vel_y = src->vel_y;
    dst->vel_z = src->vel_z;
    dst->input_tick = src->input_tick;
    dst->update_seq = src->update_seq;
    dst->product_level = KITH_FABRIC_LEVEL_FULL;
}

static void fabric_fill_artifact_reduced(kith_fabric_artifact_t *dst,
                                         const kith_sim_artifact_t *src)
{
    dst->actor_id = src->actor_id;
    dst->pos_x = src->pos_x;
    dst->pos_y = src->pos_y;
    dst->pos_z = src->pos_z;
    dst->vel_x = src->vel_x;
    dst->vel_y = src->vel_y;
    dst->vel_z = src->vel_z;
    dst->input_tick = 0u;
    dst->update_seq = 0u;
    dst->product_level = KITH_FABRIC_LEVEL_REDUCED;
}

static int fabric_snapshot_count(kith_sim_t *sim, const kith_fabric_cell_key_t *key, size_t *out)
{
    kith_sim_artifact_key_t sk;
    fabric_sim_key(key, &sk);
    return kith_sim_snapshot_cell(sim, &sk, nullptr, 0u, out);
}

// Grow the scratch to hold at least @p rows. Caller holds snapshot_lock.
static int fabric_snapshot_ensure_scratch_locked(kith_fabric_t *f, size_t rows)
{
    if (rows <= f->snapshot_scratch_cap)
    {
        return 0;
    }
    kith_sim_artifact_t *grown =
        kith_realloc(f->allocator, f->snapshot_scratch, rows * sizeof(*grown));
    if (!grown)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    f->snapshot_scratch = grown;
    f->snapshot_scratch_cap = rows;
    return 0;
}

// Stage one cell's raw sim rows into the grow-only scratch. The caller holds
// snapshot_lock. On success *out_rows points at the staged rows (do not
// dereference when *out_rows_n is zero) and *out_rows_n carries their count.
static int fabric_snapshot_stage_locked(kith_fabric_t *f,
                                        const kith_sim_artifact_key_t *sk,
                                        const kith_sim_artifact_t **out_rows,
                                        size_t *out_rows_n)
{
    size_t rows = 0u;
    int rc = 0;
    for (int attempt = 0; attempt < FABRIC_SNAPSHOT_COPY_ATTEMPTS; ++attempt)
    {
        if (f->snapshot_scratch_cap == 0u)
        {
            // Cold scratch: the out=NULL form is the documented count
            // probe; size the scratch from it and copy on the next attempt.
            size_t count = 0u;
            rc = kith_sim_snapshot_cell(f->sim, sk, nullptr, 0u, &count);
            if (rc != 0 || count == 0u)
            {
                break;
            }
            rc = fabric_snapshot_ensure_scratch_locked(f, count);
            if (rc != 0)
            {
                break;
            }
            continue;
        }
        rc =
            kith_sim_snapshot_cell(f->sim, sk, f->snapshot_scratch, f->snapshot_scratch_cap, &rows);
        if (rc != kith_error_return(KITH_ERANGE))
        {
            // Success, or a failure truncation handling must not touch.
            break;
        }
        // Truncated by a publish landing between the probe and the copy:
        // rows carries the required count; grow and retry. Exhausting the
        // attempts propagates the final -KITH_ERANGE with that count.
        rc = fabric_snapshot_ensure_scratch_locked(f, rows);
        if (rc != 0)
        {
            break;
        }
    }
    if (rc != 0)
    {
        *out_rows_n = 0u;
        return rc;
    }
    *out_rows = f->snapshot_scratch;
    *out_rows_n = rows;
    return 0;
}

static int fabric_snapshot_full_or_reduced(kith_fabric_t *f,
                                           const kith_fabric_cell_key_t *key,
                                           kith_fabric_product_level_t level,
                                           kith_fabric_artifact_t *out,
                                           size_t max,
                                           size_t *out_count)
{
    // A count probe (out == NULL) returns the cell's artifact count without
    // fetching or rendering: callers sizing a buffer use this form, and the
    // copy path reports -KITH_ERANGE with the required count when a buffer
    // turns out too small instead of dropping rows silently.
    if (out == nullptr)
    {
        return fabric_snapshot_count(f->sim, key, out_count);
    }
    kith_sim_artifact_key_t sk;
    fabric_sim_key(key, &sk);
    pthread_mutex_lock(&f->snapshot_lock);
    const kith_sim_artifact_t *rows = nullptr;
    size_t rows_n = 0u;
    int rc = fabric_snapshot_stage_locked(f, &sk, &rows, &rows_n);
    if (rc == 0)
    {
        // Render order follows the staged rows; the sim snapshot order is
        // unspecified but deterministic, and no consumer of the rendered
        // artifacts depends on it.
        size_t copied = (rows_n < max) ? rows_n : max;
        for (size_t i = 0u; i < copied; ++i)
        {
            if (level == KITH_FABRIC_LEVEL_REDUCED)
            {
                fabric_fill_artifact_reduced(&out[i], &rows[i]);
            }
            else
            {
                fabric_fill_artifact_full(&out[i], &rows[i]);
            }
        }
        if (rows_n > max)
        {
            rc = kith_error_return(KITH_ERANGE);
        }
        if (out_count != nullptr)
        {
            *out_count = rows_n;
        }
    }
    pthread_mutex_unlock(&f->snapshot_lock);
    return rc;
}

// Crowd centroid accumulation runs in 128-bit arithmetic: a cell's position
// sum overflows int64 once the population's coordinates spread wide, and
// the truncated wide mean of int64 values always fits int64, so the
// accumulation and the narrowing stay exact. The population is memory-
// bounded far below the 2^64 rows at which the wide sum itself overflows.

static int fabric_snapshot_crowd(kith_fabric_t *f,
                                 const kith_fabric_cell_key_t *key,
                                 kith_fabric_artifact_t *out,
                                 size_t max,
                                 size_t *out_count)
{
    kith_sim_artifact_key_t sk;
    fabric_sim_key(key, &sk);
    pthread_mutex_lock(&f->snapshot_lock);
    const kith_sim_artifact_t *rows = nullptr;
    size_t rows_n = 0u;
    int rc = fabric_snapshot_stage_locked(f, &sk, &rows, &rows_n);
    size_t result = 0u;
    if (rc == 0 && rows_n > 0u)
    {
        result = 1u;
        if (out != nullptr && max > 0u)
        {
            kith_i128_t sx = 0;
            kith_i128_t sy = 0;
            kith_i128_t sz = 0;
            for (size_t i = 0u; i < rows_n; ++i)
            {
                sx += rows[i].pos_x;
                sy += rows[i].pos_y;
                sz += rows[i].pos_z;
            }
            kith_i128_t n = (kith_i128_t)rows_n;
            out[0].actor_id = 0u;
            out[0].pos_x = (int64_t)(sx / n);
            out[0].pos_y = (int64_t)(sy / n);
            out[0].pos_z = (int64_t)(sz / n);
            out[0].vel_x = 0;
            out[0].vel_y = 0;
            out[0].vel_z = 0;
            out[0].input_tick = 0u;
            out[0].update_seq = 0u;
            out[0].product_level = KITH_FABRIC_LEVEL_CROWD;
        }
    }
    pthread_mutex_unlock(&f->snapshot_lock);
    if (rc != 0)
    {
        return rc;
    }
    if (out_count != nullptr)
    {
        if (out != nullptr)
        {
            *out_count = (rows_n > 0u) ? 1u : 0u;
        }
        else
        {
            *out_count = result;
        }
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_fabric_snapshot_cell(kith_fabric_t *f,
                                                     const kith_fabric_cell_key_t *key,
                                                     kith_fabric_product_level_t level,
                                                     kith_fabric_artifact_t *out,
                                                     size_t max,
                                                     size_t *out_count)
{
    if (!f || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (level == KITH_FABRIC_LEVEL_CROWD)
    {
        return fabric_snapshot_crowd(f, key, out, max, out_count);
    }
    return fabric_snapshot_full_or_reduced(f, key, level, out, max, out_count);
}

/*---------------------------------------------------------------------------
 * subscriptions
 *-------------------------------------------------------------------------*/

static int fabric_sub_init(kith_fabric_subscription_t *sub, kith_fabric_t *f, size_t buckets)
{
    sub->allocator = f->allocator;
    sub->nodes = kith_alloc_zero(sub->allocator, buckets, sizeof(*sub->nodes));
    if (!sub->nodes)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    if (pthread_mutex_init(&sub->lock, nullptr) != 0)
    {
        kith_free(sub->allocator, sub->nodes);
        sub->nodes = nullptr;
        return kith_error_return(KITH_ENOMEM);
    }
    sub->fabric = f;
    sub->buckets = buckets;
    sub->count = 0u;
    return 0;
}

static struct fabric_sub_node *fabric_sub_find(
    kith_fabric_subscription_t *sub, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    size_t mask = fabric_mask(sub->buckets);
    size_t i = (size_t)fabric_cell_mix(zone, cx, cy, cz, lod) & mask;
    for (size_t probe = 0u; probe < sub->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct fabric_sub_node *n = &sub->nodes[idx];
        if (!n->used)
        {
            return nullptr;
        }
        if (n->deleted)
        {
            continue;
        }
        if (n->key.zone == zone && n->key.cell_x == cx && n->key.cell_y == cy &&
            n->key.cell_z == cz && n->key.lod == lod)
        {
            return n;
        }
    }
    return nullptr;
}

[[nodiscard]] KITH_API int kith_fabric_create_subscription(kith_fabric_t *f,
                                                           kith_fabric_subscription_t **out_sub)
{
    if (!out_sub)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_sub = nullptr;
    if (!f)
    {
        return kith_error_return(KITH_EINVAL);
    }
    kith_fabric_subscription_t *sub = kith_alloc_zero(f->allocator, 1, sizeof(*sub));
    if (!sub)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    size_t buckets = (size_t)fabric_resolve_buckets(KITH_FABRIC_DEFAULT_BUCKET_COUNT);
    int rc = fabric_sub_init(sub, f, buckets);
    if (rc != 0)
    {
        kith_free(f->allocator, sub);
        return rc;
    }
    pthread_mutex_lock(&f->lock);
    rc = fabric_sub_table_add(f, sub);
    pthread_mutex_unlock(&f->lock);
    if (rc != 0)
    {
        pthread_mutex_destroy(&sub->lock);
        kith_free(f->allocator, sub->nodes);
        kith_free(f->allocator, sub);
        return rc;
    }
    *out_sub = sub;
    return 0;
}

KITH_API void kith_fabric_subscription_destroy(kith_fabric_subscription_t *sub)
{
    if (!sub)
    {
        return;
    }
    if (sub->fabric)
    {
        pthread_mutex_lock(&sub->fabric->lock);
        fabric_sub_table_remove(sub->fabric, sub);
        pthread_mutex_unlock(&sub->fabric->lock);
    }
    pthread_mutex_destroy(&sub->lock);
    kith_free(sub->allocator, sub->nodes);
    kith_free(sub->allocator, sub);
}

[[nodiscard]] KITH_API int kith_fabric_subscription_add(kith_fabric_subscription_t *sub,
                                                        const kith_fabric_cell_key_t *key)
{
    if (!sub || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&sub->lock);
    struct fabric_sub_node *existing =
        fabric_sub_find(sub, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (existing)
    {
        pthread_mutex_unlock(&sub->lock);
        return 0;
    }
    size_t mask = fabric_mask(sub->buckets);
    size_t i =
        (size_t)fabric_cell_mix(key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod) & mask;
    struct fabric_sub_node *first_tomb = nullptr;
    for (size_t probe = 0u; probe < sub->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct fabric_sub_node *n = &sub->nodes[idx];
        if (!n->used)
        {
            struct fabric_sub_node *target = first_tomb ? first_tomb : n;
            target->used = true;
            target->deleted = false;
            target->pending = false;
            target->key = *key;
            sub->count += 1u;
            pthread_mutex_unlock(&sub->lock);
            return 0;
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
    if (first_tomb)
    {
        first_tomb->deleted = false;
        first_tomb->pending = false;
        first_tomb->key = *key;
        sub->count += 1u;
        pthread_mutex_unlock(&sub->lock);
        return 0;
    }
    pthread_mutex_unlock(&sub->lock);
    return kith_error_return(KITH_ENOMEM);
}

[[nodiscard]] KITH_API int kith_fabric_subscription_remove(kith_fabric_subscription_t *sub,
                                                           const kith_fabric_cell_key_t *key)
{
    if (!sub || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&sub->lock);
    struct fabric_sub_node *n =
        fabric_sub_find(sub, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (n)
    {
        n->deleted = true;
        if (sub->count > 0u)
        {
            sub->count -= 1u;
        }
    }
    pthread_mutex_unlock(&sub->lock);
    return 0;
}

KITH_API size_t kith_fabric_subscription_size(const kith_fabric_subscription_t *sub)
{
    if (!sub)
    {
        return 0u;
    }
    pthread_mutex_lock(&((kith_fabric_subscription_t *)sub)->lock);
    size_t n = sub->count;
    pthread_mutex_unlock(&((kith_fabric_subscription_t *)sub)->lock);
    return n;
}

/*---------------------------------------------------------------------------
 * drain
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_fabric_drain(kith_fabric_t *f,
                                             kith_fabric_subscription_t *sub,
                                             kith_fabric_cell_product_t *out,
                                             size_t max,
                                             size_t *out_count)
{
    if (!f || !sub)
    {
        return kith_error_return(KITH_EINVAL);
    }
    /* Fabric lock guards the cell store the drain probes; sub lock guards
     * the interest set it iterates. Fabric-first ordering matches fanout. */
    pthread_mutex_lock(&f->lock);
    pthread_mutex_lock(&sub->lock);
    size_t total = 0u;
    size_t copied = 0u;
    for (size_t i = 0u; i < sub->buckets; ++i)
    {
        struct fabric_sub_node *n = &sub->nodes[i];
        if (!n->used || n->deleted || !n->pending)
        {
            continue;
        }
        if (out == nullptr)
        {
            // Counting mode (out == NULL) is a pure query: it reports the
            // pending count without clearing flags.
            total += 1u;
            continue;
        }
        if (copied == max)
        {
            // Buffer full: the cell stays pending for the next drain.
            continue;
        }
        const struct fabric_shard *sh = &f->shards[fabric_shard_for(n->key.zone)];
        const struct fabric_cell_node *c = fabric_cell_lookup(
            sh, n->key.zone, n->key.cell_x, n->key.cell_y, n->key.cell_z, n->key.lod);
        if (c)
        {
            fabric_fill_product(&out[copied], c);
            copied += 1u;
        }
        // The flag clears for a delivered product, or for a cell that has
        // left the store (nothing can be delivered for it). Every other
        // undelivered cell stays pending for the next drain.
        n->pending = false;
    }
    pthread_mutex_unlock(&sub->lock);
    pthread_mutex_unlock(&f->lock);
    if (out != nullptr && copied > 1u)
    {
        qsort(out, copied, sizeof(*out), fabric_prod_cmp_seq_desc);
    }
    if (out_count)
    {
        *out_count = out != nullptr ? copied : total;
    }
    return 0;
}

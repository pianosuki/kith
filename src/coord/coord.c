/* Single translation unit of the coord module: sharded cell tracking, density
 * estimation, and split/merge thresholds with dwell hysteresis. The shared
 * structures live in coord_internal.h; the public contract is
 * include/kith/coord/coord.h. */

#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>

#include "coord/coord_internal.h"
#include "kith/types.h"
#include "kith/version.h"

/*---------------------------------------------------------------------------
 * shared helpers
 *-------------------------------------------------------------------------*/

static size_t coord_mask(size_t buckets)
{
    return buckets - 1u;
}

static uint32_t coord_next_pow2(uint32_t v)
{
    uint32_t r = 1u;
    while (r < v)
    {
        r <<= 1u;
    }
    return r;
}

static uint32_t coord_resolve_buckets(uint32_t requested)
{
    uint32_t b = coord_next_pow2(requested);
    if (b < 16u)
    {
        b = 16u;
    }
    return b;
}

static size_t coord_shard_for(uint32_t zone)
{
    return (size_t)(zone % KITH_COORD_CELL_SHARDS);
}

/*---------------------------------------------------------------------------
 * params / defaults
 *-------------------------------------------------------------------------*/

static void coord_resolve_params(kith_coord_params_t *out)
{
    if (out->cell_bucket_count == 0u)
    {
        out->cell_bucket_count = KITH_COORD_DEFAULT_CELL_BUCKETS;
    }
    if (out->density_stride == 0u)
    {
        out->density_stride = KITH_COORD_DEFAULT_DENSITY_STRIDE;
    }
    if (out->split_threshold == 0u)
    {
        out->split_threshold = KITH_COORD_DEFAULT_SPLIT_THRESHOLD;
    }
    if (out->merge_threshold == 0u)
    {
        out->merge_threshold = KITH_COORD_DEFAULT_MERGE_THRESHOLD;
    }
    if (out->split_min_dwell_ms == 0u)
    {
        out->split_min_dwell_ms = KITH_COORD_DEFAULT_SPLIT_DWELL_MS;
    }
    if (out->merge_min_dwell_ms == 0u)
    {
        out->merge_min_dwell_ms = KITH_COORD_DEFAULT_MERGE_DWELL_MS;
    }
}

static bool coord_params_validate(const kith_coord_params_t *params, kith_error_t *out_err)
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

/* Validate @p params (NULL selects the size-versioned defaults) and apply
 * the resolver. Returns 0, or the negated error the params carry. */
static int coord_params_ready(const kith_coord_params_t *params, kith_coord_params_t *resolved)
{
    if (params)
    {
        kith_error_t err = KITH_OK;
        if (!coord_params_validate(params, &err))
        {
            return kith_error_return(err);
        }
        *resolved = *params;
    }
    else
    {
        memset(resolved, 0, sizeof(*resolved));
        resolved->size = sizeof(*resolved);
        resolved->abi_version = KITH_ABI_VERSION;
    }
    coord_resolve_params(resolved);
    return 0;
}

static bool coord_bus_params_validate(const kith_coord_bus_params_t *params, kith_error_t *out_err)
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
 * cell ownership table
 *-------------------------------------------------------------------------*/

static const struct coord_cell_node *coord_cell_lookup(
    const struct coord_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    size_t mask = coord_mask(sh->buckets);
    size_t i = (size_t)coord_cell_mix(zone, cx, cy, cz, lod) & mask;
    for (size_t probe = 0u; probe < sh->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        const struct coord_cell_node *c = &sh->cells[idx];
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

static struct coord_cell_node *coord_cell_find(
    struct coord_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    return (struct coord_cell_node *)coord_cell_lookup(sh, zone, cx, cy, cz, lod);
}

static struct coord_cell_node *coord_cell_create(
    struct coord_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    size_t mask = coord_mask(sh->buckets);
    size_t i = (size_t)coord_cell_mix(zone, cx, cy, cz, lod) & mask;
    struct coord_cell_node *first_tomb = nullptr;
    for (size_t probe = 0u; probe < sh->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct coord_cell_node *c = &sh->cells[idx];
        if (!c->used)
        {
            struct coord_cell_node *target = first_tomb ? first_tomb : c;
            target->used = true;
            target->deleted = false;
            target->zone = zone;
            target->cell_x = cx;
            target->cell_y = cy;
            target->cell_z = cz;
            target->lod = lod;
            target->instance_id = 0u;
            target->authority_epoch = 0u;
            sh->count += 1u;
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
    return first_tomb;
}

static int coord_shard_init(struct coord_shard *sh, size_t buckets, const kith_allocator_t *alloc)
{
    sh->cells = kith_alloc_zero(alloc, buckets, sizeof(*sh->cells));
    if (!sh->cells)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    sh->buckets = buckets;
    sh->count = 0u;
    return 0;
}

static void coord_shard_fini(struct coord_shard *sh, const kith_allocator_t *alloc)
{
    kith_free(alloc, sh->cells);
    memset(sh, 0, sizeof(*sh));
}

/*---------------------------------------------------------------------------
 * hash-fallback authority
 *-------------------------------------------------------------------------*/

static uint32_t
coord_hash_fallback(const kith_coord_t *coord, uint32_t zone, int32_t cx, int32_t cy)
{
    if (!coord->bus || coord->bus->member_count <= 1u)
    {
        return coord->instance_id;
    }
    uint32_t members = (uint32_t)coord->bus->member_count;
    uint32_t h = ((uint32_t)cx + (uint32_t)cy + zone) % members;
    if (h >= members)
    {
        h = 0u;
    }
    return coord->bus->members[h].instance_id;
}

/*---------------------------------------------------------------------------
 * density table
 *-------------------------------------------------------------------------*/

static struct coord_density_node *coord_density_find(struct coord_density_node *nodes,
                                                     size_t buckets,
                                                     uint32_t zone,
                                                     int32_t cx,
                                                     int32_t cy,
                                                     int32_t cz,
                                                     uint8_t lod)
{
    size_t mask = coord_mask(buckets);
    size_t i = (size_t)coord_cell_mix(zone, cx, cy, cz, lod) & mask;
    for (size_t probe = 0u; probe < buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct coord_density_node *n = &nodes[idx];
        if (!n->used)
        {
            return nullptr;
        }
        if (n->deleted)
        {
            continue;
        }
        if (n->zone == zone && n->cell_x == cx && n->cell_y == cy && n->cell_z == cz &&
            n->lod == lod)
        {
            return n;
        }
    }
    return nullptr;
}

static struct coord_density_node *coord_density_create(struct coord_density_node *nodes,
                                                       size_t buckets,
                                                       uint32_t zone,
                                                       int32_t cx,
                                                       int32_t cy,
                                                       int32_t cz,
                                                       uint8_t lod)
{
    size_t mask = coord_mask(buckets);
    size_t i = (size_t)coord_cell_mix(zone, cx, cy, cz, lod) & mask;
    struct coord_density_node *first_tomb = nullptr;
    for (size_t probe = 0u; probe < buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct coord_density_node *n = &nodes[idx];
        if (!n->used)
        {
            struct coord_density_node *target = first_tomb ? first_tomb : n;
            target->used = true;
            target->deleted = false;
            target->zone = zone;
            target->cell_x = cx;
            target->cell_y = cy;
            target->cell_z = cz;
            target->lod = lod;
            target->actor_count = 0u;
            target->split_since_ms = 0u;
            target->merge_since_ms = 0u;
            target->last_report_ms = 0u;
            return target;
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

static int
coord_density_init(struct coord_density_node **out, size_t buckets, const kith_allocator_t *alloc)
{
    *out = kith_alloc_zero(alloc, buckets, sizeof(**out));
    if (!*out)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * bus membership
 *-------------------------------------------------------------------------*/

static int
coord_bus_member_add(struct kith_coord_bus *bus, uint32_t instance_id, uint64_t heartbeat_ms)
{
    if (bus->member_count == bus->member_cap)
    {
        size_t cap = bus->member_cap == 0u ? 8u : bus->member_cap * 2u;
        struct coord_bus_member *arr =
            kith_realloc(bus->allocator, bus->members, cap * sizeof(*arr));
        if (!arr)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        bus->members = arr;
        bus->member_cap = cap;
    }
    bus->members[bus->member_count].instance_id = instance_id;
    bus->members[bus->member_count].heartbeat_ms = heartbeat_ms;
    bus->members[bus->member_count].owned_cell_count = 0u;
    bus->members[bus->member_count].active_input_count = 0u;
    bus->member_count += 1u;
    return 0;
}

static struct coord_bus_member *coord_bus_member_find(struct kith_coord_bus *bus,
                                                      uint32_t instance_id)
{
    for (size_t i = 0u; i < bus->member_count; ++i)
    {
        if (bus->members[i].instance_id == instance_id)
        {
            return &bus->members[i];
        }
    }
    return nullptr;
}

/*---------------------------------------------------------------------------
 * bus zone subscriptions
 *-------------------------------------------------------------------------*/

static struct coord_bus_zone_sub *coord_zone_sub_find(struct kith_coord_bus *bus, uint32_t zone)
{
    for (size_t i = 0u; i < bus->zone_sub_count; ++i)
    {
        if (bus->zone_subs[i].zone == zone)
        {
            return &bus->zone_subs[i];
        }
    }
    return nullptr;
}

static int coord_zone_sub_grow(struct kith_coord_bus *bus)
{
    size_t cap = bus->zone_sub_cap == 0u ? 16u : bus->zone_sub_cap * 2u;
    struct coord_bus_zone_sub *arr =
        kith_realloc(bus->allocator, bus->zone_subs, cap * sizeof(*arr));
    if (!arr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    bus->zone_subs = arr;
    bus->zone_sub_cap = cap;
    return 0;
}

/*---------------------------------------------------------------------------
 * coord handle lifecycle
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_coord_create(const kith_coord_params_t *params,
                                             kith_coord_bus_t *bus,
                                             const kith_allocator_t *alloc,
                                             kith_coord_t **out_coord)
{
    if (!out_coord)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_coord = nullptr;

    kith_coord_params_t resolved;
    const int params_rc = coord_params_ready(params, &resolved);
    if (params_rc != 0)
    {
        return params_rc;
    }

    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    kith_coord_t *c = kith_alloc_zero(allocator, 1, sizeof(*c));
    if (!c)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    c->allocator = allocator;
    c->bus = bus;
    c->instance_id = resolved.instance_id;
    c->authority_epoch_counter = 0u;
    c->split_threshold = resolved.split_threshold;
    c->merge_threshold = resolved.merge_threshold;
    c->split_min_dwell_ms = resolved.split_min_dwell_ms;
    c->merge_min_dwell_ms = resolved.merge_min_dwell_ms;
    c->density_stride = resolved.density_stride;
    c->tick_counter = 0u;

    size_t buckets = (size_t)coord_resolve_buckets(resolved.cell_bucket_count);
    for (size_t i = 0u; i < KITH_COORD_CELL_SHARDS; ++i)
    {
        int rc = coord_shard_init(&c->shards[i], buckets, allocator);
        if (rc != 0)
        {
            for (size_t j = 0u; j < i; ++j)
            {
                coord_shard_fini(&c->shards[j], allocator);
            }
            kith_free(allocator, c);
            return rc;
        }
    }

    size_t density_buckets = (size_t)coord_resolve_buckets(KITH_COORD_DEFAULT_CELL_BUCKETS);
    int rc = coord_density_init(&c->density, density_buckets, allocator);
    if (rc != 0)
    {
        for (size_t i = 0u; i < KITH_COORD_CELL_SHARDS; ++i)
        {
            coord_shard_fini(&c->shards[i], allocator);
        }
        kith_free(allocator, c);
        return rc;
    }
    c->density_buckets = density_buckets;
    c->density_count = 0u;

    *out_coord = c;
    return 0;
}

KITH_API void kith_coord_destroy(kith_coord_t *coord)
{
    if (!coord)
    {
        return;
    }
    for (size_t i = 0u; i < KITH_COORD_CELL_SHARDS; ++i)
    {
        coord_shard_fini(&coord->shards[i], coord->allocator);
    }
    kith_free(coord->allocator, coord->density);
    kith_free(coord->allocator, coord);
}

/*---------------------------------------------------------------------------
 * cell ownership
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_coord_authority(const kith_coord_t *coord,
                                                const kith_fabric_cell_key_t *key,
                                                kith_coord_authority_t *out_authority)
{
    if (!coord || !key || !out_authority)
    {
        return kith_error_return(KITH_EINVAL);
    }
    const struct coord_shard *sh = &coord->shards[coord_shard_for(key->zone)];
    const struct coord_cell_node *c =
        coord_cell_lookup(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (c)
    {
        out_authority->instance_id = c->instance_id;
        out_authority->authority_epoch = c->authority_epoch;
        return 0;
    }
    out_authority->instance_id = coord_hash_fallback(coord, key->zone, key->cell_x, key->cell_y);
    out_authority->authority_epoch = 0u;
    return 0;
}

[[nodiscard]] KITH_API int kith_coord_set_authority(kith_coord_t *coord,
                                                    const kith_fabric_cell_key_t *key,
                                                    uint32_t instance_id)
{
    if (!coord || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    coord->authority_epoch_counter += 1u;
    uint32_t epoch = coord->authority_epoch_counter;

    struct coord_shard *sh = &coord->shards[coord_shard_for(key->zone)];
    struct coord_cell_node *node =
        coord_cell_find(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (node)
    {
        node->instance_id = instance_id;
        node->authority_epoch = epoch;
        return 0;
    }
    node = coord_cell_create(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (!node)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    node->instance_id = instance_id;
    node->authority_epoch = epoch;
    return 0;
}

[[nodiscard]] KITH_API int kith_coord_clear_authority(kith_coord_t *coord,
                                                      const kith_fabric_cell_key_t *key)
{
    if (!coord || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct coord_shard *sh = &coord->shards[coord_shard_for(key->zone)];
    struct coord_cell_node *node =
        coord_cell_find(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (node)
    {
        node->deleted = true;
        if (sh->count > 0u)
        {
            sh->count -= 1u;
        }
    }
    coord->authority_epoch_counter += 1u;
    return 0;
}

KITH_API uint32_t kith_coord_cell_count(const kith_coord_t *coord)
{
    if (!coord)
    {
        return 0u;
    }
    uint32_t total = 0u;
    for (size_t i = 0u; i < KITH_COORD_CELL_SHARDS; ++i)
    {
        total += (uint32_t)coord->shards[i].count;
    }
    return total;
}

[[nodiscard]] KITH_API uint32_t kith_coord_owned_cell_count(const kith_coord_t *coord,
                                                            uint32_t instance_id)
{
    if (!coord)
    {
        return 0u;
    }
    uint32_t total = 0u;
    for (size_t i = 0u; i < KITH_COORD_CELL_SHARDS; ++i)
    {
        const struct coord_shard *sh = &coord->shards[i];
        for (size_t j = 0u; j < sh->buckets; ++j)
        {
            const struct coord_cell_node *c = &sh->cells[j];
            if (c->used && !c->deleted && c->instance_id == instance_id)
            {
                total += 1u;
            }
        }
    }
    return total;
}

[[nodiscard]] KITH_API int kith_coord_snapshot_zone(const kith_coord_t *coord,
                                                    uint32_t zone,
                                                    uint8_t lod,
                                                    kith_coord_cell_entry_t *out,
                                                    size_t max,
                                                    size_t *out_count)
{
    if (!coord)
    {
        return kith_error_return(KITH_EINVAL);
    }
    const struct coord_shard *sh = &coord->shards[coord_shard_for(zone)];
    size_t total = 0u;
    size_t copied = 0u;
    for (size_t i = 0u; i < sh->buckets; ++i)
    {
        const struct coord_cell_node *c = &sh->cells[i];
        if (!c->used || c->deleted || c->zone != zone || c->lod != lod)
        {
            continue;
        }
        if (out != nullptr && copied < max)
        {
            out[copied].key.zone = c->zone;
            out[copied].key.cell_x = c->cell_x;
            out[copied].key.cell_y = c->cell_y;
            out[copied].key.cell_z = c->cell_z;
            out[copied].key.lod = c->lod;
            out[copied].key.pad[0] = 0u;
            out[copied].key.pad[1] = 0u;
            out[copied].key.pad[2] = 0u;
            out[copied].instance_id = c->instance_id;
            out[copied].authority_epoch = c->authority_epoch;
            copied += 1u;
        }
        total += 1u;
    }
    if (out_count)
    {
        *out_count = out != nullptr ? copied : total;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * split/merge coordinator
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_coord_report_density(kith_coord_t *coord,
                                                     const kith_fabric_cell_key_t *key,
                                                     uint32_t actor_count,
                                                     uint64_t now_ms)
{
    if (!coord || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct coord_density_node *n = coord_density_find(coord->density,
                                                      coord->density_buckets,
                                                      key->zone,
                                                      key->cell_x,
                                                      key->cell_y,
                                                      key->cell_z,
                                                      key->lod);
    if (!n)
    {
        n = coord_density_create(coord->density,
                                 coord->density_buckets,
                                 key->zone,
                                 key->cell_x,
                                 key->cell_y,
                                 key->cell_z,
                                 key->lod);
        if (!n)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        coord->density_count += 1u;
    }
    n->actor_count = actor_count;
    n->last_report_ms = now_ms;

    if (actor_count >= coord->split_threshold)
    {
        if (n->split_since_ms == 0u)
        {
            n->split_since_ms = now_ms;
        }
        n->merge_since_ms = 0u;
    }
    else if (actor_count <= coord->merge_threshold)
    {
        if (n->merge_since_ms == 0u)
        {
            n->merge_since_ms = now_ms;
        }
        n->split_since_ms = 0u;
    }
    else
    {
        n->split_since_ms = 0u;
        n->merge_since_ms = 0u;
    }
    return 0;
}

static uint32_t coord_pick_split_target(const kith_coord_t *coord, uint32_t local_id)
{
    if (!coord->bus || coord->bus->member_count <= 1u)
    {
        return local_id;
    }
    // A split to the local instance is rejected downstream by
    // construction, so the picker ranks only the other members. Ties
    // resolve in membership order; remote members report no load until a
    // transport carries load reports.
    uint32_t best_id = local_id;
    uint32_t best_load = 0u;
    bool have_best = false;
    for (size_t i = 0u; i < coord->bus->member_count; ++i)
    {
        const struct coord_bus_member *m = &coord->bus->members[i];
        if (m->instance_id == local_id)
        {
            continue;
        }
        if (!have_best || m->owned_cell_count < best_load)
        {
            best_id = m->instance_id;
            best_load = m->owned_cell_count;
            have_best = true;
        }
    }
    return best_id;
}

static int coord_broadcast_rebalance(kith_coord_t *coord,
                                     const kith_fabric_cell_key_t *key,
                                     uint32_t source_id,
                                     uint32_t target_id,
                                     uint32_t epoch)
{
    if (!coord->bus)
    {
        return 0;
    }
    kith_coord_rebalance_contract_t contract;
    contract.key = *key;
    contract.source_instance_id = source_id;
    contract.target_instance_id = target_id;
    contract.authority_epoch = epoch;
    contract.pad = 0u;
    return kith_coord_bus_publish(
        coord->bus, KITH_COORD_BUS_EVENT_REBALANCE, key->zone, &contract, sizeof(contract));
}

static int coord_tick_split(kith_coord_t *coord, struct coord_density_node *n)
{
    struct coord_shard *sh = &coord->shards[coord_shard_for(n->zone)];
    struct coord_cell_node *cell =
        coord_cell_find(sh, n->zone, n->cell_x, n->cell_y, n->cell_z, n->lod);
    if (cell)
    {
        return 0;
    }
    uint32_t target = coord_pick_split_target(coord, coord->instance_id);
    if (target == coord->instance_id)
    {
        return 0;
    }
    coord->authority_epoch_counter += 1u;
    uint32_t epoch = coord->authority_epoch_counter;
    struct coord_cell_node *node =
        coord_cell_create(sh, n->zone, n->cell_x, n->cell_y, n->cell_z, n->lod);
    if (!node)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    node->instance_id = target;
    node->authority_epoch = epoch;
    kith_fabric_cell_key_t key;
    key.zone = n->zone;
    key.cell_x = n->cell_x;
    key.cell_y = n->cell_y;
    key.cell_z = n->cell_z;
    key.lod = n->lod;
    key.pad[0] = 0u;
    key.pad[1] = 0u;
    key.pad[2] = 0u;
    int rc = coord_broadcast_rebalance(coord, &key, coord->instance_id, target, epoch);
    if (rc != 0)
    {
        return rc;
    }
    n->split_since_ms = 0u;
    return 0;
}

static int coord_tick_merge(kith_coord_t *coord, struct coord_density_node *n)
{
    struct coord_shard *sh = &coord->shards[coord_shard_for(n->zone)];
    struct coord_cell_node *cell =
        coord_cell_find(sh, n->zone, n->cell_x, n->cell_y, n->cell_z, n->lod);
    if (!cell)
    {
        return 0;
    }
    coord->authority_epoch_counter += 1u;
    uint32_t epoch = coord->authority_epoch_counter;
    cell->deleted = true;
    if (sh->count > 0u)
    {
        sh->count -= 1u;
    }
    kith_fabric_cell_key_t key;
    key.zone = n->zone;
    key.cell_x = n->cell_x;
    key.cell_y = n->cell_y;
    key.cell_z = n->cell_z;
    key.lod = n->lod;
    key.pad[0] = 0u;
    key.pad[1] = 0u;
    key.pad[2] = 0u;
    int rc = coord_broadcast_rebalance(coord, &key, cell->instance_id, 0u, epoch);
    if (rc != 0)
    {
        return rc;
    }
    n->merge_since_ms = 0u;
    return 0;
}

[[nodiscard]] KITH_API int kith_coord_tick(kith_coord_t *coord, uint64_t now_ms)
{
    if (!coord)
    {
        return kith_error_return(KITH_EINVAL);
    }
    coord->tick_counter += 1u;
    if (coord->density_stride > 0u && (coord->tick_counter % coord->density_stride) != 0u)
    {
        return 0;
    }
    if (!coord->bus || coord->bus->member_count <= 1u)
    {
        return 0;
    }
    // Publish the local member's load from the coord's own table: the
    // overrides targeting this instance are the cells it owns. Remote
    // members report no load until a transport carries load reports.
    struct coord_bus_member *self = coord_bus_member_find(coord->bus, coord->instance_id);
    if (self != nullptr)
    {
        self->owned_cell_count = kith_coord_owned_cell_count(coord, coord->instance_id);
    }
    for (size_t i = 0u; i < coord->density_buckets; ++i)
    {
        struct coord_density_node *n = &coord->density[i];
        if (!n->used || n->deleted)
        {
            continue;
        }
        if (n->split_since_ms != 0u && (now_ms - n->split_since_ms) >= coord->split_min_dwell_ms)
        {
            int rc = coord_tick_split(coord, n);
            if (rc != 0)
            {
                return rc;
            }
        }
        if (n->merge_since_ms != 0u && (now_ms - n->merge_since_ms) >= coord->merge_min_dwell_ms)
        {
            int rc = coord_tick_merge(coord, n);
            if (rc != 0)
            {
                return rc;
            }
        }
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_coord_on_rebalance(kith_coord_t *coord,
                                                   const kith_coord_rebalance_contract_t *contract)
{
    if (!coord || !contract)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct coord_shard *sh = &coord->shards[coord_shard_for(contract->key.zone)];
    if (contract->target_instance_id == 0u)
    {
        struct coord_cell_node *node = coord_cell_find(sh,
                                                       contract->key.zone,
                                                       contract->key.cell_x,
                                                       contract->key.cell_y,
                                                       contract->key.cell_z,
                                                       contract->key.lod);
        if (node)
        {
            node->deleted = true;
            if (sh->count > 0u)
            {
                sh->count -= 1u;
            }
        }
        return 0;
    }
    struct coord_cell_node *node = coord_cell_find(sh,
                                                   contract->key.zone,
                                                   contract->key.cell_x,
                                                   contract->key.cell_y,
                                                   contract->key.cell_z,
                                                   contract->key.lod);
    if (node)
    {
        node->instance_id = contract->target_instance_id;
        node->authority_epoch = contract->authority_epoch;
        return 0;
    }
    node = coord_cell_create(sh,
                             contract->key.zone,
                             contract->key.cell_x,
                             contract->key.cell_y,
                             contract->key.cell_z,
                             contract->key.lod);
    if (!node)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    node->instance_id = contract->target_instance_id;
    node->authority_epoch = contract->authority_epoch;
    return 0;
}

/*---------------------------------------------------------------------------
 * bus handle lifecycle
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_coord_bus_create(const kith_coord_bus_params_t *params,
                                                 const kith_allocator_t *alloc,
                                                 kith_coord_bus_t **out_bus)
{
    if (!out_bus)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_bus = nullptr;

    kith_coord_bus_params_t resolved;
    if (params)
    {
        kith_error_t err = KITH_OK;
        if (!coord_bus_params_validate(params, &err))
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

    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    kith_coord_bus_t *bus = kith_alloc_zero(allocator, 1, sizeof(*bus));
    if (!bus)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    bus->allocator = allocator;
    bus->instance_id = resolved.instance_id;
    bus->transport = resolved.transport;
    bus->members = nullptr;
    bus->member_count = 0u;
    bus->member_cap = 0u;
    bus->zone_subs = nullptr;
    bus->zone_sub_count = 0u;
    bus->zone_sub_cap = 0u;
    bus->pending = nullptr;
    bus->pending_count = 0u;
    bus->pending_cap = 0u;
    bus->drained = nullptr;
    bus->drained_count = 0u;

    int rc = coord_bus_member_add(bus, resolved.instance_id, 0u);
    if (rc != 0)
    {
        kith_free(allocator, bus);
        return rc;
    }
    *out_bus = bus;
    return 0;
}

KITH_API void kith_coord_bus_destroy(kith_coord_bus_t *bus)
{
    if (!bus)
    {
        return;
    }
    kith_free(bus->allocator, bus->members);
    kith_free(bus->allocator, bus->zone_subs);
    for (size_t i = 0u; i < bus->pending_count; ++i)
    {
        kith_free(bus->allocator, bus->pending[i].payload);
    }
    kith_free(bus->allocator, bus->pending);
    for (size_t i = 0u; i < bus->drained_count; ++i)
    {
        kith_free(bus->allocator, bus->drained[i].payload);
    }
    kith_free(bus->allocator, bus->drained);
    kith_free(bus->allocator, bus);
}

/*---------------------------------------------------------------------------
 * bus membership
 *-------------------------------------------------------------------------*/

KITH_API uint32_t kith_coord_bus_instance_id(const kith_coord_bus_t *bus)
{
    if (!bus)
    {
        return 0u;
    }
    return bus->instance_id;
}

KITH_API uint32_t kith_coord_bus_member_count(const kith_coord_bus_t *bus)
{
    if (!bus)
    {
        return 0u;
    }
    return (uint32_t)bus->member_count;
}

[[nodiscard]] KITH_API int kith_coord_bus_member_status(const kith_coord_bus_t *bus,
                                                        uint32_t index,
                                                        kith_coord_bus_member_status_t *out)
{
    if (!bus || !out)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (index >= bus->member_count)
    {
        return kith_error_return(KITH_ERANGE);
    }
    out->instance_id = bus->members[index].instance_id;
    out->heartbeat_ms = bus->members[index].heartbeat_ms;
    out->owned_cell_count = bus->members[index].owned_cell_count;
    out->active_input_count = bus->members[index].active_input_count;
    return 0;
}

[[nodiscard]] KITH_API int kith_coord_bus_add_member(kith_coord_bus_t *bus, uint32_t instance_id)
{
    if (!bus || instance_id == 0u)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (coord_bus_member_find(bus, instance_id) != nullptr)
    {
        return kith_error_return(KITH_EEXIST);
    }
    return coord_bus_member_add(bus, instance_id, 0u);
}

[[nodiscard]] KITH_API int kith_coord_bus_tick(kith_coord_bus_t *bus, uint64_t now_ms)
{
    if (!bus)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (bus->member_count > 0u)
    {
        bus->members[0].heartbeat_ms = now_ms;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * bus zone subscriptions
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_coord_bus_subscribe(kith_coord_bus_t *bus, uint32_t zone)
{
    if (!bus)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct coord_bus_zone_sub *sub = coord_zone_sub_find(bus, zone);
    if (sub)
    {
        sub->refcount += 1u;
        return 0;
    }
    if (bus->zone_sub_count == bus->zone_sub_cap)
    {
        int rc = coord_zone_sub_grow(bus);
        if (rc != 0)
        {
            return rc;
        }
    }
    bus->zone_subs[bus->zone_sub_count].zone = zone;
    bus->zone_subs[bus->zone_sub_count].refcount = 1u;
    bus->zone_sub_count += 1u;
    return 0;
}

[[nodiscard]] KITH_API int kith_coord_bus_unsubscribe(kith_coord_bus_t *bus, uint32_t zone)
{
    if (!bus)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct coord_bus_zone_sub *sub = coord_zone_sub_find(bus, zone);
    if (!sub || sub->refcount == 0u)
    {
        return 0;
    }
    sub->refcount -= 1u;
    if (sub->refcount == 0u)
    {
        size_t idx = (size_t)(sub - bus->zone_subs);
        bus->zone_subs[idx] = bus->zone_subs[bus->zone_sub_count - 1u];
        bus->zone_sub_count -= 1u;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * bus publish / drain
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_coord_bus_publish(kith_coord_bus_t *bus,
                                                  kith_coord_bus_event_type_t event_type,
                                                  uint32_t zone,
                                                  const void *payload,
                                                  uint32_t payload_len)
{
    if (!bus)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (bus->pending_count == bus->pending_cap)
    {
        size_t cap = bus->pending_cap == 0u ? 64u : bus->pending_cap * 2u;
        struct coord_bus_event_slot *arr =
            kith_realloc(bus->allocator, bus->pending, cap * sizeof(*arr));
        if (!arr)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        bus->pending = arr;
        bus->pending_cap = cap;
    }
    struct coord_bus_event_slot *slot = &bus->pending[bus->pending_count];
    slot->event_type = event_type;
    slot->zone = zone;
    slot->source_instance_id = bus->instance_id;
    slot->timestamp_ms = 0u;
    slot->payload_len = payload_len;
    slot->payload = nullptr;
    if (payload_len > 0u)
    {
        slot->payload = kith_alloc(bus->allocator, payload_len);
        if (!slot->payload)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        memcpy(slot->payload, payload, payload_len);
    }
    bus->pending_count += 1u;
    return 0;
}

[[nodiscard]] KITH_API int kith_coord_bus_drain(kith_coord_bus_t *bus,
                                                kith_coord_bus_event_t *out,
                                                size_t max,
                                                size_t *out_count)
{
    if (!bus)
    {
        return kith_error_return(KITH_EINVAL);
    }
    for (size_t i = 0u; i < bus->drained_count; ++i)
    {
        kith_free(bus->allocator, bus->drained[i].payload);
    }
    kith_free(bus->allocator, bus->drained);
    bus->drained = bus->pending;
    bus->drained_count = bus->pending_count;
    bus->pending = nullptr;
    bus->pending_count = 0u;
    bus->pending_cap = 0u;

    size_t copied = 0u;
    for (size_t i = 0u; i < bus->drained_count; ++i)
    {
        if (out != nullptr && copied < max)
        {
            out[copied].event_type = bus->drained[i].event_type;
            out[copied].zone = bus->drained[i].zone;
            out[copied].source_instance_id = bus->drained[i].source_instance_id;
            out[copied].payload_len = bus->drained[i].payload_len;
            out[copied].timestamp_ms = bus->drained[i].timestamp_ms;
            out[copied].payload = bus->drained[i].payload;
            copied += 1u;
        }
    }
    if (out_count)
    {
        *out_count = out != nullptr ? copied : bus->drained_count;
    }
    return 0;
}

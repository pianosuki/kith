/* Sharded artifact and cell store for the sim module: open-addressed hashes
 * keyed by actor id and (zone, cell, lod), attach and detach with O(1) back-
 * pointer fixup, and publish-product snapshots. Sits below sim.c (the handle);
 * sim_internal.h declares the shared structures. */

#include <stdckdint.h>
#include <string.h>

#include <pthread.h>

#include "kith/types.h"
#include "sim/sim_internal.h"

/*---------------------------------------------------------------------------
 * hash helpers
 *-------------------------------------------------------------------------*/

static size_t sim_store_mask(size_t buckets)
{
    return buckets - 1u;
}

static size_t sim_hash_actor(uint64_t k)
{
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return (size_t)k;
}

static uint64_t sim_cell_mix(uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    uint64_t k = (uint64_t)zone;
    k = k * 131u + (uint64_t)(uint32_t)cx;
    k = k * 131u + (uint64_t)(uint32_t)cy;
    k = k * 131u + (uint64_t)(uint32_t)cz;
    k = k * 131u + (uint64_t)lod;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    return k;
}

static uint32_t sim_next_pow2(uint32_t v)
{
    uint32_t r = 1u;
    while (r < v)
    {
        r <<= 1u;
    }
    return r;
}

static size_t sim_shard_for(uint32_t zone)
{
    return (size_t)(zone % KITH_SIM_ARTIFACT_SHARDS);
}

/*---------------------------------------------------------------------------
 * load factor and growth
 *-------------------------------------------------------------------------*/

/** Grow trigger threshold. A shard grows when adding one more entry would
 *  push the live-plus-tombstone load above 3/4 of capacity, keeping the
 *  open-addressed probe chains short. The denominator is a power of two so
 *  the compiler reduces the comparison to a shift. */
#define SIM_SHARD_LOAD_NUM 3u
#define SIM_SHARD_LOAD_DEN 4u

/*---------------------------------------------------------------------------
 * cell index
 *-------------------------------------------------------------------------*/

static bool sim_cell_match(
    const struct sim_cell_node *c, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    return c->zone == zone && c->cell_x == cx && c->cell_y == cy && c->cell_z == cz &&
           c->lod == lod;
}

static const struct sim_cell_node *sim_cell_lookup(
    const struct sim_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    size_t mask = sim_store_mask(sh->cell_buckets);
    size_t i = (size_t)sim_cell_mix(zone, cx, cy, cz, lod) & mask;
    for (size_t probe = 0u; probe < sh->cell_buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        const struct sim_cell_node *c = &sh->cells[idx];
        if (!c->used)
        {
            return nullptr;
        }
        if (c->deleted)
        {
            continue;
        }
        if (sim_cell_match(c, zone, cx, cy, cz, lod))
        {
            return c;
        }
    }
    return nullptr;
}

static struct sim_cell_node *
sim_cell_find(struct sim_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    return (struct sim_cell_node *)sim_cell_lookup(sh, zone, cx, cy, cz, lod);
}

static struct sim_cell_node *sim_cell_find_or_create(
    struct sim_shard *sh, uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    struct sim_cell_node *existing = sim_cell_find(sh, zone, cx, cy, cz, lod);
    if (existing)
    {
        return existing;
    }
    size_t mask = sim_store_mask(sh->cell_buckets);
    size_t i = (size_t)sim_cell_mix(zone, cx, cy, cz, lod) & mask;
    struct sim_cell_node *first_tombstone = nullptr;
    for (size_t probe = 0u; probe < sh->cell_buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct sim_cell_node *c = &sh->cells[idx];
        if (!c->used)
        {
            // Reclaim the first tombstone seen along the probe chain if one
            // exists; otherwise take the fresh slot. Reclamation shrinks the
            // tombstone count so the grow trigger keeps an accurate load.
            struct sim_cell_node *target = first_tombstone ? first_tombstone : c;
            if (first_tombstone && sh->cell_tombstones > 0u)
            {
                sh->cell_tombstones -= 1u;
            }
            target->used = true;
            target->deleted = false;
            target->zone = zone;
            target->cell_x = cx;
            target->cell_y = cy;
            target->cell_z = cz;
            target->lod = lod;
            target->authority_epoch = 0u;
            target->latest_seq = 0u;
            target->actor_count = 0u;
            // A reclaimed tombstone had its member list freed on drop; a fresh
            // slot is zero-initialized. Reset explicitly so a stale pointer
            // never survives reclamation.
            target->members = nullptr;
            target->member_count = 0u;
            target->member_cap = 0u;
            sh->cell_count += 1u;
            return target;
        }
        if (c->deleted)
        {
            if (!first_tombstone)
            {
                first_tombstone = c;
            }
            continue;
        }
    }
    return first_tombstone;
}

static void sim_cell_members_free(struct sim_shard *sh, struct sim_cell_node *c);

static void sim_cell_drop(struct sim_shard *sh, struct sim_cell_node *c)
{
    if (c->actor_count > 0u)
    {
        c->actor_count -= 1u;
    }
    if (c->actor_count == 0u)
    {
        c->deleted = true;
        if (sh->cell_count > 0u)
        {
            sh->cell_count -= 1u;
        }
        // The slot is now a tombstone: it still occupies a probe slot until
        // sim_cell_find_or_create reclaims it or a grow rehashes it away.
        sh->cell_tombstones += 1u;
        // The cell is dead: release its member index so the heap-backed
        // indices do not leak. A reclaimed tombstone starts fresh (members
        // NULL) the next time a publish repopulates the cell.
        sim_cell_members_free(sh, c);
    }
}

/*---------------------------------------------------------------------------
 * per-cell member index
 *-------------------------------------------------------------------------*/

/** Append @p idx (an @c arts[] position) to @p c's member list, growing the
 *  list as needed, and record the member's slot on the artifact node so a
 *  subsequent unlink is O(1) by index. Returns 0 on success, negative kith_error
 *  on allocation failure; on failure @p c is left untouched. */
static int sim_cell_member_append(struct sim_shard *sh, struct sim_cell_node *c, size_t idx)
{
    if (c->member_count >= c->member_cap)
    {
        size_t new_cap = c->member_cap ? c->member_cap * 2u : 4u;
        size_t *new_members =
            kith_realloc(sh->allocator, c->members, new_cap * sizeof(*new_members));
        if (!new_members)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        c->members = new_members;
        c->member_cap = new_cap;
    }
    c->members[c->member_count] = idx;
    sh->arts[idx].member_slot = c->member_count;
    c->member_count += 1u;
    return 0;
}

/** Unlink @p idx from @p c's member list by swapping the last entry into its
 *  slot and fixing the moved entry's @c member_slot. O(1); does not shrink
 *  the list capacity. The artifact at @p idx must currently be a member of
 *  @p c. */
static void sim_cell_member_unlink(struct sim_shard *sh, struct sim_cell_node *c, size_t idx)
{
    size_t slot = sh->arts[idx].member_slot;
    if (slot >= c->member_count || c->members[slot] != idx)
    {
        return;
    }
    size_t last = c->member_count - 1u;
    if (slot != last)
    {
        c->members[slot] = c->members[last];
        sh->arts[c->members[slot]].member_slot = slot;
    }
    c->member_count = last;
}

/** Release a cell's member list. Called when the cell dies (actor count
 *  reaches zero) and on shard teardown so the heap indices do not leak. */
static void sim_cell_members_free(struct sim_shard *sh, struct sim_cell_node *c)
{
    kith_free(sh->allocator, c->members);
    c->members = nullptr;
    c->member_count = 0u;
    c->member_cap = 0u;
}

/** Rebuild every live cell's member list from the dense @c arts array after a
 *  compaction that reordered @c arts (remove_zone). In-place fixup is unsafe:
 *  a moving artifact's old index is referenced by an arbitrary neighbor in
 *  some cell's list, so the lists are rebuilt wholesale from the new dense
 *  order. O(art_count + cells); remove_zone is a rare zone-teardown path. */
static int sim_shard_rebuild_cell_members(struct sim_shard *sh)
{
    for (size_t i = 0u; i < sh->cell_buckets; ++i)
    {
        struct sim_cell_node *c = &sh->cells[i];
        if (c->used && !c->deleted)
        {
            sim_cell_members_free(sh, c);
        }
    }
    for (size_t i = 0u; i < sh->art_count; ++i)
    {
        const kith_sim_artifact_t *art = &sh->arts[i].art;
        struct sim_cell_node *c = sim_cell_find(
            sh, art->key.zone, art->key.cell_x, art->key.cell_y, art->key.cell_z, art->key.lod);
        if (!c)
        {
            // A published artifact always has a live cell; a missing cell
            // means the index is corrupt upstream. Fail loudly rather than
            // silently dropping the actor from snapshots.
            return kith_error_return(KITH_ESTATE);
        }
        int rc = sim_cell_member_append(sh, c, i);
        if (rc != 0)
        {
            return rc;
        }
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * actor index
 *-------------------------------------------------------------------------*/

static bool sim_actor_slot_live(const struct sim_shard *sh, size_t idx)
{
    return sh->actor_used[idx] && !sh->actor_deleted[idx];
}

/** Probe the open-addressed actor hash for @p actor_id. Returns the slot
 *  index if a live entry matches, SIZE_MAX if the actor is absent (the probe
 *  hit an unused slot before a match). A tombstone slot does not end the
 *  probe — the chain continues past it — matching the cell-hash discipline. */
static size_t sim_actor_probe(const struct sim_shard *sh, uint64_t actor_id)
{
    size_t mask = sim_store_mask(sh->actor_buckets);
    size_t i = sim_hash_actor(actor_id) & mask;
    for (size_t probe = 0u; probe < sh->actor_buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        if (!sh->actor_used[idx])
        {
            return SIZE_MAX;
        }
        if (sim_actor_slot_live(sh, idx) && sh->actor_keys[idx] == actor_id)
        {
            return idx;
        }
    }
    return SIZE_MAX;
}

/** Insert @p actor_id into the shard's actor hash, reclaiming the first
 *  tombstone along the probe chain if one exists (so a remove-then-republish
 *  churn is net-zero on the hash). @p out_idx receives the dense artifact
 *  position the new entry occupies (always @c sh->art_count, the next free
 *  dense slot). The caller must have ensured capacity via
 *  @c sim_shard_ensure_publish_capacity. */
static int sim_actor_insert(struct sim_shard *sh, uint64_t actor_id, size_t *out_idx)
{
    if (sh->art_count >= sh->art_cap)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    size_t mask = sim_store_mask(sh->actor_buckets);
    size_t i = sim_hash_actor(actor_id) & mask;
    size_t first_tombstone = SIZE_MAX;
    for (size_t probe = 0u; probe < sh->actor_buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        if (!sh->actor_used[idx])
        {
            // Reclaim the first tombstone seen along the chain if one exists;
            // otherwise take the fresh slot. Reclamation keeps remove+republish
            // churn from leaking hash slots.
            size_t target = (first_tombstone != SIZE_MAX) ? first_tombstone : idx;
            if (first_tombstone != SIZE_MAX && sh->actor_tombstones > 0u)
            {
                sh->actor_tombstones -= 1u;
            }
            sh->actor_used[target] = true;
            sh->actor_deleted[target] = false;
            sh->actor_keys[target] = actor_id;
            sh->actor_dense[target] = sh->art_count;
            *out_idx = sh->art_count;
            return 0;
        }
        if (sh->actor_deleted[idx])
        {
            if (first_tombstone == SIZE_MAX)
            {
                first_tombstone = idx;
            }
            continue;
        }
        if (sh->actor_keys[idx] == actor_id)
        {
            // A live entry for this actor already exists. The publish path
            // probes first and routes updates to sim_publish_update, so this
            // branch is a defensive no-op for a stray insert.
            *out_idx = sh->actor_dense[idx];
            return 0;
        }
    }
    if (first_tombstone != SIZE_MAX)
    {
        if (sh->actor_tombstones > 0u)
        {
            sh->actor_tombstones -= 1u;
        }
        sh->actor_deleted[first_tombstone] = false;
        sh->actor_keys[first_tombstone] = actor_id;
        sh->actor_dense[first_tombstone] = sh->art_count;
        *out_idx = sh->art_count;
        return 0;
    }
    return kith_error_return(KITH_ENOMEM);
}

/** Return the dense artifact position for @p actor_id in O(1): probe the
 *  hash and read actor_dense[slot]. Returns SIZE_MAX if the actor is absent. */
static size_t sim_actor_dense_index(const struct sim_shard *sh, uint64_t actor_id)
{
    size_t slot = sim_actor_probe(sh, actor_id);
    if (slot == SIZE_MAX)
    {
        return SIZE_MAX;
    }
    return sh->actor_dense[slot];
}

/** Mark the actor's hash slot as a tombstone. The dense array entry is
 *  swap-removed by the caller; this function only updates the hash. The slot
 *  stays @c actor_used but becomes @c actor_deleted, so future probes step
 *  past it (preserving probe chains for other actors) while a subsequent
 *  insert of the same actor_id can reclaim it in place. */
static void sim_actor_mark_tombstone(struct sim_shard *sh, size_t slot)
{
    if (!sh->actor_used[slot] || sh->actor_deleted[slot])
    {
        return;
    }
    sh->actor_deleted[slot] = true;
    sh->actor_dense[slot] = SIZE_MAX;
    sh->actor_tombstones += 1u;
}

/*---------------------------------------------------------------------------
 * shard / store lifecycle
 *-------------------------------------------------------------------------*/

static int sim_shard_init(struct sim_shard *sh, size_t buckets, const kith_allocator_t *alloc)
{
    sh->allocator = alloc;
    sh->arts = kith_alloc_zero(alloc, buckets, sizeof(*sh->arts));
    sh->actor_keys = kith_alloc_zero(alloc, buckets, sizeof(*sh->actor_keys));
    sh->actor_used = kith_alloc_zero(alloc, buckets, sizeof(*sh->actor_used));
    sh->actor_dense = kith_alloc_zero(alloc, buckets, sizeof(*sh->actor_dense));
    sh->actor_deleted = kith_alloc_zero(alloc, buckets, sizeof(*sh->actor_deleted));
    sh->cells = kith_alloc_zero(alloc, buckets, sizeof(*sh->cells));
    if (!sh->arts || !sh->actor_keys || !sh->actor_used || !sh->actor_dense || !sh->actor_deleted ||
        !sh->cells)
    {
        kith_free(alloc, sh->arts);
        kith_free(alloc, sh->actor_keys);
        kith_free(alloc, sh->actor_used);
        kith_free(alloc, sh->actor_dense);
        kith_free(alloc, sh->actor_deleted);
        kith_free(alloc, sh->cells);
        memset(sh, 0, sizeof(*sh));
        return kith_error_return(KITH_ENOMEM);
    }
    sh->art_count = 0u;
    sh->art_cap = buckets;
    sh->actor_buckets = buckets;
    sh->actor_tombstones = 0u;
    sh->cell_buckets = buckets;
    sh->cell_count = 0u;
    sh->cell_tombstones = 0u;
    return 0;
}

static void sim_shard_fini(struct sim_shard *sh)
{
    for (size_t i = 0u; i < sh->cell_buckets; ++i)
    {
        // Each live cell owns a heap-backed member index; free it before the
        // cells array is released so the indices do not leak.
        sim_cell_members_free(sh, &sh->cells[i]);
    }
    kith_free(sh->allocator, sh->arts);
    kith_free(sh->allocator, sh->actor_keys);
    kith_free(sh->allocator, sh->actor_used);
    kith_free(sh->allocator, sh->actor_dense);
    kith_free(sh->allocator, sh->actor_deleted);
    kith_free(sh->allocator, sh->cells);
    memset(sh, 0, sizeof(*sh));
}

/** Rehash the shard's actor and cell indexes into pre-allocated buffers of
 *  @p new_cap slots. Only live actor entries are re-inserted (tombstones are
 *  dropped), so the dense positions stored on the surviving slots stay
 *  accurate — they index into arts[], which is realloc'd separately and
 *  keeps its dense ordering. The caller resets actor_tombstones after this
 *  returns. Cell tombstones are dropped the same way. */
static void sim_shard_rehash_into(struct sim_shard *sh,
                                  uint64_t *new_keys,
                                  bool *new_used,
                                  bool *new_deleted,
                                  size_t *new_dense,
                                  struct sim_cell_node *new_cells,
                                  size_t new_cap)
{
    size_t new_mask = new_cap - 1u;

    for (size_t i = 0u; i < sh->actor_buckets; ++i)
    {
        if (!sh->actor_used[i] || sh->actor_deleted[i])
        {
            continue;
        }
        uint64_t key = sh->actor_keys[i];
        size_t slot = sim_hash_actor(key) & new_mask;
        while (new_used[slot])
        {
            slot = (slot + 1u) & new_mask;
        }
        new_used[slot] = true;
        new_deleted[slot] = false;
        new_keys[slot] = key;
        new_dense[slot] = sh->actor_dense[i];
    }

    // After growth the load is at most half of the trigger threshold, so
    // every live cell finds a slot without exhausting the probe.
    for (size_t i = 0u; i < sh->cell_buckets; ++i)
    {
        const struct sim_cell_node *c = &sh->cells[i];
        if (!c->used || c->deleted)
        {
            continue;
        }
        size_t slot =
            (size_t)sim_cell_mix(c->zone, c->cell_x, c->cell_y, c->cell_z, c->lod) & new_mask;
        while (new_cells[slot].used)
        {
            slot = (slot + 1u) & new_mask;
        }
        new_cells[slot] = *c;
        // Transfer ownership of the member index to the rehashed cell so the
        // about-to-be-freed old cells array does not double-own the pointer.
        // The old array is freed wholesale (not per-cell), so nulling here is
        // the only guard against a subsequent fini walking both copies.
        sh->cells[i].members = nullptr;
        sh->cells[i].member_count = 0u;
        sh->cells[i].member_cap = 0u;
    }
}

/** Double the shard's three tables and rehash the live entries into the new
 *  capacity. The dense artifact array grows by realloc; the actor and cell
 *  hashes are rehashed from scratch. Cell tombstones are dropped on rehash
 *  (their slots become fresh), which is the only path that shrinks the
 *  tombstone count other than per-slot reclamation.
 *
 *  Allocation is staged so a failure leaves the shard fully consistent with
 *  its prior capacity: every allocation that can fail completes before the
 *  rehash, because the rehash transfers each live cell's member list into
 *  the new cell table and that transfer is not undoable. The dense array is
 *  realloc'd first (realloc preserves its contents and order), then the
 *  hash buffers are allocated, and only then does the rehash run; the old
 *  buffers are freed after the new ones are in place. */
static int sim_shard_grow(struct sim_shard *sh)
{
    size_t new_cap;
    if (ckd_mul(&new_cap, sh->cell_buckets, (size_t)2u) || new_cap <= sh->cell_buckets)
    {
        return kith_error_return(KITH_ENOMEM);
    }

    uint64_t *new_keys = kith_alloc_zero(sh->allocator, new_cap, sizeof(*new_keys));
    bool *new_used = kith_alloc_zero(sh->allocator, new_cap, sizeof(*new_used));
    bool *new_deleted = kith_alloc_zero(sh->allocator, new_cap, sizeof(*new_deleted));
    size_t *new_dense = kith_alloc_zero(sh->allocator, new_cap, sizeof(*new_dense));
    struct sim_cell_node *new_cells = kith_alloc_zero(sh->allocator, new_cap, sizeof(*new_cells));
    if (!new_keys || !new_used || !new_deleted || !new_dense || !new_cells)
    {
        kith_free(sh->allocator, new_keys);
        kith_free(sh->allocator, new_used);
        kith_free(sh->allocator, new_deleted);
        kith_free(sh->allocator, new_dense);
        kith_free(sh->allocator, new_cells);
        return kith_error_return(KITH_ENOMEM);
    }

    struct sim_artifact_node *new_arts =
        kith_realloc(sh->allocator, sh->arts, new_cap * sizeof(*new_arts));
    if (!new_arts)
    {
        kith_free(sh->allocator, new_keys);
        kith_free(sh->allocator, new_used);
        kith_free(sh->allocator, new_deleted);
        kith_free(sh->allocator, new_dense);
        kith_free(sh->allocator, new_cells);
        return kith_error_return(KITH_ENOMEM);
    }

    sim_shard_rehash_into(sh, new_keys, new_used, new_deleted, new_dense, new_cells, new_cap);

    kith_free(sh->allocator, sh->actor_keys);
    kith_free(sh->allocator, sh->actor_used);
    kith_free(sh->allocator, sh->actor_deleted);
    kith_free(sh->allocator, sh->actor_dense);
    kith_free(sh->allocator, sh->cells);
    sh->arts = new_arts;
    sh->actor_keys = new_keys;
    sh->actor_used = new_used;
    sh->actor_deleted = new_deleted;
    sh->actor_dense = new_dense;
    sh->cells = new_cells;
    // Zero the freshly allocated tail of the dense array so unused slots are
    // clean for future appends and snapshot reads.
    memset(&sh->arts[sh->art_cap], 0, (new_cap - sh->art_cap) * sizeof(*sh->arts));
    sh->art_cap = new_cap;
    sh->actor_buckets = new_cap;
    sh->actor_tombstones = 0u;
    sh->cell_buckets = new_cap;
    sh->cell_tombstones = 0u;
    return 0;
}

/** Grow the shard before a publish that may add an actor or a cell, so the
 *  downstream publish path never holds a pointer into a table that a
 *  mid-publish grow would realloc away. @p may_add_actor is true on the
 *  insert path (a new dense entry) and false on the update path. */
static int sim_shard_ensure_publish_capacity(struct sim_shard *sh, bool may_add_actor)
{
    while (true)
    {
        if (may_add_actor)
        {
            size_t actor_load = sh->art_count + sh->actor_tombstones + 1u;
            if (actor_load > (sh->actor_buckets * SIM_SHARD_LOAD_NUM) / SIM_SHARD_LOAD_DEN)
            {
                int rc = sim_shard_grow(sh);
                if (rc != 0)
                {
                    return rc;
                }
                continue;
            }
        }
        size_t cell_load = sh->cell_count + sh->cell_tombstones + 1u;
        if (cell_load > (sh->cell_buckets * SIM_SHARD_LOAD_NUM) / SIM_SHARD_LOAD_DEN)
        {
            int rc = sim_shard_grow(sh);
            if (rc != 0)
            {
                return rc;
            }
            continue;
        }
        return 0;
    }
}

int sim_store_create(const kith_allocator_t *alloc,
                     uint32_t bucket_count,
                     struct kith_sim_artifact_store **out)
{
    *out = nullptr;
    uint32_t buckets = sim_next_pow2(bucket_count);
    if (buckets < 16u)
    {
        buckets = 16u;
    }
    struct kith_sim_artifact_store *s = kith_alloc_zero(alloc, 1, sizeof(*s));
    if (!s)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    s->allocator = alloc;
    if (pthread_mutex_init(&s->lock, nullptr) != 0)
    {
        kith_free(alloc, s);
        return kith_error_return(KITH_ENOMEM);
    }
    for (size_t i = 0u; i < KITH_SIM_ARTIFACT_SHARDS; ++i)
    {
        int rc = sim_shard_init(&s->shards[i], buckets, alloc);
        if (rc != 0)
        {
            for (size_t j = 0u; j < i; ++j)
            {
                sim_shard_fini(&s->shards[j]);
            }
            pthread_mutex_destroy(&s->lock);
            kith_free(alloc, s);
            return rc;
        }
    }
    s->total_count = 0u;
    *out = s;
    return 0;
}

void sim_store_destroy(struct kith_sim_artifact_store *s)
{
    if (!s)
    {
        return;
    }
    for (size_t i = 0u; i < KITH_SIM_ARTIFACT_SHARDS; ++i)
    {
        sim_shard_fini(&s->shards[i]);
    }
    pthread_mutex_destroy(&s->lock);
    kith_free(s->allocator, s);
}

/*---------------------------------------------------------------------------
 * artifact fill
 *-------------------------------------------------------------------------*/

static void
sim_fill_key(kith_sim_artifact_key_t *k, const kith_sim_artifact_key_t *src, uint64_t seq)
{
    k->zone = src->zone;
    k->cell_x = src->cell_x;
    k->cell_y = src->cell_y;
    k->cell_z = src->cell_z;
    k->lod = src->lod;
    k->pad[0] = 0u;
    k->pad[1] = 0u;
    k->pad[2] = 0u;
    k->authority_epoch = src->authority_epoch;
    k->publish_seq = seq;
}

static void sim_fill_artifact(kith_sim_artifact_t *art,
                              const kith_sim_artifact_key_t *key,
                              uint64_t seq,
                              const kith_sim_actor_t *actor)
{
    sim_fill_key(&art->key, key, seq);
    art->actor_id = actor->id;
    art->pos_x = actor->pos_x;
    art->pos_y = actor->pos_y;
    art->pos_z = actor->pos_z;
    art->vel_x = actor->vel_x;
    art->vel_y = actor->vel_y;
    art->vel_z = actor->vel_z;
    art->input_tick = actor->input_tick;
    art->update_seq = actor->update_seq;
}

/*---------------------------------------------------------------------------
 * publish
 *-------------------------------------------------------------------------*/

static int sim_publish_update(struct sim_shard *sh,
                              size_t idx,
                              const kith_sim_artifact_key_t *key,
                              const kith_sim_actor_t *actor,
                              uint64_t *out_seq)
{
    kith_sim_artifact_t *old = &sh->arts[idx].art;

    // When the actor stays in the same cell, the cell's actor_count does not
    // change and latest_seq must keep climbing. Dropping and recreating the
    // cell resets latest_seq and breaks the cell hash's probe chain.
    bool same_cell = old->key.zone == key->zone && old->key.cell_x == key->cell_x &&
                     old->key.cell_y == key->cell_y && old->key.cell_z == key->cell_z &&
                     old->key.lod == key->lod;

    struct sim_cell_node *cell;
    if (same_cell)
    {
        cell = sim_cell_find(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
        if (!cell)
        {
            return kith_error_return(KITH_ESTATE);
        }
        // The artifact already holds a member slot in this cell; only its
        // payload and the cell's latest_seq change.
        cell->latest_seq += 1u;
        cell->authority_epoch = key->authority_epoch;
        sim_fill_artifact(old, key, cell->latest_seq, actor);
    }
    else
    {
        // Move: unlink from the old cell's member index, then link into the
        // new cell. Unlinking first lets a link failure re-append to the old
        // cell and restore the prior state, so the artifact is never left
        // unlinked (and absent from snapshots) on a rare allocation failure.
        struct sim_cell_node *old_cell = sim_cell_find(
            sh, old->key.zone, old->key.cell_x, old->key.cell_y, old->key.cell_z, old->key.lod);
        if (old_cell)
        {
            sim_cell_member_unlink(sh, old_cell, idx);
        }
        cell =
            sim_cell_find_or_create(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
        int mrc = sim_cell_member_append(sh, cell, idx);
        if (mrc != 0 || !cell)
        {
            if (old_cell)
            {
                (void)sim_cell_member_append(sh, old_cell, idx);
            }
            return cell ? mrc : kith_error_return(KITH_ENOMEM);
        }
        cell->latest_seq += 1u;
        cell->actor_count += 1u;
        cell->authority_epoch = key->authority_epoch;
        sim_fill_artifact(old, key, cell->latest_seq, actor);
        if (old_cell)
        {
            sim_cell_drop(sh, old_cell);
        }
    }
    if (out_seq)
    {
        *out_seq = cell->latest_seq;
    }
    return 0;
}

static int sim_publish_insert(struct kith_sim_artifact_store *s,
                              struct sim_shard *sh,
                              const kith_sim_artifact_key_t *key,
                              const kith_sim_actor_t *actor,
                              uint64_t *out_seq)
{
    size_t idx = 0u;
    int rc = sim_actor_insert(sh, actor->id, &idx);
    if (rc != 0)
    {
        return rc;
    }
    struct sim_cell_node *new_cell =
        sim_cell_find_or_create(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (!new_cell)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    // Link the artifact into its cell's member index before advancing any
    // count, so an allocation failure leaves the shard as it was: the actor
    // hash entry is rolled back and no snapshot reads an unlinked artifact.
    int mrc = sim_cell_member_append(sh, new_cell, idx);
    if (mrc != 0)
    {
        size_t slot = sim_actor_probe(sh, actor->id);
        if (slot != SIZE_MAX)
        {
            sim_actor_mark_tombstone(sh, slot);
        }
        return mrc;
    }
    new_cell->latest_seq += 1u;
    new_cell->actor_count += 1u;
    new_cell->authority_epoch = key->authority_epoch;
    sim_fill_artifact(&sh->arts[idx].art, key, new_cell->latest_seq, actor);
    sh->art_count += 1u;
    uint64_t total = 0u;
    if (ckd_add(&total, s->total_count, 1u))
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    s->total_count = total;
    if (out_seq)
    {
        *out_seq = new_cell->latest_seq;
    }
    return 0;
}

int sim_store_publish(struct kith_sim_artifact_store *s,
                      const kith_sim_artifact_key_t *key,
                      const kith_sim_actor_t *actor,
                      uint64_t *out_seq)
{
    pthread_mutex_lock(&s->lock);
    struct sim_shard *sh = &s->shards[sim_shard_for(key->zone)];
    size_t existing = sim_actor_dense_index(sh, actor->id);
    // Grow before any shard pointer is taken downstream so a mid-publish
    // realloc cannot dangle the artifact pointer sim_publish_update holds.
    // The dense index stays valid across the grow: realloc preserves the
    // dense array contents and order, and the actor hash rehash does not
    // reorder the dense entries. The lock serializes this against the
    // reactor's snapshot reads (the gateway cache refresh path).
    int rc = sim_shard_ensure_publish_capacity(sh, existing == SIZE_MAX);
    if (rc != 0)
    {
        pthread_mutex_unlock(&s->lock);
        return rc;
    }
    if (existing != SIZE_MAX)
    {
        rc = sim_publish_update(sh, existing, key, actor, out_seq);
    }
    else
    {
        rc = sim_publish_insert(s, sh, key, actor, out_seq);
    }
    pthread_mutex_unlock(&s->lock);
    return rc;
}

/*---------------------------------------------------------------------------
 * remove
 *-------------------------------------------------------------------------*/

int sim_store_remove_actor(struct kith_sim_artifact_store *s, uint64_t actor_id)
{
    pthread_mutex_lock(&s->lock);
    struct sim_shard *sh = nullptr;
    for (size_t si = 0u; si < KITH_SIM_ARTIFACT_SHARDS; ++si)
    {
        if (sim_actor_probe(&s->shards[si], actor_id) != SIZE_MAX)
        {
            sh = &s->shards[si];
            break;
        }
    }
    if (!sh)
    {
        pthread_mutex_unlock(&s->lock);
        return 0;
    }
    size_t slot = sim_actor_probe(sh, actor_id);
    size_t idx = sh->actor_dense[slot];
    kith_sim_artifact_t *art = &sh->arts[idx].art;
    struct sim_cell_node *c = sim_cell_find(
        sh, art->key.zone, art->key.cell_x, art->key.cell_y, art->key.cell_z, art->key.lod);
    if (c)
    {
        // Unlink the artifact from its cell's member index before dropping the
        // cell: the drop may free the member array (when this was the last
        // actor), and the dense swap below fixes the moved artifact's slot.
        sim_cell_member_unlink(sh, c, idx);
        sim_cell_drop(sh, c);
    }
    size_t last = sh->art_count - 1u;
    if (idx != last)
    {
        // Swap the last dense entry into the vacated slot, then fix the moved
        // actor's dense index on its hash slot so a future publish/remove finds
        // it at its new position. The moved actor's hash slot is found by probe
        // (O(1) amortized); without this fixup the swap dangles its dense
        // pointer and corrupts subsequent publishes.
        sh->arts[idx] = sh->arts[last];
        size_t moved_slot = sim_actor_probe(sh, sh->arts[idx].art.actor_id);
        if (moved_slot != SIZE_MAX)
        {
            sh->actor_dense[moved_slot] = idx;
        }
        // The swapped artifact moved from dense index `last` to `idx`; its
        // cell's member entry still references `last`. Fix it in place so the
        // cell snapshot gathers the artifact at its new index.
        struct sim_cell_node *mc = sim_cell_find(sh,
                                                 sh->arts[idx].art.key.zone,
                                                 sh->arts[idx].art.key.cell_x,
                                                 sh->arts[idx].art.key.cell_y,
                                                 sh->arts[idx].art.key.cell_z,
                                                 sh->arts[idx].art.key.lod);
        if (mc && sh->arts[idx].member_slot < mc->member_count &&
            mc->members[sh->arts[idx].member_slot] == last)
        {
            mc->members[sh->arts[idx].member_slot] = idx;
        }
    }
    sh->art_count -= 1u;
    // Tombstone the removed actor's hash slot: keep actor_used true so probes
    // for other actors step past it (preserving their chains), set
    // actor_deleted so a probe for this actor_id does not match, and let a
    // subsequent insert of the same actor_id reclaim the slot in place, so
    // remove+republish churn never grows the table.
    sim_actor_mark_tombstone(sh, slot);
    if (s->total_count > 0u)
    {
        s->total_count -= 1u;
    }
    pthread_mutex_unlock(&s->lock);
    return 0;
}

int sim_store_remove_zone(struct kith_sim_artifact_store *s, uint32_t zone)
{
    pthread_mutex_lock(&s->lock);
    struct sim_shard *sh = &s->shards[sim_shard_for(zone)];
    size_t kept = 0u;
    for (size_t i = 0u; i < sh->art_count; ++i)
    {
        kith_sim_artifact_t *art = &sh->arts[i].art;
        if (art->key.zone == zone)
        {
            struct sim_cell_node *c = sim_cell_find(
                sh, art->key.zone, art->key.cell_x, art->key.cell_y, art->key.cell_z, art->key.lod);
            if (c)
            {
                sim_cell_drop(sh, c);
            }
            // Tombstone the removed actor's hash slot so its slot does not
            // linger as a stale live entry pointing at a dense position that
            // the compaction below reassigns.
            size_t slot = sim_actor_probe(sh, art->actor_id);
            if (slot != SIZE_MAX)
            {
                sim_actor_mark_tombstone(sh, slot);
            }
            if (s->total_count > 0u)
            {
                s->total_count -= 1u;
            }
            continue;
        }
        if (kept != i)
        {
            sh->arts[kept] = sh->arts[i];
            // The moved actor's dense position changed; fix its dense index on
            // its hash slot so a future publish/remove finds it at kept.
            size_t slot = sim_actor_probe(sh, sh->arts[kept].art.actor_id);
            if (slot != SIZE_MAX)
            {
                sh->actor_dense[slot] = kept;
            }
        }
        kept += 1u;
    }
    sh->art_count = kept;
    // The compaction above reordered the dense array, so every surviving
    // cell's member index now points at stale positions. Rebuild the member
    // lists wholesale from the new dense order (in-place fixup is unsafe: a
    // moving artifact's old index is referenced by an arbitrary cell's list).
    int rebuild_rc = sim_shard_rebuild_cell_members(sh);
    if (rebuild_rc != 0)
    {
        pthread_mutex_unlock(&s->lock);
        return rebuild_rc;
    }
    pthread_mutex_unlock(&s->lock);
    return 0;
}

/*---------------------------------------------------------------------------
 * snapshots
 *-------------------------------------------------------------------------*/

int sim_store_snapshot_cell(const struct kith_sim_artifact_store *s,
                            const kith_sim_artifact_key_t *key,
                            kith_sim_artifact_t *out,
                            size_t max,
                            size_t *out_count)
{
    struct kith_sim_artifact_store *store = (struct kith_sim_artifact_store *)s;
    pthread_mutex_lock(&store->lock);
    const struct sim_shard *sh = &s->shards[sim_shard_for(key->zone)];
    // The cell's member index lists exactly the artifacts occupying this
    // cell, so the snapshot gathers them directly instead of scanning the
    // whole shard's dense array. The gather order is unspecified but
    // deterministic for a given sequence of store mutations; callers that
    // need a specific order sort their copy.
    const struct sim_cell_node *c =
        sim_cell_lookup(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    size_t total = c ? c->member_count : 0u;
    size_t copied = 0u;
    int rc = 0;
    if (out != nullptr)
    {
        copied = (total < max) ? total : max;
        for (size_t i = 0u; i < copied; ++i)
        {
            out[i] = sh->arts[c->members[i]].art;
        }
        if (total > max)
        {
            rc = kith_error_return(KITH_ERANGE);
        }
    }
    if (out_count != nullptr)
    {
        // The cell's artifact count at snapshot time: the number of valid
        // entries in out on success (copied equals total whenever the call
        // is not truncated), and the required buffer size on -KITH_ERANGE
        // (a retry sizes its buffer from it).
        *out_count = total;
    }
    pthread_mutex_unlock(&store->lock);
    return rc;
}

static void sim_fill_cell_product(kith_sim_cell_product_t *p, const struct sim_cell_node *c)
{
    p->zone = c->zone;
    p->cell_x = c->cell_x;
    p->cell_y = c->cell_y;
    p->cell_z = c->cell_z;
    p->lod = c->lod;
    p->pad[0] = 0u;
    p->pad[1] = 0u;
    p->pad[2] = 0u;
    p->actor_count = c->actor_count;
    p->latest_publish_seq = c->latest_seq;
    p->authority_epoch = c->authority_epoch;
}

int sim_store_snapshot_zone_cells(const struct kith_sim_artifact_store *s,
                                  uint32_t zone,
                                  uint8_t lod,
                                  kith_sim_cell_product_t *out,
                                  size_t max,
                                  size_t *out_count)
{
    struct kith_sim_artifact_store *store = (struct kith_sim_artifact_store *)s;
    pthread_mutex_lock(&store->lock);
    size_t total = 0u;
    size_t copied = 0u;
    const struct sim_shard *sh = &s->shards[sim_shard_for(zone)];
    for (size_t i = 0u; i < sh->cell_buckets; ++i)
    {
        const struct sim_cell_node *c = &sh->cells[i];
        if (!c->used || c->deleted || c->zone != zone || c->lod != lod)
        {
            continue;
        }
        if (out != nullptr && copied < max)
        {
            sim_fill_cell_product(&out[copied], c);
            copied += 1u;
        }
        total += 1u;
    }
    if (out_count)
    {
        *out_count = out != nullptr ? copied : total;
    }
    pthread_mutex_unlock(&store->lock);
    return 0;
}

int sim_store_cell_product(const struct kith_sim_artifact_store *s,
                           const kith_sim_artifact_key_t *key,
                           kith_sim_cell_product_t *out_product)
{
    struct kith_sim_artifact_store *store = (struct kith_sim_artifact_store *)s;
    pthread_mutex_lock(&store->lock);
    const struct sim_shard *sh = &s->shards[sim_shard_for(key->zone)];
    const struct sim_cell_node *c =
        sim_cell_lookup(sh, key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
    if (!c)
    {
        pthread_mutex_unlock(&store->lock);
        return kith_error_return(KITH_ENOENT);
    }
    sim_fill_cell_product(out_product, c);
    pthread_mutex_unlock(&store->lock);
    return 0;
}

uint64_t sim_store_count(const struct kith_sim_artifact_store *s)
{
    struct kith_sim_artifact_store *store = (struct kith_sim_artifact_store *)s;
    pthread_mutex_lock(&store->lock);
    uint64_t n = s->total_count;
    pthread_mutex_unlock(&store->lock);
    return n;
}

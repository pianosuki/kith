/* Fabric cell cache for the gateway: sharded open-addressed hashes keyed by
 * kith_fabric_cell_key_t, refcounted entries refreshed from a fabric
 * subscription, and a per-stripe mutex. A cell maps to one of
 * GATEWAY_CACHE_STRIPE_COUNT stripes by its hash, so a worker-thread
 * subscribe/unsubscribe on one cell does not block the reactor composing or
 * refreshing a cell homed in a different stripe. Drives view.c (relevance
 * composition) and is owned by gateway.c. */

#include "gateway/cache/cache.h"

#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>

#include "kith/types.h"

/*---------------------------------------------------------------------------
 * helpers
 *-------------------------------------------------------------------------*/

static uint32_t gateway_cache_next_pow2(uint32_t v)
{
    uint32_t r = 1u;
    while (r < v)
    {
        r <<= 1u;
    }
    return r;
}

static uint64_t gateway_key_hash(const kith_fabric_cell_key_t *key)
{
    return gateway_cell_mix(key->zone, key->cell_x, key->cell_y, key->cell_z, key->lod);
}

static bool gateway_key_eq(const kith_fabric_cell_key_t *a, const kith_fabric_cell_key_t *b)
{
    return a->zone == b->zone && a->cell_x == b->cell_x && a->cell_y == b->cell_y &&
           a->cell_z == b->cell_z && a->lod == b->lod;
}

static int gateway_artifact_id_cmp(const void *a, const void *b)
{
    uint64_t ia = ((const kith_fabric_artifact_t *)a)->actor_id;
    uint64_t ib = ((const kith_fabric_artifact_t *)b)->actor_id;
    if (ia < ib)
    {
        return -1;
    }
    if (ia > ib)
    {
        return 1;
    }
    return 0;
}

static size_t gateway_cache_stripe_index(const kith_fabric_cell_key_t *key)
{
    return (size_t)(gateway_key_hash(key) & (GATEWAY_CACHE_STRIPE_COUNT - 1u));
}

bool gateway_cache_stripe_saturated(struct gateway_cache *c, const kith_fabric_cell_key_t *key)
{
    gateway_cache_lock_key(c, key);
    const struct gateway_cache_stripe *s = &c->stripes[gateway_cache_stripe_index(key)];
    // Create's probe fails exactly when every slot is live (no never-used
    // slot, no tombstone), so the live count against the slot count is the
    // O(1) equivalent of "a new cell in this stripe fails its probe".
    // A cell already cached through another subscriber's window re-finds
    // and refcounts on attempt; skipping here defers that landing to a
    // pass after the stripe frees a slot — the retained entry heals either
    // way.
    const bool saturated = (s->count == s->buckets);
    gateway_cache_unlock_key(c, key);
    return saturated;
}

// Mint a fresh content sequence for a node whose observable cached content
// is being created or replaced. The relaxed RMW dispenses values unique
// across node lifetimes: creation runs on worker threads under the new
// node's stripe lock, refresh runs on the reactor thread under the target's,
// and stripes do not order one another, so the shared counter must be
// atomic even though every node's own sequence is lock-protected. All other
// ordering rides the stripe locks both sides already hold.
static uint64_t gateway_cache_mint_content_seq(struct gateway_cache *c)
{
    return atomic_fetch_add_explicit(&c->content_seq_clock, 1u, memory_order_relaxed);
}

/*---------------------------------------------------------------------------
 * cache lifecycle
 *-------------------------------------------------------------------------*/

static void gateway_cache_fini_stripes(struct gateway_cache *c)
{
    for (size_t i = 0u; i < GATEWAY_CACHE_STRIPE_COUNT; ++i)
    {
        struct gateway_cache_stripe *s = &c->stripes[i];
        if (!s->nodes)
        {
            // An uninitialized stripe (init rollback) has no mutex to
            // destroy and no nodes to free.
            continue;
        }
        for (size_t j = 0u; j < s->buckets; ++j)
        {
            kith_free(c->allocator, s->nodes[j].artifacts);
            kith_free(c->allocator, s->nodes[j].staging);
        }
        kith_free(c->allocator, s->nodes);
        s->nodes = nullptr;
        s->buckets = 0u;
        s->count = 0u;
        pthread_mutex_destroy(&s->lock);
    }
}

int gateway_cache_init(struct gateway_cache *c,
                       kith_fabric_t *fabric,
                       size_t buckets,
                       const kith_allocator_t *alloc)
{
    c->allocator = alloc;
    // Per-stripe bucket count: the requested total divided across the
    // stripes, rounded to a power of two with a 16-slot floor.
    size_t per = buckets / GATEWAY_CACHE_STRIPE_COUNT;
    size_t b = (size_t)gateway_cache_next_pow2((uint32_t)per);
    if (b < 16u)
    {
        b = 16u;
    }
    for (size_t i = 0u; i < GATEWAY_CACHE_STRIPE_COUNT; ++i)
    {
        struct gateway_cache_stripe *s = &c->stripes[i];
        s->nodes = kith_alloc_zero(c->allocator, b, sizeof(*s->nodes));
        if (!s->nodes)
        {
            gateway_cache_fini_stripes(c);
            return kith_error_return(KITH_ENOMEM);
        }
        s->buckets = b;
        s->count = 0u;
        if (pthread_mutex_init(&s->lock, nullptr) != 0)
        {
            // A failed mutex_init leaves the mutex uninitialized; drop the
            // nodes so fini_stripes skips it, then roll back prior stripes.
            kith_free(c->allocator, s->nodes);
            s->nodes = nullptr;
            s->buckets = 0u;
            gateway_cache_fini_stripes(c);
            return kith_error_return(KITH_ESTATE);
        }
    }
    c->last_refresh_ms = 0u;
    // Seed above the composer's 0 "node absent" sentinel so the very first
    // minted sequence cannot collide with it.
    atomic_store_explicit(&c->content_seq_clock, 1u, memory_order_relaxed);
    c->sub = nullptr;

    int rc = kith_fabric_create_subscription(fabric, &c->sub);
    if (rc != 0)
    {
        gateway_cache_fini_stripes(c);
        return rc;
    }
    return 0;
}

void gateway_cache_fini(struct gateway_cache *c)
{
    if (!c->stripes[0].nodes)
    {
        return;
    }
    gateway_cache_fini_stripes(c);
    if (c->sub)
    {
        kith_fabric_subscription_destroy(c->sub);
        c->sub = nullptr;
    }
}

/*---------------------------------------------------------------------------
 * node lookup / create / evict (caller holds the relevant stripe lock)
 *-------------------------------------------------------------------------*/

struct gateway_cache_node *gateway_cache_find(const struct gateway_cache *c,
                                              const kith_fabric_cell_key_t *key)
{
    uint64_t h = gateway_key_hash(key);
    const struct gateway_cache_stripe *s =
        &c->stripes[(size_t)(h & (GATEWAY_CACHE_STRIPE_COUNT - 1u))];
    if (!s->nodes)
    {
        return nullptr;
    }
    size_t mask = gateway_mask(s->buckets);
    size_t i = (size_t)(h >> GATEWAY_CACHE_STRIPE_SHIFT) & mask;
    for (size_t probe = 0u; probe < s->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct gateway_cache_node *n = &s->nodes[idx];
        if (!n->used)
        {
            return nullptr;
        }
        if (n->deleted)
        {
            continue;
        }
        if (gateway_key_eq(&n->key, key))
        {
            return n;
        }
    }
    return nullptr;
}

/** Claim @p target (a never-used slot or a reclaimed tombstone) as a live
 *  node for @p key: initialize every field, mint a fresh content sequence,
 *  and count the entry in its stripe. Caller holds @p s's lock. The stripe
 *  count is what `gateway_cache_stripe_saturated` reads, so every claim
 *  counts — a reclaimed slot is as live as a fresh one. */
static void gateway_cache_node_claim(struct gateway_cache *c,
                                     struct gateway_cache_stripe *s,
                                     struct gateway_cache_node *n,
                                     const kith_fabric_cell_key_t *key)
{
    n->used = true;
    n->deleted = false;
    n->key = *key;
    n->refcount = 0u;
    n->authority_epoch = 0u;
    n->latest_publish_seq = 0u;
    n->refreshed_at_ms = 0u;
    n->content_seq = gateway_cache_mint_content_seq(c);
    n->actor_count = 0u;
    n->product_level = KITH_FABRIC_LEVEL_FULL;
    n->artifacts = nullptr;
    n->artifact_count = 0u;
    n->artifact_cap = 0u;
    n->staging = nullptr;
    n->staging_cap = 0u;
    n->sum_x = 0;
    n->sum_y = 0;
    n->sum_z = 0;
    s->count += 1u;
}

static struct gateway_cache_node *gateway_cache_create(struct gateway_cache *c,
                                                       const kith_fabric_cell_key_t *key)
{
    uint64_t h = gateway_key_hash(key);
    struct gateway_cache_stripe *s = &c->stripes[(size_t)(h & (GATEWAY_CACHE_STRIPE_COUNT - 1u))];
    size_t mask = gateway_mask(s->buckets);
    size_t i = (size_t)(h >> GATEWAY_CACHE_STRIPE_SHIFT) & mask;
    struct gateway_cache_node *first_tomb = nullptr;
    for (size_t probe = 0u; probe < s->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct gateway_cache_node *n = &s->nodes[idx];
        if (!n->used)
        {
            // Reclaiming the tombstone (the earliest slot in the probe
            // chain) over taking the fresh slot shortens future probes.
            struct gateway_cache_node *target = first_tomb ? first_tomb : n;
            gateway_cache_node_claim(c, s, target, key);
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
    // The probe exhausted every slot: with tombstones present the earliest
    // one is reclaimed, and with none the stripe is full (count == buckets)
    // and the create fails.
    if (first_tomb)
    {
        gateway_cache_node_claim(c, s, first_tomb, key);
    }
    return first_tomb;
}

static void gateway_cache_evict(struct gateway_cache *c, struct gateway_cache_node *n)
{
    struct gateway_cache_stripe *s = &c->stripes[gateway_cache_stripe_index(&n->key)];
    kith_free(c->allocator, n->artifacts);
    n->artifacts = nullptr;
    n->artifact_count = 0u;
    n->artifact_cap = 0u;
    kith_free(c->allocator, n->staging);
    n->staging = nullptr;
    n->staging_cap = 0u;
    n->sum_x = 0;
    n->sum_y = 0;
    n->sum_z = 0;
    n->deleted = true;
    n->used = true;
    if (s->count > 0u)
    {
        s->count -= 1u;
    }
}

/*---------------------------------------------------------------------------
 * snapshot a cell's artifacts from the fabric into a cache node
 *-------------------------------------------------------------------------*/

// Copy attempts for one cell refresh. The first covers a cold staging
// buffer (count probe, then copy); the remainder absorb publishes landing
// between the fabric's probe and copy. Exhaustion retains the prior
// snapshot and reports -KITH_ERANGE rather than dropping actors.
enum
{
    GATEWAY_CACHE_COPY_ATTEMPTS = 3
};

// Grow the node's staging buffer to hold at least @p rows. Grow-only: a
// buffer never shrinks, trading per-cell memory at the observed peak for a
// steady-state refresh with no allocation. Nodes are stripe-array slots, so
// the cache's allocator is threaded in rather than stored per node. Caller
// holds the node's stripe lock.
static int gateway_cache_ensure_staging(struct gateway_cache_node *n,
                                        size_t rows,
                                        const kith_allocator_t *alloc)
{
    if (rows <= n->staging_cap)
    {
        return 0;
    }
    kith_fabric_artifact_t *grown = kith_realloc(alloc, n->staging, rows * sizeof(*grown));
    if (!grown)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    n->staging = grown;
    n->staging_cap = rows;
    return 0;
}

// Stage one cell's artifacts from the fabric into the node's staging
// buffer, retrying on truncation with the required size reported by the
// fabric. On success *out_copied carries the staged row count (zero when
// the cell is empty). Caller holds the node's stripe lock.
static int gateway_cache_stage_cell(kith_fabric_t *fabric,
                                    struct gateway_cache_node *n,
                                    kith_fabric_product_level_t level,
                                    const kith_allocator_t *alloc,
                                    size_t *out_copied)
{
    size_t copied = 0u;
    int rc = 0;
    for (int attempt = 0; attempt < GATEWAY_CACHE_COPY_ATTEMPTS; ++attempt)
    {
        if (n->staging == nullptr)
        {
            // Cold staging: the out=NULL form is the documented count
            // probe; size the buffer from it and copy on the next attempt.
            size_t count = 0u;
            rc = kith_fabric_snapshot_cell(fabric, &n->key, level, nullptr, 0u, &count);
            if (rc != 0 || count == 0u)
            {
                break;
            }
            rc = gateway_cache_ensure_staging(n, count, alloc);
            if (rc != 0)
            {
                break;
            }
            continue;
        }
        rc = kith_fabric_snapshot_cell(fabric, &n->key, level, n->staging, n->staging_cap, &copied);
        if (rc != kith_error_return(KITH_ERANGE))
        {
            break;
        }
        // Truncated by a publish landing between the fabric's probe and its
        // copy: copied carries the required count; grow and retry.
        rc = gateway_cache_ensure_staging(n, copied, alloc);
        if (rc != 0)
        {
            break;
        }
    }
    *out_copied = copied;
    return rc;
}

int gateway_cache_refresh_artifacts(struct gateway_cache *c,
                                    kith_fabric_t *fabric,
                                    struct gateway_cache_node *n,
                                    kith_fabric_product_level_t level)
{
    size_t copied = 0u;
    int rc = gateway_cache_stage_cell(fabric, n, level, c->allocator, &copied);
    if (rc != 0)
    {
        // Every failure path in the staging helper leaves the prior
        // snapshot in place: stale but non-empty is better than empty for
        // view composition — an actor held at its last known position
        // rather than dropped. The content sequence stays put too, so the
        // composer's skip baseline keeps describing this snapshot.
        return rc;
    }
    if (copied == 0u)
    {
        // The cell is legitimately empty: clear the front snapshot and zero
        // the crowd sums so the composer reports an empty cell, not a stale
        // one. The buffers are retained (grow-only) for the next refresh.
        // This is the only path that clears a populated cell.
        n->artifact_count = 0u;
        n->sum_x = 0;
        n->sum_y = 0;
        n->sum_z = 0;
        n->content_seq = gateway_cache_mint_content_seq(c);
        return 0;
    }
    // Sort by actor_id once per refresh so every session's subscriber
    // lookup is a binary search, then swap the staging buffer into the
    // front slot: the displaced front becomes the next refresh's staging
    // target, so a steady-state refresh performs no allocation.
    qsort(n->staging, copied, sizeof(*n->staging), gateway_artifact_id_cmp);
    kith_fabric_artifact_t *front = n->artifacts;
    size_t front_cap = n->artifact_cap;
    n->artifacts = n->staging;
    n->artifact_cap = n->staging_cap;
    n->staging = front;
    n->staging_cap = front_cap;
    n->artifact_count = copied;
    // Accumulate the position sums so the composer's crowd aggregate reads
    // O(1) per cell; the cost amortizes across all sessions sharing this
    // cached cell.
    n->sum_x = 0;
    n->sum_y = 0;
    n->sum_z = 0;
    for (size_t i = 0u; i < copied; ++i)
    {
        n->sum_x += n->artifacts[i].pos_x;
        n->sum_y += n->artifacts[i].pos_y;
        n->sum_z += n->artifacts[i].pos_z;
    }
    n->content_seq = gateway_cache_mint_content_seq(c);
    return 0;
}

/*---------------------------------------------------------------------------
 * stripe lock helpers
 *-------------------------------------------------------------------------*/

void gateway_cache_lock_key(struct gateway_cache *c, const kith_fabric_cell_key_t *key)
{
    pthread_mutex_lock(&c->stripes[gateway_cache_stripe_index(key)].lock);
}

void gateway_cache_unlock_key(const struct gateway_cache *c, const kith_fabric_cell_key_t *key)
{
    gateway_mutex_unlock(&c->stripes[gateway_cache_stripe_index(key)].lock);
}

size_t gateway_cache_lock_window(struct gateway_cache *c,
                                 const kith_fabric_cell_key_t *keys,
                                 size_t count,
                                 size_t *out_stripes)
{
    size_t n = 0u;
    for (size_t i = 0u; i < count; ++i)
    {
        size_t s = gateway_cache_stripe_index(&keys[i]);
        bool dup = false;
        for (size_t j = 0u; j < n; ++j)
        {
            if (out_stripes[j] == s)
            {
                dup = true;
                break;
            }
        }
        if (!dup)
        {
            out_stripes[n++] = s;
        }
    }
    // Insertion sort ascending so multi-stripe acquisition is deadlock-free:
    // every caller (composer, stats) acquires stripes in the same global
    // index order, and a single-stripe caller (subscribe/unsubscribe) holds
    // at most one lock. n is at most GATEWAY_CACHE_STRIPE_COUNT.
    for (size_t i = 1u; i < n; ++i)
    {
        size_t key = out_stripes[i];
        size_t j = i;
        while (j > 0u && out_stripes[j - 1u] > key)
        {
            out_stripes[j] = out_stripes[j - 1u];
            --j;
        }
        out_stripes[j] = key;
    }
    for (size_t i = 0u; i < n; ++i)
    {
        pthread_mutex_lock(&c->stripes[out_stripes[i]].lock);
    }
    return n;
}

void gateway_cache_unlock_window(const struct gateway_cache *c, const size_t *stripes, size_t n)
{
    // Release in reverse acquisition order (LIFO). A zero-count window
    // acquired nothing and releases nothing.
    for (size_t i = 0u; i < n; ++i)
    {
        size_t idx = stripes[n - 1u - i];
        gateway_mutex_unlock(&c->stripes[idx].lock);
    }
}

/*---------------------------------------------------------------------------
 * public cache API
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_gateway_subscribe(kith_gateway_t *gateway,
                                                  const kith_fabric_cell_key_t *key)
{
    if (!gateway || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct gateway_cache *c = &gateway->cache;
    gateway_cache_lock_key(c, key);
    struct gateway_cache_node *n = gateway_cache_find(c, key);
    if (n)
    {
        n->refcount += 1u;
        // A tracked node holding no artifacts has never been covered by a
        // product header (those arrive through the drain and own every
        // product-metadata field), so without this reconcile interest
        // materializing in an occupied cell stays invisible until the
        // first bump lands and the following drain runs. The reconcile
        // reads current sim truth under the held stripe lock — including
        // rows other publishers wrote moments earlier — and failure keeps
        // the empty node for retry (the cell's next subscribe or
        // drained product refills it).
        if (n->artifact_count == 0u)
        {
            int reconcile_rc =
                gateway_cache_refresh_artifacts(c, gateway->fabric, n, KITH_FABRIC_LEVEL_FULL);
            (void)reconcile_rc;
        }
        gateway_cache_unlock_key(c, key);
        return 0;
    }
    n = gateway_cache_create(c, key);
    if (!n)
    {
        gateway_cache_unlock_key(c, key);
        return kith_error_return(KITH_ENOMEM);
    }
    n->refcount = 1u;
    // Register the cell on the fabric under the stripe lock, mirroring the
    // prior global-lock ordering. The fabric subscription is internally
    // synchronized, so this is about preserving the cache node ↔ fabric
    // membership pairing, not about fabric-side safety.
    int rc = kith_fabric_subscription_add(c->sub, key);
    if (rc != 0)
    {
        gateway_cache_evict(c, n);
        gateway_cache_unlock_key(c, key);
        return rc;
    }
    // Snapshot the cell while still holding the stripe lock — from a worker
    // thread here, with the same lock order (stripe → fabric) the reactor's
    // refresh path uses. Registering membership first means a publish that
    // lands mid-snapshot either predates it (visible to the copy) or marks
    // the now-tracked cell pending for the next refresh. A failed snapshot
    // leaves the entry empty rather than failing the subscription; the
    // cell's next publish makes it pending again and the refresh fills it.
    int populate_rc =
        gateway_cache_refresh_artifacts(c, gateway->fabric, n, KITH_FABRIC_LEVEL_FULL);
    (void)populate_rc;
    gateway_cache_unlock_key(c, key);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_unsubscribe(kith_gateway_t *gateway,
                                                    const kith_fabric_cell_key_t *key)
{
    if (!gateway || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct gateway_cache *c = &gateway->cache;
    gateway_cache_lock_key(c, key);
    struct gateway_cache_node *n = gateway_cache_find(c, key);
    if (!n)
    {
        gateway_cache_unlock_key(c, key);
        return 0;
    }
    if (n->refcount > 0u)
    {
        n->refcount -= 1u;
    }
    if (n->refcount > 0u)
    {
        gateway_cache_unlock_key(c, key);
        return 0;
    }
    int sub_rc = kith_fabric_subscription_remove(c->sub, key);
    (void)sub_rc;
    gateway_cache_evict(c, n);
    gateway_cache_unlock_key(c, key);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_cache_refresh(kith_gateway_t *gateway, uint64_t now_ms)
{
    if (!gateway)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct gateway_cache *c = &gateway->cache;
    // Interval gate on reactor-thread state. The public API is documented
    // @thread_safety unsafe — call from the reactor thread.
    if (c->last_refresh_ms != 0u && now_ms != 0u)
    {
        uint64_t delta = now_ms - c->last_refresh_ms;
        if (delta < gateway->cache_refresh_interval_ms)
        {
            return 0;
        }
    }
    c->last_refresh_ms = now_ms;

    // Drain pending product headers. The fabric subscription is internally
    // synchronized, so no cache-side lock guards the drain. Unlike the
    // snapshot copy below, this probe-then-copy pair needs no
    // truncation-retry contract: products that arrive between the two calls
    // stay pending in the subscription and drain on the next window.
    size_t pending = 0u;
    int rc = kith_fabric_drain(gateway->fabric, c->sub, nullptr, 0u, &pending);
    if (rc != 0)
    {
        return rc;
    }
    if (pending == 0u)
    {
        return 0;
    }
    kith_fabric_cell_product_t *products =
        kith_alloc_zero(c->allocator, pending, sizeof(*products));
    if (!products)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    size_t drained = 0u;
    rc = kith_fabric_drain(gateway->fabric, c->sub, products, pending, &drained);
    if (rc != 0)
    {
        kith_free(c->allocator, products);
        return rc;
    }
    // Refresh each drained cell under its own stripe lock. A cell evicted by
    // a worker between the drain and here is skipped (find returns NULL); a
    // cell freshly subscribed is populated with current artifacts.
    for (size_t i = 0u; i < drained; ++i)
    {
        gateway_cache_lock_key(c, &products[i].key);
        struct gateway_cache_node *n = gateway_cache_find(c, &products[i].key);
        if (!n)
        {
            gateway_cache_unlock_key(c, &products[i].key);
            continue;
        }
        // Re-snapshot the cell's artifacts first; the helper reads only the
        // cell key and the fabric, not the node's product metadata, so it is
        // safe to call before updating the metadata. On success, advance the
        // product metadata to match the fresh snapshot; on failure the prior
        // snapshot is retained (stage-then-swap inside the helper), so
        // leaving the prior metadata untouched keeps the node at a fully
        // consistent prior state for another cycle rather than advertising a
        // refresh that did not take.
        int refresh_rc =
            gateway_cache_refresh_artifacts(c, gateway->fabric, n, KITH_FABRIC_LEVEL_FULL);
        if (refresh_rc == 0)
        {
            n->authority_epoch = products[i].authority_epoch;
            n->latest_publish_seq = products[i].publish_seq;
            n->actor_count = products[i].actor_count;
            n->product_level = products[i].product_level;
            n->refreshed_at_ms = now_ms;
        }
        gateway_cache_unlock_key(c, &products[i].key);
    }
    kith_free(c->allocator, products);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_cache_snapshot_cell(const kith_gateway_t *gateway,
                                                            const kith_fabric_cell_key_t *key,
                                                            kith_gateway_cell_snapshot_t *out)
{
    if (!gateway || !key || !out)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct gateway_cache *c = (struct gateway_cache *)&gateway->cache;
    gateway_cache_lock_key(c, key);
    const struct gateway_cache_node *n = gateway_cache_find(&gateway->cache, key);
    if (!n)
    {
        gateway_cache_unlock_key(c, key);
        return kith_error_return(KITH_ENOENT);
    }
    memset(out, 0, sizeof(*out));
    out->key = n->key;
    out->authority_epoch = n->authority_epoch;
    out->latest_publish_seq = n->latest_publish_seq;
    out->refreshed_at_ms = n->refreshed_at_ms;
    out->refcount = n->refcount;
    out->actor_count = n->actor_count;
    out->product_level = n->product_level;
    out->pad = 0u;
    gateway_cache_unlock_key(c, key);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_cache_stats(const kith_gateway_t *gateway,
                                                    kith_gateway_cache_stats_t *out_stats)
{
    if (!gateway || !out_stats)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct gateway_cache *c = (struct gateway_cache *)&gateway->cache;
    // Lock all stripes ascending for a consistent count snapshot. This also
    // blocks an in-flight subscribe (which takes its stripe lock before
    // calling kith_fabric_subscription_add), so subscribed_cell_count reads
    // coherently with cell_count.
    for (size_t i = 0u; i < GATEWAY_CACHE_STRIPE_COUNT; ++i)
    {
        pthread_mutex_lock(&c->stripes[i].lock);
    }
    size_t total = 0u;
    for (size_t i = 0u; i < GATEWAY_CACHE_STRIPE_COUNT; ++i)
    {
        total += c->stripes[i].count;
    }
    out_stats->cell_count = (uint32_t)total;
    out_stats->subscribed_cell_count = (uint32_t)kith_fabric_subscription_size(c->sub);
    out_stats->last_refresh_ms = c->last_refresh_ms;
    for (size_t i = 0u; i < GATEWAY_CACHE_STRIPE_COUNT; ++i)
    {
        size_t idx = GATEWAY_CACHE_STRIPE_COUNT - 1u - i;
        pthread_mutex_unlock(&c->stripes[idx].lock);
    }
    return 0;
}

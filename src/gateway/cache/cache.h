#pragma once

#include <stddef.h>

#include "gateway/gateway_internal.h"

/**
 * Cache subsystem internals. The shared cell cache is an open-addressed
 * hash of refcounted per-cell entries. A fabric subscription backs the
 * subscription union: subscribe increments a cell's refcount (or creates
 * the entry and adds the cell to the fabric subscription on first
 * interest); unsubscribe decrements and evicts at zero. Refresh drains
 * the fabric subscription's pending products and re-snapshots changed
 * cells' full-fidelity artifacts so the relevance composer can score
 * candidates without re-querying the fabric per subscriber.
 */

/** Initialize a cache with @p buckets total hash buckets, divided evenly
 *  across the GATEWAY_CACHE_STRIPE_COUNT stripes (each rounded to a power
 *  of two, minimum 16), with @p alloc as the instance the stripe tables and
 *  artifact buffers release through. Creates the fabric subscription on
 *  @p fabric. Returns 0 on success, negative kith_error on failure. */
[[nodiscard]] int gateway_cache_init(struct gateway_cache *c,
                                     kith_fabric_t *fabric,
                                     size_t buckets,
                                     const kith_allocator_t *alloc);

/** Free every stripe's node array and artifact buffers, destroy the stripe
 *  mutexes, and release the fabric subscription. */
void gateway_cache_fini(struct gateway_cache *c);

/** Find a cache node by cell key, or NULL when the cell is not cached.
 *  Does not lock: the caller holds the stripe lock for @p key (via
 *  gateway_cache_lock_key for a single-cell operation, or as one of the
 *  stripes acquired by gateway_cache_lock_window for a composition) or is
 *  a cache.c internal. */
struct gateway_cache_node *gateway_cache_find(const struct gateway_cache *c,
                                              const kith_fabric_cell_key_t *key);

/** Whether the stripe that owns @p key has no free slot: a subscribe of a
 *  NEW cell homed there would fail its probe with -KITH_ENOMEM. O(1) —
 *  create's probe fails exactly when every slot is live, so the stripe's
 *  live count against its slot count answers it without a walk. The
 *  tick pass's retained-add retry consults this before attempting so a
 *  saturated stripe costs a hash and a compare per pass instead of a
 *  full locked probe. The trade: a cell already cached through another
 *  subscriber's window would re-find and refcount on attempt; the skip
 *  defers that landing until the stripe frees a slot. */
bool gateway_cache_stripe_saturated(struct gateway_cache *c, const kith_fabric_cell_key_t *key);

/** Acquire the stripe that owns @p key. subscribe/unsubscribe/snapshot/
 *  refresh-per-cell call this; it serializes a single-cell mutation or
 *  read against a composition or refresh touching the same stripe. */
void gateway_cache_lock_key(struct gateway_cache *c, const kith_fabric_cell_key_t *key);

/** Release the stripe that owns @p key. @p c is const — releasing
 *  synchronizes, it does not modify the cache. */
void gateway_cache_unlock_key(const struct gateway_cache *c, const kith_fabric_cell_key_t *key);

/** Acquire every distinct stripe that owns one of @p count @p keys, in
 *  ascending stripe-index order (deadlock-free against a single-stripe
 *  caller, which holds at most one lock). Deduplicates so a stripe shared
 *  by several keys is locked once. Fills @p out_stripes (caller-allocated,
 *  GATEWAY_CACHE_STRIPE_COUNT entries) with the locked stripe indices and
 *  returns the count. A zero-key window locks nothing and returns 0. The
 *  relevance composer holds the result across its whole scan and subject
 *  build — the heap entries borrow pointers into the nodes' artifact
 *  arrays — so a worker-thread window add/remove cannot free a node's
 *  artifacts mid-read. */
size_t gateway_cache_lock_window(struct gateway_cache *c,
                                 const kith_fabric_cell_key_t *keys,
                                 size_t count,
                                 size_t *out_stripes);

/** Release @p n stripes acquired by gateway_cache_lock_window,
 *  in reverse acquisition order. @p c is const — releasing synchronizes,
 *  it does not modify the cache. */
void gateway_cache_unlock_window(const struct gateway_cache *c, const size_t *stripes, size_t n);

/** Re-snapshot one cached cell's artifacts from current sim truth via the
 *  fabric into @p n, replacing whatever artifact set the node holds.
 *  Caller holds the stripe lock owning @p n (a single-cell lock or as part
 *  of an acquired window set). Returns 0 on success (count and crowd sums
 *  refreshed, content sequence minted so composers observe the change);
 *  on failure the prior artifact set is retained and an error returned.
 *  Product metadata (@c latest_publish_seq, @c authority_epoch,
 *  @c actor_count, @c product_level, @c refreshed_at_ms) stays owned by
 *  the drain path and is never touched here. */
[[nodiscard]] int gateway_cache_refresh_artifacts(struct gateway_cache *c,
                                                  kith_fabric_t *fabric,
                                                  struct gateway_cache_node *n,
                                                  kith_fabric_product_level_t level);

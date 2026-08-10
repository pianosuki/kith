#ifndef KITH_FABRIC_FABRIC_H
#define KITH_FABRIC_FABRIC_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

/**
 * Append-only cell stream storage, tiered cell products, and
 * subscription-indexed fanout.
 *
 * A fabric handle owns a cell stream (per-zone append-only store of cell
 * products) and a subscription table (per-gateway interest sets). The
 * composition root creates the handle with a borrowed sim handle, publishes
 * cell products after each sim step, and gateways subscribe to cells and
 * drain pending products each tick. The fabric borrows the sim handle to
 * query artifacts for subscription-indexed fanout: published products
 * reference the sim's artifact store rather than duplicating actor state.
 *
 * @section fabric_stream Cell stream
 *
 * Cell products are keyed by a 5-tuple: (zone, cell_x, cell_y, cell_z, lod).
 * The cell coordinates are 3D-native (2D sim models set cell_z = 0). The
 * authority_epoch guards against stale products after an authority
 * transition: a publish with an epoch older than the stored epoch is
 * rejected. The publish_seq is monotonic per cell; a newer product
 * supersedes an older one at the same key. Products are immutable once
 * published.
 *
 * @section fabric_products Product levels
 *
 * Each cell product carries a product level: full (complete position,
 * velocity, and input tick), reduced (position and velocity with reduced
 * metadata), or crowd (aggregate representation for dense cells). The
 * fabric stores full-fidelity products sourced from the sim; the
 * kith_fabric_snapshot_cell function renders artifacts at a requested level
 * by querying the borrowed sim and applying the representation tier. The
 * relevance composer in the gateway selects the level per subscriber.
 *
 * @section fabric_subscriptions Subscriptions
 *
 * A subscription is a per-gateway interest set of cells. The fabric fans
 * out only to subscribed gateways, never to all gateways. When a cell is
 * published, the fabric marks every subscription containing that cell as
 * having pending products. The gateway drains its subscription each tick to
 * receive the changed cell product headers, then snapshots the individual
 * cells at the appropriate level to compose the delivery set. The unit of
 * publication is the cell stream, never the actor and never the connection.
 */

/**
 * @defgroup kith_fabric Cell stream fabric
 * @{
 */

/**
 * Format invariants. The underlying type is fixed so a constant stored in
 * an ABI surface stays a fixed width.
 */
enum kith_fabric_format : unsigned int
{
    /** Number of cell-stream hash shards (compartment count). */
    KITH_FABRIC_CELL_SHARDS = 16u,
};

/**
 * Default configuration values. A kith_fabric_params_t field set to 0
 * selects the corresponding default at create time. The underlying type is
 * fixed.
 */
enum kith_fabric_default : unsigned int
{
    /** Default cell-stream hash bucket count per shard. */
    KITH_FABRIC_DEFAULT_BUCKET_COUNT = 4096u,
};

/**
 * Representation tiers for cell products. The fabric sources full-fidelity
 * artifacts from the sim and renders lower-fidelity tiers at query time.
 */
enum kith_fabric_product_level : unsigned int
{
    /** Full fidelity: complete position, velocity, and input tick. */
    KITH_FABRIC_LEVEL_FULL = 0u,
    /** Reduced fidelity: position and velocity, reduced metadata. */
    KITH_FABRIC_LEVEL_REDUCED = 1u,
    /** Crowd aggregate: batched representation for dense cells. */
    KITH_FABRIC_LEVEL_CROWD = 2u,
};

/** Alias of enum kith_fabric_product_level. */
typedef enum kith_fabric_product_level kith_fabric_product_level_t;

/**
 * Opaque fabric handle.
 *
 * @ownership callee — created by kith_fabric_create, destroyed by
 *           kith_fabric_destroy. Owns the cell stream and the subscription
 *           table. Borrows the sim handle (not owned; the caller destroys
 *           it separately).
 */
typedef struct kith_fabric kith_fabric_t;

/**
 * Opaque subscription handle. A subscription is a per-gateway interest set
 * of cells. The fabric marks a subscription dirty when a subscribed cell is
 * published; the gateway drains pending products each tick.
 *
 * @ownership callee — created by kith_fabric_create_subscription, destroyed
 *           by kith_fabric_subscription_destroy.
 */
typedef struct kith_fabric_subscription kith_fabric_subscription_t;

/**
 * Fabric handle creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_fabric_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. Additive fields occupy the reserved
 * slots.
 */
struct kith_fabric_params
{
    /** Must be sizeof(kith_fabric_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Cell-stream hash bucket count per shard; 0 selects the default. */
    uint32_t cell_bucket_count;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_fabric_params. */
typedef struct kith_fabric_params kith_fabric_params_t;

/**
 * Cell locator. Identifies one cell in the stream by zone, 3D grid
 * coordinates, and level of detail. Exposed-layout value type. The 5-tuple
 * (zone, cell_x, cell_y, cell_z, lod) identifies a cell subscription target
 * and a cell product key prefix.
 */
struct kith_fabric_cell_key
{
    /** Zone identifier (caller-supplied namespace). */
    uint32_t zone;
    /** Cell grid X coordinate. */
    int32_t cell_x;
    /** Cell grid Y coordinate. */
    int32_t cell_y;
    /** Cell grid Z coordinate (0 for 2D sim models). */
    int32_t cell_z;
    /** Level of detail / granularity tier. */
    uint8_t lod;
    /** Padding for alignment. */
    uint8_t pad[3];
};

/** Alias of struct kith_fabric_cell_key. */
typedef struct kith_fabric_cell_key kith_fabric_cell_key_t;

/**
 * Cell product header. Describes the state of one cell in the stream:
 * its locator, authority epoch, publish sequence, source product level,
 * and actor count. Exposed-layout value type. Returned by
 * kith_fabric_snapshot_zone_cells, kith_fabric_cell_product, and
 * kith_fabric_drain.
 */
struct kith_fabric_cell_product
{
    /** Cell locator. */
    kith_fabric_cell_key_t key;
    /** Authority epoch (stale-product guard; incremented on authority change). */
    uint32_t authority_epoch;
    /** Monotonic publish sequence (newer supersedes older at the same cell). */
    uint64_t publish_seq;
    /** Source product level (full-fidelity from the sim). */
    kith_fabric_product_level_t product_level;
    /** Number of actors in this cell (queried from the borrowed sim). */
    uint32_t actor_count;
};

/** Alias of struct kith_fabric_cell_product. */
typedef struct kith_fabric_cell_product kith_fabric_cell_product_t;

/**
 * One actor's rendered state in a cell at a product level. Exposed-layout
 * value type. Returned by kith_fabric_snapshot_cell as a copy (no shared
 * ownership). Positions and velocities are Q16.16 fixed-point, matching the
 * sim's artifact representation.
 */
struct kith_fabric_artifact
{
    /** Opaque actor identifier (caller-supplied, treated as a key). */
    uint64_t actor_id;
    /** Position X in Q16.16 fixed-point. */
    int64_t pos_x;
    /** Position Y in Q16.16 fixed-point. */
    int64_t pos_y;
    /** Position Z in Q16.16 fixed-point (0 for 2D sim models). */
    int64_t pos_z;
    /** Velocity X in Q16.16 fixed-point. */
    int64_t vel_x;
    /** Velocity Y in Q16.16 fixed-point. */
    int64_t vel_y;
    /** Velocity Z in Q16.16 fixed-point (0 for 2D sim models). */
    int64_t vel_z;
    /** Last input tick applied when this artifact was sourced. */
    uint32_t input_tick;
    /** The actor's publisher-minted movement-application counter at source
     *  time (0 when the publisher never minted it). Zeroed in reduced and
     *  crowd renderings, which do not vouch for per-actor coverage. */
    uint32_t update_seq;
    /** Render level this artifact was produced at. */
    kith_fabric_product_level_t product_level;
};

/** Alias of struct kith_fabric_artifact. */
typedef struct kith_fabric_artifact kith_fabric_artifact_t;

/**
 * Build a fabric handle from @p params and a borrowed sim handle.
 *
 * @param params    Creation parameters; @c size and @c abi_version must
 *                  match the runtime generation. NULL selects all defaults.
 * @param sim       Borrowed sim handle. The fabric queries the sim's
 *                  artifact store during fanout and snapshot. NULL is an
 *                  error. The caller retains ownership; the fabric does
 *                  not destroy it.
 * @param alloc     Allocator for the new handle, its cell-stream shards,
 *                  its subscription table, and every subscription and
 *                  snapshot scratch buffer the handle grows, used again
 *                  when kith_fabric_destroy frees them. NULL selects the
 *                  default allocator; a supplied allocator is validated
 *                  (see kith_allocator_t) and must outlive the handle.
 * @param out_fabric Receives the new handle on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p out_fabric or @p sim is NULL, or
 *                    @p alloc is missing an operation,
 *                  - -KITH_EABIVER if @p params or @p alloc has an
 *                    incompatible abi_version,
 *                  - -KITH_ESIZE if @p params or @p alloc has an
 *                    undersized size,
 *                  - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another kith_fabric_create on
 *               the same @p out_fabric slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_fabric_destroy. @p sim is borrowed, not owned.
 */
[[nodiscard]] KITH_API int kith_fabric_create(const kith_fabric_params_t *params,
                                              kith_sim_t *sim,
                                              const kith_allocator_t *alloc,
                                              kith_fabric_t **out_fabric);

/**
 * Release a fabric handle, its cell stream, and its subscription table.
 * Passing NULL is a no-op. The borrowed sim handle is not freed (the caller
 * owns it and destroys it separately). Subscriptions still registered on
 * the fabric are released here; a subscription destroyed earlier removed
 * itself from the table, and every outstanding subscription handle dangles
 * after this call.
 *
 * @param fabric Fabric handle. NULL is a no-op.
 * @thread_safety unsafe — no publish/subscribe/snapshot/drain may be in
 *                flight on @p fabric when this is called.
 * @ownership callee — @p fabric is consumed and freed by the call.
 */
KITH_API void kith_fabric_destroy(kith_fabric_t *fabric);

/**
 * Publish or refresh a cell product in the stream. The cell is located by
 * @p key; the @p authority_epoch guards against stale publishes (a publish
 * with an epoch older than the stored epoch is rejected). If a product
 * already exists for @p key, it is superseded: the epoch and publish_seq
 * are updated. The fabric queries the borrowed sim for the actor count in
 * this cell.
 *
 * @param fabric           Fabric handle. NULL is an error.
 * @param key              Cell locator. NULL is an error.
 * @param authority_epoch  Current authority epoch for this cell.
 * @param out_seq          Receives the assigned publish_seq. May be NULL.
 * @return                 0 on success, negative kith_error on failure:
 *                         - -KITH_EINVAL if @p fabric or @p key is NULL,
 *                         - -KITH_EPERM if @p authority_epoch is stale,
 *                         - -KITH_ENOMEM on allocation failure.
 * @thread_safety safe — the cell store and subscription fanout are
 *                      internally synchronized against a concurrent
 *                      drain or subscription mutation.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_fabric_publish(kith_fabric_t *fabric,
                                               const kith_fabric_cell_key_t *key,
                                               uint32_t authority_epoch,
                                               uint64_t *out_seq);

/**
 * Remove a cell product from the stream.
 *
 * @param fabric Fabric handle. NULL is an error.
 * @param key    Cell locator. NULL is an error.
 * @return       0 on success (even if no product existed),
 *               - -KITH_EINVAL if @p fabric or @p key is NULL.
 * @thread_safety safe.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_fabric_remove_cell(kith_fabric_t *fabric,
                                                   const kith_fabric_cell_key_t *key);

/**
 * Remove all cell products for one zone.
 *
 * @param fabric Fabric handle. NULL is an error.
 * @param zone   Zone whose products to remove.
 * @return       0 on success (even if no products existed),
 *               - -KITH_EINVAL if @p fabric is NULL.
 * @thread_safety safe.
 * @ownership caller — @p fabric is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_fabric_remove_zone(kith_fabric_t *fabric, uint32_t zone);

/**
 * Snapshot all artifacts in one cell at a product level. The cell is located
 * by @p key. The fabric queries the borrowed sim for the cell's artifacts
 * and renders them at @p level. The artifacts are copied into @p out; the
 * caller owns the copies. Artifact order is unspecified but deterministic
 * for a given sequence of store mutations; callers that require a specific
 * order sort their copy.
 *
 * @param fabric    Fabric handle. NULL is an error.
 * @param key       Cell locator. NULL is an error.
 * @param level     Product level to render at.
 * @param out       Output buffer. May be NULL when @p max is 0 (only
 *                  counts).
 * @param max       Maximum number of artifacts to copy.
 * @param out_count Receives the cell's artifact count at snapshot time: the
 *                  number of valid entries in @p out on success, or the
 *                  required buffer size when truncated. May be NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p fabric or @p key is NULL,
 *                  - -KITH_ERANGE if the cell holds more than @p max
 *                    artifacts at a full or reduced level: the first @p max
 *                    entries of @p out receive valid artifacts and @p
 *                    out_count receives the required count; retry with a
 *                    buffer of at least @p out_count entries.
 * @thread_safety safe — concurrent calls serialize on an internal snapshot
 *                       scratch mutex (all product levels); the borrowed sim
 *                       handle is internally synchronized.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_fabric_snapshot_cell(kith_fabric_t *fabric,
                                                     const kith_fabric_cell_key_t *key,
                                                     kith_fabric_product_level_t level,
                                                     kith_fabric_artifact_t *out,
                                                     size_t max,
                                                     size_t *out_count);

/**
 * Snapshot cell product headers for all cells in one zone at one lod. The
 * headers are copied into @p out; the caller owns the copies.
 *
 * @param fabric    Fabric handle. NULL is an error.
 * @param zone      Zone identifier.
 * @param lod       Level of detail tier.
 * @param out       Output buffer. May be NULL when @p max is 0 (only counts).
 * @param max       Maximum number of headers to copy.
 * @param out_count Receives the number of headers copied into @p out. May
 *                  be NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p fabric is NULL.
 * @thread_safety safe.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_fabric_snapshot_zone_cells(kith_fabric_t *fabric,
                                                           uint32_t zone,
                                                           uint8_t lod,
                                                           kith_fabric_cell_product_t *out,
                                                           size_t max,
                                                           size_t *out_count);

/**
 * Snapshot one cell product header. The cell is located by @p key.
 *
 * @param fabric      Fabric handle. NULL is an error.
 * @param key         Cell locator. NULL is an error.
 * @param out_product Receives the product header on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p fabric, @p key, or @p out_product
 *                      is NULL,
 *                    - -KITH_ENOENT if the cell has no product.
 * @thread_safety safe.
 * @ownership caller — @p key is borrowed for the call only; @p out_product is
 *           the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_fabric_cell_product(kith_fabric_t *fabric,
                                                    const kith_fabric_cell_key_t *key,
                                                    kith_fabric_cell_product_t *out_product);

/**
 * Return the total number of cell products currently retained in the
 * stream.
 *
 * @param fabric Fabric handle.
 * @return       The number of cell products currently retained.
 * @thread_safety safe.
 * @ownership caller — @p fabric is borrowed for the call only.
 */
KITH_API uint64_t kith_fabric_product_count(const kith_fabric_t *fabric);

/**
 * Return the monotonic total number of successful @c kith_fabric_publish
 * calls since the fabric handle was created. The composition root samples
 * this per tick and records the delta, so a @c /metrics scrape yields the
 * publishes/sec the per-tick
 * capacity model reconciles against the publish-cadence budget. The counter
 * is incremented on the publishing thread (a worker under free-threaded
 * Python) as a relaxed atomic.
 *
 * @param fabric Fabric handle.
 * @return       The monotonic successful-publish total.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 * @ownership caller — @p fabric is borrowed for the call only.
 */
KITH_API uint64_t kith_fabric_publish_total(const kith_fabric_t *fabric);

/**
 * Create a subscription (per-gateway interest set). The subscription tracks
 * which cells a gateway wants to receive and accumulates pending product
 * headers for changed cells. The subscription and its interest set allocate
 * through the fabric's stored allocator.
 *
 * @param fabric  Fabric handle. NULL is an error.
 * @param out_sub Receives the new subscription handle on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p fabric or @p out_sub is NULL,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another
 *               kith_fabric_create_subscription writing the same @p out_sub
 *               slot.
 * @ownership callee — the caller destroys the subscription with
 *           kith_fabric_subscription_destroy; kith_fabric_destroy consumes
 *           any subscription still registered on it.
 */
[[nodiscard]] KITH_API int kith_fabric_create_subscription(kith_fabric_t *fabric,
                                                           kith_fabric_subscription_t **out_sub);

/**
 * Destroy a subscription. Passing NULL is a no-op. The subscription is
 * removed from the fabric's subscription table; the fabric stops fanning
 * out to it.
 *
 * @param sub Subscription handle. NULL is a no-op.
 * @thread_safety unsafe — @p sub must not be used by another thread
 *                concurrently with this call (lifecycle).
 * @ownership callee — @p sub is consumed and freed by the call.
 */
KITH_API void kith_fabric_subscription_destroy(kith_fabric_subscription_t *sub);

/**
 * Add a cell to a subscription's interest set. Idempotent: adding a cell
 * that is already subscribed returns 0. The interest set is a fixed-size
 * table of @c KITH_FABRIC_DEFAULT_BUCKET_COUNT cells; it does not grow.
 *
 * @param sub Subscription handle. NULL is an error.
 * @param key Cell locator. NULL is an error.
 * @return    0 on success, negative kith_error on failure:
 *            - -KITH_EINVAL if @p sub or @p key is NULL,
 *            - -KITH_ENOMEM when the interest set is full.
 * @thread_safety safe — the interest set is internally synchronized.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_fabric_subscription_add(kith_fabric_subscription_t *sub,
                                                        const kith_fabric_cell_key_t *key);

/**
 * Remove a cell from a subscription's interest set. Idempotent: removing a
 * cell that is not subscribed returns 0.
 *
 * @param sub Subscription handle. NULL is an error.
 * @param key Cell locator. NULL is an error.
 * @return    0 on success (even if the cell was not subscribed),
 *            - -KITH_EINVAL if @p sub or @p key is NULL.
 * @thread_safety safe.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_fabric_subscription_remove(kith_fabric_subscription_t *sub,
                                                           const kith_fabric_cell_key_t *key);

/**
 * Return the number of cells in a subscription's interest set.
 *
 * @param sub Subscription handle.
 * @return    The number of cells in the interest set.
 * @thread_safety safe.
 * @ownership caller — @p sub is borrowed for the call only.
 */
KITH_API size_t kith_fabric_subscription_size(const kith_fabric_subscription_t *sub);

/**
 * Drain pending cell product headers for a subscription. Returns the
 * products that changed since the last drain, in publish_seq order (newest
 * first). Equal publish_seq orders by ascending cell key (zone, cell_x,
 * cell_y, cell_z, lod). The pending flag is cleared for the returned
 * products. A pending
 * cell whose product does not fit in @p max stays pending for the next
 * drain, and a pending cell that has left the store is dropped without a
 * header (its product cannot be delivered). The gateway uses this
 * to learn which cells changed, then snapshots the individual cells at the
 * appropriate level to compose the delivery set.
 *
 * @param fabric    Fabric handle. NULL is an error.
 * @param sub       Subscription handle. NULL is an error.
 * @param out       Output buffer. May be NULL when @p max is 0 (only counts).
 * @param max       Maximum number of product headers to copy.
 * @param out_count Receives the number of headers copied into @p out. May
 *                  be NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p fabric or @p sub is NULL.
 * @thread_safety safe — the interest set and cell store are internally
 *                       synchronized against a concurrent publish or
 *                       subscription mutation.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_fabric_drain(kith_fabric_t *fabric,
                                             kith_fabric_subscription_t *sub,
                                             kith_fabric_cell_product_t *out,
                                             size_t max,
                                             size_t *out_count);

/** @} */

#endif /* KITH_FABRIC_FABRIC_H */

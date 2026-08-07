#ifndef KITH_AOI_AOI_H
#define KITH_AOI_AOI_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"
#include "kith/version.h"

/**
 * Spatial query library for generic 3D objects.
 *
 * A handle owns a spatial hash index of objects. Each object carries an
 * opaque identifier, a 3D position, a bounding-sphere radius, and a
 * caller-supplied user pointer. The index buckets objects by the grid cell
 * their position falls into, so a spatial predicate scans only the buckets
 * intersecting the query region instead of every object.
 *
 * This is a pure query library. The functions return the identifiers and
 * positions of objects matching a spatial predicate; the caller composes
 * delivery from the results. The library does not push state to any
 * subscriber and does not track connections, sessions, or actors. A
 * relevance composer queries the index for objects near a location, then
 * scores and bounds the result set itself.
 *
 * @section aoi_coords Coordinates
 *
 * Positions and radii are Q16.16 fixed-point (@c int64_t with
 * @c KITH_AOI_FIX_SHIFT fractional bits), matching the sim and fabric
 * artifact representation so the three modules share one coordinate
 * system without conversion. The cell grid is 3D: an object's cell is
 * derived by dividing its position by the configured cell size per axis.
 * Two-dimensional callers set @c pos_z to 0 and a cell size whose Z axis
 * collapses everything onto the @c z = 0 plane.
 *
 * @section aoi_index Index
 *
 * The index holds two structures: an object index keyed by identifier
 * (for lookup, update, and remove by id) and a cell index keyed by cell
 * coordinate (for spatial queries). Moving an object detaches it from its
 * old cell and re-attaches it to the new one, an O(1) incremental update
 * rather than a full re-scan. Queries walk the cells intersecting the
 * query region and visit only the objects in those cells.
 *
 * The handle is owned by the composition root and passed by pointer;
 * there is no global accessor. All state lives on the handle. The index
 * is not synchronized; the caller drives it from one thread or guards
 * access externally.
 */

/**
 * @defgroup kith_aoi Spatial query
 * @{
 */

/**
 * Format invariants. The underlying type is fixed so a constant stored in
 * an ABI surface stays a fixed width.
 */
enum kith_aoi_format : unsigned int
{
    /** Q16.16 fractional-bit count (positions and radii carry 16 fractional bits). */
    KITH_AOI_FIX_SHIFT = 16u,
};

/**
 * Default configuration values. A @c kith_aoi_params_t field set to 0
 * selects the corresponding default at create time. The underlying type is
 * fixed.
 */
enum kith_aoi_default : unsigned int
{
    /** Default cell-index hash bucket count. */
    KITH_AOI_DEFAULT_BUCKET_COUNT = 4096u,
    /**
     * Default cell size: 16 world units in Q16.16. The cell grid derives
     * each cell coordinate by dividing a position by this value per axis.
     */
    KITH_AOI_DEFAULT_CELL_SIZE = (16u << KITH_AOI_FIX_SHIFT),
    /**
     * Bucket-count ceiling (a limit, not a default): creation rejects a
     * requested bucket_count above this value with -KITH_EINVAL. The
     * power-of-two rounding of a larger count leaves the 32-bit bucket
     * domain, so the request is refused before any allocation.
     */
    KITH_AOI_MAX_BUCKET_COUNT = 0x80000000u,
};

/**
 * Opaque spatial index handle.
 *
 * @ownership callee — created by kith_aoi_create, destroyed by
 *           kith_aoi_destroy. Owns the object index, the cell index, and
 *           the stored object records.
 */
typedef struct kith_aoi kith_aoi_t;

/**
 * Spatial index creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_aoi_params_t) and @p abi_version to KITH_ABI_VERSION at their
 * compile time; the runtime rejects structs from an incompatible generation
 * or an undersized size. Additive fields occupy the reserved slots.
 *
 * Sizing contract: both indexes are sized once at creation — the bucket
 * count is rounded up to a power of two (floor 16) and is never rehashed or
 * grown. Object slots recycle through tombstones, so the live object
 * population can churn within a fixed table. Cell slots are never
 * reclaimed: every distinct cell coordinate any object has ever occupied
 * consumes its slot for the handle's lifetime, so a workload's cell demand
 * is its roaming area across the whole run, not its concurrent population.
 */
struct kith_aoi_params
{
    /** Must be sizeof(kith_aoi_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Cell size in Q16.16 world units. Each axis divides a position by this
     * value to derive the cell coordinate. 0 selects
     * KITH_AOI_DEFAULT_CELL_SIZE. Must be positive when non-zero.
     */
    int64_t cell_size;
    /**
     * Cell-index hash bucket count; 0 selects KITH_AOI_DEFAULT_BUCKET_COUNT.
     * Values above KITH_AOI_MAX_BUCKET_COUNT are rejected at creation.
     */
    uint32_t bucket_count;
    /** Padding for alignment. */
    uint32_t pad;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_aoi_params. */
typedef struct kith_aoi_params kith_aoi_params_t;

/**
 * One indexed spatial object. Exposed-layout value type. The caller fills
 * this struct and passes it to kith_aoi_insert and kith_aoi_update; the
 * index copies the fields into its own storage. Query visitors receive a
 * pointer to the stored copy.
 */
struct kith_aoi_object
{
    /** Opaque object identifier (caller-supplied, treated as a key). */
    uint64_t id;
    /** Position X in Q16.16 fixed-point. */
    int64_t pos_x;
    /** Position Y in Q16.16 fixed-point. */
    int64_t pos_y;
    /** Position Z in Q16.16 fixed-point (0 for 2D callers). */
    int64_t pos_z;
    /** Bounding-sphere radius in Q16.16 fixed-point. */
    int64_t radius;
    /** Caller-supplied opaque payload, stored and returned unchanged. */
    void *user_ptr;
};

/** Alias of struct kith_aoi_object. */
typedef struct kith_aoi_object kith_aoi_object_t;

/**
 * Sphere query descriptor. Exposed-layout value type. A sphere query
 * visits every object whose bounding sphere intersects @p sphere. The
 * center and radius are Q16.16 fixed-point.
 */
struct kith_aoi_sphere
{
    /** Center X in Q16.16 fixed-point. */
    int64_t cx;
    /** Center Y in Q16.16 fixed-point. */
    int64_t cy;
    /** Center Z in Q16.16 fixed-point. */
    int64_t cz;
    /** Radius in Q16.16 fixed-point. */
    int64_t radius;
};

/** Alias of struct kith_aoi_sphere. */
typedef struct kith_aoi_sphere kith_aoi_sphere_t;

/**
 * Axis-aligned box query descriptor. Exposed-layout value type. A box
 * query visits every object whose bounding sphere intersects the box. The
 * bounds are Q16.16 fixed-point; @c min components are inclusive lower
 * bounds and @c max components are inclusive upper bounds.
 */
struct kith_aoi_box
{
    /** Inclusive lower X bound in Q16.16 fixed-point. */
    int64_t min_x;
    /** Inclusive lower Y bound in Q16.16 fixed-point. */
    int64_t min_y;
    /** Inclusive lower Z bound in Q16.16 fixed-point. */
    int64_t min_z;
    /** Inclusive upper X bound in Q16.16 fixed-point. */
    int64_t max_x;
    /** Inclusive upper Y bound in Q16.16 fixed-point. */
    int64_t max_y;
    /** Inclusive upper Z bound in Q16.16 fixed-point. */
    int64_t max_z;
};

/** Alias of struct kith_aoi_box. */
typedef struct kith_aoi_box kith_aoi_box_t;

/**
 * Visitor callback for spatial queries.
 *
 * Called once per object matching the query predicate. The @p obj pointer
 * borrows the index's stored copy for the call only; the caller does not
 * free it. Return @c true to continue iteration, @c false to stop early.
 * Stopping early is not an error; the query function returns 0.
 *
 * @param obj   The matching object (borrowed for the call).
 * @param user  Caller-supplied context passed through from the query call.
 * @return      @c true to continue, @c false to stop iteration.
 */
typedef bool (*kith_aoi_visit_fn)(const kith_aoi_object_t *obj, void *user);

/**
 * Build a spatial index from @p params.
 *
 * @param params  Creation parameters; @c size and @c abi_version must match
 *                the runtime generation. NULL selects all defaults.
 * @param alloc   Allocator for the new handle, its object and cell indexes,
 *                and every cell member array the index grows, used again
 *                when kith_aoi_destroy frees them. NULL selects the default
 *                allocator; a supplied allocator is validated (see
 *                kith_allocator_t) and must outlive the handle.
 * @param out_aoi Receives the new handle on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p out_aoi is NULL, @p cell_size
 *                  is negative, @p bucket_count exceeds
 *                  KITH_AOI_MAX_BUCKET_COUNT, or @p alloc is missing
 *                  an operation,
 *                - -KITH_EABIVER if @p params or @p alloc has an
 *                  incompatible abi_version,
 *                - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                  size,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another kith_aoi_create on the
 *                same @p out_aoi slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_aoi_destroy.
 */
[[nodiscard]] KITH_API int kith_aoi_create(const kith_aoi_params_t *params,
                                           const kith_allocator_t *alloc,
                                           kith_aoi_t **out_aoi);

/**
 * Release a spatial index and all stored object records. Passing NULL is a
 * no-op. Visitor callbacks are not invoked during destruction.
 *
 * @param aoi Spatial index handle. NULL is a no-op.
 * @thread_safety unsafe — no query or mutation may be in flight on @p aoi
 *                when this is called.
 * @ownership callee — @p aoi is consumed and freed by the call.
 */
KITH_API void kith_aoi_destroy(kith_aoi_t *aoi);

/**
 * Insert a new object into the index. The object is placed in the cell
 * derived from its position and the configured cell size. The @c id must
 * not already be present.
 *
 * @param aoi Spatial index handle. NULL is an error.
 * @param obj Object to insert. NULL is an error. The fields are copied.
 * @return    0 on success, negative kith_error on failure:
 *            - -KITH_EINVAL if @p aoi or @p obj is NULL,
 *            - -KITH_EEXIST if @p obj->id is already indexed,
 *            - -KITH_EBUSY if the object index or the cell index is at
 *              capacity; retrying without a removal or a recreation with
 *              a larger bucket_count fails identically (object slots free
 *              up through removal, cell slots only through destroy),
 *            - -KITH_ENOMEM if a cell's member array cannot grow.
 * @thread_safety unsafe — the index is not synchronized.
 * @ownership caller — @p obj is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_aoi_insert(kith_aoi_t *aoi, const kith_aoi_object_t *obj);

/**
 * Update an indexed object by identifier. The object is re-bucketed: it is
 * detached from its old cell and attached to the cell derived from the new
 * position. The @c radius and @c user_ptr are replaced. The @c id must
 * already be present.
 *
 * @param aoi Spatial index handle. NULL is an error.
 * @param obj New object state. NULL is an error. The @c id selects the
 *            record to update; the remaining fields are copied.
 * @return    0 on success, negative kith_error on failure:
 *            - -KITH_EINVAL if @p aoi or @p obj is NULL,
 *            - -KITH_ENOENT if @p obj->id is not indexed,
 *            - -KITH_EBUSY if the move targets a new cell and the cell
 *              index is at capacity; the object stays in its old cell,
 *            - -KITH_ENOMEM if a cell's member array cannot grow; the
 *              object stays in its old cell.
 * @thread_safety unsafe — the index is not synchronized.
 * @ownership caller — @p obj is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_aoi_update(kith_aoi_t *aoi, const kith_aoi_object_t *obj);

/**
 * Remove an object by identifier. Idempotent: removing an identifier that
 * is not indexed returns 0.
 *
 * @param aoi Spatial index handle. NULL is an error.
 * @param id  Object identifier to remove.
 * @return    0 on success (even if the identifier was not indexed),
 *            - -KITH_EINVAL if @p aoi is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p aoi is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_aoi_remove(kith_aoi_t *aoi, uint64_t id);

/**
 * Look up one object by identifier and copy its state into @p out_obj.
 *
 * @param aoi     Spatial index handle. NULL is an error.
 * @param id      Object identifier to look up.
 * @param out_obj Receives a copy of the object state on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p aoi or @p out_obj is NULL,
 *                - -KITH_ENOENT if @p id is not indexed.
 * @thread_safety unsafe.
 * @ownership caller — @p out_obj is the caller's output storage.
 */
[[nodiscard]] KITH_API int
kith_aoi_lookup(const kith_aoi_t *aoi, uint64_t id, kith_aoi_object_t *out_obj);

/**
 * Visit every object whose bounding sphere intersects @p sphere. The
 * visitor is called in cell order; the order of objects within a cell is
 * unspecified. If @p visit returns @c false, iteration stops and
 * the function returns 0. The containment decision is exact for every
 * representable position and radius, so no coordinate bound is required
 * from the caller.
 *
 * @param aoi    Spatial index handle. NULL is an error.
 * @param sphere Sphere query descriptor. NULL is an error.
 * @param visit  Visitor callback. NULL is an error.
 * @param user   Caller-supplied context passed verbatim to @p visit.
 * @return       0 on success (including early stop), negative kith_error
 *               on failure:
 *               - -KITH_EINVAL if @p aoi, @p sphere, or @p visit is NULL.
 * @thread_safety unsafe — the index is not synchronized; @p visit must not
 *                mutate the index.
 * @ownership caller — @p user is the caller's context; the object pointer
 *           passed to @p visit is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_aoi_query_sphere(const kith_aoi_t *aoi,
                                                 const kith_aoi_sphere_t *sphere,
                                                 kith_aoi_visit_fn visit,
                                                 void *user);

/**
 * Visit every object whose bounding sphere intersects the axis-aligned box
 * @p box. The visitor is called in cell order; the order of objects within
 * a cell is unspecified. If @p visit returns @c false, iteration stops and
 * the function returns 0. The containment decision is exact for every
 * representable position, box bound, and radius, so no coordinate bound is
 * required from the caller.
 *
 * @param aoi  Spatial index handle. NULL is an error.
 * @param box   Box query descriptor. NULL is an error.
 * @param visit Visitor callback. NULL is an error.
 * @param user  Caller-supplied context passed verbatim to @p visit.
 * @return      0 on success (including early stop), negative kith_error on
 *              failure:
 *              - -KITH_EINVAL if @p aoi, @p box, or @p visit is NULL.
 * @thread_safety unsafe — the index is not synchronized; @p visit must not
 *                mutate the index.
 * @ownership caller — @p user is the caller's context; the object pointer
 *           passed to @p visit is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_aoi_query_box(const kith_aoi_t *aoi,
                                              const kith_aoi_box_t *box,
                                              kith_aoi_visit_fn visit,
                                              void *user);

/**
 * Return the number of objects currently indexed.
 *
 * @param aoi Spatial index handle.
 * @return    The number of objects currently indexed.
 * @thread_safety unsafe.
 * @ownership caller — @p aoi is borrowed for the call only.
 */
KITH_API size_t kith_aoi_size(const kith_aoi_t *aoi);

/** @} */

#endif /* KITH_AOI_AOI_H */

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "kith/aoi/aoi.h"

/**
 * Private AOI internals. The library is a single translation unit plus this
 * shared header; the structures here are not visible outside the library.
 * The index keeps two open-addressed hashes: an object-id index for
 * lookup/update/remove by identifier, and a cell-coordinate index for
 * spatial queries. Moving an object detaches it from its old cell and
 * re-attaches it to the new one in O(1) via a back-pointer to its position
 * in the cell's member array. Query regions expand by the largest stored
 * object radius so a single center-cell bucket never misses an object
 * whose sphere reaches into the region from a neighboring cell.
 */

/*---------------------------------------------------------------------------
 * cell coordinate derivation
 *-------------------------------------------------------------------------*/

/**
 * Floor a Q16.16 position onto its cell coordinate along one axis. The cell
 * index space is int32; positions beyond it share the boundary cells (the
 * distance predicates remain the membership authority), so saturation keeps
 * object and region derivation consistent where a wrapping cast would
 * scatter far positions into arbitrary cells.
 */
static inline int32_t aoi_cell_of(int64_t pos, int64_t cell_size)
{
    int64_t q = pos / cell_size;
    int64_t r = pos % cell_size;
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

/*---------------------------------------------------------------------------
 * params validation
 *-------------------------------------------------------------------------*/

/**
 * Validate caller creation parameters: the size-version gate, the cell
 * size, and the bucket-count ceiling. Exposed for the bucket-boundary
 * unit test, which compiles aoi.c directly.
 */
KITH_LOCAL bool aoi_params_validate(const kith_aoi_params_t *params, kith_error_t *out_err);

/**
 * Round a requested bucket count up to a power of two and clamp to the
 * 16-bucket floor. Total for every request at or below
 * KITH_AOI_MAX_BUCKET_COUNT; creation validates that ceiling before this
 * runs.
 */
KITH_LOCAL uint32_t aoi_resolve_buckets(uint32_t requested);

/*---------------------------------------------------------------------------
 * indexes
 *-------------------------------------------------------------------------*/

/** One stored object plus its current cell attachment. */
struct aoi_obj_node
{
    /** Stored object copy (id, position, radius, user pointer). */
    kith_aoi_object_t obj;
    /** Cell the object currently resides in (for detach). */
    int32_t cell_x;
    int32_t cell_y;
    int32_t cell_z;
    /** Index of this object within its cell's member array (O(1) detach). */
    size_t cell_pos;
    /** Slot occupied (probing stops at an unused slot). */
    bool used;
    /** Slot vacated; probing continues past a tombstone. */
    bool deleted;
};

/** One cell header with its member object-slot indices. */
struct aoi_cell_node
{
    /** Object-slot indices for objects whose center falls in this cell. */
    size_t *members;
    size_t member_count;
    size_t member_cap;
    int32_t cell_x;
    int32_t cell_y;
    int32_t cell_z;
    /** Slot occupied. Cells are created on demand and never reclaimed. */
    bool used;
};

/*---------------------------------------------------------------------------
 * handle
 *-------------------------------------------------------------------------*/

struct kith_aoi
{
    /** Allocator resolved at create; the handle, both indexes, and every
     *  cell member array allocate and free through it. */
    const kith_allocator_t *allocator;
    /** Object-id index (open-addressed, tombstoned). */
    struct aoi_obj_node *objs;
    size_t obj_buckets;
    size_t obj_count;
    /** Cell-coordinate index (open-addressed, append-only). */
    struct aoi_cell_node *cells;
    size_t cell_buckets;
    /** Cell size in Q16.16 fixed-point (per-axis divisor). */
    int64_t cell_size;
    /** Monotonic upper bound on stored object radii (Q16.16). Grows only;
     *  expands query regions so a center-bucketed object whose
     *  sphere reaches into the region is never missed. */
    int64_t max_radius;
};

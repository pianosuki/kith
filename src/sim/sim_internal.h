#pragma once

#include <stddef.h>
#include <stdint.h>

#include <pthread.h>

#include "kith/sim/sim.h"

/**
 * Private simulation internals shared across the sim library translation
 * units (sim.c, model_registry.c, artifact_store.c, models/tile2d.c,
 * models/free2d.c). The built-in models live inside this library and share
 * these definitions so they can allocate the model handle and call the
 * common physics helpers.
 */

/*---------------------------------------------------------------------------
 * mutex helpers
 *-------------------------------------------------------------------------*/

/* Internally-synchronized read APIs take const handles: the mutex is guard
 * state, not the data it protects. The cast lives here so the const surface
 * holds at every call site. */
static inline void sim_mutex_lock(const pthread_mutex_t *m)
{
    pthread_mutex_lock((pthread_mutex_t *)m);
}

static inline void sim_mutex_unlock(const pthread_mutex_t *m)
{
    pthread_mutex_unlock((pthread_mutex_t *)m);
}

/*---------------------------------------------------------------------------
 * fixed-point (Q16.16) helpers
 *-------------------------------------------------------------------------*/

/** Reference tick duration in milliseconds (20 Hz default tick rate). */
#define SIM_REF_TICK_MS 50u

/** Convert an integer model unit to Q16.16 fixed-point. */
static inline int64_t sim_to_fix(uint32_t v)
{
    return (int64_t)v << KITH_SIM_FIX_SHIFT;
}

/** Convert a float model unit to Q16.16 fixed-point. */
static inline int64_t sim_float_to_fix(float v)
{
    return (int64_t)(v * (float)(1u << KITH_SIM_FIX_SHIFT));
}

/** Absolute value of a Q16.16 fixed-point value. */
static inline int64_t sim_abs_fix(int64_t v)
{
    return (v < 0) ? -v : v;
}

/*---------------------------------------------------------------------------
 * model registry
 *-------------------------------------------------------------------------*/

struct sim_registry_entry
{
    char *name;
    kith_sim_model_vtable_t vtable;
};

struct kith_sim_model_registry
{
    struct sim_registry_entry *entries;
    size_t count;
    size_t cap;
};

int sim_registry_init(struct kith_sim_model_registry *r, const kith_allocator_t *alloc);
void sim_registry_fini(struct kith_sim_model_registry *r, const kith_allocator_t *alloc);
int sim_registry_register(struct kith_sim_model_registry *r,
                          const kith_allocator_t *alloc,
                          const char *name,
                          const kith_sim_model_vtable_t *vtable);
const kith_sim_model_vtable_t *sim_registry_find(const struct kith_sim_model_registry *r,
                                                 const char *name);
int sim_registry_register_builtins(struct kith_sim_model_registry *r,
                                   const kith_allocator_t *alloc);

/*---------------------------------------------------------------------------
 * sim handle
 *-------------------------------------------------------------------------*/

struct kith_sim
{
    /** Allocator resolved at create; the handle, its model registry, and its
     *  artifact store allocate and free through it. */
    const kith_allocator_t *allocator;
    struct kith_sim_model_registry registry;
    struct kith_sim_artifact_store *store;
    uint32_t tick_hz;
};

/*---------------------------------------------------------------------------
 * model instance
 *-------------------------------------------------------------------------*/

/**
 * A model instance. The framework fills @c vtable from the registered entry
 * and @c allocator from the create call after @c init returns; the model
 * implementation sets neither and reads @c allocator wherever it releases or
 * grows model-owned memory (destroy included). The implementation fills @c
 * state with its own allocation during @c init, allocated through the @c
 * alloc parameter init receives. The @c destroy vtable callback frees @c
 * state and the model struct itself.
 */
struct kith_sim_model
{
    const kith_allocator_t *allocator;
    kith_sim_model_vtable_t vtable;
    void *state;
};

/*---------------------------------------------------------------------------
 * artifact store
 *-------------------------------------------------------------------------*/

/** One stored artifact plus bookkeeping for the dense array. */
struct sim_artifact_node
{
    kith_sim_artifact_t art;
    /** Index of this artifact's slot within its cell's @c members array, or
     *  SIZE_MAX when the artifact is not yet linked into a cell. Maintained
     *  by the per-cell member-index list so unlink and swap-remove fixup are
     *  O(1) by direct index instead of a linear scan of the cell's members. */
    size_t member_slot;
};

/** One cell header tracked in the per-shard cell index. */
struct sim_cell_node
{
    uint32_t zone;
    int32_t cell_x;
    int32_t cell_y;
    int32_t cell_z;
    uint8_t lod;
    bool used;
    /** Tombstone flag: the slot was occupied and then vacated. Probing
     *  continues past a tombstone; find_or_create may reclaim it. */
    bool deleted;
    uint32_t authority_epoch;
    uint64_t latest_seq;
    uint32_t actor_count;
    /** Dense indices into the shard's @c arts array for every artifact that
     *  currently occupies this cell, contiguous and gather-friendly. The
     *  cell snapshot iterates this array instead of scanning the whole shard,
     *  so a snapshot is O(actors-in-cell) rather than O(actors-in-shard).
     *  Each listed artifact stores its position here in @c member_slot so
     *  unlink and the swap-remove fixup are O(1). Grows only; freed when the
     *  cell's actor count reaches zero (sim_cell_drop). */
    size_t *members;
    size_t member_count;
    size_t member_cap;
};

struct sim_shard
{
    /** Allocator resolved at store create; every table this shard owns
     *  allocates and frees through it. */
    const kith_allocator_t *allocator;
    struct sim_artifact_node *arts;
    size_t art_count;
    size_t art_cap;

    uint64_t *actor_keys;
    bool *actor_used;
    /** Dense artifact positions, parallel to @c actor_keys / @c actor_used.
     *  @c actor_dense[slot] is the index into @c arts for the actor that owns
     *  @c actor_keys[slot], or @c SIZE_MAX when the slot is free or a
     *  tombstone. Keeping the dense index on the hash slot makes publish and
     *  remove O(1) (probe once, read the dense index) instead of the O(n)
     *  linear scan a separate dense array would require. */
    size_t *actor_dense;
    /** Tombstone flags, parallel to @c actor_used. A slot is occupied by a
     *  tombstone when @c actor_used is true and @c actor_deleted is true:
     *  the actor was removed and the slot awaits reclamation by
     *  @c sim_actor_insert. Tracked separately from @c actor_used so a probe
     *  chain keeps stepping past tombstones while @c sim_actor_insert can
     *  reclaim the first one it sees, mirroring the cell hash's tombstone
     *  discipline. */
    bool *actor_deleted;
    size_t actor_buckets;
    /** Vacated actor slots awaiting reclamation by sim_actor_insert. Tracked
     *  so the grow trigger accounts for tombstone load in O(1). */
    size_t actor_tombstones;

    struct sim_cell_node *cells;
    size_t cell_buckets;
    size_t cell_count;
    /** Vacated cell slots awaiting reclamation by sim_cell_find_or_create.
     *  Tracked so the grow trigger can account for tombstone load in O(1)
     *  instead of scanning every cell bucket on each publish. */
    size_t cell_tombstones;
};

struct kith_sim_artifact_store
{
    /** Allocator resolved at create; the store, its shard tables, and every
     *  cell member array allocate and free through it. */
    const kith_allocator_t *allocator;
    struct sim_shard shards[KITH_SIM_ARTIFACT_SHARDS];
    uint64_t total_count;
    /** Guards every shard (tables + counts) and @c total_count. The
     *  reactor thread reads the store (the gateway cache refresh reaches
     *  it via the fabric snapshot path) while worker threads mutate it
     *  (publish / remove from Python handlers); without this lock the
     *  grow/rehash path would free tables the reactor is reading. Matches
     *  the fabric store's single-lock discipline. */
    pthread_mutex_t lock;
};

int sim_store_create(const kith_allocator_t *alloc,
                     uint32_t bucket_count,
                     struct kith_sim_artifact_store **out);
void sim_store_destroy(struct kith_sim_artifact_store *s);
uint64_t sim_store_count(const struct kith_sim_artifact_store *s);
int sim_store_publish(struct kith_sim_artifact_store *s,
                      const kith_sim_artifact_key_t *key,
                      const kith_sim_actor_t *actor,
                      uint64_t *out_seq);
int sim_store_remove_actor(struct kith_sim_artifact_store *s, uint64_t actor_id);
int sim_store_remove_zone(struct kith_sim_artifact_store *s, uint32_t zone);
int sim_store_snapshot_cell(const struct kith_sim_artifact_store *s,
                            const kith_sim_artifact_key_t *key,
                            kith_sim_artifact_t *out,
                            size_t max,
                            size_t *out_count);
int sim_store_snapshot_zone_cells(const struct kith_sim_artifact_store *s,
                                  uint32_t zone,
                                  uint8_t lod,
                                  kith_sim_cell_product_t *out,
                                  size_t max,
                                  size_t *out_count);
int sim_store_cell_product(const struct kith_sim_artifact_store *s,
                           const kith_sim_artifact_key_t *key,
                           kith_sim_cell_product_t *out_product);

/*---------------------------------------------------------------------------
 * pending-input map (shared by built-in models)
 *
 * Concurrency contract: put and get are safe to call concurrently for
 * DIFFERENT actor ids. Same-actor operations carry no mechanical ordering —
 * the caller serializes them (the ordering is semantic: which input a step
 * consumes). get returns a pointer INTO the map; the mutex does not protect
 * that pointer once the actor's next put overwrites the node, and a used
 * node is single-owner (only that actor's own put overwrites it), so under
 * the caller's same-actor serialization the pointer stays valid until the
 * actor's next put. The node array never grows or rehashes, so node
 * addresses are stable for the map's lifetime. A put for an actor with no
 * free node is dropped silently: the bucket count is the map's sizing
 * contract for the distinct-actor population, and no error signals the
 * drop.
 *-------------------------------------------------------------------------*/

struct sim_pending_node
{
    uint64_t actor_id;
    kith_sim_input_t input;
    bool used;
};

struct sim_pending_map
{
    struct sim_pending_node *nodes;
    size_t buckets;
    pthread_mutex_t lock;
};

int sim_pending_init(struct sim_pending_map *m, size_t buckets, const kith_allocator_t *alloc);
void sim_pending_fini(struct sim_pending_map *m, const kith_allocator_t *alloc);
void sim_pending_put(struct sim_pending_map *m, uint64_t actor_id, const kith_sim_input_t *in);
const kith_sim_input_t *sim_pending_get(const struct sim_pending_map *m, uint64_t actor_id);

/*---------------------------------------------------------------------------
 * shared physics (Q16.16 velocity ramp + position integration)
 *-------------------------------------------------------------------------*/

struct sim_physics
{
    int64_t base_speed_fix;
    int64_t run_speed_fix;
    int64_t accel_fix;
    int64_t decel_fix;
    int64_t move_eps_fix;
};

/**
 * Ramp the actor velocity toward the input-derived target and integrate the
 * position by one tick. The @c dt_ms parameter scales the step relative to
 * the 50 ms reference tick (@c SIM_REF_TICK_MS). The Z axis is left
 * untouched (2D models keep pos_z / vel_z at zero).
 */
void sim_physics_step(struct sim_physics *p,
                      kith_sim_actor_t *actor,
                      const kith_sim_input_t *in,
                      uint32_t dt_ms);

/*---------------------------------------------------------------------------
 * built-in model vtables
 *-------------------------------------------------------------------------*/

const kith_sim_model_vtable_t *tile2d_vtable(void);
const kith_sim_model_vtable_t *free2d_vtable(void);

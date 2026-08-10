#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include <pthread.h>

#include "kith/fabric/fabric.h"

/**
 * Private fabric internals. The fabric library is a single translation
 * unit plus this shared header; the structures here are not visible
 * outside the library. The cell store mirrors the sim's sharded
 * open-addressed hash: 16 compartments keyed by zone, with tombstone
 * slots for probe-chain integrity. The subscription interest set uses
 * the same open-addressed shape with a per-entry pending flag so that
 * publish marks changed cells in place and drain reads them back.
 *
 * The fabric is the structural meeting point of a reactor-thread
 * drainer (the gateway cache refresh) and a worker-thread publisher
 * (the publish path and the gateway window add/remove path). A fabric
 * mutex guards the cell store and the subscription table; each
 * subscription carries its own mutex guarding its interest set. Lock
 * ordering is fabric -> subscription: a path that needs both acquires
 * the fabric lock first, then the subscription lock. The snapshot
 * scratch mutex (below) is a leaf: it is acquired alone inside
 * kith_fabric_snapshot_cell, and the only lock taken while it is held
 * is the sim artifact store's own internal lock; no sim or gateway
 * path acquires another kith lock first and then this one, so no cycle
 * exists.
 */

/*---------------------------------------------------------------------------
 * cell key hashing
 *-------------------------------------------------------------------------*/

/** Mix a 5-tuple cell key into a 64-bit hash for bucket selection.
 *  Multiply-shift over the zone, then each coordinate, then a finalizer. */
static inline uint64_t
fabric_cell_mix(uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
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

/*---------------------------------------------------------------------------
 * cell store
 *-------------------------------------------------------------------------*/

/** One retained cell product in the stream. */
struct fabric_cell_node
{
    uint32_t zone;
    int32_t cell_x;
    int32_t cell_y;
    int32_t cell_z;
    uint8_t lod;
    /** Slot occupied (probing stops at an unused slot). */
    bool used;
    /** Slot vacated; probing continues past a tombstone. */
    bool deleted;
    uint32_t authority_epoch;
    uint64_t publish_seq;
    uint32_t actor_count;
};

/** One hash shard of the cell store (compartment keyed by zone). */
struct fabric_shard
{
    /** Allocator resolved at create; shard tables allocate and free
     *  through it. */
    const kith_allocator_t *allocator;
    struct fabric_cell_node *cells;
    size_t buckets;
    /** Live cells: occupied slots that are not tombstones. */
    size_t count;
    /** Vacated slots retained for probe-chain integrity. The insert path
     *  grows the shard when live-plus-tombstone load crosses the growth
     *  threshold, and the rehash drops them. */
    size_t tombstones;
};

/*---------------------------------------------------------------------------
 * subscription interest set
 *-------------------------------------------------------------------------*/

/** One cell in a subscription's interest set, with a pending-changed flag. */
struct fabric_sub_node
{
    kith_fabric_cell_key_t key;
    bool used;
    bool deleted;
    /** Set by publish when this cell changes; cleared by drain. */
    bool pending;
};

/*---------------------------------------------------------------------------
 * handles
 *-------------------------------------------------------------------------*/

struct kith_fabric
{
    /** Allocator resolved at create; the handle, the shards, the
     *  subscription table, the subscriptions, and the snapshot scratch
     *  allocate and free through it. */
    const kith_allocator_t *allocator;
    /** Borrowed sim handle (not owned; queried for actor counts/artifacts). */
    kith_sim_t *sim;
    struct fabric_shard shards[KITH_FABRIC_CELL_SHARDS];
    /** Subscription table (fanout iterates this). */
    struct kith_fabric_subscription **subs;
    size_t sub_count;
    size_t sub_cap;
    uint64_t total_count;
    /** Monotonic count of successful @c kith_fabric_publish calls. Incremented
     *  on the publishing thread as a relaxed atomic (the publish path runs on
     *  a worker under free-threaded Python); the composition root reads the
     *  per-tick delta and records it as @c kith_fabric_publishes_total. */
    _Atomic uint64_t publish_total;
    /** Guards the cell store (shards + total_count) and the subscription
     *  table. Held across publish, remove, drain, and the cell-store
     *  snapshot readers. Acquired before a subscription lock whenever a
     *  path needs both. */
    pthread_mutex_t lock;
    /** Serializes the snapshot scratch below. A leaf lock acquired only
     *  inside kith_fabric_snapshot_cell (see the lock-ordering rules in the
     *  file header). */
    pthread_mutex_t snapshot_lock;
    /** Staging area for one cell's raw sim rows between the sim copy and
     *  the render into the caller's output buffer. Grow-only across calls
     *  so a steady-state refresh performs no allocation; sized to the
     *  largest single cell snapshotted so far. Guarded by snapshot_lock,
     *  and the render runs while it is held so the pointer never escapes. */
    kith_sim_artifact_t *snapshot_scratch;
    /** Capacity (rows) of snapshot_scratch. */
    size_t snapshot_scratch_cap;
};

struct kith_fabric_subscription
{
    /** Allocator copied from the owning fabric at create; the handle and
     *  its interest set allocate and free through it. */
    const kith_allocator_t *allocator;
    /** Back-pointer for removal from the fabric's subscription table. */
    struct kith_fabric *fabric;
    struct fabric_sub_node *nodes;
    size_t buckets;
    size_t count;
    /** Guards the interest set (nodes + count). Acquired by add, remove,
     *  size, drain, and the per-subscription portion of fanout. A path
     *  that also touches the cell store holds the fabric lock first. */
    pthread_mutex_t lock;
};

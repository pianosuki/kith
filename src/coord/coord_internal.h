#pragma once

#include <stddef.h>
#include <stdint.h>

#include "kith/coord/coord.h"

/**
 * Private coord internals. The coord library is a single translation unit
 * plus this shared header; the structures here are not visible outside the
 * library. The coord owns a sharded open-addressed hash of cell ownership
 * overrides (16 compartments keyed by zone, with tombstone slots for
 * probe-chain integrity), a density hash for the split/merge coordinator,
 * and a borrowed bus handle. The bus owns the loopback transport's inbound
 * event queue, a refcounted zone subscription table, and a membership table
 * with one member in the loopback transport (the local instance).
 */

/*---------------------------------------------------------------------------
 * cell key hashing
 *-------------------------------------------------------------------------*/

/** Mix a 5-tuple cell key into a 64-bit hash for bucket selection.
 *  Mirrors the fabric's cell mix so coord and fabric bucketing align. */
static inline uint64_t
coord_cell_mix(uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
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
 * cell ownership table
 *-------------------------------------------------------------------------*/

/** One cell's ownership override entry in the cell table. */
struct coord_cell_node
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
    /** Instance ID that owns this cell (0 = hash-fallback or unowned). */
    uint32_t instance_id;
    /** Authority epoch (stale-product guard; set from the global counter). */
    uint32_t authority_epoch;
};

/** One hash shard of the cell ownership table (compartment keyed by zone). */
struct coord_shard
{
    struct coord_cell_node *cells;
    size_t buckets;
    size_t count;
};

/*---------------------------------------------------------------------------
 * density table (split/merge coordinator)
 *-------------------------------------------------------------------------*/

/** One cell's density entry, tracked for split/merge evaluation. */
struct coord_density_node
{
    uint32_t zone;
    int32_t cell_x;
    int32_t cell_y;
    int32_t cell_z;
    uint8_t lod;
    /** Slot occupied. */
    bool used;
    /** Slot vacated; probing continues past a tombstone. */
    bool deleted;
    /** Latest reported actor count in this cell. */
    uint32_t actor_count;
    /** Monotonic timestamp when the actor count first crossed the split
     *  threshold (0 = not currently above threshold). */
    uint64_t split_since_ms;
    /** Monotonic timestamp when the actor count first dropped below the
     *  merge threshold (0 = not currently below threshold). */
    uint64_t merge_since_ms;
    /** Monotonic timestamp of the last density report. */
    uint64_t last_report_ms;
};

/*---------------------------------------------------------------------------
 * bus
 *-------------------------------------------------------------------------*/

/** One cluster member in the membership table. */
struct coord_bus_member
{
    /** Instance ID. */
    uint32_t instance_id;
    /** Monotonic millisecond timestamp of the last heartbeat. */
    uint64_t heartbeat_ms;
    /** Number of cells this instance owns (overrides). */
    uint32_t owned_cell_count;
    /** Number of active inputs this instance is processing. */
    uint32_t active_input_count;
};

/** One refcounted zone subscription entry. */
struct coord_bus_zone_sub
{
    /** Zone identifier. */
    uint32_t zone;
    /** Subscription refcount (0 = not subscribed). */
    uint32_t refcount;
};

/** One pending inbound event in the loopback ring. */
struct coord_bus_event_slot
{
    /** Event type. */
    kith_coord_bus_event_type_t event_type;
    /** Zone the event pertains to. */
    uint32_t zone;
    /** Source instance ID. */
    uint32_t source_instance_id;
    /** Monotonic millisecond timestamp. */
    uint64_t timestamp_ms;
    /** Payload length in bytes (0 = no payload). */
    uint32_t payload_len;
    /** Payload data (owned, malloc'd; NULL when payload_len is 0). */
    uint8_t *payload;
};

/*---------------------------------------------------------------------------
 * handles
 *-------------------------------------------------------------------------*/

struct kith_coord
{
    /** Allocator resolved at create; the handle, the cell ownership
     *  shards, and the density table allocate and free through it. */
    const kith_allocator_t *allocator;
    /** Borrowed bus handle (not owned; may be NULL in embedded topology). */
    kith_coord_bus_t *bus;
    /** Local instance ID (0 = embedded). */
    uint32_t instance_id;
    /** Global authority epoch counter (bumped on each authority change). */
    uint32_t authority_epoch_counter;
    /** Cell ownership table (override map). */
    struct coord_shard shards[KITH_COORD_CELL_SHARDS];
    /** Density table for split/merge evaluation. */
    struct coord_density_node *density;
    size_t density_buckets;
    size_t density_count;
    /** Resolved split threshold (actor count to trigger a split). */
    uint32_t split_threshold;
    /** Resolved merge threshold (actor count below which to merge). */
    uint32_t merge_threshold;
    /** Resolved split minimum dwell time in milliseconds. */
    uint64_t split_min_dwell_ms;
    /** Resolved merge minimum dwell time in milliseconds. */
    uint64_t merge_min_dwell_ms;
    /** Resolved density-report stride in ticks. */
    uint32_t density_stride;
    /** Tick counter for density-stride gating. */
    uint32_t tick_counter;
};

struct kith_coord_bus
{
    /** Allocator resolved at create; the handle, the membership and
     *  zone-subscription tables, and the pending and drained event rings
     *  with their payloads allocate and free through it. */
    const kith_allocator_t *allocator;
    /** Local instance ID (0 = embedded). */
    uint32_t instance_id;
    /** Transport type. */
    kith_coord_bus_transport_t transport;
    /** Membership table. In the loopback transport, one member: the local
     *  instance. */
    struct coord_bus_member *members;
    size_t member_count;
    size_t member_cap;
    /** Refcounted zone subscription table. */
    struct coord_bus_zone_sub *zone_subs;
    size_t zone_sub_count;
    size_t zone_sub_cap;
    /** Pending inbound events (published, not yet drained). */
    struct coord_bus_event_slot *pending;
    size_t pending_count;
    size_t pending_cap;
    /** Events returned by the previous drain (payloads valid until next
     *  drain). Freed and swapped with pending on each drain call. */
    struct coord_bus_event_slot *drained;
    size_t drained_count;
};

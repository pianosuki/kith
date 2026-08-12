#ifndef KITH_COORD_COORD_H
#define KITH_COORD_COORD_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/fabric/fabric.h"
#include "kith/types.h"
#include "kith/version.h"

/**
 * Cell ownership map, authority epochs, split/merge coordination, and
 * the inter-node coordination bus.
 *
 * A coord handle owns the cell ownership table (which instance has
 * authority for each cell), the authority epoch counter (a stale-product
 * guard bumped on each authority change), and the split/merge coordinator
 * state (density entries and periodic evaluation). The composition root
 * creates the handle with a borrowed bus handle (which may be NULL in the
 * embedded topology, where a single instance owns every cell).
 *
 * @section coord_ownership Cell ownership
 *
 * Each cell is identified by a @c kith_fabric_cell_key_t (zone, 3D grid
 * coordinates, lod). The coord maintains an override map: when a cell is
 * split to a specific instance, an override entry maps the cell key to
 * that instance with a new authority epoch. When no override exists,
 * authority falls back to a hash distribution across the active cluster
 * members. The composition root queries the coord each tick to decide
 * which actors to step (only those in cells where the local instance has
 * authority) and which epoch to pass to the fabric's publish guard.
 *
 * @section coord_split_merge Split/merge
 *
 * The split/merge coordinator monitors cell density. When a cell's actor
 * count exceeds the split threshold for longer than the split dwell time,
 * the coordinator picks a target instance (the least-loaded member other
 * than itself, by owned cell count; ties resolve in membership order),
 * sets the override, bumps the authority epoch, and broadcasts a rebalance
 * contract via the bus. The local member's load is maintained from the
 * coord's own table; remote members report no load until a transport
 * carries load reports. When a cell's actor count drops below the merge
 * threshold for longer than the merge dwell time, the coordinator clears
 * the override and broadcasts a rebalance with target 0 (revert to hash
 * distribution).
 *
 * Split/merge moves simulation authority and fabric publish authority
 * together as one coordinated ownership update. The fabric's epoch-guarded
 * handoff governs the overlap between outgoing and incoming authority on
 * the affected cell streams: the rebalance contract's authority epoch is
 * what the epoch guards compare, and the fabric's supersession
 * deduplicates products once the new authority's sequence overtakes the
 * old one.
 *
 * @section coord_bus Coordination bus
 *
 * The bus handle owns the inter-node transport, the zone subscription
 * refcounts, and the membership table. The composition root creates the
 * bus with a transport type (loopback for the embedded topology and
 * testing; future transports are additive enum values). The coord borrows
 * the bus to broadcast rebalance contracts and to receive inbound events.
 * The composition root drains the bus each tick and dispatches rebalance
 * contracts to the coord via @c kith_coord_on_rebalance.
 *
 * The bus carries cell-stream-oriented events (rebalance contracts,
 * membership changes, snapshot requests), never per-actor state frames.
 * The unit of inter-node traffic is the cell, not the actor.
 */

/**
 * @defgroup kith_coord Coordination
 * @{
 */

/**
 * Format invariants. The underlying type is fixed so a constant stored in
 * an ABI surface stays a fixed width.
 */
enum kith_coord_format : unsigned int
{
    /** Number of cell-table hash shards (compartment count). */
    KITH_COORD_CELL_SHARDS = 16u,
};

/**
 * Default configuration values. A @c kith_coord_params_t field set to 0
 * selects the corresponding default at create time. The underlying type
 * is fixed.
 */
enum kith_coord_default : unsigned int
{
    /** Default cell-table hash bucket count per shard. */
    KITH_COORD_DEFAULT_CELL_BUCKETS = 4096u,
    /** Default density-report stride in ticks (report every Nth tick). */
    KITH_COORD_DEFAULT_DENSITY_STRIDE = 64u,
    /** Default split threshold: actor count to trigger a split. */
    KITH_COORD_DEFAULT_SPLIT_THRESHOLD = 200u,
    /** Default merge threshold: actor count below which to merge. */
    KITH_COORD_DEFAULT_MERGE_THRESHOLD = 50u,
    /** Default split min dwell time in milliseconds. */
    KITH_COORD_DEFAULT_SPLIT_DWELL_MS = 5000u,
    /** Default merge min dwell time in milliseconds. */
    KITH_COORD_DEFAULT_MERGE_DWELL_MS = 10000u,
};

/**
 * Bus transport type. The loopback transport is an in-process ring;
 * future transports (Redis, TCP) are additive enum values. A field set
 * to 0 selects the loopback transport.
 */
enum kith_coord_bus_transport : unsigned int
{
    /** In-process loopback ring (embedded topology and testing). */
    KITH_COORD_BUS_TRANSPORT_LOOPBACK = 0u,
};

/** Alias of enum kith_coord_bus_transport. */
typedef enum kith_coord_bus_transport kith_coord_bus_transport_t;

/**
 * Bus event type. The bus carries cell-stream-oriented events, never
 * per-actor state frames.
 */
enum kith_coord_bus_event_type : unsigned int
{
    /** Cell authority rebalance (split/merge decision; payload is a rebalance contract). */
    KITH_COORD_BUS_EVENT_REBALANCE = 0u,
    /** Cluster membership change (member join or leave). */
    KITH_COORD_BUS_EVENT_MEMBERSHIP = 1u,
    /** Cell snapshot request (initial state seeding during handoff). */
    KITH_COORD_BUS_EVENT_SNAPSHOT_REQUEST = 2u,
};

/** Alias of enum kith_coord_bus_event_type. */
typedef enum kith_coord_bus_event_type kith_coord_bus_event_type_t;

/**
 * Opaque coordination handle.
 *
 * @ownership callee — created by kith_coord_create, destroyed by
 *           kith_coord_destroy. Owns the cell ownership table, the
 *           authority epoch counter, and the split/merge coordinator
 *           state. Borrows the bus handle (not owned; the caller destroys
 *           it separately). The bus may be NULL (embedded topology).
 */
typedef struct kith_coord kith_coord_t;

/**
 * Opaque coordination bus handle.
 *
 * @ownership callee — created by kith_coord_bus_create, destroyed by
 *           kith_coord_bus_destroy. Owns the transport, zone subscription
 *           refcounts, and the membership table.
 */
typedef struct kith_coord_bus kith_coord_bus_t;

/**
 * Coord handle creation parameters. Size-versioned: callers set @p size
 * to sizeof(kith_coord_params_t) and @p abi_version to KITH_ABI_VERSION
 * at their compile time; the runtime rejects structs from an
 * incompatible generation or an undersized size. A field set to 0
 * selects the corresponding @c kith_coord_default value.
 */
struct kith_coord_params
{
    /** Must be sizeof(kith_coord_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Local instance ID (0 = embedded; the coord owns every cell). */
    uint32_t instance_id;
    /** Cell-table hash bucket count per shard; 0 selects the default. */
    uint32_t cell_bucket_count;
    /** Density-report stride in ticks; 0 selects the default. */
    uint32_t density_stride;
    /** Split threshold: actor count to trigger a split; 0 selects the default. */
    uint32_t split_threshold;
    /** Merge threshold: actor count below which to merge; 0 selects the default. */
    uint32_t merge_threshold;
    /** Split min dwell time in ms; 0 selects the default. */
    uint64_t split_min_dwell_ms;
    /** Merge min dwell time in ms; 0 selects the default. */
    uint64_t merge_min_dwell_ms;
    /** Padding for alignment. */
    uint32_t pad;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_coord_params. */
typedef struct kith_coord_params kith_coord_params_t;

/**
 * Bus handle creation parameters. Size-versioned.
 */
struct kith_coord_bus_params
{
    /** Must be sizeof(kith_coord_bus_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Local instance ID (0 = embedded; the bus has one member). */
    uint32_t instance_id;
    /** Transport type; 0 selects the loopback transport. */
    kith_coord_bus_transport_t transport;
    /** Padding for alignment. */
    uint32_t pad;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_coord_bus_params. */
typedef struct kith_coord_bus_params kith_coord_bus_params_t;

/**
 * Cell authority. Describes which instance owns a cell and at which
 * authority epoch. Exposed-layout value type. Returned by
 * @c kith_coord_authority.
 */
struct kith_coord_authority
{
    /** Instance ID that owns this cell (0 = unowned or hash-fallback). */
    uint32_t instance_id;
    /** Authority epoch (stale-product guard; bumped on each authority change). */
    uint32_t authority_epoch;
};

/** Alias of struct kith_coord_authority. */
typedef struct kith_coord_authority kith_coord_authority_t;

/**
 * One cell's ownership entry. Exposed-layout value type. Returned by
 * @c kith_coord_snapshot_zone as a copy array (no shared ownership).
 */
struct kith_coord_cell_entry
{
    /** Cell locator. */
    kith_fabric_cell_key_t key;
    /** Instance ID that owns this cell. */
    uint32_t instance_id;
    /** Authority epoch for this cell. */
    uint32_t authority_epoch;
};

/** Alias of struct kith_coord_cell_entry. */
typedef struct kith_coord_cell_entry kith_coord_cell_entry_t;

/**
 * Rebalance contract. The payload of a @c KITH_COORD_BUS_EVENT_REBALANCE
 * event. Carries the cell being rebalanced, the source and target
 * instance IDs, and the new authority epoch. Exposed-layout value type.
 * Passed to @c kith_coord_on_rebalance to apply an incoming rebalance
 * from the bus.
 */
struct kith_coord_rebalance_contract
{
    /** Cell being rebalanced. */
    kith_fabric_cell_key_t key;
    /** Old authority instance ID (0 = was hash-fallback). */
    uint32_t source_instance_id;
    /** New authority instance ID (0 = clearing to hash-fallback). */
    uint32_t target_instance_id;
    /** New authority epoch (stale-product guard). */
    uint32_t authority_epoch;
    /** Padding for alignment. */
    uint32_t pad;
};

/** Alias of struct kith_coord_rebalance_contract. */
typedef struct kith_coord_rebalance_contract kith_coord_rebalance_contract_t;

/**
 * One bus event. Exposed-layout value type. Returned by
 * @c kith_coord_bus_drain as a copy array. The @c payload field borrows
 * the bus's internal queue storage and is valid until the next
 * @c kith_coord_bus_drain call.
 */
struct kith_coord_bus_event
{
    /** Event type. */
    kith_coord_bus_event_type_t event_type;
    /** Zone the event pertains to. */
    uint32_t zone;
    /** Source instance ID. */
    uint32_t source_instance_id;
    /** Payload length in bytes. */
    uint32_t payload_len;
    /** Timestamp of the event. The loopback publish path stamps no
     *  clock, so the value is always 0. */
    uint64_t timestamp_ms;
    /** Event payload, borrowed from the bus's internal queue. Valid until
     * the next @c kith_coord_bus_drain call. NULL when payload_len is 0. */
    const void *payload;
};

/** Alias of struct kith_coord_bus_event. */
typedef struct kith_coord_bus_event kith_coord_bus_event_t;

/**
 * One cluster member's status. Exposed-layout value type. Returned by
 * @c kith_coord_bus_member_status.
 */
struct kith_coord_bus_member_status
{
    /** Instance ID. */
    uint32_t instance_id;
    /** Monotonic millisecond timestamp of the last heartbeat. */
    uint64_t heartbeat_ms;
    /** Number of cells this instance owns (overrides targeting it). The
     *  local member's count is maintained from its coord's table; remote
     *  members report 0 until a transport carries load reports. */
    uint32_t owned_cell_count;
    /** Number of active inputs this instance is processing. No transport
     *  carries input counts, so the value is always 0. */
    uint32_t active_input_count;
};

/** Alias of struct kith_coord_bus_member_status. */
typedef struct kith_coord_bus_member_status kith_coord_bus_member_status_t;

/**
 * Build a coordination handle from @p params and an optional borrowed bus
 * handle. When @p bus is NULL, the coord operates in embedded mode: every
 * cell's authority is @p params.instance_id (default 0), the split/merge
 * coordinator never fires (no other instances to split to), and
 * @c kith_coord_on_rebalance is a no-op. When @p bus is non-NULL, the
 * coord broadcasts rebalance contracts via the bus and the composition
 * root drains inbound events and dispatches them to
 * @c kith_coord_on_rebalance.
 *
 * @param params       Creation parameters; @c size and @c abi_version must
 *                     match the runtime generation. NULL selects all
 *                     defaults.
 * @param bus          Borrowed bus handle. May be NULL (embedded topology).
 *                     Must outlive the coord handle if non-NULL.
 * @param alloc        Allocator for the new handle, its cell ownership
 *                     shards, and its density table, used again when
 *                     kith_coord_destroy frees them. NULL selects the
 *                     default allocator; a supplied allocator is validated
 *                     (see kith_allocator_t) and must outlive the handle.
 * @param out_coord    Receives the new handle on success.
 * @return            0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p out_coord is NULL, or @p alloc
 *                       is missing an operation,
 *                     - -KITH_EABIVER if @p params or @p alloc has an
 *                       incompatible abi_version,
 *                     - -KITH_ESIZE if @p params or @p alloc has an
 *                       undersized size,
 *                     - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another kith_coord_create on
 *                the same @p out_coord slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_coord_destroy. @p bus is borrowed, not owned.
 */
[[nodiscard]] KITH_API int kith_coord_create(const kith_coord_params_t *params,
                                             kith_coord_bus_t *bus,
                                             const kith_allocator_t *alloc,
                                             kith_coord_t **out_coord);

/**
 * Release a coord handle, its cell table, authority epoch counter, and
 * split/merge coordinator state. Passing NULL is a no-op. The borrowed
 * bus handle is not freed (the caller owns it and destroys it
 * separately).
 *
 * @param coord Coord handle. NULL is a no-op.
 * @thread_safety unsafe — no authority/snapshot/tick may be in flight on
 *                @p coord when this is called.
 * @ownership callee — @p coord is consumed and freed by the call.
 */
KITH_API void kith_coord_destroy(kith_coord_t *coord);

/**
 * Resolve the authority for a cell. When an override exists for @p key,
 * returns the override's instance ID and epoch. When no override exists,
 * returns the hash-fallback instance ID (the local instance ID when the
 * bus is NULL or has one member; otherwise a hash of the cell coordinates
 * across the active membership) with epoch 0.
 *
 * @param coord       Coord handle. NULL is an error.
 * @param key         Cell locator. NULL is an error.
 * @param out_authority Receives the authority on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p coord, @p key, or
 *                      @p out_authority is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p key is borrowed for the call only;
 *           @p out_authority is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_coord_authority(const kith_coord_t *coord,
                                                const kith_fabric_cell_key_t *key,
                                                kith_coord_authority_t *out_authority);

/**
 * Set a cell's authority override. Bumps the authority epoch and applies
 * the override in the cell table. Used by the split/merge coordinator to
 * migrate a cell to a target instance. If an override already exists for
 * @p key, it is replaced.
 *
 * @param coord       Coord handle. NULL is an error.
 * @param key         Cell locator. NULL is an error.
 * @param instance_id Target instance ID.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p coord or @p key is NULL,
 *                    - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_set_authority(kith_coord_t *coord,
                                                    const kith_fabric_cell_key_t *key,
                                                    uint32_t instance_id);

/**
 * Clear a cell's authority override. Bumps the authority epoch and removes
 * the override from the cell table. After this call, @c kith_coord_authority
 * returns the hash-fallback instance for this cell. Idempotent: clearing a
 * cell with no override returns 0.
 *
 * @param coord Coord handle. NULL is an error.
 * @param key   Cell locator. NULL is an error.
 * @return      0 on success (even if no override existed),
 *              - -KITH_EINVAL if @p coord or @p key is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_clear_authority(kith_coord_t *coord,
                                                      const kith_fabric_cell_key_t *key);

/**
 * Return the total number of cells with authority overrides in the cell
 * table.
 *
 * @param coord Coord handle.
 * @return      The number of cells with authority overrides.
 * @thread_safety unsafe.
 * @ownership caller — @p coord is borrowed for the call only.
 */
KITH_API uint32_t kith_coord_cell_count(const kith_coord_t *coord);

/**
 * Return the number of cells owned (overridden) by @p instance_id.
 *
 * @param coord       Coord handle.
 * @param instance_id Instance ID to count.
 * @return            The number of cells owned by @p instance_id.
 * @thread_safety unsafe.
 * @ownership caller — @p coord is borrowed for the call only.
 */
[[nodiscard]] KITH_API uint32_t kith_coord_owned_cell_count(const kith_coord_t *coord,
                                                            uint32_t instance_id);

/**
 * Snapshot all overridden cells in one zone at one lod. The entries are
 * copied into @p out; the caller owns the copies. Entries are returned in
 * no particular order.
 *
 * @param coord     Coord handle. NULL is an error.
 * @param zone      Zone identifier.
 * @param lod       Level of detail tier.
 * @param out       Output buffer. May be NULL when @p max is 0 (only counts).
 * @param max       Maximum number of entries to copy.
 * @param out_count Receives the number of entries copied into @p out. May
 *                  be NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p coord is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_coord_snapshot_zone(const kith_coord_t *coord,
                                                    uint32_t zone,
                                                    uint8_t lod,
                                                    kith_coord_cell_entry_t *out,
                                                    size_t max,
                                                    size_t *out_count);

/**
 * Report cell density for the split/merge coordinator. The composition root
 * calls this each tick (or every @c density_stride ticks) with the actor
 * count for each cell. The coordinator accumulates density entries and
 * evaluates split/merge decisions in @c kith_coord_tick.
 *
 * @param coord       Coord handle. NULL is an error.
 * @param key         Cell locator. NULL is an error.
 * @param actor_count Current actor count in this cell.
 * @param now_ms      Current monotonic time in milliseconds.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p coord or @p key is NULL.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_report_density(kith_coord_t *coord,
                                                     const kith_fabric_cell_key_t *key,
                                                     uint32_t actor_count,
                                                     uint64_t now_ms);

/**
 * Run the split/merge coordinator's periodic evaluation. Iterates density
 * entries, checks split/merge thresholds against dwell times, and for each
 * decision: picks a target instance (the least-loaded member other than
 * this coord; ties resolve in membership order), sets the override, bumps
 * the authority epoch, and broadcasts a rebalance contract via the bus.
 * The local member's owned cell count is refreshed from the coord's own
 * table before the evaluation; remote members report no load until a
 * transport carries load reports. The call is gated by the configured
 * cadence and may be a no-op.
 *
 * @param coord  Coord handle. NULL is an error.
 * @param now_ms Current monotonic time in milliseconds.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p coord is NULL.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p coord is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_tick(kith_coord_t *coord, uint64_t now_ms);

/**
 * Apply an incoming rebalance contract received from the bus. The
 * composition root drains the bus, extracts rebalance events, and calls
 * this function to apply each contract. When @p contract.target_instance_id
 * is 0, the override is cleared (merge); otherwise the override is set to
 * the target instance with the contract's authority epoch.
 *
 * @param coord    Coord handle. NULL is an error.
 * @param contract Rebalance contract. NULL is an error.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p coord or @p contract is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p contract is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_on_rebalance(kith_coord_t *coord,
                                                   const kith_coord_rebalance_contract_t *contract);

/**
 * Build a coordination bus handle from @p params.
 *
 * @param params   Creation parameters; @c size and @c abi_version must
 *                 match the runtime generation. NULL selects all defaults.
 * @param alloc    Allocator for the new handle, its membership and
 *                 zone-subscription tables, and its pending and drained
 *                 event rings with their payloads, used again when
 *                 kith_coord_bus_destroy frees them. NULL selects the
 *                 default allocator; a supplied allocator is validated
 *                 (see kith_allocator_t) and must outlive the handle.
 * @param out_bus  Receives the new handle on success.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p out_bus is NULL, or @p alloc is
 *                   missing an operation,
 *                 - -KITH_EABIVER if @p params or @p alloc has an
 *                   incompatible abi_version,
 *                 - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                   size,
 *                 - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another
 *                kith_coord_bus_create on the same @p out_bus slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_coord_bus_destroy.
 */
[[nodiscard]] KITH_API int kith_coord_bus_create(const kith_coord_bus_params_t *params,
                                                 const kith_allocator_t *alloc,
                                                 kith_coord_bus_t **out_bus);

/**
 * Release a bus handle, its transport, zone subscriptions, and membership
 * table. Passing NULL is a no-op.
 *
 * @param bus Bus handle. NULL is a no-op.
 * @thread_safety unsafe — no publish/drain/subscribe/tick may be in
 *                flight on @p bus when this is called.
 * @ownership callee — @p bus is consumed and freed by the call.
 */
KITH_API void kith_coord_bus_destroy(kith_coord_bus_t *bus);

/**
 * Return this instance's ID.
 *
 * @param bus Bus handle.
 * @return    The instance ID.
 * @thread_safety unsafe.
 * @ownership caller — @p bus is borrowed for the call only.
 */
KITH_API uint32_t kith_coord_bus_instance_id(const kith_coord_bus_t *bus);

/**
 * Return the number of active cluster members.
 *
 * @param bus Bus handle.
 * @return    The number of active cluster members.
 * @thread_safety unsafe.
 * @ownership caller — @p bus is borrowed for the call only.
 */
KITH_API uint32_t kith_coord_bus_member_count(const kith_coord_bus_t *bus);

/**
 * Copy one member's status into @p out. @p index is zero-based and must
 * be less than @c kith_coord_bus_member_count.
 *
 * @param bus   Bus handle. NULL is an error.
 * @param index Member index (0-based).
 * @param out   Receives the member status on success.
 * @return      0 on success, negative kith_error on failure:
 *              - -KITH_EINVAL if @p bus or @p out is NULL,
 *              - -KITH_ERANGE if @p index is out of range.
 * @thread_safety unsafe.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_coord_bus_member_status(const kith_coord_bus_t *bus,
                                                        uint32_t index,
                                                        kith_coord_bus_member_status_t *out);

/**
 * Register an additional cluster member on the bus. The bus created by
 * @c kith_coord_bus_create carries exactly one member (the local instance);
 * this function extends the membership table so the split/merge coordinator
 * can pick a real target instance when the local cell density exceeds the
 * split threshold. In the loopback transport this constructs a real
 * multi-member cluster in-process: two coord handles borrowing one bus see
 * the same membership table, the density-driven evaluator fires on every
 * member coord (each targets a member other than itself), and rebalance
 * contracts published on the bus transfer cell authority across the
 * instance boundary.
 *
 * @param bus         Bus handle. NULL is an error.
 * @param instance_id Instance ID of the joining member. Must not be 0 (0 is
 *                    reserved for the embedded/unowned sentinel) and must not
 *                    match an existing member's instance_id.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p bus is NULL or @p instance_id is 0,
 *                    - -KITH_EEXIST if @p instance_id is already a member,
 *                    - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — no member_status/tick/split evaluation may be in
 *                flight on @p bus when this is called.
 * @ownership caller — @p bus is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_bus_add_member(kith_coord_bus_t *bus, uint32_t instance_id);

/**
 * Run the bus's periodic membership maintenance. Refreshes the local
 * member's heartbeat. No transport refreshes remote heartbeats, so the
 * membership table is static and no member is ever pruned.
 *
 * @param bus   Bus handle. NULL is an error.
 * @param now_ms Current monotonic time in milliseconds.
 * @return      0 on success, negative kith_error on failure:
 *              - -KITH_EINVAL if @p bus is NULL.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p bus is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_bus_tick(kith_coord_bus_t *bus, uint64_t now_ms);

/**
 * Subscribe to a zone's bus events. Refcounted: multiple callers may
 * subscribe to the same zone; the subscription is active while the
 * refcount is positive.
 *
 * @param bus  Bus handle. NULL is an error.
 * @param zone Zone to subscribe to.
 * @return     0 on success, negative kith_error on failure:
 *             - -KITH_EINVAL if @p bus is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p bus is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_bus_subscribe(kith_coord_bus_t *bus, uint32_t zone);

/**
 * Unsubscribe from a zone's bus events. Refcounted: decrements the
 * refcount; the subscription is removed when the refcount reaches zero.
 * Idempotent: unsubscribing a zone with no subscription returns 0.
 *
 * @param bus  Bus handle. NULL is an error.
 * @param zone Zone to unsubscribe from.
 * @return     0 on success (even if not subscribed),
 *             - -KITH_EINVAL if @p bus is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p bus is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_coord_bus_unsubscribe(kith_coord_bus_t *bus, uint32_t zone);

/**
 * Publish an event to a zone's subscribers. The @p payload is copied into
 * the bus's internal queue; the caller may free @p payload immediately
 * after the call. In the loopback transport, the event is enqueued to the
 * inbound ring and is available to @c kith_coord_bus_drain on the next
 * call.
 *
 * @param bus         Bus handle. NULL is an error.
 * @param event_type  Event type.
 * @param zone        Zone to publish to.
 * @param payload     Event payload. May be NULL when @p payload_len is 0.
 *                    Copied into the bus's internal queue.
 * @param payload_len Payload length in bytes.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p bus is NULL,
 *                    - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe.
 * @ownership caller — @p payload is borrowed for the call only (copied
 *           internally).
 */
[[nodiscard]] KITH_API int kith_coord_bus_publish(kith_coord_bus_t *bus,
                                                  kith_coord_bus_event_type_t event_type,
                                                  uint32_t zone,
                                                  const void *payload,
                                                  uint32_t payload_len);

/**
 * Drain pending inbound bus events. Copies event headers into @p out; the
 * @c payload field of each event borrows the bus's internal queue and is
 * valid until the next @c kith_coord_bus_drain call. Events are returned
 * in the order they were published.
 *
 * @param bus       Bus handle. NULL is an error.
 * @param out       Output buffer. May be NULL when @p max is 0 (only counts).
 * @param max       Maximum number of events to copy.
 * @param out_count Receives the number of events copied into @p out. May
 *                  be NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p bus is NULL.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p out is the caller's output storage. The
 *           @c payload field of each event borrows the bus's internal
 *           queue and is valid until the next drain call.
 */
[[nodiscard]] KITH_API int kith_coord_bus_drain(kith_coord_bus_t *bus,
                                                kith_coord_bus_event_t *out,
                                                size_t max,
                                                size_t *out_count);

/** @} */

#endif /* KITH_COORD_COORD_H */

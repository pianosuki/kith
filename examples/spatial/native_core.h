#ifndef SPATIAL_NATIVE_CORE_H
#define SPATIAL_NATIVE_CORE_H

#include <stdint.h>

#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/sim/sim.h"

/* Native movement-apply core for the spatial game library: the actor table,
 * the stripe-locked per-actor apply sequences, the per-tick dirty-cell set
 * with its authority epochs, the identity-gate drop counter, and the replay
 * record buffer, all in C so the movement path never enters the Python
 * interpreter. The movement message handler registers on the gateway with
 * the pool-dispatched flag; the per-tick flush and the
 * control-plane accessors are driven from examples/spatial/native.py via
 * ctypes. */

#ifdef __cplusplus
extern "C"
{
#endif

    /** Opaque native-core handle. */
    typedef struct spatial_native spatial_native_t;

    /**
     * One applied-movement replay record (the v1 MoveEvent echo of one decoded
     * input: no sim state is read to build it).
     */
    struct spatial_move_record
    {
        /** Actor the input names. */
        uint64_t actor_id;
        /** Input tick from the frame. */
        uint32_t input_tick;
        /** Move components from the frame. */
        int16_t move_x;
        /** Move components from the frame. */
        int16_t move_y;
        /** Move components from the frame. */
        int16_t move_z;
        /** Input flags from the frame. */
        uint8_t flags;
        /** Explicit tail padding; 26 payload bytes land on the 8-byte
         *  boundary (sizeof 32). */
        uint8_t pad[7];
    };

    /**
     * Creation parameters. Borrowed plane handles must outlive the core.
     */
    struct spatial_native_params
    {
        /** Must be sizeof(struct spatial_native_params). */
        uint32_t size;
        /** Must be KITH_ABI_VERSION. */
        uint32_t abi_version;
        /** Zone id every actor publishes into. */
        uint32_t zone_id;
        /** Chebyshev radius of the per-subscriber window diffed on a crossing. */
        uint32_t cell_radius;
        /** Cell span per axis in Q16.16 fixed-point (cell = floor(pos / span)). */
        uint64_t cell_size_q16;
        /** Actor-table capacity; actor ids are 1-based and bounded by this. */
        uint32_t max_actors;
        /** Borrowed sim store the apply sequence publishes artifacts into. */
        kith_sim_t *sim;
        /** Borrowed model the apply sequence drives through its vtable. */
        kith_sim_model_t *model;
        /** Borrowed fabric the per-tick flush publishes cell products into. */
        kith_fabric_t *fabric;
        /** Reserved for future additive fields. Must be zero-filled. */
        void *reserved[4];
    };

    /**
     * Build a native core. Pre-allocates the actor table, the stripe locks, the
     * dirty-cell set, the authority-epoch table, and the record ring.
     *
     * @param params     Creation parameters; @c size and @c abi_version must
     *                   match. NULL is an error.
     * @param out_native Receives the new handle on success.
     * @return           0 on success, negative on failure:
     *                   - -1 if any parameter is invalid,
     *                   - -2 on allocation or lock initialization failure.
     * @thread_safety unsafe — call once before registering handlers.
     * @ownership callee — the caller destroys the handle with
     *           spatial_native_destroy.
     */
    int spatial_native_create(const struct spatial_native_params *params,
                              spatial_native_t **out_native);

    /**
     * Release every resource held by the core. Passing NULL is a no-op. The
     * caller must ensure no handler dispatch or flush is in flight.
     *
     * @thread_safety unsafe — must not race with any core call.
     * @ownership callee — @p native is consumed and freed by the call.
     */
    void spatial_native_destroy(spatial_native_t *native);

    /**
     * Register the pool-dispatched movement handler for @p msg_type on the
     * gateway (the @c KITH_GATEWAY_HANDLER_POOL flag). The handler
     * runs the full per-input apply sequence on a worker thread.
     *
     * @param native   Native core handle. NULL is an error.
     * @param gateway  Borrowed gateway handle; must outlive the core. NULL is an
     *                 error.
     * @param msg_type Wire message type id the handler owns.
     * @return         0 on success, -1 on invalid arguments, or the gateway's
     *                 registration error.
     * @thread_safety unsafe — call from the composition root before run.
     * @ownership caller — both handles are borrowed for the registration only.
     */
    int spatial_native_register_movement_handler(spatial_native_t *native,
                                                 kith_gateway_t *gateway,
                                                 uint16_t msg_type);

    /**
     * Insert a freshly allocated actor and publish its initial state: the
     * artifact lands in the position-derived cell and both the new and the old
     * cell bookkeeping update the way the per-input apply does. The actor id
     * must be within the table capacity and not yet inserted.
     *
     * @param native  Native core handle. NULL is an error.
     * @param actor   Initial actor state; @p actor.id selects the slot.
     * @return        0 on success, -1 when the slot is out of range or already
     *                occupied, or the publish error.
     * @thread_safety safe — serialized internally against applies and spawns.
     * @ownership caller — @p actor is borrowed for the call only.
     */
    int spatial_native_actor_insert(spatial_native_t *native, const kith_sim_actor_t *actor);

    /**
     * Set an actor's absolute position, publish the updated state, and copy the
     * updated actor to @p out_actor. Zeroes velocity and preserves the input
     * tick, flags, and update_seq, matching the control-plane teleport
     * contract.
     *
     * @param native  Native core handle. NULL is an error.
     * @param actor_id Actor to reposition.
     * @param pos_x   New X position in Q16.16.
     * @param pos_y   New Y position in Q16.16.
     * @param pos_z   New Z position in Q16.16.
     * @param out_actor Receives the updated state; may be NULL.
     * @return        0 on success, -1 when the actor is absent, or the publish
     *                error.
     * @thread_safety safe — the actor's stripe serializes against applies.
     * @ownership caller — @p out_actor is the caller's output storage.
     */
    int spatial_native_actor_teleport(spatial_native_t *native,
                                      uint64_t actor_id,
                                      int64_t pos_x,
                                      int64_t pos_y,
                                      int64_t pos_z,
                                      kith_sim_actor_t *out_actor);

    /**
     * Copy one actor's current state. Returns -1 when the actor is absent.
     *
     * @thread_safety safe — the actor's stripe serializes against applies.
     * @ownership caller — @p out_actor is the caller's output storage.
     */
    int spatial_native_actor_get(spatial_native_t *native,
                                 uint64_t actor_id,
                                 kith_sim_actor_t *out_actor);

    /**
     * Snapshot every inserted actor, quiesced: the control lock excludes
     * concurrent inserts and every stripe is held for the duration, so no
     * apply can replace a value mid-copy. @p cap bounds the copy; the call
     * fails with -2 when the live population exceeds it.
     *
     * @thread_safety safe — quiesced internally; never runs on the apply hot
     *                path.
     * @ownership caller — @p out_actors is the caller's output storage.
     */
    int spatial_native_actor_snapshot(spatial_native_t *native,
                                      kith_sim_actor_t *out_actors,
                                      uint32_t cap,
                                      uint32_t *out_count);

    /**
     * Apply one movement input through the model vtable on the control path:
     * stripe-held apply, step, store, publish, dirty marks, and a replay
     * record — the identity gate and the subscription-window diff do not apply
     * (no session drives this path). Returns the stepped actor.
     *
     * @param native   Native core handle. NULL is an error.
     * @param actor_id Actor to move.
     * @param input    Decoded movement input.
     * @param dt_ms    Simulation step length in milliseconds.
     * @param out_actor Receives the stepped state; may be NULL.
     * @return        0 on success, -1 when the actor is absent, or the model /
     *                publish error.
     * @thread_safety safe — the actor's stripe serializes same-actor applies.
     * @ownership caller — @p out_actor is the caller's output storage.
     */
    int spatial_native_apply(spatial_native_t *native,
                             uint64_t actor_id,
                             const kith_sim_input_t *input,
                             uint32_t dt_ms,
                             kith_sim_actor_t *out_actor);

    /**
     * Mark the actor's current cell dirty so the next flush re-broadcasts its
     * product (the chat path). Returns -1 when the actor is absent.
     *
     * @thread_safety safe — the actor's stripe and the bookkeeping lock
     *                serialize internally.
     * @ownership caller — @p native is borrowed for the call only.
     */
    int spatial_native_mark_actor_cell_dirty(spatial_native_t *native, uint64_t actor_id);

    /**
     * Read the monotonic count of movement frames dropped by the identity gate
     * (a frame naming an actor other than the session's bound one).
     *
     * @thread_safety safe — the counter is atomic.
     * @ownership caller — @p native is borrowed for the call only.
     */
    uint64_t spatial_native_gate_drops(const spatial_native_t *native);

    /**
     * Read the core's overflow counters: replay records dropped to a full ring
     * (only when the flush stalls far beyond any operating point) and dirty
     * marks dropped to dirty-set overflow (unreachable; see
     * spatial_native_flush). Both are zero at every honest operating point;
     * a nonzero value is a sizing failure, not a tolerated loss.
     *
     * @thread_safety safe — the counters are atomic.
     * @ownership caller — the out-parameters are the caller's output storage.
     */
    void spatial_native_overflows(const spatial_native_t *native,
                                  uint64_t *out_record_drops,
                                  uint64_t *out_dirty_drops);

    /**
     * Drain one per-tick flush: every cell dirtied since the last flush is
     * bumped exactly once (epoch read-modify-write under the bookkeeping lock,
     * fabric publish outside it; a stale-epoch publish is suppressed and
     * counted), then up to @p cap buffered replay records are copied out. A
     * record appended after the drain's snapshot waits for the next tick, so
     * the drain is bounded and each record drains exactly once. *out_more is
     * non-zero when the snapshot exceeded @p cap and the caller must flush
     * again to finish it.
     *
     * @param native      Native core handle. NULL is an error.
     * @param out_records Caller's record buffer; may be NULL only when @p cap
     *                    is 0.
     * @param cap         Record buffer capacity.
     * @param out_count   Receives the number of records copied.
     * @param out_more    Receives 1 when records remain in the snapshot.
     * @param out_bumped  Receives the number of cells bumped this flush.
     * @param out_eperm   Receives the number of stale-epoch publishes
     *                    suppressed this flush.
     * @return           0 on success, -1 on invalid arguments, -3 when the
     *                   authority-epoch table is full, or the fabric publish
     *                   error. A dirty mark dropped to dirty-set overflow
     *                   does not fail the flush; the drop is counted in the
     *                   overflow census spatial_native_overflows reports.
     * @thread_safety safe — ticks do not overlap; the drain coexists with
     *                concurrent applies.
     * @ownership caller — @p out_records is the caller's output storage.
     */
    int spatial_native_flush(spatial_native_t *native,
                             struct spatial_move_record *out_records,
                             uint32_t cap,
                             uint32_t *out_count,
                             uint32_t *out_more,
                             uint64_t *out_bumped,
                             uint64_t *out_eperm);

#ifdef __cplusplus
}
#endif

#endif /* SPATIAL_NATIVE_CORE_H */

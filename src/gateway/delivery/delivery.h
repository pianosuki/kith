#pragma once

#include <stddef.h>
#include <stdint.h>

#include "gateway/gateway_internal.h"

/**
 * Delivery subsystem internals. When the gateway's
 * @c replication_batch_type_id is non-zero, delivery packs the session's
 * full composed view-subject set into one multi-subject frame per refresh
 * (a 4-byte count header followed by N serialized subject records) and
 * enqueues it as a single net frame. Otherwise each subject is encoded as
 * a separate frame using @c replication_type_id. The reactor flushes the
 * connection's output queue separately; the gateway only composes and
 * enqueues.
 */

/** One serialized view subject (68 bytes, big-endian / network byte order,
 *  matching the proto frame header):
 *   0-7   actor_id      (uint64)
 *   8-15  pos_x         (int64, Q16.16)
 *   16-23 pos_y         (int64, Q16.16)
 *   24-31 pos_z         (int64, Q16.16)
 *   32-39 vel_x         (int64, Q16.16)
 *   40-47 vel_y         (int64, Q16.16)
 *   48-55 vel_z         (int64, Q16.16)
 *   56-59 input_tick    (uint32)
 *   60-63 update_seq    (uint32)
 *   64-67 product_level (uint32)
 */
#define GATEWAY_DELIVERY_PAYLOAD_SIZE 68u

/** Multi-subject batch frame header (4 bytes, big-endian / network byte
 *  order, matching the proto frame header):
 *   0-1 subject_count (uint16)
 *   2-3 reserved       (uint16)
 */
#define GATEWAY_DELIVERY_BATCH_HEADER_SIZE 4u

/**
 * Membership-event discriminator for the product_level word (bytes 64-67)
 * of a serialized record. Records whose product_level
 * word has this bit set are membership events, not subject states: the
 * actor_id field names the subject the event concerns (0 for the crowd
 * aggregate), the kind occupies the low bits of the product_level word,
 * and the position/velocity/input_tick/update_seq fields carry zeros.
 * State records never set the marker bit: representation tiers enumerate
 * below it.
 *
 * Membership events ride in-band, ordered before the state records of the
 * same pass, and are never suppressed; they make absence explicit so a
 * subject leaving the view is distinguishable from silence.
 */
#define GATEWAY_DELIVERY_EVENT_MARKER 0x80000000u

/** Kinds of membership event carried in a marked record's product_level
 *  word (below the marker bit). The underlying type is fixed so the wire
 *  values stay pinned. */
enum gateway_delivery_event_kind : unsigned int
{
    /** The subject entered the delivered view set. */
    GATEWAY_DELIVERY_EVENT_ENTER = 0u,
    /** The subject left the delivered view set but remains observable in
     *  the subscription window (budget eviction, crowd absorption). */
    GATEWAY_DELIVERY_EVENT_EXIT = 1u,
    /** The crowd aggregate engaged for this subscriber. */
    GATEWAY_DELIVERY_EVENT_CROWD_ENTER = 2u,
    /** The crowd aggregate dissolved for this subscriber. */
    GATEWAY_DELIVERY_EVENT_CROWD_EXIT = 3u,
    /** The subject is gone from every cached window cell (removed at the
     *  source, or departed the subscription area). */
    GATEWAY_DELIVERY_EVENT_VANISH = 4u,
};

/** Serialize one membership-event record into a 68-byte payload buffer:
 *  actor_id big-endian, zeros elsewhere, product_level word set to the
 *  event marker plus @p kind. */
void gateway_delivery_serialize_event(uint64_t actor_id, unsigned int kind, void *out);

/** Measure, allocate, and encode one frame of @p type_id carrying
 *  @p payload on the gateway's proto and net handles. Returns the built
 *  frame (caller releases), or NULL when the payload does not encode or
 *  the frame cannot be allocated. */
[[nodiscard]] kith_net_frame_t *gateway_delivery_frame_build(kith_gateway_t *gateway,
                                                             uint16_t type_id,
                                                             const void *payload,
                                                             size_t payload_len);

/** Serialize one view subject into a 68-byte payload buffer. */
void gateway_delivery_serialize(const kith_gateway_view_subject_t *s, void *out);

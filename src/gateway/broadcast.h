#pragma once

#include <stddef.h>
#include <stdint.h>

#include "gateway/gateway_internal.h"

/**
 * Cell-scoped broadcast queue internals. The queue is a fixed-depth FIFO
 * of pending requests embedded in the gateway handle: submissions copy
 * their cell key and payload and append under the queue mutex from any
 * thread; the reactor thread drains the queue once per tick, before the
 * session pass, and fans each request's single encoded frame to every
 * session whose subscription window covers the request's cell. Submit
 * refusals and undelivered recipients count on the gateway handle.
 */

/** Initialize the queue. Returns 0 on success, -KITH_ENOMEM when the queue
 *  mutex cannot initialize. */
[[nodiscard]] int gateway_broadcast_init(kith_gateway_t *gateway);

/** Submit one broadcast request. The payload is copied through the
 *  gateway's allocator. Returns 0, or -KITH_EINVAL (bad arguments),
 *  -KITH_EOVERFLOW (payload_len exceeds UINT32_MAX), -KITH_EPROTO (the
 *  payload does not encode), -KITH_EAGAIN (queue full; counted),
 *  -KITH_ESTATE (gateway shutting down), -KITH_ENOMEM (copy failure). */
[[nodiscard]] int gateway_broadcast_submit(kith_gateway_t *gateway,
                                           const kith_fabric_cell_key_t *cell,
                                           uint16_t type_id,
                                           const void *payload,
                                           size_t payload_len);

/** Drain pending requests: fan one encoded frame per request to every
 *  session whose window covers the request's cell, release the payloads,
 *  and count undelivered recipients. Reactor thread only (the tick calls
 *  this ahead of its session pass). A frozen queue drains nothing —
 *  teardown owns the release path. */
void gateway_broadcast_drain(kith_gateway_t *gateway);

/** Freeze the queue and release every pending request's payload, counting
 *  each as a drop: accepted requests the gateway will never deliver. Runs
 *  under the queue mutex for the swap, so a late submit observes the
 *  freeze and refuses -KITH_ESTATE. */
void gateway_broadcast_teardown(kith_gateway_t *gateway);

/** Release the queue mutex at handle teardown. */
void gateway_broadcast_fini(kith_gateway_t *gateway);

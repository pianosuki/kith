#pragma once

#include "gateway/gateway_internal.h"

/**
 * Delivery-strategy internals shared across the gateway library: the
 * built-in preset vtables and the encode-and-enqueue core the presets
 * (and, via the public wrapper, games) use to place frames on a session's
 * connection. Registry storage and lookup live on the gateway handle
 * (gateway_internal.h).
 */

/** The factory-default "full" preset: resend every selected subject every
 *  pass, batch-framed when configured. */
const kith_gateway_delivery_vtable_t *gateway_delivery_full_vtable(void);

/** Encode @p payload as one frame of @p type_id on the gateway's proto
 *  handle and enqueue it on @p conn. Returns 0 on success, negative
 *  kith_error on failure (-KITH_EPROTO encode reject, -KITH_ENOMEM frame
 *  allocation, -KITH_ESTATE closed connection, -KITH_EAGAIN backpressure).
 *  Shared core of kith_gateway_deliver_frame and the built-in strategies. */
int gateway_delivery_enqueue(kith_gateway_t *gateway,
                             kith_net_conn_t *conn,
                             uint16_t type_id,
                             const void *payload,
                             size_t payload_len);

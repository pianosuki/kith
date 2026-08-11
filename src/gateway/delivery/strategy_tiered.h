#pragma once

#include "gateway/gateway_internal.h"

/** The "tiered" preset: full-record encoding with rate-limited scheduling
 *  and record-granular change suppression against the per-session ledger. */
const kith_gateway_delivery_vtable_t *gateway_delivery_tiered_vtable(void);

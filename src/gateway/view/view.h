#pragma once

#include <stddef.h>
#include <stdint.h>

#include "gateway/gateway_internal.h"

/**
 * View subsystem internals. The relevance composer builds a per-subscriber
 * scored view set from the shared cell cache. Each refresh streams the
 * candidate artifacts of the cached cells into a bounded max-heap keyed by
 * prior-view stickiness then squared distance, keeps the best candidates up
 * to the configured budget, classifies them by subject class (self, actor,
 * crowd), selects a representation tier per subject, and emits a crowd
 * aggregate for the overflow.
 */

/** Initialize view state (no allocation until the first refresh) and bind
 *  @p alloc as the instance every growable buffer releases through. */
void gateway_view_init(struct gateway_view_state *v, const kith_allocator_t *alloc);

/** Free the view set and prior-id buffers. */
void gateway_view_fini(struct gateway_view_state *v);

/** Zero the compose scratch (its buffers grow lazily during composition)
 *  and bind @p alloc as the instance every growable buffer releases
 *  through. */
void gateway_view_scratch_init(struct gateway_compose_scratch *scratch,
                               const kith_allocator_t *alloc);

/** Free the compose scratch buffers. */
void gateway_view_scratch_fini(struct gateway_compose_scratch *scratch);

/** Hand the session's pending membership delta to the caller and mark it
 *  consumed. Returns the event count (0 when already consumed) and points
 *  @p out_events at the internal delta array, valid until the next full
 *  recomposition. The bound delivery strategy calls this once per deliver
 *  pass; events it fails to enqueue ride in the strategy's own state. */
size_t gateway_view_take_membership(kith_gateway_session_t *session,
                                    const struct gateway_view_event **out_events);

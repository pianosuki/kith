#pragma once

#include <stddef.h>

#include "gateway/gateway_internal.h"

/**
 * Session subsystem internals. The session table is an open-addressed hash
 * of session pointers keyed by connection pointer. Sessions are heap-
 * allocated (stable addresses) and inserted/removed by the connection they
 * bind to. The gateway's dispatch path looks up a session by connection
 * pointer to find the subscriber context for an incoming frame.
 */

/** Initialize a session table with @p max_sessions capacity. Returns 0 on
 *  success, negative kith_error on allocation failure. */
[[nodiscard]] int gateway_session_table_init(struct gateway_session_table *t,
                                             size_t max_sessions,
                                             const kith_allocator_t *alloc);

/** Free a session table's slot array. Does not free the sessions themselves
 *  (the caller owns them). Nulls each session's gateway back-pointer so a
 *  subsequent session_destroy does not touch the freed table. */
void gateway_session_table_fini(struct gateway_session_table *t);

/** Look up the session bound to @p conn, or NULL when no session is bound. */
struct kith_gateway_session *gateway_session_lookup_by_conn(const struct gateway_session_table *t,
                                                            const kith_net_conn_t *conn);

/** Insert @p s into the table keyed by its connection. Returns 0 on success,
 *  -KITH_EBUSY when the table is full, -KITH_EEXIST when a session is
 *  already bound to the same connection. */
[[nodiscard]] int gateway_session_insert(struct gateway_session_table *t,
                                         struct kith_gateway_session *s);

/** Remove the session bound to @p conn from the table. No-op when no
 *  session is bound. */
void gateway_session_remove(struct gateway_session_table *t, const kith_net_conn_t *conn);

/** Increment the session's dispatch refcount. Pins the session across the
 *  reactor→worker handoff so a worker that reads session fields after the
 *  reactor has already closed the connection and dropped the owner
 *  reference (kith_gateway_session_destroy) does not touch freed memory.
 *  Mirrors kith_net_conn_acquire. Passing NULL is a no-op. */
void gateway_session_acquire(struct kith_gateway_session *session);

/** Decrement the session's dispatch refcount and free the session's own
 *  resources (view state, subscription window, the connection reference
 *  acquired at create, and the struct itself) when the count reaches zero.
 *  The owner reference is dropped by kith_gateway_session_destroy (which
 *  first removes the session from the table); each in-flight dispatch
 *  drops its reference here, on the worker thread. The teardown touches
 *  no gateway or session-table state, so it is safe to run on a worker
 *  thread. Passing NULL is a no-op. Mirrors kith_net_conn_release. */
void gateway_session_release(struct kith_gateway_session *session);

/** One tick pass's retry-and-census work for one session, under a single
 *  window_lock hold. Retries up to GATEWAY_WINDOW_RETRY_PASS_CAP retained
 *  window adds starting at the ring's rotation head (an O(1) capacity
 *  guard on each leg skips the attempt when the cell cache's stripe or the
 *  fabric interest set has no free slot, so a saturated pass pays no
 *  probe); a stale retention — the caller landed the add itself — is
 *  dropped uncounted, and a landing drops its entry and counts
 *  kith_gateway_window_retry_adds. Reports whether the session is bound to
 *  an actor while tracking no cells (the seed-failure census): @p
 *  out_without_cells receives 1 for such a session, 0 otherwise. Reactor
 *  thread only (the tick's session pass). */
void gateway_window_tick_pass(struct kith_gateway_session *session, size_t *out_without_cells);

/** Which of @p keys the session's subscription window contains. Takes the
 *  window lock once; bit i of the returned mask is set when the window
 *  contains @p keys[i]. @p key_count must not exceed 64. Reactor thread
 *  only (the caller walks the session table it owns). */
uint64_t gateway_session_window_matches(struct kith_gateway_session *session,
                                        const kith_fabric_cell_key_t *keys,
                                        size_t key_count);

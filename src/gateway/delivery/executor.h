#pragma once

#include <stdint.h>

#include "gateway/gateway_internal.h"

/**
 * Delivery executor internals. Moves the tick's per-session
 * deliver pass off the reactor thread: the pass either skips a session
 * whose previous deliver is still in flight or submits a zero-allocation
 * job (the session's embedded record) to a gateway-owned pool of threads.
 * The compose side waits for an in-flight job under a per-pass budget so a
 * composition never overwrites a view the worker is reading.
 *
 * Worker-side plain reads of a session's view fields are licensed by the
 * submit/dequeue handoff of the underlying task queue (its mutex pair
 * establishes happens-before from submit to dequeue) plus the compose
 * wait's release/acquire pairing; the job callback documents the
 * dependency.
 */

struct gateway_delivery_executor;

/** Create an executor with @p worker_count threads and a task queue of
 *  @p task_capacity nodes — sized to max_sessions, which is tight: at most
 *  one in-flight job per session makes steady-state exhaustion impossible.
 *  @p alloc is stored on the executor and routes the executor, its worker
 *  pool, and the claimed thread-local scratches. Returns 0 on success,
 *  negative kith_error on failure.
 *  @ownership callee — freed by gateway_delivery_executor_fini. */
[[nodiscard]] int gateway_delivery_executor_create(uint32_t worker_count,
                                                   uint32_t task_capacity,
                                                   const kith_allocator_t *alloc,
                                                   struct gateway_delivery_executor **out_executor);

/** Drain and free @p executor: queued and running jobs complete (each
 *  releases its session reference), then the worker threads' claimed
 *  scratches are freed. Safe to pass NULL. The gateway's own scratch is
 *  NOT freed here — gateway_handle_teardown frees it after this returns,
 *  which is what pins the teardown order (drain, scratches, gateway
 *  scratch). */
void gateway_delivery_executor_fini(struct gateway_delivery_executor *executor);

/** Deliver one pass for @p session through @p executor: a session whose
 *  previous deliver is still in flight is skipped (counted), otherwise a
 *  job carrying @p now_ms is submitted. On queue exhaustion (KITH_EBUSY)
 *  the submission is dropped and counted; delivery never falls back to
 *  inline. Reactor thread only. */
void gateway_delivery_executor_deliver(kith_gateway_t *gateway,
                                       struct gateway_delivery_executor *executor,
                                       struct kith_gateway_session *session,
                                       uint64_t now_ms);

/** Wait until @p session has no deliver in flight, or until the monotonic
 *  @p deadline_ns passes. True when the session is idle (compose may
 *  proceed), false on expiry (compose is skipped this tick). Reactor
 *  thread only; this is the executor's one reactor-block point. */
bool gateway_delivery_executor_wait_idle(struct kith_gateway_session *session,
                                         uint64_t deadline_ns);

/** Snapshot the executor's counters into @p out_stats. */
void gateway_delivery_executor_stats_fill(const struct gateway_delivery_executor *executor,
                                          kith_gateway_delivery_executor_stats_t *out_stats);

/** Return the calling thread's delivery scratch: the thread-local scratch
 *  claimed by an executor thread, or the gateway's own scratch on the
 *  reactor thread (and in inline mode). Returns NULL when an executor
 *  thread's scratch cannot be allocated — the caller drops the batch
 *  (counted backpressure), never sharing a buffer across threads. */
struct gateway_delivery_scratch *gateway_delivery_scratch(kith_gateway_t *gateway);

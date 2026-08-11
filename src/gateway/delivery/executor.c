/* Delivery executor: submits the tick's per-session deliver
 * passes to a gateway-owned pool of threads and serializes them per
 * session with an in-flight flag. Owns the thread-local scratch registry,
 * the per-pass wait-idle poll, and the executor counters. Siblings:
 * delivery.c (the dispatch it drives), strategy.c (the vtables the job
 * reaches), gateway.c (creation, the pass loop, and teardown order). */

#include "gateway/delivery/executor.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <time.h>

#include "gateway/delivery/strategy.h"
#include "gateway/session/session.h"
#include "kith/types.h"
#include "kith/version.h"

/** Poll granularity for the compose wait. Steady state never waits (the
 *  flag is clear), so the sleep only shapes the backpressure path: fine
 *  enough to resume within the budget's slack, coarse enough to stay out
 *  of the scheduler's way. */
#define GATEWAY_EXECUTOR_WAIT_POLL_NS (50u * 1000u)

/** The calling executor thread's scratch claim, or NULL off the executor. */
static _Thread_local struct gateway_scratch_node *t_scratch_node = nullptr;

struct gateway_delivery_scratch *gateway_delivery_scratch(kith_gateway_t *gateway)
{
    if (t_scratch_node)
    {
        return &t_scratch_node->scratch;
    }
    struct gateway_delivery_executor *executor = gateway->delivery_executor;
    if (!executor)
    {
        return &gateway->delivery;
    }
    // Executor thread's first job: claim a private scratch so the full
    // strategy's batch fill never shares a buffer with the reactor thread
    // or another worker. Allocation failure returns NULL; the caller drops
    // the batch as counted backpressure rather than share a buffer.
    struct gateway_scratch_node *node = kith_alloc_zero(gateway->allocator, 1u, sizeof(*node));
    if (!node)
    {
        return nullptr;
    }
    node->scratch.allocator = gateway->allocator;
    pthread_mutex_lock(&executor->scratch_lock);
    node->next = executor->claimed_scratches;
    executor->claimed_scratches = node;
    pthread_mutex_unlock(&executor->scratch_lock);
    t_scratch_node = node;
    return &node->scratch;
}

/** Job body executed on an executor thread. The worker's plain reads of
 *  the session's view fields are licensed by the task queue's mutex pair
 *  (submit to dequeue happens-before) and the compose wait's release/
 *  acquire pairing; replacing the mutex-protected queue with a lock-free
 *  one without equivalent fences introduces torn reads. The deliver
 *  return code is discarded: failures stay counter-visible
 *  through the strategy's dropped stats and are not reactor-path errors. */
static void gateway_delivery_job_run(void *ctx)
{
    auto job = (struct gateway_delivery_job *)ctx;
    struct kith_gateway_session *session = job->session;
    struct gateway_delivery_executor *executor = job->gateway->delivery_executor;

    // Claim the thread's scratch before the strategy may need it; the
    // accessor returns that claim for every subsequent job on the thread.
    (void)gateway_delivery_scratch(job->gateway);

    uint64_t start_ns = gateway_now_ns();
    (void)kith_gateway_deliver(job->gateway, session, job->now_ms, nullptr);
    atomic_fetch_add_explicit(
        &executor->deliver_ns_pending, gateway_now_ns() - start_ns, memory_order_relaxed);
    atomic_fetch_sub_explicit(&executor->inflight_current, 1u, memory_order_relaxed);
    // Last write to the session: release, so the reactor's acquire loads
    // pair with every write the deliver made. The session reference (taken
    // at submit) is dropped afterwards.
    atomic_store_explicit(&session->delivery_in_flight, false, memory_order_release);
    gateway_session_release(session);
}

void gateway_delivery_executor_deliver(kith_gateway_t *gateway,
                                       struct gateway_delivery_executor *executor,
                                       struct kith_gateway_session *session,
                                       uint64_t now_ms)
{
    if (atomic_load_explicit(&session->delivery_in_flight, memory_order_acquire))
    {
        atomic_fetch_add_explicit(&executor->inflight_skips_total, 1u, memory_order_relaxed);
        return;
    }
    // The flag, the session reference, and the in-flight count land
    // before the submit: the worker's decrement must observe a count of
    // at least one, so a job completing inside the submit window cannot
    // wrap the count through zero or hide the peak from the watermark.
    // The reference is acquired before submit so the worker's release
    // can never drop the count below the owner's.
    atomic_store_explicit(&session->delivery_in_flight, true, memory_order_release);
    gateway_session_acquire(session);
    session->delivery_job.gateway = gateway;
    session->delivery_job.session = session;
    session->delivery_job.now_ms = now_ms;
    const uint64_t current =
        atomic_fetch_add_explicit(&executor->inflight_current, 1u, memory_order_relaxed) + 1u;
    int rc =
        kith_worker_submit(executor->workers, gateway_delivery_job_run, &session->delivery_job);
    if (rc != 0)
    {
        // Queue exhaustion (KITH_EBUSY): no job exists, so the clears are
        // uncontended. Delivery never falls back to inline; the skipped
        // pass is the counted, one-tick degradation.
        atomic_store_explicit(&session->delivery_in_flight, false, memory_order_release);
        gateway_session_release(session);
        atomic_fetch_sub_explicit(&executor->inflight_current, 1u, memory_order_relaxed);
        atomic_fetch_add_explicit(&executor->ebusy_skips_total, 1u, memory_order_relaxed);
        return;
    }
    atomic_fetch_add_explicit(&executor->jobs_submitted_total, 1u, memory_order_relaxed);
    uint64_t watermark =
        atomic_load_explicit(&executor->inflight_high_watermark, memory_order_relaxed);
    while (current > watermark &&
           !atomic_compare_exchange_weak_explicit(&executor->inflight_high_watermark,
                                                  &watermark,
                                                  current,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed))
    {
    }
}

bool gateway_delivery_executor_wait_idle(struct kith_gateway_session *session, uint64_t deadline_ns)
{
    if (!atomic_load_explicit(&session->delivery_in_flight, memory_order_acquire))
    {
        return true;
    }
    for (;;)
    {
        if (!atomic_load_explicit(&session->delivery_in_flight, memory_order_acquire))
        {
            return true;
        }
        if (gateway_now_ns() >= deadline_ns)
        {
            return false;
        }
        struct timespec pause = {.tv_sec = 0, .tv_nsec = (long)GATEWAY_EXECUTOR_WAIT_POLL_NS};
        nanosleep(&pause, nullptr);
    }
}

void gateway_delivery_executor_stats_fill(const struct gateway_delivery_executor *executor,
                                          kith_gateway_delivery_executor_stats_t *out_stats)
{
    out_stats->jobs_submitted_total =
        atomic_load_explicit(&executor->jobs_submitted_total, memory_order_relaxed);
    out_stats->inflight_skips_total =
        atomic_load_explicit(&executor->inflight_skips_total, memory_order_relaxed);
    out_stats->ebusy_skips_total =
        atomic_load_explicit(&executor->ebusy_skips_total, memory_order_relaxed);
    out_stats->wait_budget_exhausted_total =
        atomic_load_explicit(&executor->wait_budget_exhausted_total, memory_order_relaxed);
    out_stats->compose_wait_timeouts_total =
        atomic_load_explicit(&executor->compose_wait_timeouts_total, memory_order_relaxed);
    out_stats->inflight_current =
        atomic_load_explicit(&executor->inflight_current, memory_order_relaxed);
    out_stats->inflight_high_watermark =
        atomic_load_explicit(&executor->inflight_high_watermark, memory_order_relaxed);
}

int gateway_delivery_executor_create(uint32_t worker_count,
                                     uint32_t task_capacity,
                                     const kith_allocator_t *alloc,
                                     struct gateway_delivery_executor **out_executor)
{
    struct gateway_delivery_executor *executor = kith_alloc_zero(alloc, 1u, sizeof(*executor));
    if (!executor)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    executor->allocator = alloc;
    kith_worker_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.worker_count = worker_count;
    params.task_capacity = task_capacity;
    int rc = kith_worker_create(&params, alloc, &executor->workers);
    if (rc != 0)
    {
        kith_free(alloc, executor);
        return rc;
    }
    if (pthread_mutex_init(&executor->scratch_lock, nullptr) != 0)
    {
        kith_worker_destroy(executor->workers);
        kith_free(alloc, executor);
        return kith_error_return(KITH_ESTATE);
    }
    *out_executor = executor;
    return 0;
}

void gateway_delivery_executor_fini(struct gateway_delivery_executor *executor)
{
    if (!executor)
    {
        return;
    }
    // Drain first: workers exit only after the queue empties and shutdown
    // is set, so every queued and running job (and its session reference)
    // completes before the threads are joined. Only then are the claimed
    // scratches freed — a worker may still be using its scratch during the
    // drain, and a joined thread never returns its claim.
    kith_worker_destroy(executor->workers);
    pthread_mutex_lock(&executor->scratch_lock);
    struct gateway_scratch_node *node = executor->claimed_scratches;
    while (node)
    {
        struct gateway_scratch_node *next = node->next;
        kith_free(executor->allocator, node->scratch.batch);
        kith_free(executor->allocator, node);
        node = next;
    }
    executor->claimed_scratches = nullptr;
    pthread_mutex_unlock(&executor->scratch_lock);
    pthread_mutex_destroy(&executor->scratch_lock);
    kith_free(executor->allocator, executor);
}

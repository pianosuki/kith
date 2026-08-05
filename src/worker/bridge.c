/* Reactor-side bridge for the worker module: bundles a worker pool with a
 * user callback so the reactor thread submits the callback to the pool from C
 * and never enters the Python interpreter. The public contract is
 * include/kith/worker/worker.h. The ready-hop allocates a per-fire work
 * record capturing fd and events; the task-hop submits the user callback
 * directly. */

#include "kith/types.h"
#include "kith/worker/worker.h"
#include "worker/worker_internal.h"

// ---------------------------------------------------------------------------
// bridge handle
// ---------------------------------------------------------------------------
// One struct serves both the ready and task bridges. The ready bridge uses
// the `ready` + `ctx` fields; the task bridge uses the `task` + `ctx`
// fields. The unused callback is NULL. The pool is shared.
struct kith_worker_bridge
{
    // Resolved from the pool at create: the bridge and its per-fire work
    // records are released through the pool's allocator.
    const kith_allocator_t *allocator;
    kith_worker_t *pool;
    kith_worker_ready_fn ready; // set by create_ready; NULL for task bridges
    kith_worker_task_cb task;   // set by create_task; NULL for ready bridges
    void *ctx;
};

// ---------------------------------------------------------------------------
// ready-hop work record + worker-side dispatch
// ---------------------------------------------------------------------------
// The reactor fires the ready-hop with (fd, events, bridge). The fd and
// events vary per fire, so the hop allocates one of these records, captures
// them, and submits it to the pool. The worker dispatch frees the record
// after calling the user callback. The record is small (32 bytes on LP64);
// the per-fire allocation matches the gateway's per-dispatch allocation.
struct bridge_ready_work
{
    // Copied from the bridge at fire time: the dispatch frees the record on
    // a worker thread through the same instance that allocated it.
    const kith_allocator_t *allocator;
    kith_worker_ready_fn cb;
    void *ctx;
    int fd;
    unsigned int events;
};

static void bridge_ready_dispatch(void *arg)
{
    struct bridge_ready_work *w = arg;
    w->cb(w->fd, w->events, w->ctx);
    kith_free(w->allocator, w);
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_worker_bridge_create_ready(kith_worker_t *pool,
                                                           kith_worker_ready_fn ready,
                                                           void *ctx,
                                                           kith_worker_bridge_t **out_bridge)
{
    if (pool == nullptr || ready == nullptr || out_bridge == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_bridge = nullptr;

    const kith_allocator_t *allocator = kith_worker_allocator(pool);
    struct kith_worker_bridge *b = kith_alloc_zero(allocator, 1, sizeof(*b));
    if (b == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    b->allocator = allocator;
    b->pool = pool;
    b->ready = ready;
    b->task = nullptr;
    b->ctx = ctx;
    *out_bridge = b;
    return 0;
}

[[nodiscard]] KITH_API int kith_worker_bridge_create_task(kith_worker_t *pool,
                                                          kith_worker_task_cb task,
                                                          void *ctx,
                                                          kith_worker_bridge_t **out_bridge)
{
    if (pool == nullptr || task == nullptr || out_bridge == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_bridge = nullptr;

    const kith_allocator_t *allocator = kith_worker_allocator(pool);
    struct kith_worker_bridge *b = kith_alloc_zero(allocator, 1, sizeof(*b));
    if (b == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    b->allocator = allocator;
    b->pool = pool;
    b->ready = nullptr;
    b->task = task;
    b->ctx = ctx;
    *out_bridge = b;
    return 0;
}

KITH_API void kith_worker_bridge_destroy(kith_worker_bridge_t *bridge)
{
    if (bridge == nullptr)
    {
        return;
    }
    kith_free(bridge->allocator, bridge);
}

// ---------------------------------------------------------------------------
// reactor-side hops (run on the reactor thread; never enter Python)
// ---------------------------------------------------------------------------
KITH_API void kith_worker_bridge_ready_cb(int fd, unsigned int events, void *bridge_ctx)
{
    struct kith_worker_bridge *b = bridge_ctx;
    if (b == nullptr)
    {
        return;
    }
    struct bridge_ready_work *w = kith_alloc_zero(b->allocator, 1, sizeof(*w));
    if (w == nullptr)
    {
        return; // OOM: drop
    }
    w->allocator = b->allocator;
    w->cb = b->ready;
    w->ctx = b->ctx;
    w->fd = fd;
    w->events = events;
    if (kith_worker_submit(b->pool, bridge_ready_dispatch, w) != 0)
    {
        kith_free(b->allocator, w); // pool full: drop
    }
}

KITH_API void kith_worker_bridge_task_cb(void *bridge_ctx)
{
    struct kith_worker_bridge *b = bridge_ctx;
    if (b == nullptr)
    {
        return;
    }
    (void)kith_worker_submit(b->pool, b->task, b->ctx);
}

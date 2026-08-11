/* Handler table and dispatch for the gateway: mutex-protected register and
 * unregister of per-message-type callbacks, worker-pool attachment, and the
 * dispatch entry point that resolves a session and either invokes the handler
 * inline or hands a payload-copy work record to the pool. The table is owned
 * by the gateway handle (gateway.c). */

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "gateway/gateway_internal.h"
#include "gateway/session/session.h"
#include "kith/types.h"
#include "kith/util/util.h"

/*---------------------------------------------------------------------------
 * handler table lifecycle
 *-------------------------------------------------------------------------*/

int gateway_handler_init(struct gateway_handler_table *t,
                         size_t capacity,
                         const kith_allocator_t *alloc)
{
    t->allocator = alloc;
    t->slots = kith_alloc_zero(alloc, capacity, sizeof(*t->slots));
    if (!t->slots)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    t->capacity = capacity;
    if (pthread_mutex_init(&t->lock, nullptr) != 0)
    {
        kith_free(alloc, t->slots);
        t->slots = nullptr;
        t->capacity = 0u;
        return kith_error_return(KITH_ESTATE);
    }
    return 0;
}

void gateway_handler_fini(struct gateway_handler_table *t)
{
    if (t->slots)
    {
        kith_free(t->allocator, t->slots);
        t->slots = nullptr;
        t->capacity = 0u;
    }
    pthread_mutex_destroy(&t->lock);
}

/*---------------------------------------------------------------------------
 * public handler API
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_gateway_register_handler(kith_gateway_t *gateway,
                                                         uint16_t msg_type,
                                                         kith_gateway_msg_handler_fn fn,
                                                         void *user_data)
{
    return kith_gateway_register_handler_flags(
        gateway, msg_type, fn, user_data, KITH_GATEWAY_HANDLER_NONE);
}

[[nodiscard]] KITH_API int kith_gateway_register_handler_flags(kith_gateway_t *gateway,
                                                               uint16_t msg_type,
                                                               kith_gateway_msg_handler_fn fn,
                                                               void *user_data,
                                                               uint32_t flags)
{
    if (!gateway || !fn)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if ((size_t)msg_type >= gateway->handlers.capacity)
    {
        return kith_error_return(KITH_EINVAL);
    }
    // A handler is a Python-bound trampoline or a pool-dispatched C function,
    // never both; the two flags name disjoint execution models for one fn.
    if ((flags & (KITH_GATEWAY_HANDLER_PYTHON | KITH_GATEWAY_HANDLER_POOL)) ==
        (KITH_GATEWAY_HANDLER_PYTHON | KITH_GATEWAY_HANDLER_POOL))
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&gateway->handlers.lock);
    gateway->handlers.slots[msg_type].fn = fn;
    gateway->handlers.slots[msg_type].user_data = user_data;
    gateway->handlers.slots[msg_type].flags = flags;
    pthread_mutex_unlock(&gateway->handlers.lock);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_unregister_handler(kith_gateway_t *gateway,
                                                           uint16_t msg_type)
{
    if (!gateway)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if ((size_t)msg_type >= gateway->handlers.capacity)
    {
        return 0;
    }
    pthread_mutex_lock(&gateway->handlers.lock);
    gateway->handlers.slots[msg_type].fn = nullptr;
    gateway->handlers.slots[msg_type].user_data = nullptr;
    gateway->handlers.slots[msg_type].flags = 0u;
    pthread_mutex_unlock(&gateway->handlers.lock);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_register_session_destroyed_handler(
    kith_gateway_t *gateway, kith_gateway_session_destroyed_fn fn, void *user_data)
{
    return kith_gateway_register_session_destroyed_handler_flags(
        gateway, fn, user_data, KITH_GATEWAY_HANDLER_NONE);
}

[[nodiscard]] KITH_API int kith_gateway_register_session_destroyed_handler_flags(
    kith_gateway_t *gateway, kith_gateway_session_destroyed_fn fn, void *user_data, uint32_t flags)
{
    if (!gateway || !fn)
    {
        return kith_error_return(KITH_EINVAL);
    }
    // One dispatch kind per registration, as for message handlers: the
    // PYTHON and POOL bits name disjoint execution models for one fn.
    if ((flags & (KITH_GATEWAY_HANDLER_PYTHON | KITH_GATEWAY_HANDLER_POOL)) ==
        (KITH_GATEWAY_HANDLER_PYTHON | KITH_GATEWAY_HANDLER_POOL))
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&gateway->destroyed.lock);
    gateway->destroyed.fn = fn;
    gateway->destroyed.user_data = user_data;
    gateway->destroyed.flags = flags;
    pthread_mutex_unlock(&gateway->destroyed.lock);
    return 0;
}

[[nodiscard]] KITH_API int
kith_gateway_unregister_session_destroyed_handler(kith_gateway_t *gateway)
{
    if (!gateway)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&gateway->destroyed.lock);
    gateway->destroyed.fn = nullptr;
    gateway->destroyed.user_data = nullptr;
    gateway->destroyed.flags = 0u;
    pthread_mutex_unlock(&gateway->destroyed.lock);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_attach_worker_pool(kith_gateway_t *gateway,
                                                           kith_worker_t *pool)
{
    if (!gateway)
    {
        return kith_error_return(KITH_EINVAL);
    }
    gateway->workers = pool;
    return 0;
}

/*---------------------------------------------------------------------------
 * pool-bound dispatch worker task
 *-------------------------------------------------------------------------*/

// A heap-allocated work record carrying one dispatch across the
// reactor→worker handoff. The payload is copied (the frame's payload buffer
// is borrowed for the dispatch call only and may be recycled before the
// worker runs); the session is refcounted (gateway_session_acquire is taken
// before the record is submitted and gateway_session_release is called at
// the end of the worker task), so the session stays alive for the dispatch
// duration even when the reactor closes the connection and calls
// kith_gateway_session_destroy before the worker runs the handler. The
// record owns its payload copy and frees itself after the handler runs.
struct gateway_dispatch_work
{
    /** Allocator copied from the gateway at dispatch; the record frees
     *  itself through this copy on the worker thread. */
    const kith_allocator_t *allocator;
    kith_gateway_msg_handler_fn fn;
    uint16_t msg_type;
    void *user_data;
    struct kith_gateway_session *session;
    uint32_t payload_len;
    /** Handler flags copied from the registration slot: the worker task
     *  reads the PYTHON bit to honor the interpreter exit gate. */
    uint32_t flags;
    // payload follows the struct (flexible-array-style; C23 permits a
    // trailing array member of unspecified size). Allocated as
    // sizeof(struct gateway_dispatch_work) + payload_len.
    uint8_t payload[];
};

static void gateway_dispatch_worker_task(void *arg)
{
    struct gateway_dispatch_work *work = arg;
    // A python-bound trampoline must not enter a finalizing interpreter: a
    // foreign C thread's entry there has no graceful path. Pool-flagged
    // native C handlers never enter the interpreter and still run.
    if ((work->flags & KITH_GATEWAY_HANDLER_PYTHON) != 0u && kith_python_finalizing())
    {
        gateway_session_release(work->session);
        kith_free(work->allocator, work);
        return;
    }
    work->fn(work->msg_type, work->payload, work->payload_len, work->session, work->user_data);
    gateway_session_release(work->session);
    kith_free(work->allocator, work);
}

/*---------------------------------------------------------------------------
 * dispatch
 *-------------------------------------------------------------------------*/

// Submit a pool-bound handler dispatch (Python trampoline or POOL-flagged
// native C handler) to the attached worker pool. The
// payload is copied into a heap-allocated work record because the frame's
// payload buffer is borrowed for this dispatch call only; the session is
// refcounted (a reference is acquired here and released at the end of the
// worker task) so it outlives the reactor→worker handoff even when the
// reactor closes the connection first. On EBUSY (or any submit error) the
// task is never run inline on the reactor thread: a reactor stalled in the
// Python interpreter — or behind a long C handler — under saturation is
// strictly worse than a dropped
// input, so the dispatch is dropped and counted instead. The composition
// root records the counted delta as
// kith_gateway_dispatch_dropped_total. Returns 0 on submit, -KITH_ENOMEM on
// work-record allocation
// failure, -KITH_EBUSY on drop.
static int gateway_dispatch_to_pool(kith_gateway_t *gateway,
                                    struct kith_gateway_session *s,
                                    kith_gateway_msg_handler_fn fn,
                                    uint16_t type_id,
                                    void *user_data,
                                    uint32_t flags,
                                    const kith_proto_frame_t *frame)
{
    size_t alloc_size = sizeof(struct gateway_dispatch_work) + (size_t)frame->payload_len;
    struct gateway_dispatch_work *work = kith_alloc(gateway->allocator, alloc_size);
    if (work == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    work->allocator = gateway->allocator;
    work->fn = fn;
    work->msg_type = type_id;
    work->user_data = user_data;
    work->session = s;
    work->payload_len = frame->payload_len;
    work->flags = flags;
    if (frame->payload_len > 0u && frame->payload != nullptr)
    {
        memcpy(work->payload, frame->payload, frame->payload_len);
    }
    gateway_session_acquire(s);
    int rc = kith_worker_submit(gateway->workers, gateway_dispatch_worker_task, work);
    if (rc != 0)
    {
        gateway_session_release(s);
        kith_free(work->allocator, work);
        atomic_fetch_add_explicit(&gateway->dispatch_dropped, 1u, memory_order_relaxed);
        return kith_error_return(KITH_EBUSY);
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_dispatch(kith_gateway_t *gateway,
                                                 kith_net_conn_t *conn,
                                                 const kith_proto_frame_t *frame)
{
    if (!gateway || !conn || !frame)
    {
        return kith_error_return(KITH_EINVAL);
    }

    struct kith_gateway_session *s = gateway_session_lookup_by_conn(&gateway->sessions, conn);
    if (!s)
    {
        return kith_error_return(KITH_ENOENT);
    }

    uint16_t type_id = frame->type_id;
    kith_gateway_msg_handler_fn fn = nullptr;
    void *user_data = nullptr;
    uint32_t flags = 0u;
    pthread_mutex_lock(&gateway->handlers.lock);
    if ((size_t)type_id < gateway->handlers.capacity)
    {
        fn = gateway->handlers.slots[type_id].fn;
        user_data = gateway->handlers.slots[type_id].user_data;
        flags = gateway->handlers.slots[type_id].flags;
    }
    pthread_mutex_unlock(&gateway->handlers.lock);

    if (!fn)
    {
        return kith_error_return(KITH_ENOENT);
    }

    // Count every dispatch that reaches a handler (inline or pool-bound) as a
    // relaxed atomic on the reactor thread. The composition root records the
    // per-tick delta as kith_gateway_dispatches_total; this is
    // the dispatches/sec term of the per-tick capacity model. Sibling to
    // dispatch_dropped (drops are a subset of this total).
    atomic_fetch_add_explicit(&gateway->dispatch_total, 1u, memory_order_relaxed);

    // Pool-bound handler (Python trampoline or POOL-flagged native C
    // handler): defer to the worker pool so the reactor thread never
    // enters the Python interpreter and never runs long C handler work
    // inline.
    if ((flags & (KITH_GATEWAY_HANDLER_PYTHON | KITH_GATEWAY_HANDLER_POOL)) != 0u)
    {
        if (gateway->workers == nullptr)
        {
            // No pool attached: the dispatch cannot run off the reactor
            // thread and never runs inline. Drop and count, mirroring
            // pool exhaustion.
            atomic_fetch_add_explicit(&gateway->dispatch_dropped, 1u, memory_order_relaxed);
            return kith_error_return(KITH_EBUSY);
        }
        return gateway_dispatch_to_pool(gateway, s, fn, type_id, user_data, flags, frame);
    }

    // An unflagged C handler: invoke inline on the reactor thread. Plain
    // C handlers are function pointers that never enter the Python
    // interpreter, so the pool hop only adds latency.
    fn(type_id, frame->payload, frame->payload_len, s, user_data);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_dispatch_drops(const kith_gateway_t *gateway,
                                                       uint64_t *out_drops)
{
    if (gateway == nullptr || out_drops == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_drops = atomic_load_explicit(&gateway->dispatch_dropped, memory_order_acquire);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_lifecycle_drops(const kith_gateway_t *gateway,
                                                        uint64_t *out_drops)
{
    if (gateway == nullptr || out_drops == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_drops = atomic_load_explicit(&gateway->lifecycle_dropped, memory_order_acquire);
    return 0;
}

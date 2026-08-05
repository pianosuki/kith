/* Public handle and thread pool for the worker module: params resolution,
 * create and destroy, a mutex-protected free list of task nodes feeding a
 * mutex- and-condvar task queue, and the worker main loop. Hosts Python
 * handler dispatch off the reactor thread; the public contract is
 * include/kith/worker/worker.h. */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>

#include "kith/types.h"
#include "kith/version.h"
#include "worker/worker_internal.h"

// ---------------------------------------------------------------------------
// params resolution
// ---------------------------------------------------------------------------
static void worker_resolve_params(kith_worker_params_t *out)
{
    if (out->worker_count == 0u)
    {
        out->worker_count = KITH_WORKER_DEFAULT_WORKER_COUNT;
    }
    if (out->worker_count > 64u)
    {
        out->worker_count = 64u;
    }
    if (out->task_capacity == 0u)
    {
        out->task_capacity = KITH_WORKER_DEFAULT_TASK_CAPACITY;
    }
}

// ---------------------------------------------------------------------------
// free-list push/pop
// ---------------------------------------------------------------------------
// The free list and the task queue share one mutex: submit pops a node and
// workers push the spent node back, both under queue_lock, so every node
// moves between the two structures inside a single critical section and is
// handed out exactly once per recycle.
static struct kith_worker_task_node *worker_free_pop(struct kith_worker *pool)
{
    struct kith_worker_task_node *node = pool->free_head;
    if (node != nullptr)
    {
        pool->free_head = node->next;
        node->next = nullptr;
    }
    return node;
}

static void worker_free_push(struct kith_worker *pool, struct kith_worker_task_node *node)
{
    node->next = pool->free_head;
    pool->free_head = node;
}

// ---------------------------------------------------------------------------
// worker thread main loop
// ---------------------------------------------------------------------------
static void *worker_main(void *arg)
{
    struct kith_worker *pool = arg;

    // Name the thread so /proc and perf traces attribute worker duty
    // without inferring it from duty-cycle behavior. pthread_setname_np
    // caps comm at 15 characters plus the terminator, which bounds the
    // rendered index below 1000.
    char name[16];
    uint32_t index = atomic_fetch_add_explicit(&pool->named_count, 1u, memory_order_relaxed);
    (void)snprintf(name, sizeof(name), "kith-worker-%u", (unsigned int)index);
    (void)pthread_setname_np(pthread_self(), name);

    // Pool threads never take SIGINT/SIGTERM: those signals coordinate
    // graceful shutdown on the embedding application's own thread. A
    // worker receiving one terminates the process by default disposition
    // before any drain, or records it on a thread that never runs the
    // interpreter's handler; blocking at entry pins delivery to the
    // threads the embedder owns.
    sigset_t shutdown_signals;
    sigemptyset(&shutdown_signals);
    sigaddset(&shutdown_signals, SIGINT);
    sigaddset(&shutdown_signals, SIGTERM);
    (void)pthread_sigmask(SIG_BLOCK, &shutdown_signals, nullptr);

    for (;;)
    {
        // Dequeue a task under the lock; run it outside the lock so other
        // workers can dequeue concurrently. The condvar wait is the only
        // blocking call on the worker thread; the reactor thread never
        // blocks here (it submits and returns).
        pthread_mutex_lock(&pool->queue_lock);
        while (pool->queue_head == nullptr &&
               !atomic_load_explicit(&pool->shutdown, memory_order_acquire))
        {
            pthread_cond_wait(&pool->queue_cv, &pool->queue_lock);
        }
        if (pool->queue_head == nullptr &&
            atomic_load_explicit(&pool->shutdown, memory_order_acquire))
        {
            // Drain complete and shutdown requested: exit.
            pthread_mutex_unlock(&pool->queue_lock);
            return nullptr;
        }
        // Pop the head. shutdown may be set concurrently with non-empty
        // queue (destroy drains pending tasks); the loop above keeps draining
        // until the queue is empty AND shutdown is set.
        struct kith_worker_task_node *node = pool->queue_head;
        pool->queue_head = node->next;
        if (pool->queue_head == nullptr)
        {
            pool->queue_tail = nullptr;
        }
        pthread_mutex_unlock(&pool->queue_lock);

        // Execute the task outside the lock. The callback owns its ctx; on
        // return the node is recycled to the free list. The callback may
        // submit further tasks (kith_worker_submit acquires queue_lock).
        node->cb(node->ctx);
        atomic_fetch_add_explicit(&pool->tasks_completed, 1u, memory_order_relaxed);
        pthread_mutex_lock(&pool->queue_lock);
        worker_free_push(pool, node);
        pthread_mutex_unlock(&pool->queue_lock);
    }
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

// Allocate the task-node pool and build the initial lock-free free list
// (every node links to the next; the last node's next is NULL).
static int worker_alloc_nodes(struct kith_worker *pool, uint32_t capacity)
{
    pool->nodes = kith_alloc_zero(pool->allocator, capacity, sizeof(*pool->nodes));
    if (pool->nodes == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    for (uint32_t i = 0u; i < capacity; i++)
    {
        pool->nodes[i].next = (i + 1u < capacity) ? &pool->nodes[i + 1u] : nullptr;
    }
    pool->free_head = &pool->nodes[0];
    return 0;
}

// Allocate the thread array and spawn the worker threads. On a spawn failure
// the already-started threads are joined and every allocation rolled back.
static int worker_start_threads(struct kith_worker *pool)
{
    pool->threads = kith_alloc_zero(pool->allocator, pool->worker_count, sizeof(*pool->threads));
    if (pool->threads == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    for (uint32_t i = 0u; i < pool->worker_count; i++)
    {
        if (pthread_create(&pool->threads[i], nullptr, worker_main, pool) != 0)
        {
            atomic_store_explicit(&pool->shutdown, true, memory_order_release);
            // Broadcast under the lock: a just-started worker between the
            // wait-predicate check and cond_wait misses an unlocked wakeup
            // and sleeps past the joins below.
            pthread_mutex_lock(&pool->queue_lock);
            pthread_cond_broadcast(&pool->queue_cv);
            pthread_mutex_unlock(&pool->queue_lock);
            for (uint32_t j = 0u; j < i; j++)
            {
                pthread_join(pool->threads[j], nullptr);
            }
            kith_free(pool->allocator, pool->threads);
            pool->threads = nullptr;
            return kith_error_return(KITH_ESTATE);
        }
    }
    return 0;
}

// The all-defaults parameter block: create applies it when the caller passes
// NULL. A zero field selects the corresponding default at resolve time.
static const kith_worker_params_t *worker_default_params(void)
{
    static const kith_worker_params_t instance = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
    };
    return &instance;
}

// Validate the caller-supplied params and allocator against this build's
// contract: params generation and size first, then the allocator's operation
// set. Returns a negative kith_error on failure.
static int worker_check_create_args(const kith_worker_params_t *params,
                                    const kith_allocator_t *alloc)
{
    if (params->size < sizeof(kith_worker_params_t))
    {
        return kith_error_return(KITH_ESIZE);
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_worker_create(const kith_worker_params_t *params,
                                              const kith_allocator_t *alloc,
                                              kith_worker_t **out_worker)
{
    if (out_worker == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_worker = nullptr;

    if (params == nullptr)
    {
        params = worker_default_params();
    }
    const int check_rc = worker_check_create_args(params, alloc);
    if (check_rc != 0)
    {
        return check_rc;
    }

    kith_worker_params_t resolved = *params;
    worker_resolve_params(&resolved);

    const kith_allocator_t *allocator = alloc != nullptr ? alloc : kith_allocator_default();
    struct kith_worker *pool = kith_alloc_zero(allocator, 1, sizeof(*pool));
    if (pool == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    pool->allocator = allocator;
    pool->worker_count = resolved.worker_count;

    if (worker_alloc_nodes(pool, resolved.task_capacity) != 0)
    {
        kith_free(allocator, pool);
        return kith_error_return(KITH_ENOMEM);
    }
    if (pthread_mutex_init(&pool->queue_lock, nullptr) != 0)
    {
        kith_free(allocator, pool->nodes);
        kith_free(allocator, pool);
        return kith_error_return(KITH_ESTATE);
    }
    if (pthread_cond_init(&pool->queue_cv, nullptr) != 0)
    {
        pthread_mutex_destroy(&pool->queue_lock);
        kith_free(allocator, pool->nodes);
        kith_free(allocator, pool);
        return kith_error_return(KITH_ESTATE);
    }
    const int start_rc = worker_start_threads(pool);
    if (start_rc != 0)
    {
        pthread_cond_destroy(&pool->queue_cv);
        pthread_mutex_destroy(&pool->queue_lock);
        kith_free(allocator, pool->nodes);
        kith_free(allocator, pool);
        return start_rc;
    }

    *out_worker = pool;
    return 0;
}

KITH_API void kith_worker_destroy(kith_worker_t *pool)
{
    if (pool == nullptr)
    {
        return;
    }
    // Signal shutdown and wake every idle worker. The worker loop drains the
    // remaining queued tasks before exiting (it only returns when the queue
    // is empty AND shutdown is set), so a pending task whose ctx aliases
    // pool-owned memory is not left dangling.
    atomic_store_explicit(&pool->shutdown, true, memory_order_release);
    pthread_mutex_lock(&pool->queue_lock);
    pthread_cond_broadcast(&pool->queue_cv);
    pthread_mutex_unlock(&pool->queue_lock);

    for (uint32_t i = 0u; i < pool->worker_count; i++)
    {
        pthread_join(pool->threads[i], nullptr);
    }

    kith_free(pool->allocator, pool->threads);
    pthread_cond_destroy(&pool->queue_cv);
    pthread_mutex_destroy(&pool->queue_lock);
    kith_free(pool->allocator, pool->nodes);
    kith_free(pool->allocator, pool);
}

// ---------------------------------------------------------------------------
// submit
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int
kith_worker_submit(kith_worker_t *pool, kith_worker_task_cb cb, void *ctx)
{
    if (pool == nullptr || cb == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    // Pop, enqueue, and signal under one lock acquisition: the free list
    // and the queue share queue_lock, so a node can never sit filled but
    // unqueued, and the EBUSY check sees the pool's true in-flight count.
    pthread_mutex_lock(&pool->queue_lock);
    struct kith_worker_task_node *node = worker_free_pop(pool);
    if (node == nullptr)
    {
        pthread_mutex_unlock(&pool->queue_lock);
        return kith_error_return(KITH_EBUSY);
    }
    node->cb = cb;
    node->ctx = ctx;
    node->next = nullptr;

    if (pool->queue_tail != nullptr)
    {
        pool->queue_tail->next = node;
        pool->queue_tail = node;
    }
    else
    {
        pool->queue_head = node;
        pool->queue_tail = node;
    }
    pthread_cond_signal(&pool->queue_cv);
    // Incremented under the lock, before the node can be dequeued, so
    // tasks_completed never transiently exceeds tasks_submitted.
    atomic_fetch_add_explicit(&pool->tasks_submitted, 1u, memory_order_relaxed);
    pthread_mutex_unlock(&pool->queue_lock);
    return 0;
}

// ---------------------------------------------------------------------------
// accessors
// ---------------------------------------------------------------------------
KITH_API uint32_t kith_worker_count(const kith_worker_t *pool)
{
    if (pool == nullptr)
    {
        return 0u;
    }
    return pool->worker_count;
}

KITH_API uint64_t kith_worker_tasks_submitted(const kith_worker_t *pool)
{
    if (pool == nullptr)
    {
        return 0u;
    }
    return atomic_load_explicit(&pool->tasks_submitted, memory_order_acquire);
}

KITH_API uint64_t kith_worker_tasks_completed(const kith_worker_t *pool)
{
    if (pool == nullptr)
    {
        return 0u;
    }
    return atomic_load_explicit(&pool->tasks_completed, memory_order_acquire);
}

KITH_LOCAL const kith_allocator_t *kith_worker_allocator(const struct kith_worker *pool)
{
    return pool->allocator;
}

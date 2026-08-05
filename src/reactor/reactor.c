/* Public handle and cross-platform frontend for the reactor: the fd hash map,
 * timer wheel, add/mod/del, and the run loop dispatching to the registered
 * backend. Shares reactor_internal.h with reactor_uring.c (the Linux io_uring
 * backend); the public contract is include/kith/reactor/reactor.h. */

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "kith/types.h"
#include "kith/version.h"
#include "reactor/reactor_internal.h"

// ---------------------------------------------------------------------------
// utilities
// ---------------------------------------------------------------------------

// Return the smallest power of 2 >= n.
static unsigned int next_pow2(unsigned int n)
{
    if (n == 0)
    {
        return 1;
    }
    n--;
    n |= n >> 1u;
    n |= n >> 2u;
    n |= n >> 4u;
    n |= n >> 8u;
    n |= n >> 16u;
    return n + 1;
}

static unsigned int fd_hash_capacity(unsigned int max_fds)
{
    return next_pow2(max_fds * 2);
}

static uint64_t now_monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

// ---------------------------------------------------------------------------
// fd entry hash map (open-addressed, linear probe, power-of-2 capacity)
// ---------------------------------------------------------------------------

// Returns the slot index, or capacity when every slot is live. The probe
// records the first tombstone it passes and prefers it over any empty slot
// found beyond it, so churn reuses vacated slots instead of burning one per
// deletion. Lookup stays correct: lookups skip tombstones, and inserting
// into a slot of the key's own probe chain never hides a live entry behind
// the new one.
static unsigned int
fd_hash_find_slot(const struct kith_fd_entry *entries, unsigned int capacity, int fd)
{
    unsigned int mask = capacity - 1;
    unsigned int first_removed = capacity;
    for (unsigned int i = 0; i < capacity; i++)
    {
        unsigned int slot = ((unsigned int)fd + i) & mask;
        if (entries[slot].fd == fd)
        {
            return slot;
        }
        if (entries[slot].fd == -1)
        {
            return first_removed < capacity ? first_removed : slot;
        }
        if (entries[slot].fd == -2 && first_removed == capacity)
        {
            first_removed = slot;
        }
    }
    return first_removed;
}

struct kith_fd_entry *reactor_fd_entry_lookup(struct kith_reactor *r, int fd)
{
    if (fd < 0 || r->fd_count == 0)
    {
        return nullptr;
    }
    unsigned int mask = r->fd_capacity - 1;
    for (unsigned int i = 0; i < r->fd_capacity; i++)
    {
        unsigned int slot = ((unsigned int)fd + i) & mask;
        if (r->fd_entries[slot].fd == fd)
        {
            return &r->fd_entries[slot];
        }
        if (r->fd_entries[slot].fd == -1)
        {
            return nullptr;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// MPSC task queue (lock-free, multi-producer single-consumer)
// ---------------------------------------------------------------------------
//
// Producers (any thread, including a worker pool thread posting a flush)
// enqueue via kith_reactor_submit: they exchange task_tail onto their node
// and link the previous tail to it (or, if the queue was empty, publish
// task_head). The reactor (single consumer) drains with task_queue_drain.
//
// The consumer must reset task_tail to nullptr when it empties the queue;
// otherwise task_tail keeps pointing at a drained (and recycled) node, and
// the next producer's exchange(task_tail, node) returns that stale node so
// the producer links node->next = node (a self-loop) and never publishes
// task_head — the task is silently dropped. Closing the queue is done with a
// compare-exchange: if a producer already enqueued a successor (task_tail no
// longer == head), the consumer waits for the producer to publish head->next
// before continuing the drain.

static void task_node_recycle(struct kith_reactor *r, struct kith_task_node *node)
{
    pthread_mutex_lock(&r->task_free_lock);
    atomic_store_explicit(&node->next, r->task_free_list, memory_order_relaxed);
    r->task_free_list = node;
    pthread_mutex_unlock(&r->task_free_lock);
}

static void task_queue_drain(struct kith_reactor *r)
{
    struct kith_task_node *head =
        atomic_exchange_explicit(&r->task_head, nullptr, memory_order_acquire);
    while (head != nullptr)
    {
        struct kith_task_node *next = atomic_load_explicit(&head->next, memory_order_acquire);
        if (next == nullptr)
        {
            // head is the observed tail. A producer that enqueued after us
            // set task_tail to its node but may not have published head->next
            // yet. Close the queue with a compare-exchange: if task_tail is
            // still head, the queue is empty now (set it to nullptr). If a
            // producer already moved task_tail, wait for it to publish
            // head->next, then continue draining the successor.
            struct kith_task_node *expected = head;
            if (atomic_compare_exchange_strong_explicit(
                    &r->task_tail, &expected, nullptr, memory_order_acq_rel, memory_order_acquire))
            {
                head->cb(head->ctx);
                task_node_recycle(r, head);
                return;
            }
            do
            {
                next = atomic_load_explicit(&head->next, memory_order_acquire);
            } while (next == nullptr);
        }
        head->cb(head->ctx);
        task_node_recycle(r, head);
        head = next;
    }
}

// ---------------------------------------------------------------------------
// timer wheel (256 slots, millisecond granularity)
// ---------------------------------------------------------------------------

static struct kith_timer_node *timer_node_alloc(struct kith_reactor *r)
{
    // Pop exactly one node: a whole-chain exchange pop leaves the recycled
    // remainder unreachable once the schedule overwrites the head's next link.
    struct kith_timer_node *head = atomic_load_explicit(&r->timer_free_list, memory_order_acquire);
    while (head != nullptr)
    {
        struct kith_timer_node *rest = head->next;
        if (atomic_compare_exchange_weak_explicit(
                &r->timer_free_list, &head, rest, memory_order_acq_rel, memory_order_acquire))
        {
            return head;
        }
    }
    return kith_alloc_zero(r->allocator, 1, sizeof(struct kith_timer_node));
}

static void timer_node_free(struct kith_reactor *r, struct kith_timer_node *node)
{
    node->next = atomic_exchange_explicit(&r->timer_free_list, node, memory_order_acq_rel);
}

static void timer_wheel_insert(struct kith_reactor *r, struct kith_timer_node *node)
{
    unsigned int slot = node->deadline_ms & (KITH_REACTOR_TIMER_SLOTS - 1);
    struct kith_timer_node **prev = &r->timer_wheel[slot];
    while (*prev != nullptr && (*prev)->deadline_ms <= node->deadline_ms)
    {
        prev = &(*prev)->next;
    }
    node->next = *prev;
    *prev = node;
}

static void timer_wheel_fire_slot(struct kith_reactor *r, unsigned int slot, uint64_t now_ms)
{
    struct kith_timer_node **prev = &r->timer_wheel[slot];
    while (*prev != nullptr)
    {
        struct kith_timer_node *node = *prev;
        if (node->deadline_ms <= now_ms)
        {
            *prev = node->next;
            kith_reactor_task_cb cb = node->cb;
            void *ctx = node->ctx;
            timer_node_free(r, node);
            r->timer_count--;
            cb(ctx);
        }
        else
        {
            prev = &(*prev)->next;
        }
    }
}

static void timer_wheel_advance(struct kith_reactor *r, uint64_t now_ms)
{
    while (r->timer_tick_ms < now_ms && r->timer_count > 0)
    {
        unsigned int slot = r->timer_tick_ms & (KITH_REACTOR_TIMER_SLOTS - 1);
        timer_wheel_fire_slot(r, slot, now_ms);
        r->timer_tick_ms++;
    }
    r->timer_tick_ms = now_ms;
}

// ---------------------------------------------------------------------------
// create-time helpers (extracted to keep kith_reactor_create under 80 lines)
// ---------------------------------------------------------------------------

// The all-defaults parameter block: create applies it when the caller passes
// NULL. A zero field selects the corresponding default at resolve time.
static const kith_reactor_params_t *reactor_default_params(void)
{
    static const kith_reactor_params_t instance = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
    };
    return &instance;
}

// Validate the caller-supplied params and allocator against this build's
// contract: params generation and size first, then the allocator's operation
// set. Returns a negative kith_error on failure.
static int reactor_check_create_args(const kith_reactor_params_t *params,
                                     const kith_allocator_t *alloc)
{
    if (params->size < sizeof(kith_reactor_params_t))
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

// Returns a negative kith_error on failure.
static int reactor_alloc_fd_entries(struct kith_reactor *r, unsigned int max_fds)
{
    r->fd_capacity = fd_hash_capacity(max_fds);
    r->fd_entries = kith_alloc_zero(r->allocator, r->fd_capacity, sizeof(struct kith_fd_entry));
    if (r->fd_entries == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    for (unsigned int i = 0; i < r->fd_capacity; i++)
    {
        r->fd_entries[i].fd = -1;
    }
    return 0;
}

// Returns a negative kith_error on failure.
static int reactor_alloc_task_nodes(struct kith_reactor *r, unsigned int count)
{
    struct kith_task_node *free_head = nullptr;
    for (unsigned int i = 0; i < count; i++)
    {
        struct kith_task_node *node =
            kith_alloc_zero(r->allocator, 1, sizeof(struct kith_task_node));
        if (node == nullptr)
        {
            while (free_head != nullptr)
            {
                struct kith_task_node *next =
                    atomic_load_explicit(&free_head->next, memory_order_relaxed);
                kith_free(r->allocator, free_head);
                free_head = next;
            }
            return kith_error_return(KITH_ENOMEM);
        }
        atomic_store_explicit(&node->next, free_head, memory_order_relaxed);
        free_head = node;
    }
    r->task_free_list = free_head;
    return 0;
}

static void reactor_free_task_nodes(struct kith_reactor *r)
{
    struct kith_task_node *node = r->task_free_list;
    r->task_free_list = nullptr;
    while (node != nullptr)
    {
        struct kith_task_node *next = atomic_load_explicit(&node->next, memory_order_relaxed);
        kith_free(r->allocator, node);
        node = next;
    }
    node = atomic_exchange_explicit(&r->task_head, nullptr, memory_order_relaxed);
    while (node != nullptr)
    {
        struct kith_task_node *next = atomic_load_explicit(&node->next, memory_order_relaxed);
        kith_free(r->allocator, node);
        node = next;
    }
}

// Free every timer node: the armed ones still hanging off the wheel slots,
// plus the fired ones parked on the recycle free list, which the wheel scan
// never reaches.
static void reactor_free_timer_nodes(struct kith_reactor *r)
{
    for (unsigned int i = 0; i < KITH_REACTOR_TIMER_SLOTS; i++)
    {
        struct kith_timer_node *tn = r->timer_wheel[i];
        while (tn != nullptr)
        {
            struct kith_timer_node *next = tn->next;
            kith_free(r->allocator, tn);
            tn = next;
        }
    }
    struct kith_timer_node *node =
        atomic_exchange_explicit(&r->timer_free_list, nullptr, memory_order_relaxed);
    while (node != nullptr)
    {
        struct kith_timer_node *next = node->next;
        kith_free(r->allocator, node);
        node = next;
    }
}

// Returns a negative kith_error on failure.
static int reactor_init_backend(struct kith_reactor *r, unsigned int max_fds)
{
#ifdef __linux__
    r->backend = kith_reactor_backend_uring;
#else
    return kith_error_return(KITH_ENOSYS);
#endif
    return r->backend.init(r->allocator, &r->backend_data, max_fds);
}

// ---------------------------------------------------------------------------
// public API
// ---------------------------------------------------------------------------

[[nodiscard]] KITH_API int kith_reactor_create(const kith_reactor_params_t *params,
                                               const kith_allocator_t *alloc,
                                               kith_reactor_t **out_reactor)
{
    if (out_reactor == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_reactor = nullptr;

    if (params == nullptr)
    {
        params = reactor_default_params();
    }
    const int check_rc = reactor_check_create_args(params, alloc);
    if (check_rc != 0)
    {
        return check_rc;
    }

    const kith_allocator_t *allocator = alloc != nullptr ? alloc : kith_allocator_default();
    unsigned int max_fds = params->max_fds != 0 ? params->max_fds : KITH_REACTOR_DEFAULT_MAX_FDS;
    unsigned int task_cap =
        params->task_capacity != 0 ? params->task_capacity : KITH_REACTOR_DEFAULT_TASK_CAPACITY;

    struct kith_reactor *r = kith_alloc_zero(allocator, 1, sizeof(*r));
    if (r == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    r->allocator = allocator;
    r->params = *params;

    if (pthread_mutex_init(&r->task_free_lock, nullptr) != 0)
    {
        kith_free(allocator, r);
        return kith_error_return(KITH_ESTATE);
    }

    int rc = reactor_alloc_fd_entries(r, max_fds);
    if (rc != 0)
    {
        pthread_mutex_destroy(&r->task_free_lock);
        kith_free(allocator, r);
        return rc;
    }

    rc = reactor_alloc_task_nodes(r, task_cap);
    if (rc != 0)
    {
        kith_free(allocator, r->fd_entries);
        pthread_mutex_destroy(&r->task_free_lock);
        kith_free(allocator, r);
        return rc;
    }

    rc = reactor_init_backend(r, max_fds);
    if (rc != 0)
    {
        reactor_free_task_nodes(r);
        kith_free(allocator, r->fd_entries);
        pthread_mutex_destroy(&r->task_free_lock);
        kith_free(allocator, r);
        return rc;
    }

    r->timer_tick_ms = now_monotonic_ms();
    atomic_store_explicit(&r->cached_time_ms, r->timer_tick_ms, memory_order_release);

    *out_reactor = r;
    return 0;
}

KITH_API void kith_reactor_destroy(kith_reactor_t *reactor)
{
    if (reactor == nullptr)
    {
        return;
    }
    // Enforce the @thread_safety contract: destroying a reactor while its
    // run loop is in flight frees the backend and node storage out from
    // under the run-loop thread. Abort so every consumer (C or Python)
    // hits the same hard stop instead of corrupting state silently; the
    // core dump is the observable record (the reactor takes no logger).
    if (atomic_load_explicit(&reactor->run_in_flight, memory_order_acquire))
    {
        abort();
    }
    reactor->backend.destroy(reactor->backend_data);
    kith_free(reactor->allocator, reactor->fd_entries);
    reactor_free_task_nodes(reactor);
    reactor_free_timer_nodes(reactor);
    pthread_mutex_destroy(&reactor->task_free_lock);
    kith_free(reactor->allocator, reactor);
}

[[nodiscard]] KITH_API int kith_reactor_add(
    kith_reactor_t *reactor, int fd, unsigned int events, kith_reactor_cb cb, void *ctx)
{
    if (reactor == nullptr || fd < 0 || cb == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    if (reactor_fd_entry_lookup(reactor, fd) != nullptr)
    {
        return kith_error_return(KITH_EEXIST);
    }

    unsigned int slot = fd_hash_find_slot(reactor->fd_entries, reactor->fd_capacity, fd);
    if (slot >= reactor->fd_capacity)
    {
        return kith_error_return(KITH_EBUSY);
    }

    int rc = reactor->backend.add(reactor->backend_data, fd, events);
    if (rc != 0)
    {
        return rc;
    }

    reactor->fd_entries[slot].fd = fd;
    reactor->fd_entries[slot].events = events;
    reactor->fd_entries[slot].cb = cb;
    reactor->fd_entries[slot].ctx = ctx;
    reactor->fd_count++;

    return 0;
}

[[nodiscard]] KITH_API int kith_reactor_mod(kith_reactor_t *reactor, int fd, unsigned int events)
{
    if (reactor == nullptr || fd < 0)
    {
        return kith_error_return(KITH_EINVAL);
    }

    struct kith_fd_entry *entry = reactor_fd_entry_lookup(reactor, fd);
    if (entry == nullptr)
    {
        return kith_error_return(KITH_ENOENT);
    }

    int rc = reactor->backend.mod(reactor->backend_data, fd, events);
    if (rc != 0)
    {
        return rc;
    }

    entry->events = events;
    return 0;
}

[[nodiscard]] KITH_API int kith_reactor_del(kith_reactor_t *reactor, int fd)
{
    if (reactor == nullptr || fd < 0)
    {
        return kith_error_return(KITH_EINVAL);
    }

    struct kith_fd_entry *entry = reactor_fd_entry_lookup(reactor, fd);
    if (entry == nullptr)
    {
        return kith_error_return(KITH_ENOENT);
    }

    int rc = reactor->backend.del(reactor->backend_data, fd);
    if (rc != 0)
    {
        return rc;
    }

    entry->fd = -2; // tombstone; fd_hash_find_slot reuses the first one
    reactor->fd_count--;
    return 0;
}

[[nodiscard]] KITH_API int
kith_reactor_submit(kith_reactor_t *reactor, kith_reactor_task_cb cb, void *ctx)
{
    if (reactor == nullptr || cb == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    pthread_mutex_lock(&reactor->task_free_lock);
    struct kith_task_node *node = reactor->task_free_list;
    if (node == nullptr)
    {
        pthread_mutex_unlock(&reactor->task_free_lock);
        return kith_error_return(KITH_EBUSY);
    }
    reactor->task_free_list = atomic_load_explicit(&node->next, memory_order_relaxed);
    pthread_mutex_unlock(&reactor->task_free_lock);

    node->cb = cb;
    node->ctx = ctx;
    atomic_store_explicit(&node->next, nullptr, memory_order_release);

    struct kith_task_node *prev =
        atomic_exchange_explicit(&reactor->task_tail, node, memory_order_acq_rel);
    if (prev != nullptr)
    {
        atomic_store_explicit(&prev->next, node, memory_order_release);
    }
    else
    {
        atomic_store_explicit(&reactor->task_head, node, memory_order_release);
    }

    reactor->backend.wake(reactor->backend_data);
    return 0;
}

[[nodiscard]] KITH_API int kith_reactor_schedule(kith_reactor_t *reactor,
                                                 uint64_t deadline_ms,
                                                 kith_reactor_task_cb cb,
                                                 void *ctx)
{
    if (reactor == nullptr || cb == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    struct kith_timer_node *node = timer_node_alloc(reactor);
    if (node == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }

    uint64_t now = atomic_load_explicit(&reactor->cached_time_ms, memory_order_acquire);
    node->deadline_ms = deadline_ms < now ? now : deadline_ms;
    node->cb = cb;
    node->ctx = ctx;
    node->next = nullptr;

    timer_wheel_insert(reactor, node);
    reactor->timer_count++;
    return 0;
}

// Inline dispatch context + callback passed to backend.wait.
struct run_dispatch
{
    struct kith_reactor *reactor;
};

static void run_dispatch_cb(int fd, unsigned int events, void *ctx)
{
    struct run_dispatch *d = (struct run_dispatch *)ctx;
    struct kith_fd_entry *entry = reactor_fd_entry_lookup(d->reactor, fd);
    if (entry != nullptr && entry->cb != nullptr)
    {
        entry->cb(fd, events, entry->ctx);
    }
}

// Compute poll timeout from earliest timer deadline.
static int run_timeout_ms(struct kith_reactor *reactor, uint64_t now)
{
    if (reactor->timer_count == 0)
    {
        return -1; // block indefinitely
    }
    uint64_t earliest = UINT64_MAX;
    for (unsigned int i = 0; i < KITH_REACTOR_TIMER_SLOTS; i++)
    {
        struct kith_timer_node *n = reactor->timer_wheel[i];
        if (n != nullptr && n->deadline_ms < earliest)
        {
            earliest = n->deadline_ms;
        }
    }
    if (earliest == UINT64_MAX)
    {
        return -1;
    }
    if (earliest <= now)
    {
        return 0;
    }
    uint64_t diff = earliest - now;
    return diff > (uint64_t)INT_MAX ? INT_MAX : (int)diff;
}

[[nodiscard]] KITH_API int kith_reactor_run(kith_reactor_t *reactor)
{
    if (reactor == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    atomic_store_explicit(&reactor->run_in_flight, true, memory_order_release);

    while (!atomic_load_explicit(&reactor->stop_requested, memory_order_acquire))
    {
        uint64_t now = now_monotonic_ms();
        atomic_store_explicit(&reactor->cached_time_ms, now, memory_order_release);

        // When nothing is registered — no fds, no pending timers, no queued
        // tasks — the loop performs one non-blocking poll pass and returns,
        // so a caller that spins kith_reactor_run on an empty reactor does
        // not block indefinitely. Submitting a task or scheduling a timer
        // from another thread before the next run call resumes the loop.
        bool idle = reactor->fd_count == 0u && reactor->timer_count == 0u &&
                    atomic_load_explicit(&reactor->task_head, memory_order_acquire) == nullptr;

        int timeout_ms = idle ? 0 : run_timeout_ms(reactor, now);

        struct run_dispatch dctx = {.reactor = reactor};
        int nfds = reactor->backend.wait(reactor->backend_data, timeout_ms, run_dispatch_cb, &dctx);

        if (nfds < 0)
        {
            atomic_store_explicit(&reactor->run_in_flight, false, memory_order_release);
            atomic_store_explicit(&reactor->stop_requested, false, memory_order_release);
            return nfds;
        }

        task_queue_drain(reactor);

        now = now_monotonic_ms();
        atomic_store_explicit(&reactor->cached_time_ms, now, memory_order_release);

        timer_wheel_advance(reactor, now);

        if (idle)
        {
            break;
        }
    }

    atomic_store_explicit(&reactor->run_in_flight, false, memory_order_release);
    // The run that returns consumes the stop request: one request ends one
    // run, and a request delivered to a run that is already returning is
    // absorbed by that return, so the next run always starts with a clean
    // flag and polls fresh.
    atomic_store_explicit(&reactor->stop_requested, false, memory_order_release);
    return 0;
}

KITH_API void kith_reactor_stop(kith_reactor_t *reactor)
{
    if (reactor == nullptr)
    {
        return;
    }
    atomic_store_explicit(&reactor->stop_requested, true, memory_order_release);
    reactor->backend.wake(reactor->backend_data);
}

KITH_API uint64_t kith_reactor_now_ms(const kith_reactor_t *reactor)
{
    if (reactor == nullptr)
    {
        return 0;
    }
    return atomic_load_explicit(&reactor->cached_time_ms, memory_order_acquire);
}

[[nodiscard]] KITH_API int kith_reactor_ring_sizes(kith_reactor_t *reactor,
                                                   unsigned int *out_sq_entries,
                                                   unsigned int *out_cq_entries)
{
    if (reactor == nullptr || out_sq_entries == nullptr || out_cq_entries == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (reactor->backend.ring_sizes == nullptr)
    {
        return kith_error_return(KITH_ENOSYS);
    }
    return reactor->backend.ring_sizes(reactor->backend_data, out_sq_entries, out_cq_entries);
}

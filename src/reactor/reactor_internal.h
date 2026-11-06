#pragma once

#include <stdatomic.h>
#include <stdint.h>

#include <pthread.h>

#ifdef __linux__
#include <liburing.h>
#endif

#include "kith/reactor/reactor.h"

#ifdef __linux__
// Kernel requests carry numeric identities only. Retired requests remain
// owned by the ring until completion or ring destruction.
struct uring_fd_entry
{
    int fd;
    unsigned int events;
    uint32_t gen;
    uint32_t last_gen; // consumed at preparation, including failed submissions
};

struct uring_backend
{
    const kith_allocator_t *alloc;
    struct io_uring ring;
    int wake_fd;
    bool wake_fd_added;
    struct uring_fd_entry *fd_entries;
    unsigned int fd_capacity;

    // Registration and cancellation share these submission boundaries.
    struct io_uring_sqe *(*get_sqe)(struct io_uring *ring);
    int (*submit)(struct io_uring *ring);
};
#endif

// ---------------------------------------------------------------------------
// fd-to-callback hash map entry
// ---------------------------------------------------------------------------
struct kith_fd_entry
{
    int fd;              // -1 = free, -2 = tombstone
    unsigned int events; // current events-of-interest mask
    kith_reactor_cb cb;  // readiness callback
    void *ctx;           // callback context
};

// ---------------------------------------------------------------------------
// timer wheel node (singly-linked list per slot, sorted by deadline)
// ---------------------------------------------------------------------------
struct kith_timer_node
{
    uint64_t deadline_ms;         // absolute monotonic deadline
    kith_reactor_task_cb cb;      // callback
    void *ctx;                    // callback context
    struct kith_timer_node *next; // next in chain (within the same slot)
};

// ---------------------------------------------------------------------------
// lock-free MPSC task queue node
// ---------------------------------------------------------------------------
struct kith_task_node
{
    kith_reactor_task_cb cb;               // task callback
    void *ctx;                             // task context
    _Atomic(struct kith_task_node *) next; // next in MPSC chain
};

// ---------------------------------------------------------------------------
// platform-specific backend vtable
//
// Each function returns 0 on success or negative kith_error on failure.
// ---------------------------------------------------------------------------
struct kith_reactor_backend
{
    // Initialize the platform backend (io_uring_queue_init), routing its
    // allocations through the given allocator; the backend stores it for
    // its own teardown. Returns 0 or a negative kith_error.
    int (*init)(const kith_allocator_t *alloc, void **out_backend_data, unsigned int max_fds);
    // Destroy platform backend (io_uring_queue_exit / close).
    void (*destroy)(void *backend_data);
    // Start polling fd for events. Returns 0, -KITH_EINVAL, -KITH_EBUSY,
    // -KITH_ENOMEM, or -KITH_EIO.
    int (*add)(void *backend_data, int fd, unsigned int events);
    // Change events of interest for fd. Returns 0, -KITH_ENOENT, -KITH_EBUSY,
    // or -KITH_EIO.
    // A mod whose mask equals the fd's currently registered mask is a no-op
    // success: the backend keeps exactly one outstanding poll per registered
    // fd, and re-registering an unchanged mask must not cancel and re-issue
    // it (a second outstanding poll completes twice per event, duplicating
    // dispatches). Callers re-issue mods every tick with mostly
    // unchanged masks, so the no-op path is the hot case.
    int (*mod)(void *backend_data, int fd, unsigned int events);
    // Retire fd before attempting kernel cancellation. Pending completions
    // cannot dispatch or rearm it, including on -KITH_EIO.
    // Returns 0, -KITH_ENOENT, or -KITH_EIO.
    int (*del)(void *backend_data, int fd);
    // Wait for events. Returns 0 on timeout/interrupt, -KITH_EIO on syscall
    // failure, or >= 1 (number of ready fds). Ready fds are delivered via the
    // dispatch callback.
    int (*wait)(void *backend_data,
                int timeout_ms,
                void (*dispatch)(int fd, unsigned int events, void *ctx),
                void *dispatch_ctx);
    // Wake a blocked wait() call from another thread. Called by
    // kith_reactor_stop / kith_reactor_submit. Returns 0 or -KITH_EIO.
    int (*wake)(void *backend_data);
    // Query the backend's ring entry counts. Returns 0 or -KITH_ENOSYS on a
    // backend without ring-based submission. A NULL function pointer
    // means the backend does not support this query.
    int (*ring_sizes)(void *backend_data, unsigned int *out_sq, unsigned int *out_cq);
};

// ---------------------------------------------------------------------------
// reactor internal state
// ---------------------------------------------------------------------------
#define KITH_REACTOR_TIMER_SLOTS 256u

struct kith_reactor
{
    const kith_allocator_t *allocator; // resolved at create; owns every free

    kith_reactor_params_t params;      // creation parameters

    // fd entries: open-addressed hash map with linear probing
    struct kith_fd_entry *fd_entries; // [fd_capacity]
    unsigned int fd_capacity;         // power of 2
    unsigned int fd_count;            // active registrations

    // timer wheel: ring of singly-linked lists
    struct kith_timer_node *timer_wheel[KITH_REACTOR_TIMER_SLOTS];
    uint64_t timer_tick_ms; // current wheel cursor (ms)
    uint64_t timer_count;   // active timers
    // free list for timer nodes
    _Atomic(struct kith_timer_node *) timer_free_list;

    // lock-free MPSC task submission queue
    _Atomic(struct kith_task_node *) task_head; // consumer (reactor thread)
    _Atomic(struct kith_task_node *) task_tail; // producer (any thread)

    // Task-node free list, guarded by task_free_lock: submitting threads
    // pop under it and the drain thread pushes under it, so each recycled
    // node is handed out exactly once per recycle. The queue above stays
    // lock-free.
    struct kith_task_node *task_free_list;
    pthread_mutex_t task_free_lock;

    // stop flag + wakeup
    atomic_bool stop_requested;

    // Set by kith_reactor_run for the duration of its loop and cleared on
    // every return path before the function exits. Read by
    // kith_reactor_destroy to enforce the @thread_safety contract: a destroy
    // while a run is in flight releases the backend and node storage out
    // from under the run-loop thread, so destroy aborts instead. Guards a
    // single in-flight run; concurrent run callers stay @thread_safety
    // unsafe per the public contract.
    atomic_bool run_in_flight;

    // cached monotonic time (updated once per loop iteration)
    _Atomic uint64_t cached_time_ms;

    // platform backend
    struct kith_reactor_backend backend;
    void *backend_data; // owned by the backend
};

// ---------------------------------------------------------------------------
// internal helpers (defined in reactor.c, used by platform backends)
// ---------------------------------------------------------------------------

// Look up an fd_entry by fd; returns NULL if not present.
struct kith_fd_entry *reactor_fd_entry_lookup(struct kith_reactor *r, int fd);

// Internal platform-specific backend.
extern const struct kith_reactor_backend kith_reactor_backend_uring;

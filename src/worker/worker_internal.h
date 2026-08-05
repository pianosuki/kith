#pragma once

#include <stdatomic.h>
#include <stdint.h>

#include <pthread.h>

#include "kith/types.h"
#include "kith/worker/worker.h"

// ---------------------------------------------------------------------------
// task queue node
// ---------------------------------------------------------------------------
// The task queue is a plain mutex-protected singly-linked list. Unlike the
// reactor's lock-free MPSC task queue, the worker pool runs Python
// callbacks (microseconds to milliseconds each), so the cost of a mutex
// acquire on the hot path is negligible relative to the work it guards.
// The reactor thread never blocks on Python; nothing requires the worker
// pool to be lock-free, and a mutex-protected list
// is the correct structure for multiple concurrent consumers.
struct kith_worker_task_node
{
    kith_worker_task_cb cb;
    void *ctx;
    struct kith_worker_task_node *next; // singly-linked list link
};

// ---------------------------------------------------------------------------
// worker pool handle
// ---------------------------------------------------------------------------
struct kith_worker
{
    // The allocator resolved at create (the caller's or the default): the
    // handle, the node pool, the thread array, and every bridge and work
    // record derived from this pool are released through it.
    const kith_allocator_t *allocator;

    // Worker threads. The array is heap-allocated; the count is fixed at
    // create time.
    pthread_t *threads;
    uint32_t worker_count;

    // Task node pool. Nodes are pre-allocated at create time
    // (task_capacity of them) and recycled for the pool's lifetime.
    // The free list is guarded by queue_lock — the same mutex as the task
    // queue — so every node moves between the free list and the queue
    // under one critical section.
    struct kith_worker_task_node *nodes;     // task_capacity pre-allocated nodes
    struct kith_worker_task_node *free_head; // guarded by queue_lock

    // Task queue (mutex + condvar). head is the next node to run; tail is
    // the most recently enqueued. Workers pop head under the lock, run the
    // callback outside the lock (so other workers can dequeue concurrently),
    // then recycle the node to the free list.
    struct kith_worker_task_node *queue_head;
    struct kith_worker_task_node *queue_tail;
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_cv;

    // Shutdown. Set by kith_worker_destroy and observed by every worker's
    // drain loop under queue_lock; the worker exits after the queue is
    // empty and shutdown is set.
    _Atomic(bool) shutdown;

    // Lifetime task totals, exposed through kith_worker_tasks_submitted and
    // kith_worker_tasks_completed. Submitted counts successful
    // kith_worker_submit calls (EBUSY rejections never increment it);
    // completed counts task callbacks that returned. submitted - completed
    // is the in-flight count (queued or executing); the composition root
    // records both deltas per tick as kith_worker_tasks_submitted_total and
    // kith_worker_tasks_completed_total. Relaxed order: each is
    // an independent monotonic counter read off the hot path.
    _Atomic uint64_t tasks_submitted;
    _Atomic uint64_t tasks_completed;

    // Handed out once per worker at thread entry to build the thread's
    // comm name; arrival order, not slot order — names need uniqueness
    // only. Relaxed order: an independent monotonic counter.
    _Atomic uint32_t named_count;
};

// ---------------------------------------------------------------------------
// internal accessors
// ---------------------------------------------------------------------------
// The allocator instance resolved at create; the bridge resolves it once to
// route its own blocks through the pool's allocator.
KITH_LOCAL const kith_allocator_t *kith_worker_allocator(const struct kith_worker *pool);

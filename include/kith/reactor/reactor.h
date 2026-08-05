#ifndef KITH_REACTOR_REACTOR_H
#define KITH_REACTOR_REACTOR_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Async event loop with platform-specific backends.
 *
 * A reactor handle polls file descriptors for readiness and dispatches
 * callbacks when events arrive. It also provides inter-thread task submission
 * via a lock-free queue and a timer wheel for scheduling deferred work.
 *
 * On Linux, the reactor uses io_uring (IORING_OP_POLL_ADD for fd readiness
 * polling) via liburing, with an eventfd for cross-thread wakeup. On other
 * platforms creation fails with KITH_ENOSYS. The backend vtable keeps a
 * second event backend a swappable addition; none ships today.
 *
 * The reactor uses a readiness
 * model: it notifies callbacks when a file descriptor becomes readable or
 * writable, and the callback performs the actual I/O (read/write/accept). This
 * matches the contract with the net module, whose blocking-fd interface and
 * desired-events hint the reactor wraps.
 *
 * The reactor owns no file descriptors, connections, worker pools, or game
 * state — it manages fd registrations, task queues, and timers only.
 * The caller decides how many reactor instances to create and how to thread
 * them; each @c kith_reactor_run call runs on the calling thread and returns
 * when @c kith_reactor_stop is called or an unrecoverable error occurs.
 */

/**
 * @defgroup kith_reactor Reactor
 * @{
 */

/**
 * Default configuration values. A @c kith_reactor_params_t field set to 0
 * selects the corresponding default at create time. The underlying type is
 * fixed for ABI stability.
 */
enum kith_reactor_default : unsigned int
{
    /**
     * Default maximum tracked file descriptors (params.max_fds = 0 → this).
     *
     * Sized so the io_uring SQ ring the backend allocates rounds to a 4096-entry
     * ring (the backend requests max_fds + 16 entries, which the kernel rounds up
     * to the next power of two). A larger default crosses that boundary and
     * doubles the per-ring memory footprint. Ring pages are memcg-accounted
     * kernel memory (since Linux 5.12); under a parallel test run, several
     * concurrent default reactors can pressure the cgroup memory budget and fail
     * with ENOMEM. 2048 tracked fds covers the framework's 1000-actor scaling
     * gate with headroom; deployments tracking more connections set max_fds
     * explicitly.
     */
    KITH_REACTOR_DEFAULT_MAX_FDS = 2048u,
    /** Default task queue capacity in nodes (params.task_capacity = 0 → this). */
    KITH_REACTOR_DEFAULT_TASK_CAPACITY = 1024u,
};

/**
 * Event bits used by @c kith_reactor_add and @c kith_reactor_mod to register
 * interest, and passed to the callback when readiness is detected. The
 * underlying type is fixed.
 */
enum kith_reactor_event : unsigned int
{
    /** Poll the fd for readability (data available to read or incoming connection). */
    KITH_REACTOR_IN = 0x01u,
    /** Poll the fd for writability (send buffer space available). */
    KITH_REACTOR_OUT = 0x02u,
    /** Report hangup on the fd (peer closed or reset the connection). */
    KITH_REACTOR_HUP = 0x08u,
    /** Report error conditions on the fd. */
    KITH_REACTOR_ERR = 0x10u,
};

/**
 * Opaque reactor handle.
 *
 * @ownership callee — created by kith_reactor_create, destroyed by
 *           kith_reactor_destroy. Owns the fd→callback hash map, the
 *           platform-specific backend state, the task queue, and the
 *           timer wheel.
 */
typedef struct kith_reactor kith_reactor_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_reactor_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. A field set to 0 selects the
 * corresponding @c kith_reactor_default value. Future additive fields occupy
 * the reserved slots so the layout below stays stable across generations.
 */
struct kith_reactor_params
{
    /** Must be sizeof(kith_reactor_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Maximum number of file descriptors the reactor can track simultaneously.
     * Determines the hash map capacity for fd→callback registrations. 0 selects
     * KITH_REACTOR_DEFAULT_MAX_FDS.
     */
    uint32_t max_fds;

    /**
     * Capacity of the pre-allocated task node free list. Each call to
     * @c kith_reactor_submit consumes one node; the node is returned to the
     * free list after the task callback fires. 0 selects
     * KITH_REACTOR_DEFAULT_TASK_CAPACITY.
     */
    uint32_t task_capacity;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_reactor_params. */
typedef struct kith_reactor_params kith_reactor_params_t;

/**
 * Callback invoked by the reactor when a registered file descriptor becomes
 * ready.
 *
 * The reactor calls this function during @c kith_reactor_run after the
 * platform-specific poll returns. The @p events argument carries the
 * readiness bits that are currently active (a subset of the bits registered
 * via @c kith_reactor_add, plus possibly HUP and ERR which are always
 * reported when they occur).
 *
 * The callback is responsible for performing the actual I/O (read/write/
 * accept) on @p fd. The registration re-arms after each dispatch, so a
 * callback that leaves the fd ready is invoked again while readiness
 * persists.
 *
 * @param fd     The ready file descriptor.
 * @param events Bitwise OR of @c kith_reactor_event values that are active.
 * @param ctx    The context pointer supplied to @c kith_reactor_add.
 */
typedef void (*kith_reactor_cb)(int fd, unsigned int events, void *ctx);

/**
 * Task callback invoked by the reactor after the current poll batch drains.
 *
 * Tasks are submitted from any thread via @c kith_reactor_submit and executed
 * on the reactor thread in FIFO order. The callback runs with no file
 * descriptor context; it is used for deferred work such as processing
 * responses from a worker pool or scheduling follow-up I/O.
 *
 * @param ctx The context pointer supplied to @c kith_reactor_submit.
 */
typedef void (*kith_reactor_task_cb)(void *ctx);

/**
 * Build a reactor handle from @p params.
 *
 * @param params     Creation parameters; @c size and @c abi_version must
 *                   match the runtime generation. NULL selects all defaults.
 * @param alloc      Allocator for the handle, its fd table, the task and
 *                   timer node pools, and the platform backend state, used
 *                   again when kith_reactor_destroy frees them. NULL selects
 *                   the default allocator; a supplied allocator is validated
 *                   (see kith_allocator_t) and must outlive the handle.
 * @param out_reactor Receives the new handle on success.
 * @return           0 on success, negative kith_error on failure:
 *                   - -KITH_EINVAL if @p out_reactor is NULL, @p alloc is
 *                     missing an operation, or the platform backend rejects
 *                     its parameters,
 *                   - -KITH_EABIVER if @p params or @p alloc has an
 *                     incompatible abi_version,
 *                   - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                     size,
 *                   - -KITH_EPERM if io_uring is disabled by kernel policy,
 *                   - -KITH_ENOMEM on allocation failure,
 *                   - -KITH_EIO on other platform backend failure.
 * @thread_safety unsafe — must not race with another kith_reactor_create on
 *                the same @p out_reactor slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_reactor_destroy.
 */
[[nodiscard]] KITH_API int kith_reactor_create(const kith_reactor_params_t *params,
                                               const kith_allocator_t *alloc,
                                               kith_reactor_t **out_reactor);

/**
 * Release all resources held by @p reactor. Passing NULL is a no-op. All
 * registered file descriptors are implicitly deregistered (their ownership
 * remains with the caller and they are not closed).
 *
 * @param reactor Reactor handle. NULL is a no-op.
 * @thread_safety unsafe — no callbacks or kith_reactor_submit may be in
 *                flight, and kith_reactor_run must not be active, when this
 *                is called.
 * @ownership callee — @p reactor is consumed and freed by the call.
 */
KITH_API void kith_reactor_destroy(kith_reactor_t *reactor);

/**
 * Register interest in readiness events on @p fd. The reactor begins polling
 * @p fd for the events in @p events and calls @p cb with @p ctx when any of
 * those events become ready. HUP and ERR are always reported if they occur
 * regardless of whether they are requested in @p events.
 *
 * @p fd must be a valid open file descriptor and must not already be
 * registered with this reactor handle. The caller retains ownership of
 * @p fd; the reactor does not close it on deregistration or destroy.
 *
 * @param reactor Reactor handle.
 * @param fd      File descriptor to poll.
 * @param events  Events of interest (bitwise OR of kith_reactor_event).
 * @param cb      Callback invoked on readiness; must not be NULL.
 * @param ctx     Opaque context pointer passed to @p cb.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p reactor is NULL, @p fd is negative, or
 *                  @p cb is NULL,
 *                - -KITH_EEXIST if @p fd is already registered,
 *                - -KITH_EBUSY if the registration table is full or the
 *                  backend cannot accept another registration,
 *                - -KITH_ENOMEM on allocation failure,
 *                - -KITH_EIO on platform backend failure.
 * @thread_safety unsafe — call from the reactor thread (the thread that calls
 *                kith_reactor_run). Must not race with kith_reactor_run on
 *                the same handle.
 * @ownership caller — @p ctx is borrowed for @p fd's registration lifetime
 *           (until @c kith_reactor_del or @c kith_reactor_destroy).
 */
[[nodiscard]] KITH_API int kith_reactor_add(
    kith_reactor_t *reactor, int fd, unsigned int events, kith_reactor_cb cb, void *ctx);

/**
 * Modify the events of interest for a registered @p fd. The
 * callback and context pointer from the original registration are preserved.
 * Calling with @p events = 0 effectively pauses the fd (no events are polled)
 * but does not deregister it. Calling with a mask equal to the fd's currently
 * registered mask is a no-op that returns 0: the backend keeps one outstanding
 * readiness registration per fd, and re-issuing an unchanged mask must not
 * tear it down and re-register it (duplicate registrations duplicate event
 * delivery). Callers that re-issue mods every tick with mostly unchanged
 * masks therefore cost nothing when nothing changed.
 *
 * @param reactor Reactor handle.
 * @param fd      File descriptor whose events to modify.
 * @param events  New events of interest (bitwise OR of kith_reactor_event).
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p reactor is NULL or @p fd is negative,
 *                - -KITH_ENOENT if @p fd is not registered,
 *                - -KITH_EIO on platform backend failure.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p reactor is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_reactor_mod(kith_reactor_t *reactor, int fd, unsigned int events);

/**
 * Deregister @p fd from the reactor. The fd stops being polled; the caller
 * retains ownership and must close @p fd separately if desired.
 *
 * It is safe to call from within the fd's own readiness callback (the
 * deregistration takes effect after the current poll batch drains).
 *
 * @param reactor Reactor handle.
 * @param fd      File descriptor to deregister.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p reactor is NULL or @p fd is negative,
 *                - -KITH_ENOENT if @p fd is not registered,
 *                - -KITH_EIO on platform backend failure.
 * @thread_safety unsafe — call from the reactor thread (or from within a
 *                readiness callback for any fd on the same reactor).
 * @ownership caller — @p reactor is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_reactor_del(kith_reactor_t *reactor, int fd);

/**
 * Submit a task for execution on the reactor thread. The task's callback is
 * invoked during the next @c kith_reactor_run drain step (after the current
 * poll batch completes) in FIFO order among submitted tasks. This can be
 * called from any thread.
 *
 * The task queue is lock-free (MPSC); the submitting thread pushes a node
 * via compare-exchange, and the reactor thread dequeues the chain. If all
 * task_capacity nodes are in flight, the call returns @c KITH_EBUSY
 * immediately.
 *
 * @param reactor Reactor handle.
 * @param cb      Task callback; must not be NULL.
 * @param ctx     Opaque context pointer passed to @p cb.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p reactor is NULL or @p cb is NULL,
 *                - -KITH_EBUSY if the task queue is full (no free nodes).
 * @thread_safety safe — callable from any thread.
 * @ownership caller — @p cb and @p ctx are borrowed until the task callback
 *           fires on the reactor thread.
 */
[[nodiscard]] KITH_API int
kith_reactor_submit(kith_reactor_t *reactor, kith_reactor_task_cb cb, void *ctx);

/**
 * Schedule a one-shot callback at @p deadline_ms (absolute monotonic clock
 * value in milliseconds). The callback fires on the reactor thread after the
 * deadline passes, before the next poll batch.
 *
 * The callback runs with the same semantics as a submitted task (it can
 * register/modify/deregister fds, submit further tasks, or schedule more
 * timers). Late timers (deadline already passed) are treated as a zero-delay
 * task.
 *
 * @param reactor     Reactor handle.
 * @param deadline_ms Absolute monotonic deadline in milliseconds (from
 *                    clock_gettime(CLOCK_MONOTONIC)). Values in the past are
 *                    clamped to the current tick.
 * @param cb          Callback; must not be NULL.
 * @param ctx         Opaque context pointer passed to @p cb.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p reactor is NULL or @p cb is NULL,
 *                    - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p cb and @p ctx are borrowed until the timer callback
 *           fires on the reactor thread.
 */
[[nodiscard]] KITH_API int kith_reactor_schedule(kith_reactor_t *reactor,
                                                 uint64_t deadline_ms,
                                                 kith_reactor_task_cb cb,
                                                 void *ctx);

/**
 * Run the event loop on the calling thread. The loop:
 * 1. Polls registered file descriptors for readiness (io_uring),
 * 2. Invokes readiness callbacks for every ready fd,
 * 3. Drains submitted tasks,
 * 4. Runs expired timers,
 * 5. Repeats until @c kith_reactor_stop is called — from a callback or task
 *    inside the loop, or from another thread before or during the run — or
 *    an unrecoverable error occurs.
 *
 * This blocks the calling thread until a stop request lands on the loop; a
 * request pending before the call returns the run on the spot. If no fds
 * are registered, no timers are pending, and no tasks are queued, the loop
 * returns immediately (one idle iteration). Otherwise the poll blocks with
 * a computed timeout derived from the earliest pending timer deadline.
 *
 * @param reactor Reactor handle.
 * @return        0 if stopped cleanly via kith_reactor_stop, or negative
 *                kith_error on unrecoverable failure:
 *                - -KITH_EINVAL if @p reactor is NULL,
 *                - -KITH_EIO on a polling or syscall failure.
 * @thread_safety unsafe — only one thread may call kith_reactor_run on a
 *                given handle.
 * @ownership caller — @p reactor is borrowed for the call; the caller destroys
 *           it with kith_reactor_destroy after run returns.
 */
[[nodiscard]] KITH_API int kith_reactor_run(kith_reactor_t *reactor);

/**
 * Signal the event loop to stop. This can be called from any thread — it is
 * the mechanism for shutting down the reactor from within a callback, a task,
 * or a signal handler. The request takes effect at the next run boundary:
 * the in-flight run drains its pending poll events and tasks and returns 0,
 * and a request delivered before the next run enters returns that run on
 * its first check.
 *
 * The run that returns consumes the request, so a subsequent run polls
 * fresh. Registered fds persist (they are not deregistered); a subsequent
 * run resumes polling them.
 *
 * @param reactor Reactor handle.
 * @thread_safety safe — callable from any thread. Uses an atomic flag +
 *                platform wakeup.
 * @ownership caller — @p reactor is borrowed for the call only.
 */
KITH_API void kith_reactor_stop(kith_reactor_t *reactor);

/**
 * Return the cached monotonic time in milliseconds, as of the start of the
 * current event loop iteration. During callbacks and tasks this returns a
 * consistent snapshot; calling @c clock_gettime(CLOCK_MONOTONIC) repeatedly
 * from inside callbacks is avoided.
 *
 * Between loop iterations (or when the reactor is not running) the value
 * reflects the last cached time, which may be stale. Callers that need a
 * fresh reading outside the event loop should use clock_gettime directly.
 *
 * @param reactor Reactor handle.
 * @return        The cached monotonic time in milliseconds.
 * @thread_safety safe — returns an atomic snapshot.
 * @ownership caller — @p reactor is borrowed for the call only.
 */
KITH_API uint64_t kith_reactor_now_ms(const kith_reactor_t *reactor);

/**
 * Query the submission and completion ring entry counts of the reactor's
 * backend.
 *
 * On the Linux io_uring backend, the kernel may clamp the requested entry
 * count to its maximum (32768 SQ entries) when IORING_SETUP_CLAMP is set,
 * and rounds up to the next power of two. This accessor returns the actual
 * counts after clamping and rounding, so callers provisioning cgroup
 * memory for the ring can budget accurately.
 *
 * @param reactor       Reactor handle.
 * @param out_sq_entries Receives the submission queue entry count.
 * @param out_cq_entries Receives the completion queue entry count.
 * @return              0 on success, negative kith_error on failure:
 *                      - -KITH_EINVAL if @p reactor, @p out_sq_entries, or
 *                        @p out_cq_entries is NULL,
 *                      - -KITH_ENOSYS on a backend without ring-based
 *                        submission.
 * @thread_safety safe — reads immutable post-initialization state.
 * @ownership caller — @p reactor is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_reactor_ring_sizes(kith_reactor_t *reactor,
                                                   unsigned int *out_sq_entries,
                                                   unsigned int *out_cq_entries);

/** @} */

#endif /* KITH_REACTOR_REACTOR_H */

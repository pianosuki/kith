#ifndef KITH_WORKER_WORKER_H
#define KITH_WORKER_WORKER_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * GIL-aware worker pool for pool-bound handler dispatch.
 *
 * A worker pool is a fixed-size set of native threads consuming a bounded
 * task queue guarded by a mutex. The reactor
 * thread submits pool-bound callbacks (gateway message handlers and
 * control-plane route handlers registered through the Python facade) to
 * the pool and continues draining file-descriptor readiness without
 * ever entering the Python interpreter; the pool's worker threads run
 * the callbacks and post any reactor-side completion work back to the
 * reactor via @c kith_reactor_submit.
 *
 * The task callback is a plain C function pointer, so the pool carries
 * any C work an embedding submits: Python-bound trampolines (the boundary
 * that keeps the reactor out of the interpreter) and
 * native C handlers flagged for pool dispatch, which move
 * long-running C work off the reactor thread the same way. The pool
 * itself is interpreter-agnostic: it never enters Python and never
 * blocks on interpreter state.
 *
 * The contract: the reactor never blocks on Python and
 * never runs game or handler logic inline; the pool exists so handler work
 * has somewhere to go that is not the reactor thread.
 * The pool is sized 1 under standard Python (the GIL serializes Python
 * execution anyway, so additional workers only add contention) and sized
 * N under free-threaded Python (real parallelism, no GIL). The size is
 * a creation parameter, not a runtime tunable; the server composition
 * root derives it from the @c python_worker_count config field.
 *
 * Tasks are dequeued in FIFO order from the single queue: the worker
 * holding the queue mutex pops the head and runs the callback outside
 * the lock while the remaining workers wait on the condition variable,
 * so a single task is never executed twice. Submitting from a worker
 * callback is permitted (the callback runs outside the queue lock, so a
 * re-entrant submit takes the lock afresh).
 *
 * Signal handling: every worker thread blocks SIGINT and SIGTERM from
 * entry, so an asynchronous shutdown signal is never delivered on a pool
 * thread — a default-disposition SIGTERM landing on a worker would
 * terminate the whole process mid-task and bypass any drain. Delivery
 * belongs to the embedding application's own threads; an embedder that
 * coordinates graceful shutdown blocks those signals in its serving
 * thread and waits for them there.
 *
 * The pool owns no game state and no file descriptors; it only owns its
 * threads, the task node free list, and the queue head/tail. The caller
 * owns every @c ctx pointer passed to @c kith_worker_submit.
 */

/**
 * @defgroup kith_worker Worker pool
 * @{
 */

/**
 * Default configuration values. A @c kith_worker_params_t field set to 0
 * selects the corresponding default at create time. The underlying type
 * is fixed for ABI stability.
 */
enum kith_worker_default : unsigned int
{
    /**
     * Default worker count (params.worker_count = 0 → this). A single
     * worker is the correct default under a standard (GIL-enabled)
     * Python interpreter: the GIL serializes Python execution, so extra
     * workers only add thread-contention and memory. A free-threaded
     * interpreter (no GIL) overrides this to a larger value via the
     * server's @c python_worker_count config field.
     */
    KITH_WORKER_DEFAULT_WORKER_COUNT = 1u,
    /**
     * Default task queue capacity in nodes (params.task_capacity = 0 →
     * this). Each @c kith_worker_submit consumes one node; the node is
     * returned to the free list after the task callback fires. The
     * capacity bounds the number of in-flight (queued or executing)
     * tasks; a submit beyond this returns @c -KITH_EBUSY.
     */
    KITH_WORKER_DEFAULT_TASK_CAPACITY = 4096u,
};

/**
 * Opaque worker pool handle.
 *
 * @ownership callee — created by kith_worker_create, destroyed by
 *           kith_worker_destroy. Owns its worker threads, the task node
 *           free list, and the queue head/tail.
 */
typedef struct kith_worker kith_worker_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_worker_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. A field set to 0 selects the
 * corresponding @c kith_worker_default value. Future additive fields
 * occupy the reserved slots so the layout below stays stable across
 * generations.
 */
struct kith_worker_params
{
    /** Must be sizeof(kith_worker_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Number of worker threads. 0 selects KITH_WORKER_DEFAULT_WORKER_COUNT.
     * Values above 64 are clamped to 64.
     */
    uint32_t worker_count;

    /**
     * Task queue capacity in pre-allocated nodes. 0 selects
     * KITH_WORKER_DEFAULT_TASK_CAPACITY.
     */
    uint32_t task_capacity;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_worker_params. */
typedef struct kith_worker_params kith_worker_params_t;

/**
 * Task callback executed on a worker thread. The callback runs with no
 * reactor context; any reactor-side completion (e.g. flushing a
 * control-plane response buffer) is the callback's responsibility to
 * schedule via @c kith_reactor_submit on the borrowed reactor handle
 * the caller stashed in @p ctx.
 *
 * @param ctx The context pointer supplied to kith_worker_submit.
 */
typedef void (*kith_worker_task_cb)(void *ctx);

/**
 * Build a worker pool handle from @p params.
 *
 * Allocates the handle, the task node free list, and spawns @p
 * worker_count native threads waiting on the queue's condition variable.
 * The pool is ready to accept tasks on return.
 *
 * @param params     Creation parameters; @c size and @c abi_version must
 *                   match the runtime generation. NULL selects all
 *                   defaults.
 * @param alloc      Allocator for the handle, the task node pool, and the
 *                   thread array, used again when kith_worker_destroy frees
 *                   them. NULL selects the default allocator; a supplied
 *                   allocator is validated (see kith_allocator_t) and must
 *                   outlive the handle.
 * @param out_worker Receives the new handle on success.
 * @return           0 on success, negative kith_error on failure:
 *                   - -KITH_EINVAL if @p out_worker is NULL or @p alloc is
 *                     missing an operation,
 *                   - -KITH_EABIVER if @p params or @p alloc has an
 *                     incompatible abi_version,
 *                   - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                     size,
 *                   - -KITH_ENOMEM on allocation failure,
 *                   - -KITH_ESTATE on thread or mutex initialization
 *                     failure.
 * @thread_safety unsafe — must not race with another kith_worker_create
 *                on the same @p out_worker slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_worker_destroy.
 */
[[nodiscard]] KITH_API int kith_worker_create(const kith_worker_params_t *params,
                                              const kith_allocator_t *alloc,
                                              kith_worker_t **out_worker);

/**
 * Release all resources held by @p pool. Passing NULL is a no-op.
 *
 * Signals every worker thread to drain the remaining queued tasks and
 * exit, joins each thread, frees the task node pool and the handle. The
 * caller must ensure no @c kith_worker_submit is in flight on another
 * thread when this is called.
 *
 * @param pool Pool handle. NULL is a no-op.
 * @thread_safety unsafe — must not race with kith_worker_submit or
 *                another kith_worker_destroy on the same handle.
 * @ownership callee — @p pool is consumed and freed by the call.
 */
KITH_API void kith_worker_destroy(kith_worker_t *pool);

/**
 * Submit a task for execution on a worker thread. The task's callback is
 * invoked on the next available worker, in FIFO dequeue order. This
 * can be called from any thread, including a reactor readiness callback
 * and a worker task callback itself.
 *
 * The submitting thread takes the pool's queue mutex, pops a node from
 * the free list, and links it onto the queue tail; a waiting worker
 * dequeues it in FIFO order. If the free list is exhausted
 * (task_capacity nodes in flight), the call returns @c -KITH_EBUSY. The
 * call blocks only on the queue mutex, held for the enqueue's few
 * pointer operations; it never waits on task completion.
 *
 * @param pool Pool handle.
 * @param cb   Task callback; must not be NULL.
 * @param ctx  Opaque context pointer passed to @p cb.
 * @return     0 on success, negative kith_error on failure:
 *             - -KITH_EINVAL if @p pool is NULL or @p cb is NULL,
 *             - -KITH_EBUSY if the task queue is full (no free nodes).
 * @thread_safety safe — callable from any thread.
 * @ownership caller — @p cb and @p ctx are borrowed for the call only (passed
 *           to @p cb when the task fires on a worker thread).
 */
[[nodiscard]] KITH_API int
kith_worker_submit(kith_worker_t *pool, kith_worker_task_cb cb, void *ctx);

/**
 * Return the number of worker threads in @p pool. Returns 0 when @p pool
 * is NULL. The count is fixed at create time and does not change across
 * the pool's lifetime.
 *
 * @param pool Pool handle.
 * @return     The number of worker threads in @p pool; 0 when @p pool is NULL.
 * @thread_safety safe — the count is immutable.
 * @ownership caller — @p pool is borrowed for the call only.
 */
KITH_API uint32_t kith_worker_count(const kith_worker_t *pool);

/**
 * Return the pool's lifetime count of successfully submitted tasks.
 * A @c kith_worker_submit that returned @c -KITH_EBUSY never increments
 * this counter, so the difference with kith_worker_tasks_completed is the
 * in-flight count (queued or executing). Returns 0 when @p pool is NULL.
 *
 * @param pool Pool handle.
 * @return     The lifetime submitted-task count; 0 when @p pool is NULL.
 * @thread_safety safe — the counter is atomic.
 * @ownership caller — @p pool is borrowed for the call only.
 */
KITH_API uint64_t kith_worker_tasks_submitted(const kith_worker_t *pool);

/**
 * Return the pool's lifetime count of task callbacks that returned.
 * Lags kith_worker_tasks_submitted by the in-flight count. Returns 0 when
 * @p pool is NULL.
 *
 * @param pool Pool handle.
 * @return     The lifetime completed-task count; 0 when @p pool is NULL.
 * @thread_safety safe — the counter is atomic.
 * @ownership caller — @p pool is borrowed for the call only.
 */
KITH_API uint64_t kith_worker_tasks_completed(const kith_worker_t *pool);

/*---------------------------------------------------------------------------
 * reactor bridge
 *-------------------------------------------------------------------------*/
// A reactor registers Python-bound callbacks with the worker pool through a
// bridge. The bridge bundles a pool handle with a user callback and context.
// The reactor registers one of the hop functions below (instead of the user's
// Python callable) as its C callback; on each fire the hop submits the user
// callback to the pool from C, so the reactor thread never enters the Python
// interpreter. A worker thread runs the user callback. The gateway plane
// already submits Python-bound handlers to the pool from C
// (kith_gateway_dispatch → kith_worker_submit); the bridge gives the
// standalone reactor the same thread-boundary discipline.
//
// The hop functions have signatures structurally identical to the reactor
// callback types (kith_reactor_cb and kith_reactor_task_cb in
// kith/reactor/reactor.h). The worker module does not depend on the reactor
// module, so the ready-hop signature is written out here as
// kith_worker_ready_fn; it is binary-compatible with kith_reactor_cb and the
// caller casts the function pointer at the registration site.

/**
 * Readiness callback invoked on a worker thread by the bridge.
 *
 * Structurally identical to @c kith_reactor_cb
 * (kith/reactor/reactor.h): @c void(int, unsigned int, void *). The
 * bridge stores a pointer of this type and calls it on a worker thread
 * after the reactor-side hop captures the fd and events.
 *
 * @param fd     The ready file descriptor.
 * @param events Active readiness bits.
 * @param ctx    The context pointer supplied at bridge creation.
 */
typedef void (*kith_worker_ready_fn)(int fd, unsigned int events, void *ctx);

/**
 * Opaque bridge handle. Created by kith_worker_bridge_create_ready or
 * kith_worker_bridge_create_task, destroyed by kith_worker_bridge_destroy.
 * The caller registers the corresponding hop function with the reactor and
 * passes this handle as the hop's context.
 *
 * @ownership callee — created by the bridge create functions, destroyed by
 *           the caller with kith_worker_bridge_destroy.
 */
typedef struct kith_worker_bridge kith_worker_bridge_t;

/**
 * Build a bridge for a readiness callback (the @c kith_reactor_add path).
 *
 * The bridge and its per-fire work records allocate through @p pool's
 * allocator.
 *
 * The caller registers @c kith_worker_bridge_ready_cb with
 * @c kith_reactor_add as the readiness callback and the returned bridge as
 * its context. On each fire the hop allocates a small work record, captures
 * the fd and events, and submits it to @p pool; a worker thread calls
 * @p ready(fd, events, ctx). The hop performs one allocation per fire
 * through @p pool's allocator (matching the gateway's per-dispatch
 * allocation) and drops the callback silently on allocation failure or
 * pool saturation (@c -KITH_EBUSY): the reactor thread must not fall back
 * to inline dispatch because that would run Python on the reactor thread.
 * The pool's @c task_capacity is the tuning knob for saturation.
 *
 * @param pool  Worker pool to submit to. Must not be NULL.
 * @param ready User readiness callback. Must not be NULL. Stored by the
 *              bridge for its lifetime; the caller keeps the callable alive
 *              until @c kith_worker_bridge_destroy.
 * @param ctx   Opaque context passed to @p ready. Borrowed by the bridge.
 * @param out_bridge Receives the new handle on success.
 * @return     0 on success, negative kith_error on failure:
 *             - -KITH_EINVAL if @p pool, @p ready, or @p out_bridge is NULL,
 *             - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another bridge create on the
 *                same @p out_bridge slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_worker_bridge_destroy.
 */
[[nodiscard]] KITH_API int kith_worker_bridge_create_ready(kith_worker_t *pool,
                                                           kith_worker_ready_fn ready,
                                                           void *ctx,
                                                           kith_worker_bridge_t **out_bridge);

/**
 * Build a bridge for a task/timer callback (the
 * @c kith_reactor_schedule path).
 *
 * The bridge allocates through @p pool's allocator.
 *
 * The caller registers @c kith_worker_bridge_task_cb with
 * @c kith_reactor_schedule as the timer callback and the returned bridge as
 * its context. On each fire the hop submits @p task with @p ctx to @p pool
 * directly (no per-fire allocation); a worker thread calls @p task(ctx).
 * The hop drops the callback silently on pool saturation
 * (@c -KITH_EBUSY); the pool's @c task_capacity is the tuning knob.
 *
 * @param pool  Worker pool to submit to. Must not be NULL.
 * @param task  User task callback. Must not be NULL. Stored by the bridge
 *              for its lifetime; the caller keeps the callable alive until
 *              @c kith_worker_bridge_destroy.
 * @param ctx   Opaque context passed to @p task. Borrowed by the bridge.
 * @param out_bridge Receives the new handle on success.
 * @return     0 on success, negative kith_error on failure:
 *             - -KITH_EINVAL if @p pool, @p task, or @p out_bridge is NULL,
 *             - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another bridge create on the
 *                same @p out_bridge slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_worker_bridge_destroy.
 */
[[nodiscard]] KITH_API int kith_worker_bridge_create_task(kith_worker_t *pool,
                                                          kith_worker_task_cb task,
                                                          void *ctx,
                                                          kith_worker_bridge_t **out_bridge);

/**
 * Release a bridge. Passing NULL is a no-op. The caller must deregister
 * the hop from the reactor (via @c kith_reactor_del for a ready bridge, or
 * by letting the one-shot timer fire for a task bridge) before destroying
 * the bridge; a fire on a destroyed bridge dereferences freed memory.
 *
 * @param bridge Bridge handle. NULL is a no-op.
 * @thread_safety unsafe — no reactor fire may submit through @p bridge when
 *                this is called.
 * @ownership callee — @p bridge is consumed and freed by the call.
 */
KITH_API void kith_worker_bridge_destroy(kith_worker_bridge_t *bridge);

/**
 * Reactor readiness hop. Register with @c kith_reactor_add as the
 * @c kith_reactor_cb and pass the bridge as @p ctx. The hop ignores neither
 * @p fd nor @p events: both are captured into a work record and delivered to
 * the user callback on a worker thread.
 *
 * @param fd          The ready file descriptor (from the reactor).
 * @param events      Active readiness bits (from the reactor).
 * @param bridge_ctx  The bridge handle (as passed to @c kith_reactor_add).
 * @thread_safety safe — callable from the reactor thread.
 * @ownership caller — @p bridge_ctx is borrowed for the call; the work
 *           record allocated by this function is freed by the worker
 *           dispatch path.
 */
KITH_API void kith_worker_bridge_ready_cb(int fd, unsigned int events, void *bridge_ctx);

/**
 * Reactor task/timer hop. Register with @c kith_reactor_schedule as the
 * @c kith_reactor_task_cb and pass the bridge as @p ctx. The hop submits the
 * user task to the pool directly; no per-fire allocation is needed.
 *
 * @param bridge_ctx  The bridge handle (as passed to
 *                    @c kith_reactor_schedule).
 * @thread_safety safe — callable from the reactor thread.
 * @ownership caller — @p bridge_ctx is borrowed for the call.
 */
KITH_API void kith_worker_bridge_task_cb(void *bridge_ctx);

/** @} */

#endif /* KITH_WORKER_WORKER_H */

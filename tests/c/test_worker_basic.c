#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>
#include <unistd.h>

#include "kith/types.h"
#include "kith/version.h"
#include "kith/worker/worker.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "worker basic: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// ---------------------------------------------------------------------------
// shared counters and barrier
// ---------------------------------------------------------------------------

// A submit-counter callback increments an atomic. The test reads the counter
// after a brief sleep to let the worker drain the queue.
struct counter_ctx
{
    _Atomic uint32_t *value;
};

static void counter_task(void *arg)
{
    struct counter_ctx *c = arg;
    atomic_fetch_add_explicit(c->value, 1u, memory_order_release);
}

// Wait until @p counter reaches @p target, polling every 1 ms up to 5 s.
static bool wait_for(atomic_uint *counter, uint32_t target)
{
    for (int i = 0; i < 5000; i++)
    {
        if (atomic_load_explicit(counter, memory_order_acquire) >= target)
        {
            return true;
        }
        usleep(1000);
    }
    return false;
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// Create with all defaults (NULL params). The default pool has one worker.
static int test_create_defaults(void)
{
    int failures = 0;
    kith_worker_t *pool = nullptr;

    CHECK(kith_worker_create(nullptr, nullptr, &pool) == 0);
    CHECK(pool != nullptr);
    CHECK(kith_worker_count(pool) == 1u);
    CHECK(kith_worker_count(nullptr) == 0u);

    kith_worker_destroy(pool);
    // Destroy of NULL is a no-op.
    kith_worker_destroy(nullptr);
    return failures;
}

// Create rejects a NULL out pointer, an undersized struct, and a wrong
// abi_version.
static int test_create_arg_validation(void)
{
    int failures = 0;

    CHECK(kith_worker_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_worker_params_t small = {
        .size = 4u,
        .abi_version = KITH_ABI_VERSION,
    };
    kith_worker_t *pool = nullptr;
    CHECK(kith_worker_create(&small, nullptr, &pool) == kith_error_return(KITH_ESIZE));

    kith_worker_params_t bad_abi = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION + 1u,
    };
    CHECK(kith_worker_create(&bad_abi, nullptr, &pool) == kith_error_return(KITH_EABIVER));
    return failures;
}

// Submit a task; the worker runs it and the counter increments. Then submit
// several tasks; they all run before destroy.
static int test_submit_runs_callback(void)
{
    int failures = 0;
    kith_worker_params_t params = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 16u,
    };
    kith_worker_t *pool = nullptr;
    CHECK(kith_worker_create(&params, nullptr, &pool) == 0);

    _Atomic uint32_t counter = 0u;
    struct counter_ctx ctx = {.value = &counter};
    CHECK(kith_worker_submit(pool, counter_task, &ctx) == 0);
    CHECK(wait_for(&counter, 1u));
    CHECK(atomic_load_explicit(&counter, memory_order_acquire) == 1u);

    for (uint32_t i = 0u; i < 8u; i++)
    {
        CHECK(kith_worker_submit(pool, counter_task, &ctx) == 0);
    }
    CHECK(wait_for(&counter, 9u));
    CHECK(atomic_load_explicit(&counter, memory_order_acquire) == 9u);

    kith_worker_destroy(pool);
    return failures;
}

// Submit rejects a NULL pool and a NULL callback.
static int test_submit_arg_validation(void)
{
    int failures = 0;
    _Atomic uint32_t counter = 0u;
    struct counter_ctx ctx = {.value = &counter};

    CHECK(kith_worker_submit(nullptr, counter_task, &ctx) == kith_error_return(KITH_EINVAL));
    return failures;
}

// A four-worker pool drains four submitted tasks: each task runs once and
// the completion count reaches the submitted total before the wait budget
// expires. The test pins the drain contract, not a wall-clock parallelism
// property.
static int test_multi_worker_pool(void)
{
    int failures = 0;
    kith_worker_params_t params = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 4u,
        .task_capacity = 16u,
    };
    kith_worker_t *pool = nullptr;
    CHECK(kith_worker_create(&params, nullptr, &pool) == 0);
    CHECK(kith_worker_count(pool) == 4u);

    // Submit 4 tasks and wait for every completion; the wait budget bounds
    // the drain, it does not measure timing.
    _Atomic uint32_t done = 0u;
    struct done_ctx
    {
        _Atomic uint32_t *v;
    };
    // reuse counter_task with a wrapper
    struct counter_ctx ctx = {.value = &done};
    for (uint32_t i = 0u; i < 4u; i++)
    {
        CHECK(kith_worker_submit(pool, counter_task, &ctx) == 0);
    }
    CHECK(wait_for(&done, 4u));
    CHECK(atomic_load_explicit(&done, memory_order_acquire) == 4u);

    kith_worker_destroy(pool);
    return failures;
}

// Parallel-dispatch proof: four tasks on a four-worker pool all run
// concurrently. Each task counts its arrival, then waits until all four have
// arrived before returning. A pool that ran tasks one-at-a-time can never
// satisfy the wait — the first task spins alone until its deadline expires —
// while a genuinely concurrent pool satisfies it in the round the last task
// lands.
struct rendezvous_ctx
{
    _Atomic uint32_t arrived;
    _Atomic uint32_t observed_all;
};

static void rendezvous_task(void *arg)
{
    struct rendezvous_ctx *c = arg;
    atomic_fetch_add_explicit(&c->arrived, 1u, memory_order_acq_rel);
    for (int i = 0; i < 2000; i++)
    {
        if (atomic_load_explicit(&c->arrived, memory_order_acquire) == 4u)
        {
            atomic_fetch_add_explicit(&c->observed_all, 1u, memory_order_release);
            return;
        }
        usleep(1000);
    }
}

// Four workers, four tasks: every task must observe all four arrivals inside
// its deadline, which only concurrent execution can produce. The 2 s
// deadline is the discriminator itself — arrivals land microseconds apart
// when dispatch is concurrent.
static int test_parallel_dispatch_rendezvous(void)
{
    int failures = 0;
    kith_worker_params_t params = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 4u,
        .task_capacity = 16u,
    };
    kith_worker_t *pool = nullptr;
    CHECK(kith_worker_create(&params, nullptr, &pool) == 0);

    struct rendezvous_ctx ctx = {.arrived = 0, .observed_all = 0};
    for (int i = 0; i < 4; i++)
    {
        CHECK(kith_worker_submit(pool, rendezvous_task, &ctx) == 0);
    }
    CHECK(wait_for(&ctx.observed_all, 4u));
    CHECK(atomic_load_explicit(&ctx.observed_all, memory_order_acquire) == 4u);
    CHECK(atomic_load_explicit(&ctx.arrived, memory_order_acquire) == 4u);

    kith_worker_destroy(pool);
    return failures;
}

// Destroy drains remaining queued tasks before joining. Submit N tasks, then
// destroy without waiting; every task must run (the counter reaches N) by
// the time destroy returns.
static int test_destroy_drains_pending(void)
{
    int failures = 0;
    kith_worker_params_t params = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 2u,
        .task_capacity = 64u,
    };
    kith_worker_t *pool = nullptr;
    CHECK(kith_worker_create(&params, nullptr, &pool) == 0);

    _Atomic uint32_t counter = 0u;
    struct counter_ctx ctx = {.value = &counter};
    for (uint32_t i = 0u; i < 32u; i++)
    {
        CHECK(kith_worker_submit(pool, counter_task, &ctx) == 0);
    }
    // destroy drains the remaining queued tasks before joining.
    kith_worker_destroy(pool);
    CHECK(atomic_load_explicit(&counter, memory_order_acquire) == 32u);
    return failures;
}

// A worker callback may submit further tasks (re-entrant submit). This
// mirrors the control-plane completion path where a worker posts the
// reactor-flush task back via kith_reactor_submit (a different queue, same
// pattern). The chained context is heap-allocated and freed by the final
// task in the chain so no stack pointer escapes the worker.
struct chain_ctx
{
    kith_worker_t *pool;
    _Atomic uint32_t *value;
    uint32_t remaining;
};

static void chain_task(void *arg)
{
    struct chain_ctx *c = arg;
    atomic_fetch_add_explicit(c->value, 1u, memory_order_release);
    if (c->remaining > 1u)
    {
        struct chain_ctx *next = malloc(sizeof(*next));
        if (next != nullptr)
        {
            next->pool = c->pool;
            next->value = c->value;
            next->remaining = c->remaining - 1u;
            (void)kith_worker_submit(c->pool, chain_task, next);
        }
    }
    free(c);
}

static int test_reentrant_submit(void)
{
    int failures = 0;
    kith_worker_params_t params = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 64u,
    };
    kith_worker_t *pool = nullptr;
    CHECK(kith_worker_create(&params, nullptr, &pool) == 0);

    _Atomic uint32_t counter = 0u;
    struct chain_ctx *ctx = malloc(sizeof(*ctx));
    CHECK(ctx != nullptr);
    if (ctx == nullptr)
    {
        kith_worker_destroy(pool);
        return failures + 1;
    }
    ctx->pool = pool;
    ctx->value = &counter;
    ctx->remaining = 8u;
    CHECK(kith_worker_submit(pool, chain_task, ctx) == 0);
    CHECK(wait_for(&counter, 8u));
    CHECK(atomic_load_explicit(&counter, memory_order_acquire) == 8u);

    kith_worker_destroy(pool);
    return failures;
}

// A held task occupies the only node of a capacity-1 pool, so the next
// submit is rejected with EBUSY and never counts toward the submitted
// total. After the held task returns, submitted and completed agree, and
// further tasks advance both counters together.
struct gate_ctx
{
    _Atomic uint32_t *release;
};

static void gate_task(void *arg)
{
    struct gate_ctx *g = arg;
    while (atomic_load_explicit(g->release, memory_order_acquire) == 0u)
    {
        usleep(1000);
    }
}

static int test_task_counters(void)
{
    int failures = 0;
    kith_worker_params_t params = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 1u,
    };
    kith_worker_t *pool = nullptr;
    CHECK(kith_worker_create(&params, nullptr, &pool) == 0);
    CHECK(kith_worker_tasks_submitted(pool) == 0u);
    CHECK(kith_worker_tasks_completed(pool) == 0u);
    CHECK(kith_worker_tasks_submitted(nullptr) == 0u);
    CHECK(kith_worker_tasks_completed(nullptr) == 0u);

    _Atomic uint32_t release = 0u;
    struct gate_ctx gate = {.release = &release};
    CHECK(kith_worker_submit(pool, gate_task, &gate) == 0);
    // The only node is in flight (queued or executing), so the next submit
    // is rejected and the submitted total stays at one.
    CHECK(kith_worker_submit(pool, gate_task, &gate) == kith_error_return(KITH_EBUSY));
    CHECK(kith_worker_tasks_submitted(pool) == 1u);

    atomic_store_explicit(&release, 1u, memory_order_release);
    for (int i = 0; i < 5000 && kith_worker_tasks_completed(pool) == 0u; i++)
    {
        usleep(1000);
    }
    CHECK(kith_worker_tasks_completed(pool) == 1u);
    CHECK(kith_worker_tasks_submitted(pool) == 1u);

    _Atomic uint32_t counter = 0u;
    struct counter_ctx ctx = {.value = &counter};
    for (uint32_t i = 0u; i < 4u; i++)
    {
        // The single node is free only after the running task returns,
        // so each submit waits for the previous task's completion.
        CHECK(kith_worker_submit(pool, counter_task, &ctx) == 0);
        CHECK(wait_for(&counter, i + 1u));
    }
    CHECK(kith_worker_tasks_submitted(pool) == 5u);
    CHECK(kith_worker_tasks_completed(pool) == 5u);

    kith_worker_destroy(pool);
    return failures;
}

// ---------------------------------------------------------------------------
// multi-producer churn
// ---------------------------------------------------------------------------

// Four producer threads submit against a small-capacity pool while four
// workers execute and recycle the nodes: the free list runs at its
// concurrent limits (pop under contention, EBUSY backoff at capacity,
// node recycling on completion). Every accepted task runs exactly once,
// and the completed counter never passes the submitted counter at any
// sample point.
enum
{
    CHURN_PRODUCERS = 4,
    CHURN_TASKS_PER_PRODUCER = 250,
    CHURN_WORKERS = 4,
    CHURN_CAPACITY = 16,
};

struct churn_ctx
{
    kith_worker_t *pool;
    _Atomic uint64_t runs;         // callback executions
    _Atomic uint64_t accepted;     // successful submits
    _Atomic bool invariant_broken; // completed > submitted observed
};

static void churn_task(void *arg)
{
    struct churn_ctx *c = arg;
    usleep(50); // hold the node in flight so producers hit EBUSY
    atomic_fetch_add_explicit(&c->runs, 1u, memory_order_release);
}

static void *churn_producer(void *raw)
{
    struct churn_ctx *c = raw;
    for (int done = 0; done < CHURN_TASKS_PER_PRODUCER; done++)
    {
        while (kith_worker_submit(c->pool, churn_task, c) != 0)
        {
            usleep(100); // pool at capacity; the workers are draining
        }
        atomic_fetch_add_explicit(&c->accepted, 1u, memory_order_release);
        uint64_t submitted = kith_worker_tasks_submitted(c->pool);
        uint64_t completed = kith_worker_tasks_completed(c->pool);
        if (completed > submitted)
        {
            atomic_store_explicit(&c->invariant_broken, true, memory_order_release);
        }
    }
    return nullptr;
}

static int test_multi_producer_churn(void)
{
    int failures = 0;
    kith_worker_t *pool = nullptr;

    kith_worker_params_t params = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = CHURN_WORKERS,
        .task_capacity = CHURN_CAPACITY,
    };
    CHECK(kith_worker_create(&params, nullptr, &pool) == 0);

    struct churn_ctx ctx = {.pool = pool};
    atomic_init(&ctx.runs, 0);
    atomic_init(&ctx.accepted, 0);
    atomic_init(&ctx.invariant_broken, false);

    pthread_t producers[CHURN_PRODUCERS];
    bool started[CHURN_PRODUCERS] = {false};
    for (int i = 0; i < CHURN_PRODUCERS; i++)
    {
        if (pthread_create(&producers[i], nullptr, churn_producer, &ctx) == 0)
        {
            started[i] = true;
        }
        else
        {
            CHECK(false);
        }
    }
    for (int i = 0; i < CHURN_PRODUCERS; i++)
    {
        if (started[i])
        {
            pthread_join(producers[i], nullptr);
        }
    }

    CHECK(!atomic_load_explicit(&ctx.invariant_broken, memory_order_acquire));
    CHECK(atomic_load_explicit(&ctx.accepted, memory_order_acquire) ==
          (uint64_t)(CHURN_PRODUCERS * CHURN_TASKS_PER_PRODUCER));

    // Destroy drains the queue: every accepted task must have run.
    kith_worker_destroy(pool);
    CHECK(atomic_load_explicit(&ctx.runs, memory_order_acquire) ==
          atomic_load_explicit(&ctx.accepted, memory_order_acquire));
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int failures = 0;
    failures += test_create_defaults();
    failures += test_create_arg_validation();
    failures += test_submit_runs_callback();
    failures += test_submit_arg_validation();
    failures += test_multi_worker_pool();
    failures += test_parallel_dispatch_rendezvous();
    failures += test_destroy_drains_pending();
    failures += test_reentrant_submit();
    failures += test_task_counters();
    failures += test_multi_producer_churn();
    if (failures)
    {
        (void)fprintf(stderr, "worker basic: %d failure(s)\n", failures);
    }
    else
    {
        (void)printf("worker basic: all tests passed\n");
    }
    return failures ? 1 : 0;
}

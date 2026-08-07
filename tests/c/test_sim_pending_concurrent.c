#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <pthread.h>

#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

/* Concurrent apply/step through one tile2d model instance. The built-in
 * models' pending-input map is shared per model instance; threads applying
 * inputs for disjoint actor sets exercise its concurrent put/get path. Each
 * thread applies a constant full-deflection input per actor and asserts
 * after every step that the actor's velocity never moved backward toward
 * zero: a put lost to an unsynchronized claim of another thread's probe
 * node makes the step consume no pending input, and the physics ramps the
 * velocity toward zero (strictly decreasing once positive), so a lost put
 * surfaces as a monotonicity violation.
 *
 * The racy surface is bounded: cross-actor claims happen only while a
 * node is unused (a used node is single-owner), so one model instance
 * offers only IDS_PER_THREAD * THREADS claim opportunities. The run sweeps
 * many fresh instances to widen that surface; an unsynchronized map makes
 * violations probabilistic rather than deterministic on any single
 * instance. */

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "sim pending concurrent: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

#define THREADS         8u
#define IDS_PER_THREAD  384u
#define ITERATIONS      8u
#define MODEL_INSTANCES 16u

static atomic_int failures;

struct worker_state
{
    kith_sim_model_t *model;
    unsigned seed;
};

static void *worker(void *arg)
{
    struct worker_state *st = arg;
    const uint64_t id_base = (uint64_t)st->seed * IDS_PER_THREAD + 1u;
    int local_failures = 0;

    kith_sim_actor_t actors[IDS_PER_THREAD];
    for (size_t i = 0u; i < IDS_PER_THREAD; ++i)
    {
        actors[i].id = id_base + (uint64_t)i;
        actors[i].pos_x = 0;
        actors[i].pos_y = 0;
        actors[i].pos_z = 0;
        actors[i].vel_x = 0;
        actors[i].vel_y = 0;
        actors[i].vel_z = 0;
        actors[i].input_tick = 0;
        actors[i].flags = 0;
        actors[i].update_seq = 0;
    }

    for (size_t iter = 0u; iter < ITERATIONS; ++iter)
    {
        for (size_t i = 0u; i < IDS_PER_THREAD; ++i)
        {
            kith_sim_actor_t *actor = &actors[i];
            const kith_sim_input_t input = {
                .input_tick = (uint32_t)iter + 1u,
                .move_x = INT16_MAX,
                .move_y = 0,
                .move_z = 0,
                .flags = 0u,
            };
            if (kith_sim_model_apply_input(st->model, actor, &input) != 0)
            {
                local_failures += check_cond(false, __LINE__);
                atomic_fetch_add(&failures, local_failures);
                return nullptr;
            }
            const int64_t vel_before = actor->vel_x;
            if (kith_sim_model_step(st->model, actor, 1u, 50u) != 0)
            {
                local_failures += check_cond(false, __LINE__);
                atomic_fetch_add(&failures, local_failures);
                return nullptr;
            }
            CHECK(actor->vel_x >= vel_before);
            CHECK(actor->input_tick == input.input_tick);
            CHECK(actor->update_seq == (uint32_t)iter + 1u);
        }
    }
    atomic_fetch_add(&failures, local_failures);
    return nullptr;
}

static int run_one_instance(void)
{
    kith_sim_t *sim = nullptr;
    kith_sim_model_t *model = nullptr;

    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        (void)fprintf(stderr, "sim pending concurrent: sim create failed\n");
        return 1;
    }
    if (kith_sim_create_model(sim, "tile2d", nullptr, nullptr, &model) != 0)
    {
        (void)fprintf(stderr, "sim pending concurrent: model create failed\n");
        kith_sim_destroy(sim);
        return 1;
    }

    pthread_t threads[THREADS];
    struct worker_state states[THREADS];
    unsigned started = 0u;
    for (unsigned i = 0u; i < THREADS; ++i)
    {
        states[i].model = model;
        states[i].seed = i;
        if (pthread_create(&threads[i], nullptr, worker, &states[i]) != 0)
        {
            break;
        }
        ++started;
    }
    for (unsigned i = 0u; i < started; ++i)
    {
        (void)pthread_join(threads[i], nullptr);
    }

    kith_sim_model_destroy(model);
    kith_sim_destroy(sim);
    return 0;
}

int main(void)
{
    for (unsigned instance = 0u; instance < MODEL_INSTANCES; ++instance)
    {
        if (run_one_instance() != 0)
        {
            return 1;
        }
    }
    if (atomic_load(&failures) != 0)
    {
        (void)fprintf(stderr, "sim pending concurrent: FAILED\n");
        return 1;
    }
    return 0;
}

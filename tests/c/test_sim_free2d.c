#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "sim free2d: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Create a free2d model with default physics, apply a diagonal input, and
// step. The actor must move on both X and Y. Z stays zero (2D model).
static int test_create_step_apply_input(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "free2d", nullptr, nullptr, &m) == 0);
    CHECK(m != nullptr);

    kith_sim_actor_t actor = {0};
    actor.id = 42u;

    kith_sim_input_t in = {
        .input_tick = 1u,
        .move_x = 32767,
        .move_y = 32767,
        .move_z = 0,
        .flags = 0,
    };
    CHECK(kith_sim_model_apply_input(m, &actor, &in) == 0);
    CHECK(actor.input_tick == 1u);
    CHECK(actor.flags == 0u);

    for (int i = 0; i < 8; ++i)
    {
        CHECK(kith_sim_model_step(m, &actor, 1u, 50u) == 0);
    }
    CHECK(actor.pos_x > 0);
    CHECK(actor.pos_y > 0);
    CHECK(actor.pos_z == 0);
    CHECK(actor.vel_z == 0);

    kith_sim_model_destroy(m);
    kith_sim_destroy(sim);
    return failures;
}

// The run flag (bit 0 of input.flags) selects run_speed instead of
// base_speed. After one tick of full east input at run speed, the actor's
// X velocity must exceed the velocity produced by the same input at walk
// speed, because run_speed (64) > base_speed (32).
static int test_run_flag_uses_run_speed(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_t *walk = nullptr;
    kith_sim_model_t *run = nullptr;
    CHECK(kith_sim_create_model(sim, "free2d", nullptr, nullptr, &walk) == 0);
    CHECK(kith_sim_create_model(sim, "free2d", nullptr, nullptr, &run) == 0);

    kith_sim_actor_t aw = {0};
    aw.id = 1u;
    kith_sim_actor_t ar = {0};
    ar.id = 2u;

    kith_sim_input_t walk_in = {
        .input_tick = 1u,
        .move_x = 32767,
        .move_y = 0,
        .flags = 0u,
    };
    kith_sim_input_t run_in = {
        .input_tick = 1u,
        .move_x = 32767,
        .move_y = 0,
        .flags = 0x1u,
    };
    CHECK(kith_sim_model_apply_input(walk, &aw, &walk_in) == 0);
    CHECK(kith_sim_model_apply_input(run, &ar, &run_in) == 0);

    // Ramp-up takes several ticks; step enough for both to reach terminal
    // velocity (accel=96 per tick, target speeds 32 and 64 in Q16.16).
    for (int i = 0; i < 16; ++i)
    {
        CHECK(kith_sim_model_step(walk, &aw, 1u, 50u) == 0);
        CHECK(kith_sim_model_step(run, &ar, 1u, 50u) == 0);
    }
    CHECK(ar.pos_x > aw.pos_x);
    CHECK(ar.vel_x > aw.vel_x);

    kith_sim_model_destroy(walk);
    kith_sim_model_destroy(run);
    kith_sim_destroy(sim);
    return failures;
}

// free2d has no collision grid, so load_behavior is a no-op that returns 0
// regardless of the path (including a nonexistent file and NULL).
static int test_load_behavior_is_noop(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "free2d", nullptr, nullptr, &m) == 0);

    CHECK(kith_sim_model_load_behavior(m, "/tmp/kith_no_such_free2d_zzz.txt") == 0);
    CHECK(kith_sim_model_load_behavior(m, nullptr) == 0);

    kith_sim_model_destroy(m);
    kith_sim_destroy(sim);
    return failures;
}

// Without any applied input, an actor at rest stays at rest.
static int test_step_no_input_holds(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "free2d", nullptr, nullptr, &m) == 0);

    kith_sim_actor_t actor = {0};
    actor.id = 9u;
    for (int i = 0; i < 4; ++i)
    {
        CHECK(kith_sim_model_step(m, &actor, 1u, 50u) == 0);
    }
    CHECK(actor.pos_x == 0);
    CHECK(actor.pos_y == 0);
    CHECK(actor.vel_x == 0);
    CHECK(actor.vel_y == 0);

    kith_sim_model_destroy(m);
    kith_sim_destroy(sim);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_step_apply_input();
    rc |= test_run_flag_uses_run_speed();
    rc |= test_load_behavior_is_noop();
    rc |= test_step_no_input_holds();
    if (rc != 0)
    {
        (void)fprintf(stderr, "sim free2d tests FAILED\n");
    }
    return rc;
}

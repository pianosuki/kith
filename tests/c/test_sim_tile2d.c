#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "sim tile2d: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Write a behavior grid to a scratch file. The grid uses '#' for solid and
// any other printable char for walkable. Returns the path the caller must
// unlink.
static int write_grid_file(char *path_buf, size_t path_cap, const char *grid_text)
{
    (void)snprintf(path_buf, path_cap, "/tmp/kith_tile2d_%d.txt", (int)getpid());
    FILE *f = fopen(path_buf, "w");
    if (!f)
    {
        return kith_error_return(KITH_EIO);
    }
    size_t n = strlen(grid_text);
    size_t written = fwrite(grid_text, 1, n, f);
    int rc = (fclose(f) == 0 && written == n) ? 0 : kith_error_return(KITH_EIO);
    return rc;
}

// Create a tile2d model with default physics, apply a rightward input, and
// step. The actor must move in +X. Z stays zero (2D model).
static int test_create_step_apply_input(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "tile2d", nullptr, nullptr, &m) == 0);
    CHECK(m != nullptr);

    kith_sim_actor_t actor = {0};
    actor.id = 100u;

    kith_sim_input_t in = {
        .input_tick = 1u,
        .move_x = 32767,
        .move_y = 0,
        .move_z = 0,
        .flags = 0,
    };
    CHECK(kith_sim_model_apply_input(m, &actor, &in) == 0);
    CHECK(actor.input_tick == 1u);

    // Step a handful of ticks; velocity ramps toward base_speed, so the
    // position must increase and the velocity must become non-negative.
    for (int i = 0; i < 8; ++i)
    {
        CHECK(kith_sim_model_step(m, &actor, 1u, 50u) == 0);
    }
    CHECK(actor.pos_x > 0);
    CHECK(actor.pos_z == 0);
    CHECK(actor.vel_z == 0);

    kith_sim_model_destroy(m);
    kith_sim_destroy(sim);
    return failures;
}

// Without any applied input, an actor decelerates to a halt and its position
// does not move (starting from rest).
static int test_step_no_input_holds(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "tile2d", nullptr, nullptr, &m) == 0);

    kith_sim_actor_t actor = {0};
    actor.id = 1u;
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

// Loading a behavior grid from a text file populates the solid/walkable map.
// An actor walking east into a '#' wall is blocked on the X axis but can
// still move on the Y axis (axis-separated collision resolution).
static int test_load_behavior_and_collision(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    // Use a slow speed so the actor advances one tile per tick and cannot
    // tunnel past the wall in a single step. collision_radius 0 means the
    // collision check samples only the actor's exact tile.
    kith_sim_config_t cfg = {
        .size = sizeof(kith_sim_config_t),
        .abi_version = KITH_ABI_VERSION,
        .base_speed = 1u,
        .run_speed = 2u,
        .accel = 96u,
        .decel = 120u,
        .move_eps = 0.25f,
        .collision_radius = 0u,
    };
    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "tile2d", &cfg, nullptr, &m) == 0);

    // Grid: wall column at x=4, walkable everywhere else.
    //   ...#.
    //   ...#.
    //   ...#.
    const char *grid = "...#.\n...#.\n...#.\n";
    char path[128];
    CHECK(write_grid_file(path, sizeof(path), grid) == 0);
    CHECK(kith_sim_model_load_behavior(m, path) == 0);
    CHECK(unlink(path) == 0);

    // Place the actor at tile (0,1) and push east. The wall sits at tile
    // x=3; the actor's collision radius (1 tile) reaches into x=3, so X
    // movement is blocked at tile x=1 (1 + radius 1 = tile 2 is walkable,
    // but tile 2 + 1 = tile 3 is solid).
    kith_sim_actor_t actor = {0};
    actor.id = 7u;
    actor.pos_x = 1LL << KITH_SIM_FIX_SHIFT;
    actor.pos_y = 1LL << KITH_SIM_FIX_SHIFT;

    kith_sim_input_t east = {
        .input_tick = 1u,
        .move_x = 32767,
        .move_y = 0,
        .flags = 0,
    };
    CHECK(kith_sim_model_apply_input(m, &actor, &east) == 0);
    for (int i = 0; i < 16; ++i)
    {
        CHECK(kith_sim_model_step(m, &actor, 1u, 50u) == 0);
    }
    // The actor must not have reached the wall tile (x=3).
    CHECK(actor.pos_x < (3LL << KITH_SIM_FIX_SHIFT));
    // The actor must have advanced (not stuck at the start).
    CHECK(actor.pos_x > (1LL << KITH_SIM_FIX_SHIFT));

    // Now push north (positive Y). Y is open at the actor's X, so the actor
    // moves.
    kith_sim_input_t north = {
        .input_tick = 2u,
        .move_x = 0,
        .move_y = 32767,
        .flags = 0,
    };
    CHECK(kith_sim_model_apply_input(m, &actor, &north) == 0);
    int64_t y_before = actor.pos_y;
    for (int i = 0; i < 8; ++i)
    {
        CHECK(kith_sim_model_step(m, &actor, 1u, 50u) == 0);
    }
    CHECK(actor.pos_y > y_before);

    kith_sim_model_destroy(m);
    kith_sim_destroy(sim);
    return failures;
}

// load_behavior with a NULL path clears any already-loaded grid: after
// clearing, the wall tile is passable and the actor moves through it.
static int test_load_behavior_clear(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_config_t cfg = {
        .size = sizeof(kith_sim_config_t),
        .abi_version = KITH_ABI_VERSION,
        .base_speed = 1u,
        .run_speed = 2u,
        .accel = 96u,
        .decel = 120u,
        .move_eps = 0.25f,
        .collision_radius = 0u,
    };
    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "tile2d", &cfg, nullptr, &m) == 0);

    const char *grid = "##\n##\n";
    char path[128];
    CHECK(write_grid_file(path, sizeof(path), grid) == 0);
    CHECK(kith_sim_model_load_behavior(m, path) == 0);
    CHECK(unlink(path) == 0);

    // Clear: passing NULL drops the grid.
    CHECK(kith_sim_model_load_behavior(m, nullptr) == 0);

    // Now the actor can move freely in any direction.
    kith_sim_actor_t actor = {0};
    actor.id = 3u;
    actor.pos_x = 1LL << KITH_SIM_FIX_SHIFT;
    actor.pos_y = 1LL << KITH_SIM_FIX_SHIFT;
    kith_sim_input_t east = {
        .input_tick = 1u,
        .move_x = 32767,
        .move_y = 0,
        .flags = 0,
    };
    CHECK(kith_sim_model_apply_input(m, &actor, &east) == 0);
    int64_t x_before = actor.pos_x;
    for (int i = 0; i < 8; ++i)
    {
        CHECK(kith_sim_model_step(m, &actor, 1u, 50u) == 0);
    }
    CHECK(actor.pos_x > x_before);

    kith_sim_model_destroy(m);
    kith_sim_destroy(sim);
    return failures;
}

// Loading from a nonexistent path fails with EIO; loading an empty file
// fails with EINVAL (parse failure: zero width or height).
static int test_load_behavior_errors(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "tile2d", nullptr, nullptr, &m) == 0);

    CHECK(kith_sim_model_load_behavior(m, "/tmp/kith_no_such_grid_zzz.txt") ==
          kith_error_return(KITH_EIO));

    char path[128];
    CHECK(write_grid_file(path, sizeof(path), "") == 0);
    CHECK(kith_sim_model_load_behavior(m, path) == kith_error_return(KITH_EINVAL));
    CHECK(unlink(path) == 0);

    kith_sim_model_destroy(m);
    kith_sim_destroy(sim);
    return failures;
}

// NULL model is rejected by every model operation with EINVAL.
static int test_arg_validation(void)
{
    int failures = 0;
    CHECK(kith_sim_model_step(nullptr, nullptr, 0, 50u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_model_apply_input(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_model_load_behavior(nullptr, "x") == kith_error_return(KITH_EINVAL));
    kith_sim_model_destroy(nullptr);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_arg_validation();
    rc |= test_create_step_apply_input();
    rc |= test_step_no_input_holds();
    rc |= test_load_behavior_and_collision();
    rc |= test_load_behavior_clear();
    rc |= test_load_behavior_errors();
    if (rc != 0)
    {
        (void)fprintf(stderr, "sim tile2d tests FAILED\n");
    }
    return rc;
}

/* tile2d movement model: tile-grid physics with a text-encoded collision grid
 * ('#' solid, other walkable), fixed-point integration, and the pending-input
 * map shared with free2d. Exposes a vtable consumed by the model registry
 * (model_registry.c). */

#include <stdckdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/types.h"
#include "kith/version.h"
#include "sim/sim_internal.h"

/*---------------------------------------------------------------------------
 * tile2d model state
 *-------------------------------------------------------------------------*/

/** Default physics constants in model units (Q16.16 applied at init). */
#define TILE2D_DEFAULT_BASE_SPEED  32u
#define TILE2D_DEFAULT_RUN_SPEED   64u
#define TILE2D_DEFAULT_ACCEL       96u
#define TILE2D_DEFAULT_DECEL       120u
#define TILE2D_DEFAULT_MOVE_EPS    0.25f
#define TILE2D_DEFAULT_COLLISION_R 1u

/** Tile is one model unit; tile coordinate = pos >> KITH_SIM_FIX_SHIFT. */
#define TILE2D_GRID_ALLOC 256u

struct tile2d_state
{
    struct sim_physics phys;
    struct sim_pending_map pending;

    int64_t collision_radius_fix;

    uint8_t *grid;
    int32_t grid_w;
    int32_t grid_h;
};

/*---------------------------------------------------------------------------
 * behavior grid (text format: '#' solid, other walkable)
 *-------------------------------------------------------------------------*/

static int
tile2d_read_file(const kith_allocator_t *alloc, const char *path, char **out_buf, size_t *out_len)
{
    FILE *f = fopen(path, "r");
    if (!f)
    {
        return kith_error_return(KITH_EIO);
    }
    char *buf = nullptr;
    size_t cap = 0u;
    size_t len = 0u;
    int c = 0;
    while ((c = fgetc(f)) != EOF)
    {
        if (len == cap)
        {
            size_t new_cap = (cap == 0u) ? 512u : cap * 2u;
            char *grown = kith_realloc(alloc, buf, new_cap);
            if (!grown)
            {
                kith_free(alloc, buf);
                (void)fclose(f);
                return kith_error_return(KITH_ENOMEM);
            }
            buf = grown;
            cap = new_cap;
        }
        buf[len] = (char)c;
        len += 1u;
    }
    if (fclose(f) != 0)
    {
        kith_free(alloc, buf);
        return kith_error_return(KITH_EIO);
    }
    *out_buf = buf;
    *out_len = len;
    return 0;
}

static void tile2d_measure(const char *buf, size_t len, int32_t *out_w, int32_t *out_h)
{
    int32_t width = 0;
    int32_t cur_w = 0;
    int32_t height = 0;
    for (size_t i = 0u; i < len; ++i)
    {
        if (buf[i] == '\n')
        {
            if (cur_w > width)
            {
                width = cur_w;
            }
            cur_w = 0;
            height += 1;
        }
        else
        {
            cur_w += 1;
        }
    }
    if (cur_w > 0)
    {
        if (cur_w > width)
        {
            width = cur_w;
        }
        height += 1;
    }
    *out_w = width;
    *out_h = height;
}

static int
tile2d_grid_load(const kith_allocator_t *alloc, struct tile2d_state *st, const char *path)
{
    char *buf = nullptr;
    size_t len = 0u;
    int rc = tile2d_read_file(alloc, path, &buf, &len);
    if (rc != 0)
    {
        return rc;
    }

    kith_free(alloc, st->grid);
    st->grid = nullptr;
    st->grid_w = 0;
    st->grid_h = 0;

    int32_t width = 0;
    int32_t height = 0;
    tile2d_measure(buf, len, &width, &height);
    if (width == 0 || height == 0)
    {
        kith_free(alloc, buf);
        return kith_error_return(KITH_EINVAL);
    }

    uint8_t *grid = kith_alloc_zero(alloc, (size_t)width * (size_t)height, 1u);
    if (!grid)
    {
        kith_free(alloc, buf);
        return kith_error_return(KITH_ENOMEM);
    }
    int32_t x = 0;
    int32_t y = 0;
    for (size_t i = 0u; i < len; ++i)
    {
        if (buf[i] == '\n')
        {
            x = 0;
            y += 1;
            continue;
        }
        if (y < height && x < width)
        {
            grid[(size_t)y * (size_t)width + (size_t)x] = (buf[i] == '#') ? 1u : 0u;
        }
        x += 1;
    }
    kith_free(alloc, buf);
    st->grid = grid;
    st->grid_w = width;
    st->grid_h = height;
    return 0;
}

/*---------------------------------------------------------------------------
 * axis-separated circle-vs-tile collision
 *-------------------------------------------------------------------------*/

static bool tile2d_blocked(const struct tile2d_state *st, int64_t px, int64_t py)
{
    if (!st->grid)
    {
        return false;
    }
    int64_t r = st->collision_radius_fix;
    int32_t min_tx = (int32_t)((px - r) >> KITH_SIM_FIX_SHIFT);
    int32_t max_tx = (int32_t)((px + r) >> KITH_SIM_FIX_SHIFT);
    int32_t min_ty = (int32_t)((py - r) >> KITH_SIM_FIX_SHIFT);
    int32_t max_ty = (int32_t)((py + r) >> KITH_SIM_FIX_SHIFT);
    for (int32_t ty = min_ty; ty <= max_ty; ++ty)
    {
        if (ty < 0 || ty >= st->grid_h)
        {
            continue;
        }
        for (int32_t tx = min_tx; tx <= max_tx; ++tx)
        {
            if (tx < 0 || tx >= st->grid_w)
            {
                continue;
            }
            if (st->grid[(size_t)ty * (size_t)st->grid_w + (size_t)tx] != 0u)
            {
                return true;
            }
        }
    }
    return false;
}

// Axis-separated collision resolution. sim_physics_step already integrated the
// position; this checks each axis's new position against the grid and reverts
// the move (zeroing velocity) when the destination is blocked.
static void
tile2d_resolve(struct tile2d_state *st, kith_sim_actor_t *a, int64_t old_x, int64_t old_y)
{
    if (!st->grid)
    {
        return;
    }
    if (tile2d_blocked(st, a->pos_x, old_y))
    {
        a->pos_x = old_x;
        a->vel_x = 0;
    }
    if (tile2d_blocked(st, a->pos_x, a->pos_y))
    {
        a->pos_y = old_y;
        a->vel_y = 0;
    }
}

/*---------------------------------------------------------------------------
 * vtable operations
 *-------------------------------------------------------------------------*/

static int
tile2d_init(const kith_sim_config_t *cfg, const kith_allocator_t *alloc, kith_sim_model_t **out)
{
    *out = nullptr;
    struct tile2d_state *st = kith_alloc_zero(alloc, 1, sizeof(*st));
    if (!st)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    // A field left at 0 selects the model default (same per-field fallback
    // as free2d). A NULL cfg reads as all-zero fields, so the defaults apply.
    st->phys.base_speed_fix =
        sim_to_fix((cfg && cfg->base_speed) ? cfg->base_speed : TILE2D_DEFAULT_BASE_SPEED);
    st->phys.run_speed_fix =
        sim_to_fix((cfg && cfg->run_speed) ? cfg->run_speed : TILE2D_DEFAULT_RUN_SPEED);
    st->phys.accel_fix = sim_to_fix((cfg && cfg->accel) ? cfg->accel : TILE2D_DEFAULT_ACCEL);
    st->phys.decel_fix = sim_to_fix((cfg && cfg->decel) ? cfg->decel : TILE2D_DEFAULT_DECEL);
    if (cfg && cfg->move_eps > 0.0f)
    {
        st->phys.move_eps_fix = sim_float_to_fix(cfg->move_eps);
    }
    else
    {
        st->phys.move_eps_fix = sim_float_to_fix(TILE2D_DEFAULT_MOVE_EPS);
    }
    // collision_radius is a per-field value where 0 is meaningful (a point
    // actor samples its own tile only), so it does NOT fall back to the
    // default when set explicitly; a NULL cfg still selects the default.
    st->collision_radius_fix = sim_to_fix(cfg ? cfg->collision_radius : TILE2D_DEFAULT_COLLISION_R);

    int rc = sim_pending_init(&st->pending, KITH_SIM_DEFAULT_BUCKET_COUNT, alloc);
    if (rc != 0)
    {
        kith_free(alloc, st);
        return rc;
    }

    kith_sim_model_t *model = kith_alloc_zero(alloc, 1, sizeof(*model));
    if (!model)
    {
        sim_pending_fini(&st->pending, alloc);
        kith_free(alloc, st);
        return kith_error_return(KITH_ENOMEM);
    }
    model->state = st;
    *out = model;
    return 0;
}

static void tile2d_destroy(kith_sim_model_t *model)
{
    if (!model)
    {
        return;
    }
    const kith_allocator_t *alloc = model->allocator;
    struct tile2d_state *st = model->state;
    if (st)
    {
        sim_pending_fini(&st->pending, alloc);
        kith_free(alloc, st->grid);
        kith_free(alloc, st);
    }
    kith_free(alloc, model);
}

static int
tile2d_step(kith_sim_model_t *model, kith_sim_actor_t *actors, size_t count, uint32_t dt_ms)
{
    if (!model)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct tile2d_state *st = model->state;
    for (size_t i = 0u; i < count; ++i)
    {
        kith_sim_actor_t *a = &actors[i];
        const kith_sim_input_t *in = sim_pending_get(&st->pending, a->id);
        int64_t old_x = a->pos_x;
        int64_t old_y = a->pos_y;
        sim_physics_step(&st->phys, a, in, dt_ms);
        tile2d_resolve(st, a, old_x, old_y);
    }
    return 0;
}

static int
tile2d_apply_input(kith_sim_model_t *model, kith_sim_actor_t *actor, const kith_sim_input_t *input)
{
    if (!model || !actor || !input)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct tile2d_state *st = model->state;
    sim_pending_put(&st->pending, actor->id, input);
    actor->input_tick = input->input_tick;
    actor->flags = input->flags;
    actor->update_seq += 1u;
    return 0;
}

static int tile2d_load_behavior(kith_sim_model_t *model, const char *path)
{
    if (!model)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct tile2d_state *st = model->state;
    if (!path)
    {
        kith_free(model->allocator, st->grid);
        st->grid = nullptr;
        st->grid_w = 0;
        st->grid_h = 0;
        return 0;
    }
    return tile2d_grid_load(model->allocator, st, path);
}

static const kith_sim_model_vtable_t tile2d_vtable_storage = {
    .size = sizeof(kith_sim_model_vtable_t),
    .abi_version = KITH_ABI_VERSION,
    .init = tile2d_init,
    .destroy = tile2d_destroy,
    .step = tile2d_step,
    .apply_input = tile2d_apply_input,
    .load_behavior = tile2d_load_behavior,
    .reserved = {nullptr},
};

const kith_sim_model_vtable_t *tile2d_vtable(void)
{
    return &tile2d_vtable_storage;
}

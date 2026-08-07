/* free2d movement model: open-plane physics with no collision grid, fixed-
 * point accel and decel toward a target velocity, and the pending-input map
 * shared with tile2d. Exposes a vtable consumed by the model registry
 * (model_registry.c); the shared physics helpers live in sim_internal.h. */

#include "kith/types.h"
#include "kith/version.h"
#include "sim/sim_internal.h"

/*---------------------------------------------------------------------------
 * free2d model state (no collision grid)
 *-------------------------------------------------------------------------*/

#define FREE2D_DEFAULT_BASE_SPEED 32u
#define FREE2D_DEFAULT_RUN_SPEED  64u
#define FREE2D_DEFAULT_ACCEL      96u
#define FREE2D_DEFAULT_DECEL      120u
#define FREE2D_DEFAULT_MOVE_EPS   0.25f

struct free2d_state
{
    struct sim_physics phys;
    struct sim_pending_map pending;
};

/*---------------------------------------------------------------------------
 * vtable operations
 *-------------------------------------------------------------------------*/

static int
free2d_init(const kith_sim_config_t *cfg, const kith_allocator_t *alloc, kith_sim_model_t **out)
{
    *out = nullptr;
    struct free2d_state *st = kith_alloc_zero(alloc, 1, sizeof(*st));
    if (!st)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    // A field left at 0 selects the model default. The whole-config NULL
    // case is covered by the same per-field fallback (cfg is NULL -> each
    // field reads as 0 -> default applies), so a caller passing a zeroed
    // struct gets the defaults rather than a zero-speed model.
    st->phys.base_speed_fix =
        sim_to_fix((cfg && cfg->base_speed) ? cfg->base_speed : FREE2D_DEFAULT_BASE_SPEED);
    st->phys.run_speed_fix =
        sim_to_fix((cfg && cfg->run_speed) ? cfg->run_speed : FREE2D_DEFAULT_RUN_SPEED);
    st->phys.accel_fix = sim_to_fix((cfg && cfg->accel) ? cfg->accel : FREE2D_DEFAULT_ACCEL);
    st->phys.decel_fix = sim_to_fix((cfg && cfg->decel) ? cfg->decel : FREE2D_DEFAULT_DECEL);
    if (cfg && cfg->move_eps > 0.0f)
    {
        st->phys.move_eps_fix = sim_float_to_fix(cfg->move_eps);
    }
    else
    {
        st->phys.move_eps_fix = sim_float_to_fix(FREE2D_DEFAULT_MOVE_EPS);
    }

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

static void free2d_destroy(kith_sim_model_t *model)
{
    if (!model)
    {
        return;
    }
    const kith_allocator_t *alloc = model->allocator;
    struct free2d_state *st = model->state;
    if (st)
    {
        sim_pending_fini(&st->pending, alloc);
        kith_free(alloc, st);
    }
    kith_free(alloc, model);
}

static int
free2d_step(kith_sim_model_t *model, kith_sim_actor_t *actors, size_t count, uint32_t dt_ms)
{
    if (!model)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct free2d_state *st = model->state;
    for (size_t i = 0u; i < count; ++i)
    {
        kith_sim_actor_t *a = &actors[i];
        const kith_sim_input_t *in = sim_pending_get(&st->pending, a->id);
        sim_physics_step(&st->phys, a, in, dt_ms);
    }
    return 0;
}

static int
free2d_apply_input(kith_sim_model_t *model, kith_sim_actor_t *actor, const kith_sim_input_t *input)
{
    if (!model || !actor || !input)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct free2d_state *st = model->state;
    sim_pending_put(&st->pending, actor->id, input);
    actor->input_tick = input->input_tick;
    actor->flags = input->flags;
    actor->update_seq += 1u;
    return 0;
}

static int free2d_load_behavior(kith_sim_model_t *model, const char *path)
{
    if (!model)
    {
        return kith_error_return(KITH_EINVAL);
    }
    (void)path;
    return 0;
}

static const kith_sim_model_vtable_t free2d_vtable_storage = {
    .size = sizeof(kith_sim_model_vtable_t),
    .abi_version = KITH_ABI_VERSION,
    .init = free2d_init,
    .destroy = free2d_destroy,
    .step = free2d_step,
    .apply_input = free2d_apply_input,
    .load_behavior = free2d_load_behavior,
    .reserved = {nullptr},
};

const kith_sim_model_vtable_t *free2d_vtable(void)
{
    return &free2d_vtable_storage;
}

/* Public handle and tick loop for the sim module: params validation, create
 * and destroy, the model registry, the artifact store, and the per-tick step
 * that advances registered models. Owns artifact_store.c and model_registry.c;
 * the public contract is include/kith/sim/sim.h. */

#include <stdckdint.h>
#include <string.h>

#include <pthread.h>

#include "kith/types.h"
#include "kith/version.h"
#include "sim/sim_internal.h"

/*---------------------------------------------------------------------------
 * params / defaults
 *-------------------------------------------------------------------------*/

static void sim_resolve_params(kith_sim_params_t *out)
{
    if (out->tick_hz == 0u)
    {
        out->tick_hz = KITH_SIM_DEFAULT_TICK_HZ;
    }
    if (out->artifact_bucket_count == 0u)
    {
        out->artifact_bucket_count = KITH_SIM_DEFAULT_BUCKET_COUNT;
    }
}

static bool sim_params_validate(const kith_sim_params_t *params, kith_error_t *out_err)
{
    if (params->size < sizeof(*params))
    {
        *out_err = KITH_ESIZE;
        return false;
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        *out_err = KITH_EABIVER;
        return false;
    }
    *out_err = KITH_OK;
    return true;
}

/*---------------------------------------------------------------------------
 * sim handle lifecycle
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_sim_create(const kith_sim_params_t *params,
                                           const kith_allocator_t *alloc,
                                           kith_sim_t **out_sim)
{
    if (!out_sim)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_sim = nullptr;

    kith_sim_params_t resolved;
    if (params)
    {
        kith_error_t err = KITH_OK;
        if (!sim_params_validate(params, &err))
        {
            return kith_error_return(err);
        }
        resolved = *params;
    }
    else
    {
        memset(&resolved, 0, sizeof(resolved));
        resolved.size = sizeof(resolved);
        resolved.abi_version = KITH_ABI_VERSION;
    }
    sim_resolve_params(&resolved);

    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    kith_sim_t *sim = kith_alloc_zero(allocator, 1, sizeof(*sim));
    if (!sim)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    sim->allocator = allocator;
    sim->tick_hz = resolved.tick_hz;

    int rc = sim_registry_init(&sim->registry, allocator);
    if (rc != 0)
    {
        kith_free(allocator, sim);
        return rc;
    }
    rc = sim_registry_register_builtins(&sim->registry, allocator);
    if (rc != 0)
    {
        sim_registry_fini(&sim->registry, allocator);
        kith_free(allocator, sim);
        return rc;
    }

    rc = sim_store_create(allocator, resolved.artifact_bucket_count, &sim->store);
    if (rc != 0)
    {
        sim_registry_fini(&sim->registry, allocator);
        kith_free(allocator, sim);
        return rc;
    }

    *out_sim = sim;
    return 0;
}

KITH_API void kith_sim_destroy(kith_sim_t *sim)
{
    if (!sim)
    {
        return;
    }
    sim_store_destroy(sim->store);
    sim_registry_fini(&sim->registry, sim->allocator);
    kith_free(sim->allocator, sim);
}

/*---------------------------------------------------------------------------
 * model registry passthrough
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int
kith_sim_register_model(kith_sim_t *sim, const char *name, const kith_sim_model_vtable_t *vtable)
{
    if (!sim)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return sim_registry_register(&sim->registry, sim->allocator, name, vtable);
}

[[nodiscard]] KITH_API int kith_sim_create_model(kith_sim_t *sim,
                                                 const char *name,
                                                 const kith_sim_config_t *cfg,
                                                 const kith_allocator_t *alloc,
                                                 kith_sim_model_t **out_model)
{
    if (!out_model)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_model = nullptr;
    if (!sim || !name)
    {
        return kith_error_return(KITH_EINVAL);
    }

    const kith_sim_model_vtable_t *vt = sim_registry_find(&sim->registry, name);
    if (!vt)
    {
        return kith_error_return(KITH_ENOENT);
    }

    // When the caller passes NULL, the model selects its own defaults. The
    // init vtable callback receives NULL in that case (it already checks
    // cfg ? cfg->field : default). Constructing a zeroed struct here
    // defeats that fallback: init sees a non-NULL cfg whose base_speed
    // and friends are 0, and adopts 0 instead of the model default.
    kith_sim_config_t resolved;
    const kith_sim_config_t *cfg_passed = nullptr;
    if (cfg)
    {
        if (cfg->size < sizeof(*cfg))
        {
            return kith_error_return(KITH_ESIZE);
        }
        if (cfg->abi_version != KITH_ABI_VERSION)
        {
            return kith_error_return(KITH_EABIVER);
        }
        resolved = *cfg;
        cfg_passed = &resolved;
    }

    // The model is independent of the sim handle after creation, so a NULL
    // allocator selects the default rather than the sim handle's instance.
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    int rc = vt->init(cfg_passed, allocator, out_model);
    if (rc != 0)
    {
        return rc;
    }
    (*out_model)->vtable = *vt;
    (*out_model)->allocator = allocator;
    return 0;
}

KITH_API void kith_sim_model_destroy(kith_sim_model_t *model)
{
    if (!model)
    {
        return;
    }
    if (model->vtable.destroy)
    {
        model->vtable.destroy(model);
    }
}

[[nodiscard]] KITH_API int
kith_sim_model_step(kith_sim_model_t *model, kith_sim_actor_t *actors, size_t count, uint32_t dt_ms)
{
    if (!model)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (count > 0u && !actors)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return model->vtable.step(model, actors, count, dt_ms);
}

[[nodiscard]] KITH_API int kith_sim_model_apply_input(kith_sim_model_t *model,
                                                      kith_sim_actor_t *actor,
                                                      const kith_sim_input_t *input)
{
    if (!model || !actor || !input)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return model->vtable.apply_input(model, actor, input);
}

[[nodiscard]] KITH_API int kith_sim_model_load_behavior(kith_sim_model_t *model, const char *path)
{
    if (!model)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return model->vtable.load_behavior(model, path);
}

/*---------------------------------------------------------------------------
 * artifact store passthrough
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_sim_publish_artifact(kith_sim_t *sim,
                                                     const kith_sim_artifact_key_t *key,
                                                     const kith_sim_actor_t *actor,
                                                     uint64_t *out_seq)
{
    if (!sim || !key || !actor)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return sim_store_publish(sim->store, key, actor, out_seq);
}

[[nodiscard]] KITH_API int kith_sim_remove_artifact(kith_sim_t *sim, uint64_t actor_id)
{
    if (!sim)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return sim_store_remove_actor(sim->store, actor_id);
}

[[nodiscard]] KITH_API int kith_sim_remove_zone(kith_sim_t *sim, uint32_t zone)
{
    if (!sim)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return sim_store_remove_zone(sim->store, zone);
}

[[nodiscard]] KITH_API int kith_sim_snapshot_cell(kith_sim_t *sim,
                                                  const kith_sim_artifact_key_t *key,
                                                  kith_sim_artifact_t *out,
                                                  size_t max,
                                                  size_t *out_count)
{
    if (!sim || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return sim_store_snapshot_cell(sim->store, key, out, max, out_count);
}

[[nodiscard]] KITH_API int kith_sim_snapshot_zone_cells(kith_sim_t *sim,
                                                        uint32_t zone,
                                                        uint8_t lod,
                                                        kith_sim_cell_product_t *out,
                                                        size_t max,
                                                        size_t *out_count)
{
    if (!sim)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return sim_store_snapshot_zone_cells(sim->store, zone, lod, out, max, out_count);
}

[[nodiscard]] KITH_API int kith_sim_cell_product(kith_sim_t *sim,
                                                 const kith_sim_artifact_key_t *key,
                                                 kith_sim_cell_product_t *out_product)
{
    if (!sim || !key || !out_product)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return sim_store_cell_product(sim->store, key, out_product);
}

KITH_API uint64_t kith_sim_artifact_count(const kith_sim_t *sim)
{
    if (!sim)
    {
        return 0u;
    }
    return sim_store_count(sim->store);
}

/*---------------------------------------------------------------------------
 * shared model helpers: pending-input map + Q16.16 physics integration
 *-------------------------------------------------------------------------*/

static size_t sim_pending_mask(const struct sim_pending_map *m)
{
    return m->buckets - 1u;
}

static size_t sim_hash_u64(uint64_t k)
{
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return (size_t)k;
}

int sim_pending_init(struct sim_pending_map *m, size_t buckets, const kith_allocator_t *alloc)
{
    if (buckets == 0u)
    {
        buckets = KITH_SIM_DEFAULT_BUCKET_COUNT;
    }
    m->nodes = kith_alloc_zero(alloc, buckets, sizeof(*m->nodes));
    if (!m->nodes)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    if (pthread_mutex_init(&m->lock, nullptr) != 0)
    {
        kith_free(alloc, m->nodes);
        m->nodes = nullptr;
        m->buckets = 0u;
        return kith_error_return(KITH_ENOMEM);
    }
    m->buckets = buckets;
    return 0;
}

void sim_pending_fini(struct sim_pending_map *m, const kith_allocator_t *alloc)
{
    if (!m->nodes)
    {
        return;
    }
    pthread_mutex_destroy(&m->lock);
    kith_free(alloc, m->nodes);
    m->nodes = nullptr;
    m->buckets = 0u;
}

void sim_pending_put(struct sim_pending_map *m, uint64_t actor_id, const kith_sim_input_t *in)
{
    pthread_mutex_lock(&m->lock);
    size_t mask = sim_pending_mask(m);
    size_t i = sim_hash_u64(actor_id) & mask;
    for (size_t probe = 0u; probe < m->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct sim_pending_node *n = &m->nodes[idx];
        if (!n->used || n->actor_id == actor_id)
        {
            n->used = true;
            n->actor_id = actor_id;
            n->input = *in;
            pthread_mutex_unlock(&m->lock);
            return;
        }
    }
    pthread_mutex_unlock(&m->lock);
}

const kith_sim_input_t *sim_pending_get(const struct sim_pending_map *m, uint64_t actor_id)
{
    sim_mutex_lock(&m->lock);
    size_t mask = sim_pending_mask(m);
    size_t i = sim_hash_u64(actor_id) & mask;
    for (size_t probe = 0u; probe < m->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        const struct sim_pending_node *n = &m->nodes[idx];
        if (!n->used)
        {
            break;
        }
        if (n->actor_id == actor_id)
        {
            const kith_sim_input_t *found = &n->input;
            sim_mutex_unlock(&m->lock);
            return found;
        }
    }
    sim_mutex_unlock(&m->lock);
    return nullptr;
}

/*---------------------------------------------------------------------------
 * velocity ramp on one axis
 *-------------------------------------------------------------------------*/

static int64_t sim_ramp_axis(int64_t current, int64_t target, int64_t accel, int64_t decel)
{
    int64_t delta = target - current;
    if (delta == 0)
    {
        return current;
    }
    bool speeding_up = (sim_abs_fix(target) > sim_abs_fix(current));
    int64_t rate = speeding_up ? accel : decel;
    int64_t abs_delta = sim_abs_fix(delta);
    if (abs_delta <= rate)
    {
        return target;
    }
    return current + ((delta > 0) ? rate : -rate);
}

void sim_physics_step(struct sim_physics *p,
                      kith_sim_actor_t *actor,
                      const kith_sim_input_t *in,
                      uint32_t dt_ms)
{
    int64_t speed = (in && (in->flags & 0x1u)) ? p->run_speed_fix : p->base_speed_fix;

    int64_t target_vx = 0;
    int64_t target_vy = 0;
    if (in)
    {
        target_vx = ((int64_t)in->move_x * speed) / 32767;
        target_vy = ((int64_t)in->move_y * speed) / 32767;
    }

    actor->vel_x = sim_ramp_axis(actor->vel_x, target_vx, p->accel_fix, p->decel_fix);
    actor->vel_y = sim_ramp_axis(actor->vel_y, target_vy, p->accel_fix, p->decel_fix);

    if (sim_abs_fix(actor->vel_x) < p->move_eps_fix && sim_abs_fix(actor->vel_y) < p->move_eps_fix)
    {
        actor->vel_x = 0;
        actor->vel_y = 0;
    }

    int64_t step_vx = (actor->vel_x * (int64_t)dt_ms) / (int64_t)SIM_REF_TICK_MS;
    int64_t step_vy = (actor->vel_y * (int64_t)dt_ms) / (int64_t)SIM_REF_TICK_MS;

    int64_t new_x = 0;
    int64_t new_y = 0;
    if (ckd_add(&new_x, actor->pos_x, step_vx) || ckd_add(&new_y, actor->pos_y, step_vy))
    {
        return;
    }
    actor->pos_x = new_x;
    actor->pos_y = new_y;
}

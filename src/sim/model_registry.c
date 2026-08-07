/* Movement-model registry for the sim module: init and fini, register by name
 * with vtable size and ABI checks plus required-callback validation, and
 * find-by-name lookup. Held by the sim handle (sim.c); the built-in models
 * (models/free2d.c, models/tile2d.c) register through it. */

#include <string.h>

#include "kith/types.h"
#include "kith/version.h"
#include "sim/sim_internal.h"

/*---------------------------------------------------------------------------
 * registry lifecycle
 *-------------------------------------------------------------------------*/

int sim_registry_init(struct kith_sim_model_registry *r, const kith_allocator_t *alloc)
{
    r->cap = 8u;
    r->count = 0u;
    r->entries = kith_alloc_zero(alloc, r->cap, sizeof(*r->entries));
    if (!r->entries)
    {
        r->cap = 0u;
        return kith_error_return(KITH_ENOMEM);
    }
    return 0;
}

void sim_registry_fini(struct kith_sim_model_registry *r, const kith_allocator_t *alloc)
{
    for (size_t i = 0u; i < r->count; ++i)
    {
        kith_free(alloc, r->entries[i].name);
        r->entries[i].name = nullptr;
    }
    kith_free(alloc, r->entries);
    r->entries = nullptr;
    r->count = 0u;
    r->cap = 0u;
}

/*---------------------------------------------------------------------------
 * register / find
 *-------------------------------------------------------------------------*/

static bool sim_vtable_callbacks_present(const kith_sim_model_vtable_t *v)
{
    return v->init && v->destroy && v->step && v->apply_input && v->load_behavior;
}

int sim_registry_register(struct kith_sim_model_registry *r,
                          const kith_allocator_t *alloc,
                          const char *name,
                          const kith_sim_model_vtable_t *vtable)
{
    if (!name || name[0] == '\0' || !vtable)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (vtable->size < sizeof(*vtable))
    {
        return kith_error_return(KITH_ESIZE);
    }
    if (vtable->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (!sim_vtable_callbacks_present(vtable))
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (sim_registry_find(r, name))
    {
        return kith_error_return(KITH_EEXIST);
    }

    if (r->count == r->cap)
    {
        size_t new_cap = (r->cap == 0u) ? 8u : r->cap * 2u;
        struct sim_registry_entry *grown =
            kith_realloc(alloc, r->entries, new_cap * sizeof(*grown));
        if (!grown)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        r->entries = grown;
        r->cap = new_cap;
    }

    char *name_copy = kith_strdup(alloc, name);
    if (!name_copy)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    r->entries[r->count].name = name_copy;
    r->entries[r->count].vtable = *vtable;
    r->count += 1u;
    return 0;
}

const kith_sim_model_vtable_t *sim_registry_find(const struct kith_sim_model_registry *r,
                                                 const char *name)
{
    if (!name)
    {
        return nullptr;
    }
    for (size_t i = 0u; i < r->count; ++i)
    {
        if (strcmp(r->entries[i].name, name) == 0)
        {
            return &r->entries[i].vtable;
        }
    }
    return nullptr;
}

/*---------------------------------------------------------------------------
 * built-in model registration
 *-------------------------------------------------------------------------*/

int sim_registry_register_builtins(struct kith_sim_model_registry *r, const kith_allocator_t *alloc)
{
    int rc = sim_registry_register(r, alloc, "tile2d", tile2d_vtable());
    if (rc != 0)
    {
        return rc;
    }
    return sim_registry_register(r, alloc, "free2d", free2d_vtable());
}

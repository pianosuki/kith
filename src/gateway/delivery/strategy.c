/* Delivery-strategy registry for the gateway: name-keyed table of copied
 * vtables, populated with the built-in presets at gateway creation and
 * extended through the public registration call. Conventions mirror the
 * sim model registry: EINVAL for null or
 * empty arguments, ESIZE for an undersized vtable, EABIVER for an
 * incompatible generation or missing required callback, EEXIST for a
 * duplicate name, ENOMEM on allocation failure, and ENOENT at lookup
 * time for an unknown name. The registry is reactor-thread-only and not
 * synchronized. */

#include "gateway/delivery/strategy.h"

#include <stdlib.h>
#include <string.h>

#include "gateway/delivery/strategy_tiered.h"
#include "kith/types.h"
#include "kith/version.h"

int gateway_delivery_registry_init(struct gateway_delivery_registry *r,
                                   const kith_allocator_t *alloc)
{
    r->allocator = alloc;
    r->entries = nullptr;
    r->count = 0u;
    r->cap = 0u;
    return 0;
}

void gateway_delivery_registry_fini(struct gateway_delivery_registry *r)
{
    for (size_t i = 0u; i < r->count; ++i)
    {
        kith_free(r->allocator, r->entries[i].name);
    }
    kith_free(r->allocator, r->entries);
    r->entries = nullptr;
    r->count = 0u;
    r->cap = 0u;
}

const kith_gateway_delivery_vtable_t *
gateway_delivery_registry_find(const struct gateway_delivery_registry *r, const char *name)
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

int gateway_delivery_registry_register(struct gateway_delivery_registry *r,
                                       const char *name,
                                       const kith_gateway_delivery_vtable_t *vtable)
{
    if (!r || !name || !vtable || name[0] == '\0')
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (vtable->size < sizeof(kith_gateway_delivery_vtable_t))
    {
        return kith_error_return(KITH_ESIZE);
    }
    if (vtable->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (!vtable->session_init || !vtable->deliver)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (gateway_delivery_registry_find(r, name))
    {
        return kith_error_return(KITH_EEXIST);
    }

    if (r->count == r->cap)
    {
        size_t new_cap = (r->cap == 0u) ? 4u : r->cap * 2u;
        struct gateway_delivery_entry *grown =
            kith_realloc(r->allocator, r->entries, new_cap * sizeof(*grown));
        if (!grown)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        r->entries = grown;
        r->cap = new_cap;
    }

    char *copied = kith_strdup(r->allocator, name);
    if (!copied)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    r->entries[r->count].name = copied;
    r->entries[r->count].vtable = *vtable;
    r->count += 1u;
    return 0;
}

int gateway_delivery_builtins_register(struct gateway_delivery_registry *r)
{
    int rc = gateway_delivery_registry_register(r, "full", gateway_delivery_full_vtable());
    if (rc != 0)
    {
        return rc;
    }
    return gateway_delivery_registry_register(r, "tiered", gateway_delivery_tiered_vtable());
}

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"
#include "sim/sim_internal.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "sim registry: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// A no-op model vtable used only to exercise the registry's own validation
// paths. The callbacks never run because these tests never instantiate the
// model; they only register and look it up by name.
static int
noop_init(const kith_sim_config_t *cfg, const kith_allocator_t *alloc, kith_sim_model_t **out)
{
    (void)cfg;
    (void)alloc;
    *out = nullptr;
    return kith_error_return(KITH_EINVAL);
}

static void noop_destroy(kith_sim_model_t *model)
{
    (void)model;
}

static int
noop_step(kith_sim_model_t *model, kith_sim_actor_t *actors, size_t count, uint32_t dt_ms)
{
    (void)model;
    (void)actors;
    (void)count;
    (void)dt_ms;
    return kith_error_return(KITH_EINVAL);
}

static int
noop_apply_input(kith_sim_model_t *model, kith_sim_actor_t *actor, const kith_sim_input_t *input)
{
    (void)model;
    (void)actor;
    (void)input;
    return kith_error_return(KITH_EINVAL);
}

static int noop_load_behavior(kith_sim_model_t *model, const char *path)
{
    (void)model;
    (void)path;
    return kith_error_return(KITH_EINVAL);
}

static kith_sim_model_vtable_t make_valid_vtable(void)
{
    kith_sim_model_vtable_t v = {
        .size = sizeof(kith_sim_model_vtable_t),
        .abi_version = KITH_ABI_VERSION,
        .init = noop_init,
        .destroy = noop_destroy,
        .step = noop_step,
        .apply_input = noop_apply_input,
        .load_behavior = noop_load_behavior,
    };
    return v;
}

// kith_sim_create auto-registers the two built-in models. Instantiating each
// by name must succeed and yield a non-null model the caller must destroy.
static int test_builtins_auto_registered(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    CHECK(sim != nullptr);

    kith_sim_model_t *tile = nullptr;
    CHECK(kith_sim_create_model(sim, "tile2d", nullptr, nullptr, &tile) == 0);
    CHECK(tile != nullptr);
    kith_sim_model_destroy(tile);

    kith_sim_model_t *free_m = nullptr;
    CHECK(kith_sim_create_model(sim, "free2d", nullptr, nullptr, &free_m) == 0);
    CHECK(free_m != nullptr);
    kith_sim_model_destroy(free_m);

    kith_sim_destroy(sim);
    return failures;
}

// A custom model: init allocates a trivial model struct, destroy frees it.
// Exercises the register-then-create path end-to-end. The framework
// fills vtable and allocator on the returned handle after init returns; the
// destroy callback reads the stored allocator back for its free.
static int
custom_init(const kith_sim_config_t *cfg, const kith_allocator_t *alloc, kith_sim_model_t **out)
{
    (void)cfg;
    kith_sim_model_t *m = kith_alloc_zero(alloc, 1, sizeof(*m));
    if (!m)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    *out = m;
    return 0;
}

static void custom_destroy(kith_sim_model_t *model)
{
    kith_free(model->allocator, model);
}

// Registering a fresh name and then instantiating it must succeed. The
// registry copies the vtable and the name string; freeing the name right
// after the call is safe.
static int test_register_then_create(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_vtable_t v = make_valid_vtable();
    v.init = custom_init;
    v.destroy = custom_destroy;

    char name[] = "custom_model";
    CHECK(kith_sim_register_model(sim, name, &v) == 0);
    // Reusing the same name must be rejected with EEXIST.
    CHECK(kith_sim_register_model(sim, name, &v) == kith_error_return(KITH_EEXIST));

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "custom_model", nullptr, nullptr, &m) == 0);
    CHECK(m != nullptr);
    kith_sim_model_destroy(m);

    kith_sim_destroy(sim);
    return failures;
}

// Looking up a name that was never registered returns ENOENT. An empty name
// is rejected at the register boundary with EINVAL.
static int test_lookup_and_reject_empty(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(sim, "never_registered", nullptr, nullptr, &m) ==
          kith_error_return(KITH_ENOENT));
    CHECK(m == nullptr);

    kith_sim_model_vtable_t v = make_valid_vtable();
    CHECK(kith_sim_register_model(sim, "", &v) == kith_error_return(KITH_EINVAL));

    kith_sim_destroy(sim);
    return failures;
}

// A vtable whose size is undersized is rejected with ESIZE before the
// abi_version or callback checks run. A vtable whose abi_version does not
// match the runtime generation is rejected at registration with EABIVER. A
// vtable missing a required callback is also rejected.
static int test_register_rejects_invalid_vtable(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_model_vtable_t bad_size = make_valid_vtable();
    bad_size.size = 4u;
    CHECK(kith_sim_register_model(sim, "bad_size", &bad_size) == kith_error_return(KITH_ESIZE));

    kith_sim_model_vtable_t bad_abi = make_valid_vtable();
    bad_abi.abi_version = KITH_ABI_VERSION
    +1u;
    CHECK(kith_sim_register_model(sim, "bad_abi", &bad_abi) == kith_error_return(KITH_EABIVER));

    kith_sim_model_vtable_t missing_step = make_valid_vtable();
    missing_step.step = nullptr;
    CHECK(kith_sim_register_model(sim, "missing_step", &missing_step) ==
          kith_error_return(KITH_EABIVER));

    kith_sim_model_vtable_t missing_init = make_valid_vtable();
    missing_init.init = nullptr;
    CHECK(kith_sim_register_model(sim, "missing_init", &missing_init) ==
          kith_error_return(KITH_EABIVER));

    kith_sim_destroy(sim);
    return failures;
}

// NULL-handle validation: every public registry entry point rejects a NULL
// sim with EINVAL rather than dereferencing.
static int test_null_handle_rejected(void)
{
    int failures = 0;
    kith_sim_model_vtable_t v = make_valid_vtable();
    CHECK(kith_sim_register_model(nullptr, "x", &v) == kith_error_return(KITH_EINVAL));

    kith_sim_model_t *m = nullptr;
    CHECK(kith_sim_create_model(nullptr, "tile2d", nullptr, nullptr, &m) ==
          kith_error_return(KITH_EINVAL));
    CHECK(m == nullptr);

    kith_sim_destroy(nullptr);
    return failures;
}

// kith_sim_create itself validates the out-pointer and the size/abi_version
// of an explicit params struct, matching the pattern of the other libraries.
static int test_create_validation(void)
{
    int failures = 0;
    CHECK(kith_sim_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_sim_params_t bad_abi = {
        .size = sizeof(kith_sim_params_t),
        .abi_version = KITH_ABI_VERSION + 1u,
    };
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(&bad_abi, nullptr, &sim) == kith_error_return(KITH_EABIVER));
    CHECK(sim == nullptr);

    kith_sim_params_t small = {
        .size = 8u,
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_sim_create(&small, nullptr, &sim) == kith_error_return(KITH_ESIZE));
    CHECK(sim == nullptr);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_validation();
    rc |= test_builtins_auto_registered();
    rc |= test_register_then_create();
    rc |= test_lookup_and_reject_empty();
    rc |= test_register_rejects_invalid_vtable();
    rc |= test_null_handle_rejected();
    if (rc != 0)
    {
        (void)fprintf(stderr, "sim registry tests FAILED\n");
    }
    return rc;
}

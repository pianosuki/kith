/* Public-only model construction and allocator ownership regression. */
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "kith/sim/sim.h"
#include "kith/types.h"

struct allocation_state
{
    size_t calls;
    size_t fail_at;
    size_t live;
};

static void *test_alloc(void *ctx, size_t size)
{
    struct allocation_state *state = ctx;
    if (++state->calls == state->fail_at)
    {
        return nullptr;
    }
    void *p = malloc(size);
    state->live += p != nullptr;
    return p;
}

static void *test_zero(void *ctx, size_t count, size_t size)
{
    struct allocation_state *state = ctx;
    if (++state->calls == state->fail_at)
    {
        return nullptr;
    }
    void *p = calloc(count, size);
    state->live += p != nullptr;
    return p;
}

static void *test_realloc(void *ctx, void *ptr, size_t size)
{
    (void)ctx;
    return realloc(ptr, size);
}

static void test_free(void *ctx, void *ptr)
{
    struct allocation_state *state = ctx;
    state->live -= ptr != nullptr;
    free(ptr);
}

static int
model_init(const kith_sim_config_t *cfg, const kith_allocator_t *alloc, kith_sim_model_t **out)
{
    (void)cfg;
    return kith_sim_model_storage_create(sizeof(uint64_t), alloc, out);
}

static int
model_step(kith_sim_model_t *model, kith_sim_actor_t *actors, size_t count, uint32_t dt_ms)
{
    uint64_t *elapsed = kith_sim_model_state(model);
    *elapsed += dt_ms;
    for (size_t i = 0; i < count; ++i)
    {
        actors[i].pos_x = (int64_t)*elapsed;
    }
    return 0;
}

static int
model_input(kith_sim_model_t *model, kith_sim_actor_t *actor, const kith_sim_input_t *input)
{
    (void)model;
    actor->input_tick = input->input_tick;
    return 0;
}

static int model_load(kith_sim_model_t *model, const char *path)
{
    (void)model;
    (void)path;
    return 0;
}

static int check(bool condition, int line)
{
    if (!condition)
    {
        (void)fprintf(stderr, "sim storage: assertion failed at %d\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(condition) failures += check((condition), __LINE__)

static int test_storage_allocation(void)
{
    int failures = 0;
    struct allocation_state state = {0};
    kith_allocator_t alloc = {.size = sizeof(alloc),
                              .abi_version = KITH_ABI_VERSION,
                              .alloc = test_alloc,
                              .alloc_zero = test_zero,
                              .realloc = test_realloc,
                              .free = test_free,
                              .user_data = &state};
    kith_sim_model_t *model = nullptr;
    CHECK(kith_sim_model_storage_create(1, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    for (size_t fail_at = 1; fail_at <= 2; ++fail_at)
    {
        state.calls = 0;
        state.fail_at = fail_at;
        CHECK(kith_sim_model_storage_create(128, &alloc, &model) == kith_error_return(KITH_ENOMEM));
        CHECK(model == nullptr && state.live == 0);
    }
    state.fail_at = 0;
    CHECK(kith_sim_model_storage_create(sizeof(max_align_t), &alloc, &model) == 0);
    unsigned char *bytes = kith_sim_model_state(model);
    CHECK((uintptr_t)bytes % alignof(max_align_t) == 0);
    for (size_t i = 0; i < sizeof(max_align_t); ++i)
    {
        CHECK(bytes[i] == 0);
    }
    kith_sim_model_storage_destroy(model);
    CHECK(state.live == 0);
    CHECK(kith_sim_model_storage_create(0, &alloc, &model) == 0);
    CHECK(kith_sim_model_state(model) == nullptr);
    kith_sim_model_storage_destroy(model);
    CHECK(state.live == 0);
    alloc.abi_version = KITH_ABI_VERSION
    +1;
    CHECK(kith_sim_model_storage_create(1, &alloc, &model) == kith_error_return(KITH_EABIVER));
    CHECK(model == nullptr);
    alloc.abi_version = KITH_ABI_VERSION;
    alloc.size = 0;
    CHECK(kith_sim_model_storage_create(1, &alloc, &model) == kith_error_return(KITH_ESIZE));
    CHECK(model == nullptr);
    alloc.size = sizeof(alloc);
    alloc.alloc = nullptr;
    CHECK(kith_sim_model_storage_create(1, &alloc, &model) == kith_error_return(KITH_EINVAL));
    CHECK(model == nullptr);
    kith_sim_model_storage_destroy(nullptr);
    CHECK(kith_sim_model_state(nullptr) == nullptr);
    return failures;
}

static int test_external_model(void)
{
    int failures = 0;
    struct allocation_state state = {0};
    kith_allocator_t alloc = {.size = sizeof(alloc),
                              .abi_version = KITH_ABI_VERSION,
                              .alloc = test_alloc,
                              .alloc_zero = test_zero,
                              .realloc = test_realloc,
                              .free = test_free,
                              .user_data = &state};
    kith_sim_model_vtable_t vt = {.size = sizeof(vt),
                                  .abi_version = KITH_ABI_VERSION,
                                  .init = model_init,
                                  .destroy = kith_sim_model_storage_destroy,
                                  .step = model_step,
                                  .apply_input = model_input,
                                  .load_behavior = model_load};
    kith_sim_t *sim = nullptr;
    kith_sim_model_t *model = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    CHECK(kith_sim_register_model(sim, "external", &vt) == 0);
    state.fail_at = 2;
    CHECK(kith_sim_create_model(sim, "external", nullptr, &alloc, &model) ==
          kith_error_return(KITH_ENOMEM));
    CHECK(model == nullptr && state.live == 0);
    state.fail_at = 0;
    CHECK(kith_sim_create_model(sim, "external", nullptr, &alloc, &model) == 0);
    kith_sim_destroy(sim);
    kith_sim_actor_t actor = {.id = 1};
    CHECK(kith_sim_model_step(model, &actor, 1, 17) == 0);
    CHECK(kith_sim_model_step(model, &actor, 1, 16) == 0);
    CHECK(actor.pos_x == 33);
    kith_sim_model_destroy(model);
    CHECK(state.live == 0);
    return failures;
}

int main(void)
{
    return test_storage_allocation() + test_external_model() != 0;
}

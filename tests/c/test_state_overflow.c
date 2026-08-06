/* The state command path rejects overflowed key and value length
 * arithmetic synchronously, before any allocation, submission, or
 * connection work — so the pins run without a live Redis, where the rest
 * of the state suite's behavioral assertions live. */

#include <stdint.h>
#include <stdio.h>

#include "kith/reactor/reactor.h"
#include "kith/state/state.h"
#include "kith/types.h"
#include "kith/version.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond_ext(bool ok, int line, int rc)
{
    if (!ok)
    {
        (void)fprintf(stderr, "state overflow: assertion at line %d failed (rc=%d)\n", line, rc);
        return 1;
    }
    return 0;
}

#define CHECK(cond)             failures += check_cond_ext((cond), __LINE__, 0)
#define CHECK_RC(cond, rc_expr) failures += check_cond_ext((cond), __LINE__, (rc_expr))

static void unused_reply(kith_state_reply_t *reply)
{
    (void)reply;
}

// Lifecycle-only reactor (never pumped), mirroring the create suite.
static int state_create_reactor(kith_reactor_t **out)
{
    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = 1u,
        .max_fds = 64,
        .task_capacity = 64,
    };
    return kith_reactor_create(&params, nullptr, out);
}

static kith_state_params_t state_params(const char *prefix, uint32_t prefix_len)
{
    kith_state_params_t params = {
        .size = sizeof(params),
        .abi_version = KITH_ABI_VERSION,
        .host = nullptr,
        .port = 0,
        .timeout_ms = 0,
        .key_prefix = prefix,
        .key_prefix_len = prefix_len,
        .reserved = {nullptr},
    };
    return params;
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// The prefixed key length overflows: a 2-byte prefix plus SIZE_MAX - 1
// wraps the first addition, before the allocation is even computed.
static int test_prefix_key_overflow(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK_RC(state_create_reactor(&reactor) == 0, 0);
    kith_state_params_t params = state_params("ab", 2u);
    CHECK_RC(kith_state_create(&params, reactor, nullptr, &state) == 0, 0);

    CHECK(kith_state_set(state, "k", SIZE_MAX - 1u, "v", 1u, unused_reply, nullptr) ==
          -(int)KITH_EOVERFLOW);
    CHECK(kith_state_get(state, "k", SIZE_MAX - 1u, unused_reply, nullptr) == -(int)KITH_EOVERFLOW);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// Without a prefix the header plus key plus value addition overflows:
// SIZE_MAX of key leaves no room for the command header.
static int test_block_size_overflow(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK_RC(state_create_reactor(&reactor) == 0, 0);
    kith_state_params_t params = state_params(nullptr, 0u);
    CHECK_RC(kith_state_create(&params, reactor, nullptr, &state) == 0, 0);

    CHECK(kith_state_set(state, "k", SIZE_MAX, "v", 1u, unused_reply, nullptr) ==
          -(int)KITH_EOVERFLOW);
    CHECK(kith_state_del(state, "k", SIZE_MAX, unused_reply, nullptr) == -(int)KITH_EOVERFLOW);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_prefix_key_overflow();
    failures += test_block_size_overflow();
    if (failures)
    {
        (void)fprintf(stderr, "state overflow: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

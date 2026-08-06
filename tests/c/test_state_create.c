#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
        (void)fprintf(stderr, "state create: assertion at line %d failed (rc=%d)\n", line, rc);
        return 1;
    }
    return 0;
}

#define CHECK(cond)             failures += check_cond_ext((cond), __LINE__, 0)
#define CHECK_RC(cond, rc_expr) failures += check_cond_ext((cond), __LINE__, (rc_expr))

// Create a reactor with a small io_uring ring. These are lifecycle tests that
// never pump the event loop, so a 64-fd ring is plenty and avoids pressuring
// the cgroup memory budget under ctest sequencing.
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

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// Create with valid params and a reactor.
static int test_create_valid(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    int rc = state_create_reactor(&reactor);
    CHECK_RC(rc == 0, rc);
    CHECK(reactor != nullptr);

    kith_state_params_t params = {
        .size = sizeof(params),
        .abi_version = KITH_ABI_VERSION,
        .host = nullptr,
        .port = 0,
        .timeout_ms = 0,
        .key_prefix = nullptr,
        .key_prefix_len = 0,
        .reserved = {nullptr},
    };

    rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK_RC(rc == 0, rc);
    CHECK(state != nullptr);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// Create with NULL params.
static int test_create_null_params(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_create_reactor(&reactor) == 0);

    int rc = kith_state_create(nullptr, reactor, nullptr, &state);
    CHECK(rc == -(int)KITH_EINVAL);
    CHECK(state == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

// Create with NULL reactor.
static int test_create_null_reactor(void)
{
    int failures = 0;
    kith_state_t *state = nullptr;

    kith_state_params_t params = {
        .size = sizeof(params),
        .abi_version = KITH_ABI_VERSION,
        .host = nullptr,
        .port = 0,
        .timeout_ms = 0,
        .key_prefix = nullptr,
        .key_prefix_len = 0,
        .reserved = {nullptr},
    };

    int rc = kith_state_create(&params, nullptr, nullptr, &state);
    CHECK(rc == -(int)KITH_EINVAL);
    CHECK(state == nullptr);

    return failures;
}

// Create with NULL out_state.
static int test_create_null_out(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    CHECK(state_create_reactor(&reactor) == 0);

    kith_state_params_t params = {
        .size = sizeof(params),
        .abi_version = KITH_ABI_VERSION,
        .host = nullptr,
        .port = 0,
        .timeout_ms = 0,
        .key_prefix = nullptr,
        .key_prefix_len = 0,
        .reserved = {nullptr},
    };

    int rc = kith_state_create(&params, reactor, nullptr, nullptr);
    CHECK(rc == -(int)KITH_EINVAL);

    kith_reactor_destroy(reactor);
    return failures;
}

// Create with invalid abi_version.
static int test_create_bad_abi(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_create_reactor(&reactor) == 0);

    kith_state_params_t params = {
        .size = sizeof(params),
        .abi_version = 999u,
        .host = nullptr,
        .port = 0,
        .timeout_ms = 0,
        .key_prefix = nullptr,
        .key_prefix_len = 0,
        .reserved = {nullptr},
    };

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == -(int)KITH_EABIVER);
    CHECK(state == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

// Create with undersized struct.
static int test_create_undersized(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_create_reactor(&reactor) == 0);

    kith_state_params_t params = {
        .size = sizeof(uint32_t) * 2,
        .abi_version = KITH_ABI_VERSION,
        .host = nullptr,
        .port = 0,
        .timeout_ms = 0,
        .key_prefix = nullptr,
        .key_prefix_len = 0,
        .reserved = {nullptr},
    };

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == -(int)KITH_ESIZE);
    CHECK(state == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

// Destroy NULL is a no-op.
static int test_destroy_null(void)
{
    int failures = 0;
    kith_state_destroy(nullptr);
    return failures;
}

// Create with custom host and port.
static int test_create_custom_host(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_create_reactor(&reactor) == 0);

    kith_state_params_t params = {
        .size = sizeof(params),
        .abi_version = KITH_ABI_VERSION,
        .host = "127.0.0.1",
        .port = 6379,
        .timeout_ms = 3000,
        .key_prefix = nullptr,
        .key_prefix_len = 0,
        .reserved = {nullptr},
    };

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);
    CHECK(state != nullptr);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// Create with key prefix.
static int test_create_with_prefix(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_create_reactor(&reactor) == 0);

    kith_state_params_t params = {
        .size = sizeof(params),
        .abi_version = KITH_ABI_VERSION,
        .host = nullptr,
        .port = 0,
        .timeout_ms = 0,
        .key_prefix = "test:",
        .key_prefix_len = 5,
        .reserved = {nullptr},
    };

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);
    CHECK(state != nullptr);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int failures = 0;
    failures += test_create_valid();
    failures += test_create_null_params();
    failures += test_create_null_reactor();
    failures += test_create_null_out();
    failures += test_create_bad_abi();
    failures += test_create_undersized();
    failures += test_destroy_null();
    failures += test_create_custom_host();
    failures += test_create_with_prefix();
    if (failures)
    {
        (void)fprintf(stderr, "state create: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

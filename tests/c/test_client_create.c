#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/client/client.h"
#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"

/*---------------------------------------------------------------------------
 * test helpers
 *-------------------------------------------------------------------------*/

static int check_cond_ext(bool ok, int line, int rc)
{
    if (!ok)
    {
        (void)fprintf(stderr, "client create: assertion at line %d failed (rc=%d)\n", line, rc);
        return 1;
    }
    return 0;
}

#define CHECK(cond)             failures += check_cond_ext((cond), __LINE__, 0)
#define CHECK_RC(cond, rc_expr) failures += check_cond_ext((cond), __LINE__, (rc_expr))

static int create_proto(kith_proto_t **out)
{
    kith_proto_params_t params = {
        .size = sizeof(kith_proto_params_t),
        .abi_version = KITH_ABI_VERSION,
    };
    return kith_proto_create(&params, nullptr, out);
}

static int create_client(kith_proto_t *proto, kith_client_t **out)
{
    kith_client_params_t params = {
        .size = sizeof(kith_client_params_t),
        .abi_version = KITH_ABI_VERSION,
    };
    return kith_client_create(&params, proto, nullptr, nullptr, out);
}

/*---------------------------------------------------------------------------
 * tests
 *-------------------------------------------------------------------------*/

static int test_create_valid(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK_RC(create_proto(&proto) == 0, 0);
    CHECK(proto != nullptr);

    int rc = create_client(proto, &client);
    CHECK_RC(rc == 0, rc);
    CHECK(client != nullptr);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_create_null_params_defaults(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);

    int rc = kith_client_create(nullptr, proto, nullptr, nullptr, &client);
    CHECK_RC(rc == 0, rc);
    CHECK(client != nullptr);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_create_null_proto(void)
{
    int failures = 0;
    kith_client_t *client = nullptr;

    kith_client_params_t params = {
        .size = sizeof(kith_client_params_t),
        .abi_version = KITH_ABI_VERSION,
    };
    int rc = kith_client_create(&params, nullptr, nullptr, nullptr, &client);
    CHECK(rc == -(int)KITH_EINVAL);
    CHECK(client == nullptr);
    return failures;
}

static int test_create_null_out(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;

    CHECK(create_proto(&proto) == 0);

    kith_client_params_t params = {
        .size = sizeof(kith_client_params_t),
        .abi_version = KITH_ABI_VERSION,
    };
    int rc = kith_client_create(&params, proto, nullptr, nullptr, nullptr);
    CHECK(rc == -(int)KITH_EINVAL);

    kith_proto_destroy(proto);
    return failures;
}

static int test_create_bad_abi(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);

    kith_client_params_t params = {
        .size = sizeof(kith_client_params_t),
        .abi_version = 999u,
    };
    int rc = kith_client_create(&params, proto, nullptr, nullptr, &client);
    CHECK(rc == -(int)KITH_EABIVER);
    CHECK(client == nullptr);

    kith_proto_destroy(proto);
    return failures;
}

static int test_create_undersized(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);

    kith_client_params_t params = {
        .size = sizeof(uint32_t) * 2u,
        .abi_version = KITH_ABI_VERSION,
    };
    int rc = kith_client_create(&params, proto, nullptr, nullptr, &client);
    CHECK(rc == -(int)KITH_ESIZE);
    CHECK(client == nullptr);

    kith_proto_destroy(proto);
    return failures;
}

static int test_destroy_null(void)
{
    int failures = 0;
    kith_client_destroy(nullptr);
    return failures;
}

static int test_create_custom_params(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);

    kith_client_params_t params = {
        .size = sizeof(kith_client_params_t),
        .abi_version = KITH_ABI_VERSION,
        .outbound_cap = 16,
        .event_cap = 32,
        .ping_type_id = 100,
        .pong_type_id = 101,
        .ping_interval_ms = 500,
    };
    int rc = kith_client_create(&params, proto, nullptr, nullptr, &client);
    CHECK_RC(rc == 0, rc);
    CHECK(client != nullptr);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_configure_bootstrap(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_bootstrap_step_t steps[2] = {0};
    steps[0].await_type_id = 200;
    steps[1].await_type_id = 201;

    int rc = kith_client_configure_bootstrap(client, steps, 2);
    CHECK(rc == 0);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_configure_bootstrap_bad_args(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    CHECK(kith_client_configure_bootstrap(nullptr, nullptr, 0) == -(int)KITH_EINVAL);
    CHECK(kith_client_configure_bootstrap(client, nullptr, 1) == -(int)KITH_EINVAL);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_configure_bootstrap_twice(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 200;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);
    CHECK(kith_client_configure_bootstrap(client, steps, 1) == -(int)KITH_ESTATE);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_bootstrap_status_idle(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_bootstrap_status_t status = {
        .size = sizeof(status),
        .abi_version = KITH_ABI_VERSION,
    };
    int rc = kith_client_bootstrap_status(client, &status);
    CHECK_RC(rc == 0, rc);
    CHECK(status.state == KITH_CLIENT_BOOTSTRAP_IDLE);
    CHECK(status.step_count == 0);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_bootstrap_status_bad_abi(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_bootstrap_status_t status = {
        .size = sizeof(status),
        .abi_version = 999u,
    };
    CHECK(kith_client_bootstrap_status(client, &status) == -(int)KITH_EABIVER);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_set_frame_handler(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_set_frame_handler(client, nullptr, nullptr);
    kith_client_set_frame_handler(nullptr, nullptr, nullptr);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * test runner
 *-------------------------------------------------------------------------*/

int main(void)
{
    int failures = 0;
    failures += test_create_valid();
    failures += test_create_null_params_defaults();
    failures += test_create_null_proto();
    failures += test_create_null_out();
    failures += test_create_bad_abi();
    failures += test_create_undersized();
    failures += test_destroy_null();
    failures += test_create_custom_params();
    failures += test_configure_bootstrap();
    failures += test_configure_bootstrap_bad_args();
    failures += test_configure_bootstrap_twice();
    failures += test_bootstrap_status_idle();
    failures += test_bootstrap_status_bad_abi();
    failures += test_set_frame_handler();
    if (failures)
    {
        (void)fprintf(stderr, "client create: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

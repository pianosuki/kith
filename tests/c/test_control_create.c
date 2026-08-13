#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "control/control_internal.h"
#include "kith/control/control.h"
#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond_ext(bool ok, int line, int rc)
{
    if (!ok)
    {
        (void)fprintf(stderr, "control create: assertion at line %d failed (rc=%d)\n", line, rc);
        return 1;
    }
    return 0;
}

#define CHECK(cond)             failures += check_cond_ext((cond), __LINE__, 0)
#define CHECK_RC(cond, rc_expr) failures += check_cond_ext((cond), __LINE__, (rc_expr))

static int create_reactor(kith_reactor_t **out)
{
    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 64,
        .task_capacity = 64,
    };
    return kith_reactor_create(&params, nullptr, out);
}

static void fill_params(kith_control_params_t *params)
{
    memset(params, 0, sizeof(*params));
    params->size = sizeof(*params);
    params->abi_version = KITH_ABI_VERSION;
}

// ---------------------------------------------------------------------------
// lifecycle tests
// ---------------------------------------------------------------------------

static int test_create_valid(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK_RC(create_reactor(&reactor) == 0, 0);
    CHECK(reactor != nullptr);

    kith_control_params_t params;
    fill_params(&params);

    int rc = kith_control_create(&params, reactor, nullptr, nullptr, nullptr, &ctrl);
    CHECK_RC(rc == 0, rc);
    CHECK(ctrl != nullptr);

    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_null_params_defaults(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);

    int rc = kith_control_create(nullptr, reactor, nullptr, nullptr, nullptr, &ctrl);
    CHECK_RC(rc == 0, rc);
    CHECK(ctrl != nullptr);

    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_null_reactor(void)
{
    int failures = 0;
    kith_control_t *ctrl = nullptr;

    kith_control_params_t params;
    fill_params(&params);

    int rc = kith_control_create(&params, nullptr, nullptr, nullptr, nullptr, &ctrl);
    CHECK(rc == -(int)KITH_EINVAL);
    CHECK(ctrl == nullptr);

    return failures;
}

static int test_create_null_out(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    CHECK(create_reactor(&reactor) == 0);

    kith_control_params_t params;
    fill_params(&params);

    int rc = kith_control_create(&params, reactor, nullptr, nullptr, nullptr, nullptr);
    CHECK(rc == -(int)KITH_EINVAL);

    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_bad_abi(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);

    kith_control_params_t params;
    fill_params(&params);
    params.abi_version = 999u;

    int rc = kith_control_create(&params, reactor, nullptr, nullptr, nullptr, &ctrl);
    CHECK(rc == -(int)KITH_EABIVER);
    CHECK(ctrl == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_undersized(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);

    kith_control_params_t params;
    fill_params(&params);
    params.size = sizeof(uint32_t) * 2;

    int rc = kith_control_create(&params, reactor, nullptr, nullptr, nullptr, &ctrl);
    CHECK(rc == -(int)KITH_ESIZE);
    CHECK(ctrl == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

static int test_destroy_null(void)
{
    int failures = 0;
    kith_control_destroy(nullptr);
    return failures;
}

static int test_create_custom_params(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);

    kith_control_params_t params;
    fill_params(&params);
    params.host = "127.0.0.1";
    params.port = 18080;
    params.max_connections = 16;
    params.read_buffer_cap = 8192;
    params.write_buffer_cap = 16384;

    int rc = kith_control_create(&params, reactor, nullptr, nullptr, nullptr, &ctrl);
    CHECK(rc == 0);
    CHECK(ctrl != nullptr);

    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// route registry tests
// ---------------------------------------------------------------------------

static int
dummy_handler(const kith_control_request_t *req, kith_control_response_t *resp, void *ctx)
{
    (void)req;
    (void)resp;
    (void)ctx;
    return 0;
}

static int test_register_route(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    kith_control_route_t route = {
        .method = "GET",
        .path = "/api/v1/test",
        .handler = dummy_handler,
        .ctx = nullptr,
    };
    int rc = kith_control_register_route(ctrl, &route);
    CHECK(rc == 0);

    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_register_route_bad_args(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    kith_control_route_t route = {
        .method = "GET",
        .path = "/test",
        .handler = dummy_handler,
        .ctx = nullptr,
    };

    CHECK(kith_control_register_route(nullptr, &route) == -(int)KITH_EINVAL);
    CHECK(kith_control_register_route(ctrl, nullptr) == -(int)KITH_EINVAL);
    route.handler = nullptr;
    CHECK(kith_control_register_route(ctrl, &route) == -(int)KITH_EINVAL);

    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// start/stop tests (no reactor run — just bind + close)
// ---------------------------------------------------------------------------

static int test_start_stop(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
                  , .port = 18081,
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    int rc = kith_control_start(ctrl);
    CHECK_RC(rc == 0, rc);

    kith_control_stop(ctrl);

    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_start_null(void)
{
    int failures = 0;
    int rc = kith_control_start(nullptr);
    CHECK(rc == -(int)KITH_EINVAL);
    return failures;
}

static int test_stop_null(void)
{
    int failures = 0;
    kith_control_stop(nullptr);
    return failures;
}

// A handle that was never started reports port 0; after start with an
// ephemeral port (0), the bound port is positive and stable across reads.
// NULL reports 0.
static int test_listen_port(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
                  , .port = 0,
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    CHECK(kith_control_listen_port(nullptr) == 0u);
    // Not started yet: no bound listener.
    CHECK(kith_control_listen_port(ctrl) == 0u);

    CHECK(kith_control_start(ctrl) == 0);
    uint16_t port = kith_control_listen_port(ctrl);
    CHECK(port > 0u);
    // Stable across reads (the listener fd and bound port are fixed).
    CHECK(kith_control_listen_port(ctrl) == port);

    kith_control_stop(ctrl);
    CHECK(kith_control_listen_port(ctrl) == 0u);

    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// listener bind address
// ---------------------------------------------------------------------------
// control_listener_create binds the resolved params.host. NULL selects the
// loopback address (the plane serves unauthenticated HTTP), an explicit
// numeric address binds that address, and an unresolvable name fails the
// create. The bound address is read back through getsockname.

static int test_listener_default_loopback(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    int fd = control_listener_create(ctrl);
    CHECK(fd >= 0);

    struct sockaddr_in bound = {0};
    socklen_t len = sizeof(bound);
    CHECK(getsockname(fd, (struct sockaddr *)&bound, &len) == 0);
    CHECK(bound.sin_addr.s_addr == htonl(INADDR_LOOPBACK));

    close(fd);
    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_listener_explicit_wildcard(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
                  , .host = "0.0.0.0"
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    int fd = control_listener_create(ctrl);
    CHECK(fd >= 0);

    struct sockaddr_in bound = {0};
    socklen_t len = sizeof(bound);
    CHECK(getsockname(fd, (struct sockaddr *)&bound, &len) == 0);
    CHECK(bound.sin_addr.s_addr == htonl(INADDR_ANY));

    close(fd);
    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_listener_explicit_numeric(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
                  , .host = "127.0.0.2"
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    int fd = control_listener_create(ctrl);
    CHECK(fd >= 0);

    struct sockaddr_in bound = {0};
    socklen_t len = sizeof(bound);
    CHECK(getsockname(fd, (struct sockaddr *)&bound, &len) == 0);
    CHECK(bound.sin_addr.s_addr == htonl(0x7F000002u));

    close(fd);
    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_listener_hostname(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
                  , .host = "localhost"
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    int fd = control_listener_create(ctrl);
    CHECK(fd >= 0);

    struct sockaddr_in bound = {0};
    socklen_t len = sizeof(bound);
    CHECK(getsockname(fd, (struct sockaddr *)&bound, &len) == 0);
    CHECK(bound.sin_addr.s_addr == htonl(INADDR_LOOPBACK));

    close(fd);
    kith_control_destroy(ctrl);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_listener_unresolvable_host(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_control_t *ctrl = nullptr;

    CHECK(create_reactor(&reactor) == 0);
    CHECK(kith_control_create(
              &(kith_control_params_t) {
                  .size = sizeof(kith_control_params_t), .abi_version = KITH_ABI_VERSION
                  , .host = "bogus.invalid"
              },
              reactor,
              nullptr,
              nullptr,
              nullptr,
              &ctrl) == 0);

    CHECK(control_listener_create(ctrl) < 0);

    kith_control_destroy(ctrl);
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
    failures += test_create_null_params_defaults();
    failures += test_create_null_reactor();
    failures += test_create_null_out();
    failures += test_create_bad_abi();
    failures += test_create_undersized();
    failures += test_destroy_null();
    failures += test_create_custom_params();
    failures += test_register_route();
    failures += test_register_route_bad_args();
    failures += test_start_stop();
    failures += test_start_null();
    failures += test_stop_null();
    failures += test_listen_port();
    failures += test_listener_default_loopback();
    failures += test_listener_explicit_wildcard();
    failures += test_listener_explicit_numeric();
    failures += test_listener_hostname();
    failures += test_listener_unresolvable_host();
    if (failures)
    {
        (void)fprintf(stderr, "control create: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

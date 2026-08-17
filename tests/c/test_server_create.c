#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "kith/gateway/gateway.h"
#include "kith/server/server.h"
#include "kith/types.h"
#include "kith/version.h"
#include "server/server_internal.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "server create: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// The host charges io_uring ring memory against a bounded budget and
// reclaims it asynchronously after each server is destroyed, so a scenario
// loop that creates servers back-to-back can transiently exhaust that
// budget (KITH_EIO); bounded backoff rides out the reclaim window without
// masking persistent failures. On a nonzero return the out parameter is
// untouched and must not be dereferenced.
static int create_server_retry(const kith_server_params_t *params, kith_server_t **out)
{
    for (unsigned int attempt = 0u; attempt < 120u; attempt++)
    {
        int rc = kith_server_create(params, nullptr, out);
        if (rc != kith_error_return(KITH_EIO))
        {
            return rc;
        }
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 50L * 1000 * 1000};
        (void)nanosleep(&pause, nullptr);
    }
    return kith_error_return(KITH_EIO);
}

// A NULL out slot is rejected with EINVAL. NULL config selects all defaults
// (distributed topology, an OS-assigned ephemeral listen port, default tick
// rate, no per-plane config source) and builds a working handle in the
// CREATED state. destroy (NULL) is a no-op.
static int test_create_arg_validation(void)
{
    int failures = 0;
    CHECK(kith_server_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(nullptr, nullptr, &s) == 0);
    CHECK(s != nullptr);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_CREATED);

    kith_server_destroy(s);
    kith_server_destroy(nullptr);
    return failures;
}

// Params are size-versioned: an incompatible abi_version is rejected with
// EABIVER and an undersized size with ESIZE. A params struct with every field
// zero (topology 0 selects distributed, port 0 selects an OS-assigned
// ephemeral port) builds a working handle. Explicit topology and instance_id
// are honored without error.
static int test_create_params_validation(void)
{
    int failures = 0;
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);
    CHECK(s != nullptr);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_CREATED);
    kith_server_destroy(s);

    // Wrong abi_version.
    params.abi_version = KITH_ABI_VERSION
    +1u;
    s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == kith_error_return(KITH_EABIVER));
    CHECK(s == nullptr);

    // Undersized size.
    params.abi_version = KITH_ABI_VERSION;
    params.size = sizeof(params) - 1u;
    s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == kith_error_return(KITH_ESIZE));
    CHECK(s == nullptr);

    // Explicit embedded topology and instance_id build a working handle.
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.topology = KITH_SERVER_TOPOLOGY_EMBEDDED;
    params.instance_id = 7u;
    s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);
    CHECK(s != nullptr);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_CREATED);
    kith_server_destroy(s);
    return failures;
}

// Both topologies wire every plane handle and reach the CREATED state. The
// embedded topology uses a NULL coord bus (the coord owns every cell); the
// distributed topology builds a loopback bus. Both destroy cleanly in reverse
// creation order.
static int test_create_topology(void)
{
    int failures = 0;
    for (unsigned int i = 0u; i < 2u; ++i)
    {
        kith_server_params_t params = {0};
        params.size = sizeof(params);
        params.abi_version = KITH_ABI_VERSION;
        params.topology =
            (i == 0u) ? KITH_SERVER_TOPOLOGY_DISTRIBUTED : KITH_SERVER_TOPOLOGY_EMBEDDED;

        kith_server_t *s = nullptr;
        CHECK(create_server_retry(&params, &s) == 0);
        CHECK(s != nullptr);
        CHECK(kith_server_status(s) == KITH_SERVER_STATUS_CREATED);
        kith_server_destroy(s);
    }
    return failures;
}

// status on a NULL handle returns CREATED (the not-yet-created state) rather
// than crashing. A freshly-created handle reports CREATED.
static int test_status(void)
{
    int failures = 0;
    CHECK(kith_server_status(nullptr) == KITH_SERVER_STATUS_CREATED);

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(nullptr, nullptr, &s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_CREATED);
    kith_server_destroy(s);
    return failures;
}

// kith_server_listen_port returns the actual bound port: 0 for a NULL handle
// or an unbound listener, a nonzero OS-assigned port when the config
// listen_port is 0 (ephemeral binding). The value is fixed once the handle
// reaches CREATED. No fixed port is used, so the test does not contend with
// the other server tests under parallel ctest.
static int test_listen_port(void)
{
    int failures = 0;
    CHECK(kith_server_listen_port(nullptr) == 0u);

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(nullptr, nullptr, &s) == 0);
    CHECK(kith_server_listen_port(s) != 0u);
    kith_server_destroy(s);
    return failures;
}

// The delivery strategy name and configuration image are copied at create
// time: the caller may release both as soon as kith_server_create returns
// (the Python facade passes temporaries). A size-versioned image whose
// declared length is 0 or beyond the sanity ceiling is rejected with EINVAL
// before any plane is created; a well-formed image builds a working handle.
static int test_create_delivery_params(void)
{
    int failures = 0;

    kith_gateway_tiered_config_t cfg = {0};
    cfg.size = sizeof(cfg);
    cfg.abi_version = KITH_ABI_VERSION;

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.topology = KITH_SERVER_TOPOLOGY_EMBEDDED;
    params.delivery_strategy = "tiered";
    params.delivery_config = &cfg;

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);
    CHECK(s != nullptr);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_CREATED);
    kith_server_destroy(s);

    // A declared length of 0 fails the sanity range.
    kith_gateway_tiered_config_t undeclared = {0};
    undeclared.abi_version = KITH_ABI_VERSION;
    params.delivery_config = &undeclared;
    s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == kith_error_return(KITH_EINVAL));
    CHECK(s == nullptr);

    // A declared length above the sanity ceiling fails too; the copy path
    // validates the declaration before trusting it as a read extent.
    kith_gateway_tiered_config_t oversize = {0};
    oversize.size = 65537u;
    oversize.abi_version = KITH_ABI_VERSION;
    params.delivery_config = &oversize;
    s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == kith_error_return(KITH_EINVAL));
    CHECK(s == nullptr);
    return failures;
}

// ---------------------------------------------------------------------------
// listener bind address
// ---------------------------------------------------------------------------
// The wiring passes params.listen_host to the transport listener. NULL binds
// the wildcard address (the resolver's first passive result, either family),
// a numeric address binds that address, "localhost" resolves to a loopback
// address of whichever family the resolver returns first, and an
// unresolvable name fails the create. The bound address is read back through
// getsockname on the net handle's listener fd.

static bool server_bound_addr(kith_server_t *s, struct sockaddr_storage *ss)
{
    if (s == nullptr || s->net == nullptr)
    {
        return false;
    }
    socklen_t len = sizeof(*ss);
    int fd = kith_net_listener_fd(s->net);
    return fd >= 0 && getsockname(fd, (struct sockaddr *)ss, &len) == 0;
}

static bool addr_is_wildcard(const struct sockaddr_storage *ss)
{
    if (ss->ss_family == AF_INET)
    {
        const struct sockaddr_in *in = (const struct sockaddr_in *)ss;
        return in->sin_addr.s_addr == htonl(INADDR_ANY);
    }
    if (ss->ss_family == AF_INET6)
    {
        const struct sockaddr_in6 *in6 = (const struct sockaddr_in6 *)ss;
        return memcmp(&in6->sin6_addr, &in6addr_any, sizeof(in6addr_any)) == 0;
    }
    return false;
}

static bool addr_is_loopback(const struct sockaddr_storage *ss)
{
    if (ss->ss_family == AF_INET)
    {
        const struct sockaddr_in *in = (const struct sockaddr_in *)ss;
        return in->sin_addr.s_addr == htonl(INADDR_LOOPBACK);
    }
    if (ss->ss_family == AF_INET6)
    {
        const struct sockaddr_in6 *in6 = (const struct sockaddr_in6 *)ss;
        return memcmp(&in6->sin6_addr, &in6addr_loopback, sizeof(in6addr_loopback)) == 0;
    }
    return false;
}

static int test_listener_default_wildcard(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(kith_server_create(nullptr, nullptr, &s) == 0);
    CHECK(s != nullptr);

    struct sockaddr_storage ss = {0};
    CHECK(server_bound_addr(s, &ss));
    CHECK(addr_is_wildcard(&ss));

    kith_server_destroy(s);
    return failures;
}

static int test_listener_pinned_numeric(void)
{
    int failures = 0;
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.listen_host = "127.0.0.1";

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);

    struct sockaddr_storage ss = {0};
    CHECK(server_bound_addr(s, &ss));
    CHECK(ss.ss_family == AF_INET);
    CHECK(addr_is_loopback(&ss));

    kith_server_destroy(s);
    return failures;
}

static int test_listener_hostname_loopback(void)
{
    int failures = 0;
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.listen_host = "localhost";

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);

    struct sockaddr_storage ss = {0};
    CHECK(server_bound_addr(s, &ss));
    CHECK(addr_is_loopback(&ss));

    kith_server_destroy(s);
    return failures;
}

static int test_listener_unresolvable_host(void)
{
    int failures = 0;
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.listen_host = "bogus.invalid";

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == kith_error_return(KITH_EIO));
    CHECK(s == nullptr);
    return failures;
}

// ---------------------------------------------------------------------------
// config-source keys
// ---------------------------------------------------------------------------
// The wiring reads the tick_hz, listen_port, and listen_host keys from the
// borrowed config source for fields the caller leaves unset. An explicit
// params field wins over the key, an absent key leaves the built-in default,
// and a present key that fails its parse or range check fails the create.
// The key's value is the field's value, so an empty listen_host value takes
// the same unset path as a NULL params field (the wildcard bind).

static char *write_config_file(const char *body)
{
    char path[] = "/tmp/kith-server-cfg-XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0)
    {
        return nullptr;
    }
    size_t len = strlen(body);
    if (write(fd, body, len) != (ssize_t)len)
    {
        (void)close(fd);
        (void)unlink(path);
        return nullptr;
    }
    (void)close(fd);
    return strdup(path);
}

static int config_from_body(const char *body, kith_config_t **out)
{
    char *path = write_config_file(body);
    if (path == nullptr)
    {
        return -1;
    }
    kith_config_params_t cp = {0};
    cp.size = sizeof(cp);
    cp.abi_version = KITH_ABI_VERSION;
    cp.file_path = path;
    int rc = kith_config_create(&cp, nullptr, out);
    (void)unlink(path);
    free(path);
    return rc;
}

static int test_config_tick_hz_applied(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(config_from_body("tick_hz = 30\n", &cfg) == 0);

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.config = cfg;

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);
    if (s == nullptr)
    {
        // A failed create leaves the handle unset; the internal-struct
        // reads below are unreachable.
        kith_config_destroy(cfg);
        return failures;
    }
    CHECK(s->tick_hz == 30u);

    kith_server_destroy(s);
    kith_config_destroy(cfg);
    return failures;
}

static int test_config_params_win(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(config_from_body("tick_hz = 30\n", &cfg) == 0);

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.tick_hz = 60u;
    params.config = cfg;

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);
    if (s == nullptr)
    {
        // A failed create leaves the handle unset; the internal-struct
        // reads below are unreachable.
        kith_config_destroy(cfg);
        return failures;
    }
    CHECK(s->tick_hz == 60u);

    kith_server_destroy(s);
    kith_config_destroy(cfg);
    return failures;
}

static int test_config_listen_port_applied(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(config_from_body("listen_port = 18184\n", &cfg) == 0);

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.config = cfg;

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);
    CHECK(kith_server_listen_port(s) == 18184u);

    kith_server_destroy(s);
    kith_config_destroy(cfg);
    return failures;
}

static int test_config_listen_host_applied(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(config_from_body("listen_host = 127.0.0.1\n", &cfg) == 0);

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.config = cfg;

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);

    struct sockaddr_storage ss = {0};
    CHECK(server_bound_addr(s, &ss));
    CHECK(ss.ss_family == AF_INET);
    CHECK(addr_is_loopback(&ss));

    kith_server_destroy(s);
    kith_config_destroy(cfg);
    return failures;
}

static int test_config_listen_host_params_win(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(config_from_body("listen_host = 127.0.0.2\n", &cfg) == 0);

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.listen_host = "127.0.0.1";
    params.config = cfg;

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);

    struct sockaddr_storage ss = {0};
    CHECK(server_bound_addr(s, &ss));
    CHECK(ss.ss_family == AF_INET);
    CHECK(addr_is_loopback(&ss));

    kith_server_destroy(s);
    kith_config_destroy(cfg);
    return failures;
}

static int test_config_listen_host_empty_is_wildcard(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(config_from_body("listen_host =\n", &cfg) == 0);

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.config = cfg;

    kith_server_t *s = nullptr;
    CHECK(create_server_retry(&params, &s) == 0);

    struct sockaddr_storage ss = {0};
    CHECK(server_bound_addr(s, &ss));
    CHECK(addr_is_wildcard(&ss));

    kith_server_destroy(s);
    kith_config_destroy(cfg);
    return failures;
}

static int test_config_bad_value_fails_create(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(config_from_body("tick_hz = thirty\n", &cfg) == 0);

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.config = cfg;

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == kith_error_return(KITH_EINVAL));
    CHECK(s == nullptr);

    kith_config_destroy(cfg);
    return failures;
}

static int test_config_range_value_fails_create(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(config_from_body("tick_hz = 70000\n", &cfg) == 0);

    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.config = cfg;

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == kith_error_return(KITH_ERANGE));
    CHECK(s == nullptr);

    kith_config_destroy(cfg);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_arg_validation();
    rc |= test_create_params_validation();
    rc |= test_create_topology();
    rc |= test_status();
    rc |= test_listen_port();
    rc |= test_create_delivery_params();
    rc |= test_listener_default_wildcard();
    rc |= test_listener_pinned_numeric();
    rc |= test_listener_hostname_loopback();
    rc |= test_listener_unresolvable_host();
    rc |= test_config_tick_hz_applied();
    rc |= test_config_params_win();
    rc |= test_config_listen_port_applied();
    rc |= test_config_listen_host_applied();
    rc |= test_config_listen_host_params_win();
    rc |= test_config_listen_host_empty_is_wildcard();
    rc |= test_config_bad_value_fails_create();
    rc |= test_config_range_value_fails_create();
    if (rc != 0)
    {
        (void)fprintf(stderr, "server create tests FAILED\n");
    }
    return rc;
}

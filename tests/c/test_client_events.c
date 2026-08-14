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
        (void)fprintf(stderr, "client events: assertion at line %d failed (rc=%d)\n", line, rc);
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
        .event_cap = 4,
        .outbound_cap = 8,
    };
    return kith_client_create(&params, proto, nullptr, nullptr, out);
}

static kith_client_event_t make_event(uint16_t type_id, const char *text)
{
    kith_client_event_t event = {0};
    event.size = sizeof(kith_client_event_t);
    event.abi_version = KITH_ABI_VERSION;
    event.ts_mono_ns = 1000u;
    event.type_id = type_id;
    event.payload_len = (uint32_t)strlen(text);
    memcpy(event.payload, text, event.payload_len);
    return event;
}

/*---------------------------------------------------------------------------
 * tests
 *-------------------------------------------------------------------------*/

static int test_publish_drain(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_event_t e1 = make_event(10, "hello");
    kith_client_event_t e2 = make_event(20, "world");

    CHECK_RC(kith_client_publish_event(client, &e1) == 0, 0);
    CHECK_RC(kith_client_publish_event(client, &e2) == 0, 0);

    kith_client_event_t out[4] = {0};
    size_t count = 0;
    int rc = kith_client_drain_events(client, out, 4, &count);
    CHECK_RC(rc == 0, rc);
    CHECK(count == 2u);
    CHECK(out[0].size == sizeof(kith_client_event_t));
    CHECK(out[0].abi_version == KITH_ABI_VERSION);
    CHECK(out[0].type_id == 10);
    CHECK(out[0].payload_len == 5u);
    CHECK(memcmp(out[0].payload, "hello", 5) == 0);
    CHECK(out[1].type_id == 20);
    CHECK(memcmp(out[1].payload, "world", 5) == 0);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_publish_overflow(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    // event_cap = 4: publish 6 events → oldest 2 overwritten.
    for (uint16_t i = 0; i < 6u; i++)
    {
        kith_client_event_t e = make_event(i, "x");
        CHECK_RC(kith_client_publish_event(client, &e) == 0, 0);
    }

    kith_client_event_t out[8] = {0};
    size_t count = 0;
    CHECK(kith_client_drain_events(client, out, 8, &count) == 0);
    CHECK(count == 4u);
    // The ring holds the last 4: type_ids 2, 3, 4, 5.
    CHECK(out[0].type_id == 2);
    CHECK(out[3].type_id == 5);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_drain_empty(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_event_t out[2] = {0};
    size_t count = 99;
    int rc = kith_client_drain_events(client, out, 2, &count);
    CHECK_RC(rc == 0, rc);
    CHECK(count == 0u);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_drain_bad_args(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_event_t out[2] = {0};
    size_t count = 0;
    CHECK(kith_client_drain_events(nullptr, out, 2, &count) == -(int)KITH_EINVAL);
    CHECK(kith_client_drain_events(client, nullptr, 2, &count) == -(int)KITH_EINVAL);
    CHECK(kith_client_drain_events(client, out, 0, &count) == -(int)KITH_EINVAL);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_publish_too_large(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_event_t e = {0};
    e.size = sizeof(kith_client_event_t);
    e.abi_version = KITH_ABI_VERSION;
    e.payload_len = KITH_CLIENT_EVENT_PAYLOAD_MAX + 1u;
    CHECK(kith_client_publish_event(client, &e) == -(int)KITH_EOVERFLOW);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

// An event whose size is undersized is rejected with ESIZE before the
// abi_version or payload-length checks run. An event whose abi_version
// does not match the runtime generation is rejected with EABIVER.
static int test_publish_rejects_invalid_event(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_event_t bad_size = make_event(1, "x");
    bad_size.size = 4u;
    CHECK(kith_client_publish_event(client, &bad_size) == -(int)KITH_ESIZE);

    kith_client_event_t bad_abi = make_event(1, "x");
    bad_abi.abi_version = KITH_ABI_VERSION
    +1u;
    CHECK(kith_client_publish_event(client, &bad_abi) == -(int)KITH_EABIVER);

    // A well-formed event still publishes after the rejections above.
    kith_client_event_t ok = make_event(2, "y");
    CHECK(kith_client_publish_event(client, &ok) == 0);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_submit_interactive(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_command_t cmd = {
        .type_id = 42,
        .flags = 0u,
        .payload = "abc",
        .payload_len = 3u,
    };
    uint64_t cmd_id = 0;
    int rc = kith_client_submit_interactive(client, &cmd, &cmd_id);
    CHECK_RC(rc == 0, rc);
    CHECK(cmd_id != 0u);

    // A frame was enqueued.
    uint8_t buf[256];
    uint32_t frame_len = 0;
    rc = kith_client_pop_outbound(client, buf, sizeof(buf), &frame_len);
    CHECK_RC(rc == 0, rc);
    CHECK(frame_len > 0u);

    // Queue is now empty.
    rc = kith_client_pop_outbound(client, buf, sizeof(buf), &frame_len);
    CHECK(rc == -(int)KITH_EAGAIN);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_submit_interactive_bad_args(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_command_t cmd = {0};
    cmd.type_id = 42;
    CHECK(kith_client_submit_interactive(nullptr, &cmd, nullptr) == -(int)KITH_EINVAL);
    CHECK(kith_client_submit_interactive(client, nullptr, nullptr) == -(int)KITH_EINVAL);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_pop_outbound_bad_args(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    uint32_t len = 0;
    CHECK(kith_client_pop_outbound(nullptr, nullptr, 0, nullptr) == -(int)KITH_EINVAL);
    CHECK(kith_client_pop_outbound(client, nullptr, 0, &len) == -(int)KITH_EINVAL);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_runtime_status(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_runtime_status_t status = {
        .size = sizeof(status),
        .abi_version = KITH_ABI_VERSION,
    };
    int rc = kith_client_runtime_status(client, &status);
    CHECK_RC(rc == 0, rc);
    CHECK(status.connected == 0u);
    CHECK(status.reconnect_attempts == 0u);

    CHECK(kith_client_on_connected(client, 1000) == 0);

    rc = kith_client_runtime_status(client, &status);
    CHECK_RC(rc == 0, rc);
    CHECK(status.connected == 1u);

    CHECK(kith_client_on_disconnected(client, 2000) == 0);

    rc = kith_client_runtime_status(client, &status);
    CHECK_RC(rc == 0, rc);
    CHECK(status.connected == 0u);
    CHECK(status.reconnect_attempts == 1u);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int create_client_reconnect(kith_proto_t *proto,
                                   uint32_t base_ms,
                                   uint32_t max_ms,
                                   uint32_t max_attempts,
                                   kith_client_t **out)
{
    kith_client_params_t params = {
        .size = sizeof(kith_client_params_t),
        .abi_version = KITH_ABI_VERSION,
        .event_cap = 4,
        .outbound_cap = 8,
        .ping_type_id = 10u,
        .ping_interval_ms = 500u,
        .reconnect_base_ms = base_ms,
        .reconnect_max_ms = max_ms,
        .reconnect_max_attempts = max_attempts,
    };
    return kith_client_create(&params, proto, nullptr, nullptr, out);
}

/* A refused connect advances the reconnect schedule one attempt per
 * failure with the doubling curve, and on_connected resets it. */
static int test_connect_failed_advances_schedule(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client_reconnect(proto, 100u, 400u, 0u, &client) == 0);

    kith_client_runtime_status_t status = {
        .size = sizeof(status),
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_client_on_connect_failed(client, 1000) == 0);
    CHECK_RC(kith_client_runtime_status(client, &status) == 0, 0);
    CHECK(status.reconnect_attempts == 1u);
    CHECK(status.next_reconnect_due_ms == 1100u);

    CHECK(kith_client_on_connect_failed(client, 1050) == 0);
    CHECK_RC(kith_client_runtime_status(client, &status) == 0, 0);
    CHECK(status.reconnect_attempts == 2u);
    CHECK(status.next_reconnect_due_ms == 1250u);

    CHECK(kith_client_on_connect_failed(client, 1060) == 0);
    CHECK_RC(kith_client_runtime_status(client, &status) == 0, 0);
    CHECK(status.reconnect_attempts == 3u);
    CHECK(status.next_reconnect_due_ms == 1460u);

    CHECK(kith_client_on_connected(client, 1500) == 0);
    CHECK_RC(kith_client_runtime_status(client, &status) == 0, 0);
    CHECK(status.reconnect_attempts == 0u);
    CHECK(status.next_reconnect_due_ms == 0u);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

/* Exhausted reconnect attempts stop the schedule: the deadline clears to
 * zero instead of advancing. */
static int test_connect_failed_stops_at_max_attempts(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client_reconnect(proto, 100u, 400u, 2u, &client) == 0);

    kith_client_runtime_status_t status = {
        .size = sizeof(status),
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_client_on_connect_failed(client, 1000) == 0);
    CHECK(kith_client_on_connect_failed(client, 1050) == 0);
    CHECK_RC(kith_client_runtime_status(client, &status) == 0, 0);
    CHECK(status.reconnect_attempts == 2u);
    CHECK(status.next_reconnect_due_ms == 1250u);

    CHECK(kith_client_on_connect_failed(client, 1100) == 0);
    CHECK_RC(kith_client_runtime_status(client, &status) == 0, 0);
    CHECK(status.reconnect_attempts == 2u);
    CHECK(status.next_reconnect_due_ms == 0u);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

/* A refused connect touches only the schedule: the outbound queue keeps
 * its frames, the keepalive deadline and the bootstrap state stay put,
 * and the connected flag stays down. */
static int test_connect_failed_leaves_connection_state(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client_reconnect(proto, 100u, 400u, 0u, &client) == 0);

    CHECK(kith_client_on_connected(client, 1000) == 0);

    kith_client_command_t cmd = {
        .type_id = 42u,
        .flags = 0u,
        .payload = "keep",
        .payload_len = 4u,
    };
    uint64_t cmd_id = 0;
    CHECK_RC(kith_client_submit_interactive(client, &cmd, &cmd_id) == 0, 0);

    kith_client_runtime_status_t status = {
        .size = sizeof(status),
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK_RC(kith_client_runtime_status(client, &status) == 0, 0);
    const uint64_t ping_due = status.next_ping_due_ms;

    CHECK(kith_client_on_connect_failed(client, 2000) == 0);

    CHECK_RC(kith_client_runtime_status(client, &status) == 0, 0);
    CHECK(status.connected == 1u);
    CHECK(status.next_ping_due_ms == ping_due);
    CHECK(status.next_reconnect_due_ms == 2100u);

    kith_client_bootstrap_status_t boot = {
        .size = sizeof(boot),
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK_RC(kith_client_bootstrap_status(client, &boot) == 0, 0);
    CHECK(boot.state == (uint32_t)KITH_CLIENT_BOOTSTRAP_READY);

    uint8_t buf[64] = {0};
    uint32_t frame_len = 0;
    int rc = kith_client_pop_outbound(client, buf, sizeof(buf), &frame_len);
    CHECK_RC(rc == 0, rc);
    CHECK(frame_len > 0u);

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
    failures += test_publish_drain();
    failures += test_publish_overflow();
    failures += test_drain_empty();
    failures += test_drain_bad_args();
    failures += test_publish_too_large();
    failures += test_publish_rejects_invalid_event();
    failures += test_submit_interactive();
    failures += test_submit_interactive_bad_args();
    failures += test_pop_outbound_bad_args();
    failures += test_runtime_status();
    failures += test_connect_failed_advances_schedule();
    failures += test_connect_failed_stops_at_max_attempts();
    failures += test_connect_failed_leaves_connection_state();
    if (failures)
    {
        (void)fprintf(stderr, "client events: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

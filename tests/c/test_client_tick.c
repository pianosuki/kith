/* Keepalive tick contract of the client handle, driven whitebox through the
 * internal struct: the due ping encodes and queues exactly one frame and
 * advances the schedule, an outstanding pong suppresses the next ping, a
 * full outbound queue reports -KITH_ESTATE, and the reconnect-due flag arms
 * only past its deadline. Compiles client.c and its rings directly — the
 * tick helper and the queue internals are connection-internal. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "client/client_internal.h"
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
        (void)fprintf(stderr, "client tick: assertion at line %d failed (rc=%d)\n", line, rc);
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

static int create_client_cap(kith_proto_t *proto, uint32_t outbound_cap, kith_client_t **out)
{
    kith_client_params_t params = {
        .size = sizeof(kith_client_params_t),
        .abi_version = KITH_ABI_VERSION,
        .outbound_cap = outbound_cap,
    };
    return kith_client_create(&params, proto, nullptr, nullptr, out);
}

/*---------------------------------------------------------------------------
 * tick contract
 *-------------------------------------------------------------------------*/

static int test_null_client(void)
{
    int failures = 0;
    CHECK(kith_client_tick(nullptr, 0u) == kith_error_return(KITH_EINVAL));
    return failures;
}

static int test_ping_due_enqueues_once(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;
    CHECK_RC(create_proto(&proto) == 0, 0);
    CHECK_RC(create_client_cap(proto, 0u, &client) == 0, 0);

    client->connected = true;
    client->ping_type_id = 9000u;
    client->ping_interval_ms = 100u;
    client->next_ping_due_ms = 0u;
    client->awaiting_pong = false;

    CHECK(kith_client_tick(client, 5000u) == 0);
    CHECK(client->awaiting_pong);
    CHECK(client->last_ping_sent_ms == 5000u);
    CHECK(client->next_ping_due_ms == 5100u);
    CHECK(client->outq.count == 1u);

    /* The pong is still outstanding: the next due tick sends nothing. */
    CHECK(kith_client_tick(client, 5100u) == 0);
    CHECK(client->outq.count == 1u);

    /* After the pong clears, the next due tick re-arms the schedule. */
    client->awaiting_pong = false;
    CHECK(kith_client_tick(client, 5200u) == 0);
    CHECK(client->outq.count == 2u);
    CHECK(client->next_ping_due_ms == 5300u);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_ping_disabled_without_type_or_interval(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;
    CHECK_RC(create_proto(&proto) == 0, 0);
    CHECK_RC(create_client_cap(proto, 0u, &client) == 0, 0);

    client->connected = true;
    client->ping_type_id = 0u;
    client->ping_interval_ms = 100u;
    CHECK(kith_client_tick(client, 1000u) == 0);
    CHECK(client->outq.count == 0u);

    client->ping_type_id = 9000u;
    client->ping_interval_ms = 0u;
    CHECK(kith_client_tick(client, 1000u) == 0);
    CHECK(client->outq.count == 0u);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_full_queue_reports_estate(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;
    CHECK_RC(create_proto(&proto) == 0, 0);
    CHECK_RC(create_client_cap(proto, 1u, &client) == 0, 0);

    client->connected = true;
    client->ping_type_id = 9000u;
    client->ping_interval_ms = 100u;
    client->next_ping_due_ms = 0u;
    client->awaiting_pong = false;

    /* The first due ping fills the one-slot queue. */
    CHECK(kith_client_tick(client, 1000u) == 0);
    CHECK(client->outq.count == 1u);

    /* The next due ping cannot enqueue: the tick fails with -KITH_ESTATE. */
    client->awaiting_pong = false;
    client->next_ping_due_ms = 0u;
    CHECK(kith_client_tick(client, 2000u) == kith_error_return(KITH_ESTATE));

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_reconnect_due_arms_past_deadline(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;
    CHECK_RC(create_proto(&proto) == 0, 0);
    CHECK_RC(create_client_cap(proto, 0u, &client) == 0, 0);

    client->connected = false;
    client->next_reconnect_due_ms = 1000u;
    client->reconnect_due = false;

    CHECK(kith_client_tick(client, 500u) == 0);
    CHECK(!client->reconnect_due);
    CHECK(kith_client_tick(client, 1000u) == 0);
    CHECK(client->reconnect_due);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_null_client();
    rc |= test_ping_due_enqueues_once();
    rc |= test_ping_disabled_without_type_or_interval();
    rc |= test_full_queue_reports_estate();
    rc |= test_reconnect_due_arms_past_deadline();
    if (rc != 0)
    {
        (void)fprintf(stderr, "client tick tests FAILED\n");
    }
    return rc;
}

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
        (void)fprintf(stderr, "client bootstrap: assertion at line %d failed (rc=%d)\n", line, rc);
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
 * step context
 *-------------------------------------------------------------------------*/

struct step_ctx
{
    uint16_t enter_type_id;
    uint32_t next_step;
    bool enter_called;
    bool reply_called;
    int reply_rc;
};

static int test_step_enter(kith_client_t *client,
                           void *ctx,
                           uint16_t *out_type_id,
                           uint8_t *out_payload, // NOLINT(readability-non-const-parameter)
                           uint32_t payload_cap,
                           uint32_t *out_payload_len)
{
    (void)client;
    (void)out_payload;
    (void)payload_cap;
    struct step_ctx *sc = (struct step_ctx *)ctx;
    sc->enter_called = true;
    *out_type_id = sc->enter_type_id;
    *out_payload_len = 0u;
    return 0;
}

static int test_step_reply(kith_client_t *client,
                           void *ctx,
                           const kith_proto_frame_t *frame,
                           uint32_t *out_next_step)
{
    (void)client;
    (void)frame;
    struct step_ctx *sc = (struct step_ctx *)ctx;
    sc->reply_called = true;
    *out_next_step = sc->next_step;
    return sc->reply_rc;
}

static int enter_fail(kith_client_t *c,
                      void *ctx,
                      uint16_t *out_type_id,     // NOLINT(readability-non-const-parameter)
                      uint8_t *out_payload,      // NOLINT(readability-non-const-parameter)
                      uint32_t payload_cap,
                      uint32_t *out_payload_len) // NOLINT(readability-non-const-parameter)
{
    (void)c;
    (void)ctx;
    (void)out_type_id;
    (void)out_payload;
    (void)payload_cap;
    (void)out_payload_len;
    return -1;
}

static kith_proto_frame_t make_frame(uint16_t type_id)
{
    kith_proto_frame_t frame = {0};
    frame.type_id = type_id;
    frame.flags = 0u;
    frame.has_correlation = false;
    frame.payload = nullptr;
    frame.payload_len = 0u;
    return frame;
}

static int get_bootstrap_state(kith_client_t *client)
{
    kith_client_bootstrap_status_t status = {
        .size = sizeof(status),
        .abi_version = KITH_ABI_VERSION,
    };
    (void)kith_client_bootstrap_status(client, &status);
    return (int)status.state;
}

/*---------------------------------------------------------------------------
 * tests
 *-------------------------------------------------------------------------*/

static int test_bootstrap_no_steps(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    // No steps configured → on_connected → READY immediately.
    int rc = kith_client_on_connected(client, 1000);
    CHECK_RC(rc == 0, rc);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_READY);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_bootstrap_auto_advance(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 0};
    struct step_ctx ctx1 = {.enter_type_id = 101, .next_step = 0};

    kith_client_bootstrap_step_t steps[2] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = nullptr; // auto-advance
    steps[0].ctx = &ctx0;
    steps[1].await_type_id = 201;
    steps[1].on_enter = test_step_enter;
    steps[1].on_reply = nullptr; // auto-advance
    steps[1].ctx = &ctx1;

    CHECK(kith_client_configure_bootstrap(client, steps, 2) == 0);

    int rc = kith_client_on_connected(client, 1000);
    CHECK_RC(rc == 0, rc);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_RUNNING);
    CHECK(ctx0.enter_called);

    // Feed a matching frame for step 0 → auto-advance to step 1.
    kith_proto_frame_t f0 = make_frame(200);
    rc = kith_client_feed_frame(client, &f0, 1000);
    CHECK_RC(rc == 0, rc);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_RUNNING);
    CHECK(ctx1.enter_called);

    // Feed a matching frame for step 1 → auto-advance → READY.
    kith_proto_frame_t f1 = make_frame(201);
    rc = kith_client_feed_frame(client, &f1, 1000);
    CHECK_RC(rc == 0, rc);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_READY);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_bootstrap_reply_callback(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 1};
    struct step_ctx ctx1 = {.enter_type_id = 101, .next_step = 2}; // 2 == step_count → READY

    kith_client_bootstrap_step_t steps[2] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;
    steps[1].await_type_id = 201;
    steps[1].on_enter = test_step_enter;
    steps[1].on_reply = test_step_reply;
    steps[1].ctx = &ctx1;

    CHECK(kith_client_configure_bootstrap(client, steps, 2) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);

    kith_proto_frame_t f0 = make_frame(200);
    CHECK_RC(kith_client_feed_frame(client, &f0, 1000) == 0, 0);
    CHECK(ctx0.reply_called);
    CHECK(ctx1.enter_called);

    kith_proto_frame_t f1 = make_frame(201);
    CHECK_RC(kith_client_feed_frame(client, &f1, 1000) == 0, 0);
    CHECK(ctx1.reply_called);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_READY);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_bootstrap_reply_fail(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 0, .reply_rc = -1};

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);

    kith_proto_frame_t f0 = make_frame(200);
    int rc = kith_client_feed_frame(client, &f0, 1000);
    CHECK(rc == -(int)KITH_ESTATE);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_FAILED);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_bootstrap_non_matching_frame(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 1};

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);

    // Feed a non-matching frame → not consumed by bootstrap, state stays RUNNING.
    kith_proto_frame_t other = make_frame(999);
    int rc = kith_client_feed_frame(client, &other, 1000);
    CHECK_RC(rc == 0, rc);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_RUNNING);
    CHECK(!ctx0.reply_called);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_bootstrap_on_disconnected_resets(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 1};

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_RUNNING);

    CHECK(kith_client_on_disconnected(client, 2000) == 0);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_IDLE);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

static int test_bootstrap_pop_outbound(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 1};

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);

    // on_enter for step 0 enqueued a frame.
    uint8_t buf[256];
    uint32_t frame_len = 0;
    int rc = kith_client_pop_outbound(client, buf, sizeof(buf), &frame_len);
    CHECK_RC(rc == 0, rc);
    CHECK(frame_len > 0u);

    // Queue is now empty.
    rc = kith_client_pop_outbound(client, buf, sizeof(buf), &frame_len);
    CHECK(rc == -(int)KITH_EAGAIN);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

// A reply callback steering past the last step fails the bootstrap: the
// feed reports success (rc 0) but the state lands in FAILED.
static int test_bootstrap_next_step_past_end(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 2}; // count is 1

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);

    kith_proto_frame_t f0 = make_frame(200);
    int rc = kith_client_feed_frame(client, &f0, 1000);
    CHECK_RC(rc == 0, rc);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_FAILED);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

// A step without an on_enter skips encoding entirely: the bootstrap runs,
// the outbound queue stays empty, and the step still consumes its reply.
static int test_bootstrap_step_without_enter(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 1};

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = nullptr;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_RUNNING);

    uint8_t buf[64];
    uint32_t frame_len = 99u;
    CHECK(kith_client_pop_outbound(client, buf, sizeof(buf), &frame_len) == -(int)KITH_EAGAIN);

    kith_proto_frame_t f0 = make_frame(200);
    CHECK_RC(kith_client_feed_frame(client, &f0, 1000) == 0, 0);
    CHECK(ctx0.reply_called);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_READY);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

// An on_enter that reports failure fails the connection transition with
// ESTATE and parks the FSM in FAILED.
static int test_bootstrap_enter_error_fails(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = enter_fail;
    steps[0].on_reply = nullptr;
    steps[0].ctx = nullptr;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);

    int rc = kith_client_on_connected(client, 1000);
    CHECK(rc == -(int)KITH_ESTATE);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_FAILED);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

// When the outbound ring has no room left, a step's enqueue fails and the
// bootstrap parks in FAILED with ESTATE. The ring cannot be pre-filled
// before on_connected: the connected transition clears it, so the failure
// is provoked on the step-1 enter during a feed.
static int test_bootstrap_encode_failure_full_ring(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 1};
    struct step_ctx ctx1 = {.enter_type_id = 101, .next_step = 2};
    kith_client_bootstrap_step_t steps[2] = {0};
    steps[0].await_type_id = 200;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;
    steps[1].await_type_id = 201;
    steps[1].on_enter = test_step_enter;
    steps[1].on_reply = nullptr;
    steps[1].ctx = &ctx1;

    CHECK(kith_client_configure_bootstrap(client, steps, 2) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);

    /* Step 0's enter holds one slot; top up the rest of the default ring
     * and confirm the next submission answers EBUSY. */
    kith_client_command_t cmd = {
        .type_id = 50u,
        .flags = 0u,
        .correlation_id = 0u,
        .payload = "x",
        .payload_len = 1u,
    };
    for (uint32_t i = 0u; i < KITH_CLIENT_DEFAULT_OUTBOUND_CAP - 1u; i++)
    {
        CHECK(kith_client_submit_interactive(client, &cmd, nullptr) == 0);
    }
    CHECK(kith_client_submit_interactive(client, &cmd, nullptr) == -(int)KITH_EBUSY);

    /* The reply advances to step 1, whose enter hits the full ring. */
    kith_proto_frame_t f0 = make_frame(200);
    int rc = kith_client_feed_frame(client, &f0, 1000);
    CHECK(rc == -(int)KITH_ESTATE);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_FAILED);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

// A step awaiting type id 0 consumes a reply of any type.
static int test_bootstrap_await_zero_matches_any(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    struct step_ctx ctx0 = {.enter_type_id = 100, .next_step = 0};

    kith_client_bootstrap_step_t steps[1] = {0};
    steps[0].await_type_id = 0;
    steps[0].on_enter = test_step_enter;
    steps[0].on_reply = test_step_reply;
    steps[0].ctx = &ctx0;

    CHECK(kith_client_configure_bootstrap(client, steps, 1) == 0);
    CHECK(kith_client_on_connected(client, 1000) == 0);

    kith_proto_frame_t odd = make_frame(777);
    CHECK_RC(kith_client_feed_frame(client, &odd, 1000) == 0, 0);
    CHECK(ctx0.reply_called);
    /* next_step stayed at 0, so the same step consumes an unrelated type. */
    ctx0.reply_called = false;
    kith_proto_frame_t other = make_frame(1234);
    CHECK_RC(kith_client_feed_frame(client, &other, 1000) == 0, 0);
    CHECK(ctx0.reply_called);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

// Feeding while the FSM is idle (never configured, or after reset) is a
// no-op success rather than an error or a state change.
static int test_bootstrap_feed_while_idle(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);

    kith_proto_frame_t f = make_frame(200);
    CHECK_RC(kith_client_feed_frame(client, &f, 1000) == 0, 0);
    CHECK(get_bootstrap_state(client) == (int)KITH_CLIENT_BOOTSTRAP_IDLE);

    kith_client_destroy(client);
    kith_proto_destroy(proto);
    return failures;
}

/* Direct calls into the compiled FSM entry points pin the argument guards
   that the public wrapper cannot reach. */
static int test_bootstrap_internal_guards(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    kith_client_t *client = nullptr;

    CHECK(create_proto(&proto) == 0);
    CHECK(create_client(proto, &client) == 0);
    struct kith_client *impl = (struct kith_client *)client;

    kith_client_bootstrap_step_t step = {0};
    CHECK(client_bootstrap_configure(nullptr, &step, 1u, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));

    struct kith_client_bootstrap local = {0};
    CHECK(client_bootstrap_configure(&local, nullptr, 1u, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_bootstrap_start(nullptr) == kith_error_return(KITH_EINVAL));

    kith_proto_frame_t frame = make_frame(1);
    CHECK(client_bootstrap_feed(nullptr, &frame) == kith_error_return(KITH_EINVAL));
    CHECK(!client_bootstrap_matches(nullptr, &frame));
    CHECK(!client_bootstrap_matches(impl, nullptr));

    client_bootstrap_free(nullptr, kith_allocator_default());
    client_bootstrap_reset(nullptr);

    /* A local FSM records configuration and refuses reconfiguration while
     * configured; free releases it back to reusable. */
    CHECK(client_bootstrap_configure(&local, &step, 1u, kith_allocator_default()) == 0);
    CHECK(local.configured && local.state == KITH_CLIENT_BOOTSTRAP_IDLE);
    CHECK(client_bootstrap_configure(&local, &step, 1u, kith_allocator_default()) ==
          kith_error_return(KITH_ESTATE));
    client_bootstrap_free(&local, kith_allocator_default());
    CHECK(!local.configured && local.steps == nullptr);
    CHECK(client_bootstrap_configure(&local, &step, 1u, kith_allocator_default()) == 0);
    client_bootstrap_free(&local, kith_allocator_default());

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
    failures += test_bootstrap_no_steps();
    failures += test_bootstrap_auto_advance();
    failures += test_bootstrap_reply_callback();
    failures += test_bootstrap_reply_fail();
    failures += test_bootstrap_non_matching_frame();
    failures += test_bootstrap_on_disconnected_resets();
    failures += test_bootstrap_pop_outbound();
    failures += test_bootstrap_next_step_past_end();
    failures += test_bootstrap_step_without_enter();
    failures += test_bootstrap_enter_error_fails();
    failures += test_bootstrap_encode_failure_full_ring();
    failures += test_bootstrap_await_zero_matches_any();
    failures += test_bootstrap_feed_while_idle();
    failures += test_bootstrap_internal_guards();
    if (failures)
    {
        (void)fprintf(stderr, "client bootstrap: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

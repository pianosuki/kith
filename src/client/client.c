/* Public handle and lifecycle for the client: params validation, create and
 * destroy, the encode-and-enqueue helper, RTT accounting, and reconnect
 * backoff. Sits above queue.c (outbound and event rings) and bootstrap.c
 * (the step machine) and below the proto module for wire encoding. */

#include <string.h>

#include "client/client_internal.h"
#include "kith/types.h"
#include "kith/version.h"

/*---------------------------------------------------------------------------
 * byte-order helpers (no util dependency)
 *-------------------------------------------------------------------------*/

static uint64_t client_htonll(uint64_t host)
{
    uint8_t bytes[8] = {
        (uint8_t)(host >> 56),
        (uint8_t)(host >> 48),
        (uint8_t)(host >> 40),
        (uint8_t)(host >> 32),
        (uint8_t)(host >> 24),
        (uint8_t)(host >> 16),
        (uint8_t)(host >> 8),
        (uint8_t)host,
    };
    uint64_t net;
    memcpy(&net, bytes, sizeof(net));
    return net;
}

static uint64_t client_ntohll(uint64_t net)
{
    uint8_t bytes[8];
    memcpy(bytes, &net, sizeof(bytes));
    return ((uint64_t)bytes[0] << 56) | ((uint64_t)bytes[1] << 48) | ((uint64_t)bytes[2] << 40) |
           ((uint64_t)bytes[3] << 32) | ((uint64_t)bytes[4] << 24) | ((uint64_t)bytes[5] << 16) |
           ((uint64_t)bytes[6] << 8) | (uint64_t)bytes[7];
}

/*---------------------------------------------------------------------------
 * params
 *-------------------------------------------------------------------------*/

static void resolve_defaults(kith_client_params_t *out)
{
    if (out->outbound_cap == 0u)
    {
        out->outbound_cap = KITH_CLIENT_DEFAULT_OUTBOUND_CAP;
    }
    if (out->event_cap == 0u)
    {
        out->event_cap = KITH_CLIENT_DEFAULT_EVENT_CAP;
    }
    if (out->ping_interval_ms == 0u)
    {
        out->ping_interval_ms = KITH_CLIENT_DEFAULT_PING_INTERVAL_MS;
    }
    if (out->reconnect_base_ms == 0u)
    {
        out->reconnect_base_ms = KITH_CLIENT_DEFAULT_RECONNECT_BASE_MS;
    }
    if (out->reconnect_max_ms == 0u)
    {
        out->reconnect_max_ms = KITH_CLIENT_DEFAULT_RECONNECT_MAX_MS;
    }
}

static bool params_validate(const kith_client_params_t *params, kith_error_t *out_err)
{
    if (params->size < sizeof(*params))
    {
        *out_err = KITH_ESIZE;
        return false;
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        *out_err = KITH_EABIVER;
        return false;
    }
    if (params->reconnect_max_ms < params->reconnect_base_ms)
    {
        *out_err = KITH_EINVAL;
        return false;
    }
    *out_err = KITH_OK;
    return true;
}

/*---------------------------------------------------------------------------
 * helpers (exposed via internal header)
 *-------------------------------------------------------------------------*/

int client_encode_and_enqueue(struct kith_client *client,
                              uint16_t type_id,
                              uint8_t flags,
                              uint64_t correlation_id,
                              const void *payload,
                              uint32_t payload_len)
{
    if (!client)
    {
        return kith_error_return(KITH_EINVAL);
    }
    size_t frame_len = kith_proto_encode(
        client->proto, type_id, flags, correlation_id, payload, payload_len, nullptr, 0u);
    if (frame_len == 0u)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    uint8_t *buf = kith_alloc(client->allocator, frame_len);
    if (!buf)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    (void)kith_proto_encode(
        client->proto, type_id, flags, correlation_id, payload, payload_len, buf, frame_len);
    int rc = client_outq_push(&client->outq, buf, (uint32_t)frame_len, client->allocator);
    // The outbound ring copies the frame into its own slot buffer, so this
    // encode buffer is owned here whether the push succeeded or not.
    kith_free(client->allocator, buf);
    return rc;
}

uint32_t client_compute_reconnect_delay(const struct kith_client *client, uint32_t attempt_number)
{
    uint64_t delay = client->reconnect_base_ms;
    const uint32_t max_delay = client->reconnect_max_ms;
    uint32_t shifts = (attempt_number > 0u) ? (attempt_number - 1u) : 0u;
    while (shifts > 0u && delay < max_delay)
    {
        delay <<= 1u;
        if (delay > max_delay)
        {
            delay = max_delay;
            break;
        }
        shifts--;
    }
    if (delay > max_delay)
    {
        delay = max_delay;
    }
    return (uint32_t)delay;
}

void client_record_rtt(struct kith_client *client, uint32_t rtt_ms)
{
    client->rtt_last_ms = rtt_ms;
    if (client->rtt_samples == 0u || rtt_ms < client->rtt_min_ms)
    {
        client->rtt_min_ms = rtt_ms;
    }
    if (client->rtt_samples == 0u || rtt_ms > client->rtt_max_ms)
    {
        client->rtt_max_ms = rtt_ms;
    }
    client->rtt_sum_ms += rtt_ms;
    if (client->rtt_samples < UINT32_MAX)
    {
        client->rtt_samples++;
    }
}

/*---------------------------------------------------------------------------
 * lifecycle
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_client_create(const kith_client_params_t *params,
                                              kith_proto_t *proto,
                                              kith_logger_t *logger,
                                              const kith_allocator_t *alloc,
                                              kith_client_t **out_client)
{
    if (!out_client)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_client = nullptr;
    if (!proto)
    {
        return kith_error_return(KITH_EINVAL);
    }

    kith_client_params_t resolved;
    if (params)
    {
        kith_error_t err = KITH_OK;
        if (!params_validate(params, &err))
        {
            return kith_error_return(err);
        }
        resolved = *params;
    }
    else
    {
        memset(&resolved, 0, sizeof(resolved));
        resolved.size = sizeof(resolved);
        resolved.abi_version = KITH_ABI_VERSION;
    }
    resolve_defaults(&resolved);

    if (alloc)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    kith_client_t *client = kith_alloc_zero(allocator, 1, sizeof(*client));
    if (!client)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    client->allocator = allocator;
    client->proto = proto;
    client->logger = logger;
    client->event_cap = resolved.event_cap;
    client->ping_type_id = resolved.ping_type_id;
    client->pong_type_id = resolved.pong_type_id;
    client->ping_interval_ms = resolved.ping_interval_ms;
    client->reconnect_base_ms = resolved.reconnect_base_ms;
    client->reconnect_max_ms = resolved.reconnect_max_ms;
    client->reconnect_max_attempts = resolved.reconnect_max_attempts;

    client->next_cmd_id = 1u;

    client->outq.slots =
        kith_alloc_zero(allocator, resolved.outbound_cap, sizeof(*client->outq.slots));
    client->outq.cap = resolved.outbound_cap;
    client->events = kith_alloc_zero(allocator, client->event_cap, sizeof(*client->events));
    if (!client->outq.slots || !client->events)
    {
        kith_free(allocator, client->outq.slots);
        kith_free(allocator, client->events);
        kith_free(allocator, client);
        return kith_error_return(KITH_ENOMEM);
    }
    *out_client = client;
    return 0;
}

KITH_API void kith_client_destroy(kith_client_t *client)
{
    if (!client)
    {
        return;
    }
    client_outq_clear(&client->outq, client->allocator);
    client_bootstrap_free(&client->bootstrap, client->allocator);
    kith_free(client->allocator, client->outq.slots);
    kith_free(client->allocator, client->events);
    kith_free(client->allocator, client);
}

/*---------------------------------------------------------------------------
 * bootstrap configuration
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_client_configure_bootstrap(
    kith_client_t *client, const kith_client_bootstrap_step_t *steps, uint32_t count)
{
    if (!client)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return client_bootstrap_configure(&client->bootstrap, steps, count, client->allocator);
}

KITH_API void
kith_client_set_frame_handler(kith_client_t *client, kith_client_frame_fn on_frame, void *ctx)
{
    if (!client)
    {
        return;
    }
    client->on_frame = on_frame;
    client->frame_ctx = ctx;
}

/*---------------------------------------------------------------------------
 * connection lifecycle
 *-------------------------------------------------------------------------*/

static void runtime_reset(struct kith_client *client)
{
    client->connected = false;
    client->awaiting_pong = false;
    client->reconnect_due = false;
    client->reconnect_attempts = 0u;
    client->last_reconnect_delay_ms = 0u;
    client->next_reconnect_due_ms = 0u;
    client->next_ping_due_ms = 0u;
    client->last_ping_sent_ms = 0u;
    client->rtt_last_ms = 0u;
    client->rtt_min_ms = 0u;
    client->rtt_max_ms = 0u;
    client->rtt_sum_ms = 0u;
    client->rtt_samples = 0u;
}

[[nodiscard]] KITH_API int kith_client_on_connected(kith_client_t *client, uint64_t now_ms)
{
    if (!client)
    {
        return kith_error_return(KITH_EINVAL);
    }
    runtime_reset(client);
    client->connected = true;
    if (client->ping_type_id != 0u && client->ping_interval_ms > 0u)
    {
        client->next_ping_due_ms = now_ms + client->ping_interval_ms;
    }
    client_outq_clear(&client->outq, client->allocator);
    return client_bootstrap_start(client);
}

static int client_schedule_reconnect(struct kith_client *client, uint64_t now_ms)
{
    // One schedule advance per failed attempt; attempts accumulate across
    // a real loss and every refused retry until the next on_connected
    // resets them.
    if (client->reconnect_max_attempts > 0u &&
        client->reconnect_attempts >= client->reconnect_max_attempts)
    {
        client->next_reconnect_due_ms = 0u;
        return 0;
    }
    client->reconnect_attempts++;
    client->last_reconnect_delay_ms =
        client_compute_reconnect_delay(client, client->reconnect_attempts);
    client->next_reconnect_due_ms = now_ms + client->last_reconnect_delay_ms;
    return 0;
}

[[nodiscard]] KITH_API int kith_client_on_disconnected(kith_client_t *client, uint64_t now_ms)
{
    if (!client)
    {
        return kith_error_return(KITH_EINVAL);
    }
    client->connected = false;
    client->awaiting_pong = false;
    client->next_ping_due_ms = 0u;
    client->reconnect_due = false;
    client_bootstrap_reset(&client->bootstrap);
    client_outq_clear(&client->outq, client->allocator);
    return client_schedule_reconnect(client, now_ms);
}

[[nodiscard]] KITH_API int kith_client_on_connect_failed(kith_client_t *client, uint64_t now_ms)
{
    if (!client)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return client_schedule_reconnect(client, now_ms);
}

/*---------------------------------------------------------------------------
 * inbound
 *-------------------------------------------------------------------------*/

static int handle_pong(struct kith_client *client, const kith_proto_frame_t *frame, uint64_t now_ms)
{
    if (client->awaiting_pong && frame->payload_len >= 8u && frame->payload)
    {
        uint64_t ts_net;
        memcpy(&ts_net, frame->payload, sizeof(ts_net));
        const uint64_t sent_ms = client_ntohll(ts_net);
        if (sent_ms == client->last_ping_sent_ms && now_ms >= sent_ms)
        {
            const uint64_t delta = now_ms - sent_ms;
            const uint32_t rtt = (delta > UINT32_MAX) ? UINT32_MAX : (uint32_t)delta;
            client_record_rtt(client, rtt);
        }
        client->awaiting_pong = false;
    }
    return 0;
}

[[nodiscard]] KITH_API int
kith_client_feed_frame(kith_client_t *client, const kith_proto_frame_t *frame, uint64_t now_ms)
{
    if (!client || !frame)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (client->pong_type_id != 0u && frame->type_id == client->pong_type_id)
    {
        return handle_pong(client, frame, now_ms);
    }
    if (client_bootstrap_matches(client, frame))
    {
        return client_bootstrap_feed(client, frame);
    }
    if (client->on_frame)
    {
        client->on_frame(client, client->frame_ctx, frame);
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * outbound
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int
kith_client_pop_outbound(kith_client_t *client, uint8_t *out_buf, uint32_t cap, uint32_t *out_len)
{
    if (!client || !out_buf || !out_len)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return client_outq_pop(&client->outq, out_buf, cap, out_len, client->allocator);
}

/*---------------------------------------------------------------------------
 * interactive command injection
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_client_submit_interactive(kith_client_t *client,
                                                          const kith_client_command_t *command,
                                                          uint64_t *out_cmd_id)
{
    if (!client || !command)
    {
        return kith_error_return(KITH_EINVAL);
    }
    int rc = client_encode_and_enqueue(client,
                                       command->type_id,
                                       command->flags,
                                       command->correlation_id,
                                       command->payload,
                                       command->payload_len);
    if (rc != 0)
    {
        return rc;
    }
    client->commands_submitted++;
    const uint64_t cmd_id = client->next_cmd_id++;
    if (out_cmd_id)
    {
        *out_cmd_id = cmd_id;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * event queue
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_client_publish_event(kith_client_t *client,
                                                     const kith_client_event_t *event)
{
    if (!client || !event)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (event->size < sizeof(*event))
    {
        return kith_error_return(KITH_ESIZE);
    }
    if (event->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    return client_eventq_push(client->events,
                              client->event_cap,
                              &client->event_head,
                              &client->event_tail,
                              &client->event_count,
                              event);
}

[[nodiscard]] KITH_API int kith_client_drain_events(kith_client_t *client,
                                                    kith_client_event_t *out_events,
                                                    size_t out_cap,
                                                    size_t *out_count)
{
    if (!client || !out_events || !out_count || out_cap == 0u)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return client_eventq_drain(client->events,
                               client->event_cap,
                               &client->event_head,
                               &client->event_tail,
                               &client->event_count,
                               out_events,
                               out_cap,
                               out_count);
}

/*---------------------------------------------------------------------------
 * tick / keepalive
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_client_tick(kith_client_t *client, uint64_t now_ms)
{
    if (!client)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (client->connected && client->ping_type_id != 0u && client->ping_interval_ms > 0u &&
        !client->awaiting_pong && now_ms >= client->next_ping_due_ms)
    {
        uint8_t payload[8];
        const uint64_t ts_net = client_htonll(now_ms);
        memcpy(payload, &ts_net, sizeof(ts_net));
        int rc = client_encode_and_enqueue(
            client, client->ping_type_id, 0u, 0u, payload, sizeof(payload));
        if (rc != 0)
        {
            return kith_error_return(KITH_ESTATE);
        }
        client->last_ping_sent_ms = now_ms;
        client->awaiting_pong = true;
        client->next_ping_due_ms = now_ms + client->ping_interval_ms;
    }
    if (!client->connected && client->next_reconnect_due_ms > 0u &&
        now_ms >= client->next_reconnect_due_ms)
    {
        client->reconnect_due = true;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * status
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_client_bootstrap_status(kith_client_t *client,
                                                        kith_client_bootstrap_status_t *out_status)
{
    if (!client || !out_status)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (out_status->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (out_status->size < sizeof(*out_status))
    {
        return kith_error_return(KITH_ESIZE);
    }
    out_status->state = client->bootstrap.state;
    out_status->current_step = client->bootstrap.current_step;
    out_status->step_count = client->bootstrap.step_count;
    return 0;
}

[[nodiscard]] KITH_API int kith_client_runtime_status(kith_client_t *client,
                                                      kith_client_runtime_status_t *out_status)
{
    if (!client || !out_status)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (out_status->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (out_status->size < sizeof(*out_status))
    {
        return kith_error_return(KITH_ESIZE);
    }
    out_status->connected = client->connected ? 1u : 0u;
    out_status->awaiting_pong = client->awaiting_pong ? 1u : 0u;
    out_status->reconnect_due = client->reconnect_due ? 1u : 0u;
    out_status->reconnect_attempts = client->reconnect_attempts;
    out_status->rtt_last_ms = client->rtt_last_ms;
    out_status->rtt_min_ms = client->rtt_samples > 0u ? client->rtt_min_ms : 0u;
    out_status->rtt_max_ms = client->rtt_max_ms;
    out_status->rtt_sum_ms = client->rtt_sum_ms;
    out_status->rtt_samples = client->rtt_samples;
    out_status->last_ping_sent_ms = client->last_ping_sent_ms;
    out_status->next_ping_due_ms = client->next_ping_due_ms;
    out_status->next_reconnect_due_ms = client->next_reconnect_due_ms;
    return 0;
}

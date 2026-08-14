/* Bootstrap sequence for the client: configures a step list, drives each
 * step's on_enter to encode and enqueue its outbound frame, and advances on
 * the matching reply. Sits below client.c (the handle); step state lives on
 * kith_client.bootstrap and frames go out via client_encode_and_enqueue. */

#include <string.h>

#include "client/client_internal.h"
#include "kith/types.h"

/*---------------------------------------------------------------------------
 * bootstrap lifecycle
 *-------------------------------------------------------------------------*/

int client_bootstrap_configure(struct kith_client_bootstrap *bs,
                               const kith_client_bootstrap_step_t *steps,
                               uint32_t count,
                               const kith_allocator_t *alloc)
{
    if (!bs)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (bs->configured)
    {
        return kith_error_return(KITH_ESTATE);
    }
    if (count > 0u && !steps)
    {
        return kith_error_return(KITH_EINVAL);
    }

    if (count > 0u)
    {
        bs->steps = kith_alloc_zero(alloc, count, sizeof(*bs->steps));
        if (!bs->steps)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        memcpy(bs->steps, steps, count * sizeof(*bs->steps));
    }
    bs->step_count = count;
    bs->current_step = 0u;
    bs->state = KITH_CLIENT_BOOTSTRAP_IDLE;
    bs->configured = true;
    return 0;
}

void client_bootstrap_free(struct kith_client_bootstrap *bs, const kith_allocator_t *alloc)
{
    if (!bs)
    {
        return;
    }
    kith_free(alloc, bs->steps);
    bs->steps = nullptr;
    bs->step_count = 0u;
    bs->current_step = 0u;
    bs->state = KITH_CLIENT_BOOTSTRAP_IDLE;
    bs->configured = false;
}

void client_bootstrap_reset(struct kith_client_bootstrap *bs)
{
    if (!bs)
    {
        return;
    }
    bs->current_step = 0u;
    bs->state = KITH_CLIENT_BOOTSTRAP_IDLE;
}

/*---------------------------------------------------------------------------
 * step enter — encode and enqueue the outbound frame for the current step
 *-------------------------------------------------------------------------*/

static int bootstrap_enter_step(struct kith_client *client, uint32_t step_idx)
{
    struct kith_client_bootstrap *bs = &client->bootstrap;
    if (step_idx >= bs->step_count)
    {
        bs->state = KITH_CLIENT_BOOTSTRAP_READY;
        return 0;
    }

    bs->current_step = step_idx;
    kith_client_bootstrap_step_t *step = &bs->steps[step_idx];
    if (!step->on_enter)
    {
        return 0;
    }

    uint16_t type_id = 0u;
    uint8_t payload[CLIENT_STEP_PAYLOAD_CAP];
    uint32_t payload_len = 0u;
    int rc =
        step->on_enter(client, step->ctx, &type_id, payload, CLIENT_STEP_PAYLOAD_CAP, &payload_len);
    if (rc != 0)
    {
        bs->state = KITH_CLIENT_BOOTSTRAP_FAILED;
        return kith_error_return(KITH_ESTATE);
    }
    rc = client_encode_and_enqueue(
        client, type_id, 0u, 0u, payload_len > 0u ? payload : nullptr, payload_len);
    if (rc != 0)
    {
        bs->state = KITH_CLIENT_BOOTSTRAP_FAILED;
        return kith_error_return(KITH_ESTATE);
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * start — reset and enter step 0 (or READY if no steps)
 *-------------------------------------------------------------------------*/

int client_bootstrap_start(struct kith_client *client)
{
    if (!client)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct kith_client_bootstrap *bs = &client->bootstrap;
    client_bootstrap_reset(bs);

    if (bs->step_count == 0u)
    {
        bs->state = KITH_CLIENT_BOOTSTRAP_READY;
        return 0;
    }
    bs->state = KITH_CLIENT_BOOTSTRAP_RUNNING;
    return bootstrap_enter_step(client, 0u);
}

/*---------------------------------------------------------------------------
 * feed — match frame against current step and dispatch reply
 *-------------------------------------------------------------------------*/

bool client_bootstrap_matches(const struct kith_client *client, const kith_proto_frame_t *frame)
{
    if (!client || !frame)
    {
        return false;
    }
    const struct kith_client_bootstrap *bs = &client->bootstrap;
    if (bs->state != KITH_CLIENT_BOOTSTRAP_RUNNING)
    {
        return false;
    }
    if (bs->current_step >= bs->step_count)
    {
        return false;
    }
    uint16_t await = bs->steps[bs->current_step].await_type_id;
    return (await == 0u) || (await == frame->type_id);
}

int client_bootstrap_feed(struct kith_client *client, const kith_proto_frame_t *frame)
{
    if (!client || !frame)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct kith_client_bootstrap *bs = &client->bootstrap;
    if (bs->state != KITH_CLIENT_BOOTSTRAP_RUNNING)
    {
        return 0;
    }

    kith_client_bootstrap_step_t *step = &bs->steps[bs->current_step];
    uint32_t next_step = bs->current_step + 1u;
    if (step->on_reply)
    {
        int rc = step->on_reply(client, step->ctx, frame, &next_step);
        if (rc != 0)
        {
            bs->state = KITH_CLIENT_BOOTSTRAP_FAILED;
            return kith_error_return(KITH_ESTATE);
        }
    }

    if (next_step > bs->step_count)
    {
        bs->state = KITH_CLIENT_BOOTSTRAP_FAILED;
        return 0;
    }
    if (next_step == bs->step_count)
    {
        bs->state = KITH_CLIENT_BOOTSTRAP_READY;
        return 0;
    }
    return bootstrap_enter_step(client, next_step);
}

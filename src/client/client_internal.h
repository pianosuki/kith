#pragma once

#include <stdint.h>

#include "kith/client/client.h"
#include "kith/logger/logger.h"
#include "kith/proto/proto.h"

// ---------------------------------------------------------------------------
// capacity limits
// ---------------------------------------------------------------------------
#define CLIENT_STEP_PAYLOAD_CAP 4096u

// ---------------------------------------------------------------------------
// outbound frame queue slot (owned byte buffer)
// ---------------------------------------------------------------------------
struct kith_client_outbound_slot
{
    uint8_t *data;
    uint32_t len;
};

// ---------------------------------------------------------------------------
// outbound frame queue (ring descriptor owned by the handle)
// ---------------------------------------------------------------------------
struct client_outq
{
    struct kith_client_outbound_slot *slots;
    uint32_t cap;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
};

// ---------------------------------------------------------------------------
// event ring record (stored copy, inline payload)
// ---------------------------------------------------------------------------
struct kith_client_event_record
{
    uint64_t ts_mono_ns;
    uint16_t type_id;
    uint32_t payload_len;
    uint8_t payload[KITH_CLIENT_EVENT_PAYLOAD_MAX];
};

// ---------------------------------------------------------------------------
// bootstrap FSM state
// ---------------------------------------------------------------------------
struct kith_client_bootstrap
{
    kith_client_bootstrap_step_t *steps;
    uint32_t step_count;
    uint32_t current_step;
    kith_client_bootstrap_state_t state;
    bool configured;
};

// ---------------------------------------------------------------------------
// client handle
// ---------------------------------------------------------------------------
struct kith_client
{
    const kith_allocator_t *allocator;

    kith_proto_t *proto;   // borrowed
    kith_logger_t *logger; // borrowed, may be NULL

    // params (copied at create time)
    uint32_t event_cap;
    uint16_t ping_type_id;
    uint16_t pong_type_id;
    uint32_t ping_interval_ms;
    uint32_t reconnect_base_ms;
    uint32_t reconnect_max_ms;
    uint32_t reconnect_max_attempts;

    // outbound queue (ring buffer)
    struct client_outq outq;

    // event ring
    struct kith_client_event_record *events;
    uint32_t event_head;
    uint32_t event_tail;
    uint32_t event_count;

    // bootstrap
    struct kith_client_bootstrap bootstrap;

    // frame handler
    kith_client_frame_fn on_frame;
    void *frame_ctx;

    // keepalive / runtime state
    bool connected;
    bool awaiting_pong;
    bool reconnect_due;
    uint32_t reconnect_attempts;
    uint32_t last_reconnect_delay_ms;
    uint64_t next_ping_due_ms;
    uint64_t last_ping_sent_ms;
    uint64_t next_reconnect_due_ms;

    // RTT stats
    uint32_t rtt_last_ms;
    uint32_t rtt_min_ms;
    uint32_t rtt_max_ms;
    uint64_t rtt_sum_ms;
    uint32_t rtt_samples;

    // command counter
    uint64_t next_cmd_id;
    uint64_t commands_submitted;
};

// ---------------------------------------------------------------------------
// queue.c — outbound frame ring + event ring
// ---------------------------------------------------------------------------

void client_outq_clear(struct client_outq *q, const kith_allocator_t *alloc);

int client_outq_push(struct client_outq *q,
                     const uint8_t *data,
                     uint32_t len,
                     const kith_allocator_t *alloc);

int client_outq_pop(struct client_outq *q,
                    uint8_t *out_buf,
                    uint32_t out_cap,
                    uint32_t *out_len,
                    const kith_allocator_t *alloc);

void client_eventq_clear(uint32_t *head, uint32_t *tail, uint32_t *count);

int client_eventq_push(struct kith_client_event_record *records,
                       uint32_t cap,
                       uint32_t *head,
                       uint32_t *tail,
                       uint32_t *count,
                       const kith_client_event_t *event);

int client_eventq_drain(struct kith_client_event_record *records,
                        uint32_t cap,
                        uint32_t *head,
                        const uint32_t *tail,
                        uint32_t *count,
                        kith_client_event_t *out_events,
                        size_t out_cap,
                        size_t *out_count);

// ---------------------------------------------------------------------------
// bootstrap.c — FSM
// ---------------------------------------------------------------------------

int client_bootstrap_configure(struct kith_client_bootstrap *bs,
                               const kith_client_bootstrap_step_t *steps,
                               uint32_t count,
                               const kith_allocator_t *alloc);

void client_bootstrap_free(struct kith_client_bootstrap *bs, const kith_allocator_t *alloc);

void client_bootstrap_reset(struct kith_client_bootstrap *bs);

int client_bootstrap_start(struct kith_client *client);

bool client_bootstrap_matches(const struct kith_client *client, const kith_proto_frame_t *frame);

int client_bootstrap_feed(struct kith_client *client, const kith_proto_frame_t *frame);

// ---------------------------------------------------------------------------
// client.c — helpers
// ---------------------------------------------------------------------------

// Encode a proto frame and enqueue it on the outbound queue. Returns 0 on
// success, negative kith_error on failure.
int client_encode_and_enqueue(struct kith_client *client,
                              uint16_t type_id,
                              uint8_t flags,
                              uint64_t correlation_id,
                              const void *payload,
                              uint32_t payload_len);

// Compute the reconnect delay for @p attempt_number using exponential
// backoff clamped to the configured max.
uint32_t client_compute_reconnect_delay(const struct kith_client *client, uint32_t attempt_number);

// Record an RTT sample.
void client_record_rtt(struct kith_client *client, uint32_t rtt_ms);

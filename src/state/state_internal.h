#pragma once

#include <async.h>
#include <hiredis.h>

#include "kith/reactor/reactor.h"
#include "kith/state/state.h"

// ---------------------------------------------------------------------------
// reactor event adapter — bridges hiredis async events to kith_reactor
// ---------------------------------------------------------------------------
struct kith_state_adapter
{
    kith_reactor_t *reactor;
    redisAsyncContext *ac;
    unsigned int event_mask;  // reactor registration mask (KITH_REACTOR_IN|OUT)
    bool connected;           // true after successful connect callback
    bool registration_failed; // a reactor registration failed; the context is
                              // freed and every pending command fails as soon
                              // as a top-level reactor context allows it
    int registration_rc;      // the failing registration's negative error code
    bool wiring;              // true while create wires the adapter callbacks
};

// ---------------------------------------------------------------------------
// per-command context — passed as privdata to hiredis async commands
// ---------------------------------------------------------------------------
enum kith_state_cmd_tag
{
    KITH_STATE_CMD_SET,
    KITH_STATE_CMD_GET,
    KITH_STATE_CMD_DEL,
    KITH_STATE_CMD_EXISTS,
};

struct kith_state_cmd
{
    enum kith_state_cmd_tag tag;
    kith_state_reply_fn callback;
    void *user_data;
    struct kith_state *state; // owning store: the allocator for this block's
                              // free and the submission task's adapter
    void *key;                // prefixed key bytes; trailing in the allocation
    size_t key_len;
    const void *value;        // value bytes for SET; trailing in the allocation
    size_t value_len;
};

// ---------------------------------------------------------------------------
// state store internals
// ---------------------------------------------------------------------------
struct kith_state
{
    const kith_allocator_t *allocator; // resolved at create; every kith-owned
                                       // block allocates and frees through it
    struct kith_state_adapter adapter;
    const char *key_prefix;            // borrowed from params
    uint32_t key_prefix_len;
};

// ---------------------------------------------------------------------------
// adapter callbacks (exposed for test visibility — not public API)
// ---------------------------------------------------------------------------
void kith_state_adapter_add_read(void *privdata);
void kith_state_adapter_del_read(void *privdata);
void kith_state_adapter_add_write(void *privdata);
void kith_state_adapter_del_write(void *privdata);
void kith_state_adapter_cleanup(void *privdata);

// ---------------------------------------------------------------------------
// reactor fd event handler — routes each readiness to its hiredis half
// ---------------------------------------------------------------------------
void kith_state_event_handler(int fd, unsigned int events, void *ctx);

// ---------------------------------------------------------------------------
// hiredis connect / disconnect callbacks
// ---------------------------------------------------------------------------
void kith_state_connect_cb(const redisAsyncContext *ac, int status);
void kith_state_disconnect_cb(const redisAsyncContext *ac, int status);

// ---------------------------------------------------------------------------
// hiredis command reply callback — dispatches to the user's kith_state_reply_fn
// ---------------------------------------------------------------------------
void kith_state_command_cb(redisAsyncContext *ac, void *reply, void *privdata);

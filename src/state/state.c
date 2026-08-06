/* Single translation unit of the state module: Redis-backed asynchronous
 * key/value access (SET, GET, DEL, EXISTS) over a hiredis async context, with
 * a reactor adapter driving read and write readiness, reply translation into
 * kith_state_reply_t, and connect and disconnect handling. The public contract
 * is include/kith/state/state.h. */

#include <stdckdint.h>
#include <string.h>

#include "kith/types.h"
#include "kith/version.h"
#include "state/state_internal.h"

#include <sys/time.h>

// ---------------------------------------------------------------------------
// helper: populate reply from a SET response
// ---------------------------------------------------------------------------
static void fill_set_reply(kith_state_reply_t *out, const redisReply *r)
{
    if (r->type == REDIS_REPLY_STATUS)
    {
        out->status = 0;
    }
    else
    {
        out->status = kith_error_return(KITH_EIO);
        out->err_str = "unexpected SET reply type";
    }
}

// ---------------------------------------------------------------------------
// helper: populate reply from a GET response
// ---------------------------------------------------------------------------
static void fill_get_reply(kith_state_reply_t *out, const redisReply *r)
{
    if (r->type == REDIS_REPLY_STRING)
    {
        out->status = 0;
        out->value = r->str;
        out->value_len = r->len;
    }
    else if (r->type == REDIS_REPLY_NIL)
    {
        out->status = 0;
    }
    else
    {
        out->status = kith_error_return(KITH_EIO);
        out->err_str = "unexpected GET reply type";
    }
}

// ---------------------------------------------------------------------------
// helper: populate reply from a DEL response
// ---------------------------------------------------------------------------
static void fill_del_reply(kith_state_reply_t *out, const redisReply *r)
{
    if (r->type == REDIS_REPLY_INTEGER)
    {
        out->status = 0;
    }
    else
    {
        out->status = kith_error_return(KITH_EIO);
        out->err_str = "unexpected DEL reply type";
    }
}

// ---------------------------------------------------------------------------
// helper: populate reply from an EXISTS response
// ---------------------------------------------------------------------------
static void fill_exists_reply(kith_state_reply_t *out, const redisReply *r)
{
    if (r->type == REDIS_REPLY_INTEGER)
    {
        out->status = 0;
        out->exists = (r->integer != 0);
    }
    else
    {
        out->status = kith_error_return(KITH_EIO);
        out->err_str = "unexpected EXISTS reply type";
    }
}

// ---------------------------------------------------------------------------
// helper: map a redisReply to a kith_state_reply_t
// ---------------------------------------------------------------------------
static void fill_reply_from_redis(kith_state_reply_t *out,
                                  const redisReply *r,
                                  const struct kith_state_cmd *cmd)
{
    out->user_data = cmd->user_data;
    out->value = nullptr;
    out->value_len = 0;
    out->exists = false;
    out->err_str = nullptr;

    if (r == nullptr)
    {
        out->status = kith_error_return(KITH_ECONNRESET);
        out->err_str = "connection lost";
        return;
    }

    if (r->type == REDIS_REPLY_ERROR)
    {
        out->status = kith_error_return(KITH_EIO);
        out->err_str = r->str;
        return;
    }

    switch (cmd->tag)
    {
        case KITH_STATE_CMD_SET:
            fill_set_reply(out, r);
            break;
        case KITH_STATE_CMD_GET:
            fill_get_reply(out, r);
            break;
        case KITH_STATE_CMD_DEL:
            fill_del_reply(out, r);
            break;
        case KITH_STATE_CMD_EXISTS:
            fill_exists_reply(out, r);
            break;
    }
}

// ---------------------------------------------------------------------------
// hiredis command reply callback
// ---------------------------------------------------------------------------
void kith_state_command_cb(redisAsyncContext *ac, void *reply, void *privdata)
{
    (void)ac;
    struct kith_state_cmd *cmd = privdata;
    kith_state_reply_t out;

    fill_reply_from_redis(&out, reply, cmd);

    cmd->callback(&out);

    // hiredis async owns the reply object and frees it after the callback
    // returns; only the per-command context is owned here.
    kith_free(cmd->state->allocator, cmd);
}

// ---------------------------------------------------------------------------
// connection failure — free the hiredis context, failing every pending command
// ---------------------------------------------------------------------------
// Top-level reactor contexts only (task drain, fd-callback entry and exit):
// hiredis frees the context inline only outside its own callbacks, and a
// nested redisAsyncFree leaves the caller dereferencing freed memory.
// redisAsyncFree fires every pending command with a NULL reply, which
// kith_state_command_cb maps to a -KITH_ECONNRESET reply.
static void state_fail_connection(struct kith_state_adapter *adapter)
{
    if (adapter->ac == nullptr)
    {
        return;
    }
    if (adapter->event_mask != 0u)
    {
        (void)kith_reactor_del(adapter->reactor, adapter->ac->c.fd);
        adapter->event_mask = 0u;
    }
    redisAsyncFree(adapter->ac);
    adapter->ac = nullptr;
}

// Record a failed reactor registration. Every nested failure site sits inside
// a hiredis entry call made from the submission task or a fd handler, and
// each of those post-checks the flag and frees the context at its own
// top-level exit; create checks it after wiring and fails instead.
static void state_registration_failed(struct kith_state_adapter *adapter, int rc)
{
    adapter->registration_failed = true;
    adapter->registration_rc = rc;
}

// ---------------------------------------------------------------------------
// reactor fd event handler
// ---------------------------------------------------------------------------
// The reactor keeps one callback per fd across add/mod, so this handler owns
// every readiness for the connection and routes each to its hiredis half.
// HUP and ERR arrive unsolicited and both halves must see them: the read half
// detects the closed peer, the write half the broken socket.
void kith_state_event_handler(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    struct kith_state_adapter *adapter = ctx;
    if (adapter->ac == nullptr)
    {
        return; // context freed mid-batch; a level-triggered refire can arrive
    }
    if (adapter->registration_failed)
    {
        state_fail_connection(adapter);
        return;
    }

    unsigned int fatal = events & ((unsigned int)KITH_REACTOR_HUP | (unsigned int)KITH_REACTOR_ERR);
    if (fatal != 0u || (events & (unsigned int)KITH_REACTOR_IN) != 0u)
    {
        redisAsyncHandleRead(adapter->ac);
    }
    if (adapter->ac == nullptr)
    {
        // The read half processed a disconnect and freed the context.
        return;
    }
    if (fatal != 0u || (events & (unsigned int)KITH_REACTOR_OUT) != 0u)
    {
        redisAsyncHandleWrite(adapter->ac);
    }
    if (adapter->registration_failed && adapter->ac != nullptr)
    {
        state_fail_connection(adapter);
    }
}

// ---------------------------------------------------------------------------
// adapter event callbacks — bridge hiredis ↔ reactor
// ---------------------------------------------------------------------------

static void recompute_events(struct kith_state_adapter *adapter)
{
    int fd = adapter->ac->c.fd;
    if (adapter->event_mask == 0u)
    {
        (void)kith_reactor_del(adapter->reactor, fd);
    }
    else
    {
        (void)kith_reactor_mod(adapter->reactor, fd, adapter->event_mask);
    }
}

void kith_state_adapter_add_read(void *privdata)
{
    struct kith_state_adapter *adapter = privdata;
    int fd = adapter->ac->c.fd;
    int rc;
    if (adapter->event_mask == 0u)
    {
        rc = kith_reactor_add(
            adapter->reactor, fd, KITH_REACTOR_IN, kith_state_event_handler, adapter);
    }
    else
    {
        rc = kith_reactor_mod(adapter->reactor, fd, adapter->event_mask | KITH_REACTOR_IN);
    }
    if (rc != 0)
    {
        state_registration_failed(adapter, rc);
        return;
    }
    adapter->event_mask |= (unsigned int)KITH_REACTOR_IN;
}

void kith_state_adapter_del_read(void *privdata)
{
    struct kith_state_adapter *adapter = privdata;
    adapter->event_mask &= ~(unsigned int)KITH_REACTOR_IN;
    recompute_events(adapter);
}

void kith_state_adapter_add_write(void *privdata)
{
    struct kith_state_adapter *adapter = privdata;
    int fd = adapter->ac->c.fd;
    int rc;
    if (adapter->event_mask == 0u)
    {
        rc = kith_reactor_add(
            adapter->reactor, fd, KITH_REACTOR_OUT, kith_state_event_handler, adapter);
    }
    else
    {
        rc = kith_reactor_mod(adapter->reactor, fd, adapter->event_mask | KITH_REACTOR_OUT);
    }
    if (rc != 0)
    {
        state_registration_failed(adapter, rc);
        return;
    }
    adapter->event_mask |= (unsigned int)KITH_REACTOR_OUT;
}

void kith_state_adapter_del_write(void *privdata)
{
    struct kith_state_adapter *adapter = privdata;
    adapter->event_mask &= ~(unsigned int)KITH_REACTOR_OUT;
    recompute_events(adapter);
}

void kith_state_adapter_cleanup(void *privdata)
{
    // The adapter is embedded in kith_state; hiredis does not own it.
    (void)privdata;
}

// ---------------------------------------------------------------------------
// hiredis connect / disconnect callbacks
// ---------------------------------------------------------------------------
void kith_state_connect_cb(const redisAsyncContext *ac, int status)
{
    struct kith_state_adapter *adapter = ac->ev.data;
    adapter->connected = (status == REDIS_OK);
}

void kith_state_disconnect_cb(const redisAsyncContext *ac, int status)
{
    struct kith_state_adapter *adapter = ac->ev.data;
    (void)status;
    adapter->connected = false;
    if (adapter->event_mask != 0u)
    {
        (void)kith_reactor_del(adapter->reactor, ac->c.fd);
        adapter->event_mask = 0u;
    }
    // The context is freed after this callback returns; drop the dangling
    // pointer so subsequent submission tasks and handlers fail cleanly.
    adapter->ac = nullptr;
}

// ---------------------------------------------------------------------------
// helper: issue a redisAsyncCommand for a prepared command context
// ---------------------------------------------------------------------------
static int
dispatch_command(redisAsyncContext *ac, enum kith_state_cmd_tag tag, struct kith_state_cmd *cmd)
{
    switch (tag)
    {
        case KITH_STATE_CMD_SET:
            return redisAsyncCommand(ac,
                                     kith_state_command_cb,
                                     cmd,
                                     "SET %b %b",
                                     cmd->key,
                                     cmd->key_len,
                                     cmd->value,
                                     cmd->value_len);
        case KITH_STATE_CMD_GET:
            return redisAsyncCommand(
                ac, kith_state_command_cb, cmd, "GET %b", cmd->key, cmd->key_len);
        case KITH_STATE_CMD_DEL:
            return redisAsyncCommand(
                ac, kith_state_command_cb, cmd, "DEL %b", cmd->key, cmd->key_len);
        case KITH_STATE_CMD_EXISTS:
            return redisAsyncCommand(
                ac, kith_state_command_cb, cmd, "EXISTS %b", cmd->key, cmd->key_len);
    }
    return REDIS_ERR;
}

// ---------------------------------------------------------------------------
// submission task — issues the command on the reactor thread
// ---------------------------------------------------------------------------
static void state_submit_task(void *ctx)
{
    struct kith_state_cmd *cmd = ctx;
    struct kith_state_adapter *adapter = &cmd->state->adapter;

    if (adapter->registration_failed && adapter->ac != nullptr)
    {
        state_fail_connection(adapter);
    }
    // Reject only when the connection is in an error state. A pending connect
    // is not an error: hiredis async buffers commands until the connection is
    // established, so a command issued before the reactor completes the
    // connect is queued and delivered once the connection is up.
    if (adapter->ac == nullptr || adapter->ac->err != REDIS_OK)
    {
        kith_state_reply_t reply = {
            .status = kith_error_return(KITH_ECONNRESET),
            .err_str = "connection lost",
            .user_data = cmd->user_data,
        };
        cmd->callback(&reply);
        kith_free(cmd->state->allocator, cmd);
        return;
    }

    int rc = dispatch_command(adapter->ac, cmd->tag, cmd);
    if (rc != REDIS_OK)
    {
        kith_state_reply_t reply = {
            .status = kith_error_return(KITH_EIO),
            .err_str = "command dispatch failed",
            .user_data = cmd->user_data,
        };
        cmd->callback(&reply);
        kith_free(cmd->state->allocator, cmd);
        return;
    }
    if (adapter->registration_failed)
    {
        // The command was queued into hiredis before the registration failure
        // surfaced; freeing the context fires it with a NULL reply and the
        // reply callback frees the command block. The block is owned by
        // hiredis from here on.
        state_fail_connection(adapter);
    }
}

// ---------------------------------------------------------------------------
// helper: validate the call and queue the command for the reactor thread
// ---------------------------------------------------------------------------
static int state_command(kith_state_t *state,
                         enum kith_state_cmd_tag tag,
                         const void *key,
                         size_t key_len,
                         const void *value,
                         size_t value_len,
                         kith_state_reply_fn callback,
                         void *user_data)
{
    if (state == nullptr || key == nullptr || key_len == 0u || callback == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    // The command is issued on the reactor thread, so the key and value
    // copy into the command block at submit time — the caller's buffers are
    // only borrowed for the call. One allocation carries the command header
    // and the prefixed key and value bytes; the reply callback frees it.
    // The lengths are caller-supplied, so the size arithmetic is checked:
    // an overflow rejects the call instead of wrapping the allocation and
    // writing past a small block.
    size_t full_key_len = 0u;
    size_t block_len = 0u;
    if (ckd_add(&full_key_len, (size_t)state->key_prefix_len, key_len) ||
        ckd_add(&block_len, full_key_len, value_len) ||
        ckd_add(&block_len, block_len, sizeof(struct kith_state_cmd)))
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    struct kith_state_cmd *cmd = kith_alloc(state->allocator, block_len);
    if (cmd == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    cmd->tag = tag;
    cmd->callback = callback;
    cmd->user_data = user_data;
    cmd->state = state;
    cmd->key = (char *)cmd + sizeof(*cmd);
    cmd->key_len = full_key_len;
    if (state->key_prefix_len != 0u)
    {
        memcpy((char *)cmd->key, state->key_prefix, state->key_prefix_len);
    }
    memcpy((char *)cmd->key + state->key_prefix_len, key, key_len);
    cmd->value_len = value_len;
    if (value_len != 0u)
    {
        cmd->value = (char *)cmd->key + full_key_len;
        memcpy((char *)cmd->value, value, value_len);
    }
    else
    {
        cmd->value = nullptr;
    }

    if (kith_reactor_submit(state->adapter.reactor, state_submit_task, cmd) != 0)
    {
        kith_free(state->allocator, cmd);
        return kith_error_return(KITH_EBUSY);
    }
    return 0;
}

// ---------------------------------------------------------------------------
// helper: apply connection defaults from the public params
// ---------------------------------------------------------------------------
static void apply_connect_defaults(const kith_state_params_t *params,
                                   const char **out_host,
                                   uint16_t *out_port,
                                   uint32_t *out_timeout_ms)
{
    *out_host = (params->host != nullptr) ? params->host : "127.0.0.1";
    *out_port = (params->port != 0u) ? params->port : (uint16_t)KITH_STATE_DEFAULT_PORT;
    *out_timeout_ms =
        (params->timeout_ms != 0u) ? params->timeout_ms : KITH_STATE_DEFAULT_TIMEOUT_MS;
}

// ---------------------------------------------------------------------------
// helper: wire the hiredis async context to the state adapter
// ---------------------------------------------------------------------------
static void wire_state_adapter(kith_state_t *state,
                               kith_reactor_t *reactor,
                               redisAsyncContext *ac,
                               uint32_t timeout_ms)
{
    // wire the adapter into the state struct before any hiredis callback
    // can fire — redisAsyncSetConnectCallback internally calls addWrite,
    // which needs adapter->reactor and adapter->ac to be valid
    state->adapter.reactor = reactor;
    state->adapter.ac = ac;
    state->adapter.event_mask = 0u;
    state->adapter.connected = false;

    // set connect timeout
    struct timeval tv;
    tv.tv_sec = (time_t)(timeout_ms / 1000u);
    tv.tv_usec = (suseconds_t)((timeout_ms % 1000u) * 1000u);
    redisAsyncSetTimeout(ac, tv);

    // configure the event adapter
    ac->ev.data = &state->adapter;
    ac->ev.addRead = kith_state_adapter_add_read;
    ac->ev.delRead = kith_state_adapter_del_read;
    ac->ev.addWrite = kith_state_adapter_add_write;
    ac->ev.delWrite = kith_state_adapter_del_write;
    ac->ev.cleanup = kith_state_adapter_cleanup;

    // redisAsyncSetConnectCallback registers the socket for writability; a
    // failure there surfaces through the wiring flag instead of a deferred
    // repair, and create aborts on it.
    state->adapter.wiring = true;
    redisAsyncSetConnectCallback(ac, kith_state_connect_cb);
    state->adapter.wiring = false;
    redisAsyncSetDisconnectCallback(ac, kith_state_disconnect_cb);
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------
// Validate the caller-supplied params and allocator against this build's
// contract: params generation and size first, then the allocator's operation
// set. Returns a negative kith_error on failure.
static int state_check_create_args(const kith_state_params_t *params, const kith_allocator_t *alloc)
{
    if (params->size < sizeof(kith_state_params_t))
    {
        return kith_error_return(KITH_ESIZE);
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_state_create(const kith_state_params_t *params,
                                             kith_reactor_t *reactor,
                                             const kith_allocator_t *alloc,
                                             kith_state_t **out_state)
{
    if (params == nullptr || reactor == nullptr || out_state == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_state = nullptr;

    const int check_rc = state_check_create_args(params, alloc);
    if (check_rc != 0)
    {
        return check_rc;
    }
    const kith_allocator_t *allocator = alloc != nullptr ? alloc : kith_allocator_default();

    kith_state_t *state = kith_alloc_zero(allocator, 1, sizeof(*state));
    if (state == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    state->allocator = allocator;

    const char *host = nullptr;
    uint16_t port = 0u;
    uint32_t timeout_ms = 0u;
    apply_connect_defaults(params, &host, &port, &timeout_ms);

    // borrow the prefix pointers from params
    if (params->key_prefix != nullptr && params->key_prefix_len > 0u)
    {
        state->key_prefix = params->key_prefix;
        state->key_prefix_len = params->key_prefix_len;
    }

    redisAsyncContext *ac = redisAsyncConnect(host, (int)port);
    if (ac == nullptr)
    {
        kith_free(allocator, state);
        return kith_error_return(KITH_ENOMEM);
    }

    wire_state_adapter(state, reactor, ac, timeout_ms);

    // The connect callback registers the socket during wiring; a registration
    // failure there leaves a connection the reactor can never service, so
    // create fails instead of returning an unusable handle.
    if (state->adapter.registration_failed)
    {
        int rc = state->adapter.registration_rc;
        state_fail_connection(&state->adapter);
        kith_free(allocator, state);
        return rc;
    }

    *out_state = state;
    return 0;
}

KITH_API void kith_state_destroy(kith_state_t *state)
{
    if (state == nullptr)
    {
        return;
    }

    // deregister from reactor if still registered
    if (state->adapter.event_mask != 0u)
    {
        (void)kith_reactor_del(state->adapter.reactor, state->adapter.ac->c.fd);
        state->adapter.event_mask = 0u;
    }

    // the hiredis context is hiredis-allocated and stays outside the
    // allocator contract
    if (state->adapter.ac != nullptr)
    {
        redisAsyncFree(state->adapter.ac);
    }

    kith_free(state->allocator, state);
}

// ---------------------------------------------------------------------------
// key-value operations
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_state_set(kith_state_t *state,
                                          const void *key,
                                          size_t key_len,
                                          const void *value,
                                          size_t value_len,
                                          kith_state_reply_fn callback,
                                          void *user_data)
{
    return state_command(
        state, KITH_STATE_CMD_SET, key, key_len, value, value_len, callback, user_data);
}

[[nodiscard]] KITH_API int kith_state_get(kith_state_t *state,
                                          const void *key,
                                          size_t key_len,
                                          kith_state_reply_fn callback,
                                          void *user_data)
{
    return state_command(state, KITH_STATE_CMD_GET, key, key_len, nullptr, 0u, callback, user_data);
}

[[nodiscard]] KITH_API int kith_state_del(kith_state_t *state,
                                          const void *key,
                                          size_t key_len,
                                          kith_state_reply_fn callback,
                                          void *user_data)
{
    return state_command(state, KITH_STATE_CMD_DEL, key, key_len, nullptr, 0u, callback, user_data);
}

[[nodiscard]] KITH_API int kith_state_exists(kith_state_t *state,
                                             const void *key,
                                             size_t key_len,
                                             kith_state_reply_fn callback,
                                             void *user_data)
{
    return state_command(
        state, KITH_STATE_CMD_EXISTS, key, key_len, nullptr, 0u, callback, user_data);
}

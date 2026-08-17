/* Public handle and lifecycle for the server: params validation, create and
 * destroy, status transitions, and shutdown signaling. Delegates per-plane
 * construction to wiring.c and the net-to-gateway bridge to wire.c; the
 * public contract is include/kith/server/server.h. */

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "server/server_internal.h"

#include <netinet/in.h>
#include <sys/socket.h>

/*---------------------------------------------------------------------------
 * params validation
 *-------------------------------------------------------------------------*/

static bool server_config_validate(const kith_server_params_t *config, kith_error_t *out_err)
{
    if (config->size < sizeof(*config))
    {
        *out_err = KITH_ESIZE;
        return false;
    }
    if (config->abi_version != KITH_ABI_VERSION)
    {
        *out_err = KITH_EABIVER;
        return false;
    }
    *out_err = KITH_OK;
    return true;
}

/*---------------------------------------------------------------------------
 * lifecycle
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_server_create(const kith_server_params_t *config,
                                              const kith_allocator_t *alloc,
                                              kith_server_t **out_server)
{
    if (out_server == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_server = nullptr;

    kith_server_params_t resolved;
    if (config != nullptr)
    {
        kith_error_t err = KITH_OK;
        if (!server_config_validate(config, &err))
        {
            return kith_error_return(err);
        }
        resolved = *config;
    }
    else
    {
        memset(&resolved, 0, sizeof(resolved));
        resolved.size = sizeof(resolved);
        resolved.abi_version = KITH_ABI_VERSION;
    }

    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    kith_server_t *s = kith_alloc_zero(allocator, 1, sizeof(*s));
    if (s == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    s->allocator = allocator;
    int rc = server_resolve_config(s, &resolved);
    if (rc != 0)
    {
        kith_free(s->allocator, s);
        return rc;
    }
    atomic_init(&s->shutdown_requested, false);
    atomic_init(&s->status, KITH_SERVER_STATUS_CREATED);
    atomic_init(&s->run_in_flight, false);
    atomic_init(&s->tick_dropped, 0u);
    // The tick-handler registration mutex is initialized before
    // server_wiring_create so the create-failure path (which calls
    // server_wiring_destroy) and kith_server_destroy both balance it with a
    // pthread_mutex_destroy in server_wiring_destroy.
    (void)pthread_mutex_init(&s->tick_lock, nullptr);

    rc = server_wiring_create(s, &resolved);
    if (rc != 0)
    {
        server_wiring_destroy(s);
        kith_free(s->allocator, s);
        return rc;
    }
    *out_server = s;
    return 0;
}

KITH_API void kith_server_destroy(kith_server_t *server)
{
    if (server == nullptr)
    {
        return;
    }
    // Enforce the @thread_safety contract: destroying a server while its run
    // loop is in flight tears down the reactor and plane handles out from
    // under the run-loop thread. The shared-plane handles the wiring releases
    // (reactor, net, gateway, ...) have no internal synchronization against a
    // concurrent run, so the teardown is undefined behavior rather than a
    // clean failure. Abort so every consumer (C or Python) hits the same hard
    // stop instead of corrupting state silently.
    if (atomic_load_explicit(&server->run_in_flight, memory_order_acquire))
    {
        (void)kith_logger_log(server->logger,
                              KITH_LOG_LEVEL_ERROR,
                              __FILE__,
                              __LINE__,
                              nullptr,
                              0,
                              "kith_server_destroy called while the run loop is in flight");
        abort();
    }
    server_wiring_destroy(server);
    kith_free(server->allocator, server);
}

/*---------------------------------------------------------------------------
 * tick callback (reactor thread)
 *-------------------------------------------------------------------------*/

static void server_tick_cb(void *ctx)
{
    kith_server_t *s = (kith_server_t *)ctx;
    uint64_t now = kith_reactor_now_ms(s->reactor);

    // Poll observer first: a nonzero return requests the drain, and the
    // shutdown check below starts it on this tick instead of the next.
    if (s->poll_fn != nullptr && s->poll_fn(s->poll_user_data) != 0)
    {
        atomic_store_explicit(&s->shutdown_requested, true, memory_order_release);
    }

    if (atomic_load_explicit(&s->shutdown_requested, memory_order_acquire))
    {
        atomic_store_explicit(&s->status, KITH_SERVER_STATUS_DRAINING, memory_order_release);
        (void)server_tick_planes(s, now);
        (void)server_tick_arm(s);
        atomic_store_explicit(&s->status, KITH_SERVER_STATUS_STOPPED, memory_order_release);
        kith_reactor_stop(s->reactor);
        return;
    }

    (void)server_tick_planes(s, now);
    (void)server_tick_arm(s);

    // Advance the monotonic tick counter and dispatch the registered
    // per-tick game-logic callback. The counter
    // advances once per normal tick, independent of whether a callback is
    // registered; a Python-bound callback is submitted to the worker pool as
    // a single task, and its publishes are observed by the next
    // tick's gateway refresh.
    s->tick_count++;
    server_dispatch_tick_hook(s, s->tick_count);

    uint64_t next = now + s->tick_interval_ms;
    int rc = kith_reactor_schedule(s->reactor, next, server_tick_cb, s);
    if (rc != 0)
    {
        // A failed reschedule kills the tick chain silently unless the run
        // loop learns of it: log, stash the code for kith_server_run to
        // report, drain the planes once, and stop the loop. The wire re-arm
        // is deliberately skipped — it issues reactor operations through
        // the same ring that just refused the schedule.
        (void)kith_logger_log(
            s->logger,
            KITH_LOG_LEVEL_ERROR,
            __FILE__,
            __LINE__,
            nullptr,
            0,
            "next-tick schedule failed; draining the server and stopping the run");
        s->tick_schedule_rc = rc;
        atomic_store_explicit(&s->status, KITH_SERVER_STATUS_DRAINING, memory_order_release);
        (void)server_tick_planes(s, now);
        atomic_store_explicit(&s->status, KITH_SERVER_STATUS_STOPPED, memory_order_release);
        kith_reactor_stop(s->reactor);
        return;
    }
}

/*---------------------------------------------------------------------------
 * run / shutdown / status
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_server_run(kith_server_t *server)
{
    if (server == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    kith_server_status_t st = atomic_load_explicit(&server->status, memory_order_acquire);
    if (st != KITH_SERVER_STATUS_CREATED && st != KITH_SERVER_STATUS_STOPPED)
    {
        return kith_error_return(KITH_ESTATE);
    }

    // Claim the run slot. The ESTATE return above rejects a caller that races
    // with another run already holding the slot; from here on the flag is set
    // and every return path clears it so kith_server_destroy can enforce its
    // @thread_safety contract.
    atomic_store_explicit(&server->run_in_flight, true, memory_order_release);
    server->tick_schedule_rc = 0;

    if (atomic_load_explicit(&server->shutdown_requested, memory_order_acquire))
    {
        atomic_store_explicit(&server->status, KITH_SERVER_STATUS_STOPPED, memory_order_release);
        atomic_store_explicit(&server->run_in_flight, false, memory_order_release);
        return 0;
    }

    atomic_store_explicit(&server->status, KITH_SERVER_STATUS_RUNNING, memory_order_release);

    uint64_t now = kith_reactor_now_ms(server->reactor);
    int rc = kith_reactor_schedule(server->reactor, now, server_tick_cb, server);
    if (rc != 0)
    {
        atomic_store_explicit(&server->status, KITH_SERVER_STATUS_STOPPED, memory_order_release);
        atomic_store_explicit(&server->run_in_flight, false, memory_order_release);
        return rc;
    }

    rc = kith_reactor_run(server->reactor);

    atomic_store_explicit(&server->status, KITH_SERVER_STATUS_STOPPED, memory_order_release);
    atomic_store_explicit(&server->run_in_flight, false, memory_order_release);
    // A tick-schedule failure stopped the loop from inside; its code is the
    // cause and reports ahead of the reactor's own return.
    if (server->tick_schedule_rc != 0)
    {
        return server->tick_schedule_rc;
    }
    return rc;
}

[[nodiscard]] KITH_API int kith_server_shutdown(kith_server_t *server)
{
    if (server == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    kith_server_status_t st = atomic_load_explicit(&server->status, memory_order_acquire);
    atomic_store_explicit(&server->shutdown_requested, true, memory_order_release);

    if (st == KITH_SERVER_STATUS_CREATED)
    {
        atomic_store_explicit(&server->status, KITH_SERVER_STATUS_STOPPED, memory_order_release);
    }
    return 0;
}

KITH_API kith_server_status_t kith_server_status(const kith_server_t *server)
{
    if (server == nullptr)
    {
        return KITH_SERVER_STATUS_CREATED;
    }
    return atomic_load_explicit(&server->status, memory_order_acquire);
}

KITH_API uint16_t kith_server_listen_port(const kith_server_t *server)
{
    if (server == nullptr)
    {
        return 0u;
    }
    int fd = kith_net_listener_fd(server->net);
    if (fd < 0)
    {
        return 0u;
    }
    struct sockaddr_storage addr = {};
    socklen_t len = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &len) != 0)
    {
        return 0u;
    }
    if (addr.ss_family == AF_INET)
    {
        return ntohs(((const struct sockaddr_in *)&addr)->sin_port);
    }
    if (addr.ss_family == AF_INET6)
    {
        return ntohs(((const struct sockaddr_in6 *)&addr)->sin6_port);
    }
    return 0u;
}

KITH_API uint16_t kith_server_control_port(const kith_server_t *server)
{
    if (server == nullptr)
    {
        return 0u;
    }
#ifdef KITH_CONTROL_PLANE_ENABLED
    return kith_control_listen_port(server->control);
#else
    return 0u;
#endif
}

/*---------------------------------------------------------------------------
 * per-tick game-logic callback
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_server_register_tick_handler(kith_server_t *server,
                                                             kith_server_tick_fn fn,
                                                             void *user_data,
                                                             kith_server_handler_flag_t flags)
{
    if (server == nullptr || fn == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&server->tick_lock);
    server->tick_fn = fn;
    server->tick_user_data = user_data;
    server->tick_flags = flags;
    pthread_mutex_unlock(&server->tick_lock);
    return 0;
}

[[nodiscard]] KITH_API int kith_server_unregister_tick_handler(kith_server_t *server)
{
    if (server == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&server->tick_lock);
    server->tick_fn = nullptr;
    server->tick_user_data = nullptr;
    server->tick_flags = KITH_SERVER_HANDLER_NONE;
    pthread_mutex_unlock(&server->tick_lock);
    return 0;
}

[[nodiscard]] KITH_API int
kith_server_register_poll_observer(kith_server_t *server, kith_server_poll_fn fn, void *user_data)
{
    if (server == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    server->poll_fn = fn;
    server->poll_user_data = user_data;
    return 0;
}

[[nodiscard]] KITH_API int kith_server_tick_drops(const kith_server_t *server, uint64_t *out_drops)
{
    if (server == nullptr || out_drops == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_drops = atomic_load_explicit(&server->tick_dropped, memory_order_acquire);
    return 0;
}

/*---------------------------------------------------------------------------
 * borrowed plane accessors
 *
 * Each plane handle is owned by the server and fixed once the wiring
 * completes. These return borrowed references the caller uses to register
 * handlers, models, queries, and routes; the caller never destroys them.
 *-------------------------------------------------------------------------*/

KITH_API kith_gateway_t *kith_server_gateway(kith_server_t *server)
{
    return (server != nullptr) ? server->gateway : nullptr;
}

KITH_API kith_sim_t *kith_server_sim(kith_server_t *server)
{
    return (server != nullptr) ? server->sim : nullptr;
}

KITH_API kith_fabric_t *kith_server_fabric(kith_server_t *server)
{
    return (server != nullptr) ? server->fabric : nullptr;
}

KITH_API kith_proto_t *kith_server_proto(kith_server_t *server)
{
    return (server != nullptr) ? server->proto : nullptr;
}

KITH_API kith_control_t *kith_server_control(kith_server_t *server)
{
    return (server != nullptr) ? server->control : nullptr;
}

KITH_API kith_db_t *kith_server_db(kith_server_t *server)
{
    return (server != nullptr) ? server->db : nullptr;
}

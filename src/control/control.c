/* Public handle and listener for the control plane: params resolution, create
 * and destroy, the listening socket, accept, and the reactor drive loop.
 * Sibling .c files implement connections (conn.c), HTTP parsing
 * (http_parser.c), routing (router.c), built-in handlers (handlers.c), and
 * the SSE event bus (event_bus.c). */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>

#include "control/control_internal.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

// ---------------------------------------------------------------------------
// listener creation
// ---------------------------------------------------------------------------
// Resolve the params host string into an IPv4 bind address. NULL selects the
// loopback address: the control plane serves unauthenticated HTTP and stays
// local unless a deployer binds it out explicitly. A non-NULL value may be
// numeric or a host name; an unresolvable name fails the bind.
static int control_resolve_host(const char *host, struct in_addr *out)
{
    if (host == NULL)
    {
        out->s_addr = htonl(INADDR_LOOPBACK);
        return 0;
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *result = NULL;
    if (getaddrinfo(host, NULL, &hints, &result) != 0 || result == NULL)
    {
        return -1;
    }
    // ai_addr's alignment is only that of struct sockaddr: copy the address
    // out rather than casting the pointer up to sockaddr_in.
    struct sockaddr_in resolved;
    memcpy(&resolved, result->ai_addr, sizeof(resolved));
    *out = resolved.sin_addr;
    freeaddrinfo(result);
    return 0;
}

int control_listener_create(struct kith_control *ctrl)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(ctrl->port);
    if (control_resolve_host(ctrl->host, &addr.sin_addr) != 0)
    {
        return -1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }

    int opt = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(fd);
        return -1;
    }

    if (listen(fd, SOMAXCONN) < 0)
    {
        close(fd);
        return -1;
    }

    return fd;
}

// ---------------------------------------------------------------------------
// resolve params into the handle fields, applying defaults
// ---------------------------------------------------------------------------
static void resolve_params(struct kith_control *ctrl, const kith_control_params_t *p)
{
    ctrl->host = p->host;
    /* A port of 0 is passed through to the listener, which binds an
     * OS-assigned ephemeral port (no fixed default is applied here). */
    ctrl->port = p->port;
    ctrl->max_connections =
        (p->max_connections != 0u) ? p->max_connections : KITH_CONTROL_DEFAULT_MAX_CONNECTIONS;
    ctrl->read_buffer_cap =
        (p->read_buffer_cap != 0u) ? p->read_buffer_cap : KITH_CONTROL_DEFAULT_READ_BUF;
    ctrl->write_buffer_cap =
        (p->write_buffer_cap != 0u) ? p->write_buffer_cap : KITH_CONTROL_DEFAULT_WRITE_BUF;
    ctrl->sse_flush_ms =
        (p->sse_flush_ms != 0u) ? p->sse_flush_ms : KITH_CONTROL_DEFAULT_SSE_FLUSH_MS;
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------
// Validate @p params (NULL selects the size-versioned defaults). Returns 0,
// or the negated error the params carry.
static int control_params_ready(const kith_control_params_t *params,
                                kith_control_params_t *resolved)
{
    if (params != NULL)
    {
        if (params->size < sizeof(kith_control_params_t))
        {
            return kith_error_return(KITH_ESIZE);
        }
        if (params->abi_version != KITH_ABI_VERSION)
        {
            return kith_error_return(KITH_EABIVER);
        }
        *resolved = *params;
    }
    else
    {
        memset(resolved, 0, sizeof(*resolved));
        resolved->size = sizeof(*resolved);
        resolved->abi_version = KITH_ABI_VERSION;
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_control_create(const kith_control_params_t *params,
                                               kith_reactor_t *reactor,
                                               kith_logger_t *logger,
                                               kith_metrics_t *metrics,
                                               const kith_allocator_t *alloc,
                                               kith_control_t **out_ctrl)
{
    if (reactor == NULL || out_ctrl == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_ctrl = NULL;

    kith_control_params_t defaults;
    const int params_rc = control_params_ready(params, &defaults);
    if (params_rc != 0)
    {
        return params_rc;
    }
    params = &defaults;

    if (alloc != NULL)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != NULL) ? alloc : kith_allocator_default();

    struct kith_control *ctrl = kith_alloc_zero(allocator, 1, sizeof(*ctrl));
    if (ctrl == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    ctrl->allocator = allocator;
    ctrl->reactor = reactor;
    ctrl->logger = logger;
    ctrl->metrics = metrics;
    ctrl->workers = NULL;
    ctrl->listener_fd = -1;
    ctrl->started = false;

    resolve_params(ctrl, params);

    uint32_t bus_cap =
        (params->event_bus_cap != 0u) ? params->event_bus_cap : KITH_CONTROL_DEFAULT_EVENT_BUS_CAP;

    ctrl->conns =
        kith_alloc_zero(allocator, ctrl->max_connections, sizeof(struct kith_control_conn));
    if (ctrl->conns == NULL)
    {
        kith_free(allocator, ctrl);
        return kith_error_return(KITH_ENOMEM);
    }

    control_event_bus_init(&ctrl->event_bus, bus_cap, allocator);
    if (ctrl->event_bus.records == NULL)
    {
        kith_free(allocator, ctrl->conns);
        kith_free(allocator, ctrl);
        return kith_error_return(KITH_ENOMEM);
    }

    ctrl->route_count = 0u;
    int rc = control_handlers_register(ctrl);
    if (rc != 0)
    {
        control_event_bus_free(&ctrl->event_bus, allocator);
        kith_free(allocator, ctrl->conns);
        kith_free(allocator, ctrl);
        return rc;
    }

    ctrl->conn_count = 0u;

    *out_ctrl = ctrl;
    return 0;
}

KITH_API void kith_control_destroy(kith_control_t *ctrl)
{
    if (ctrl == NULL)
    {
        return;
    }

    kith_control_stop(ctrl);

    for (uint32_t i = 0u; i < ctrl->conn_count; i++)
    {
        struct kith_control_conn *conn = &ctrl->conns[i];
        if (conn->fd >= 0)
        {
            (void)kith_reactor_del(ctrl->reactor, conn->fd);
            close(conn->fd);
        }
        kith_free(ctrl->allocator, conn->read_buf);
        kith_free(ctrl->allocator, conn->write_buf);
        kith_free(ctrl->allocator, conn->header_scratch);
    }
    kith_free(ctrl->allocator, ctrl->conns);

    for (uint32_t i = 0u; i < ctrl->route_count; i++)
    {
        // routes have inline arrays (no heap allocs to free)
    }

    control_event_bus_free(&ctrl->event_bus, ctrl->allocator);
    kith_free(ctrl->allocator, ctrl);
}

// ---------------------------------------------------------------------------
// start / stop
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_control_start(kith_control_t *ctrl)
{
    if (ctrl == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (ctrl->started)
    {
        return kith_error_return(KITH_ESTATE);
    }

    ctrl->listener_fd = control_listener_create(ctrl);
    if (ctrl->listener_fd < 0)
    {
        return kith_error_return(KITH_EIO);
    }

    int rc = kith_reactor_add(
        ctrl->reactor, ctrl->listener_fd, KITH_REACTOR_IN, control_listener_event_handler, ctrl);
    if (rc != 0)
    {
        close(ctrl->listener_fd);
        ctrl->listener_fd = -1;
        return rc;
    }

    ctrl->started = true;

    // kith_reactor_schedule takes an absolute monotonic deadline; passing the
    // flush interval alone is always in the past and fires immediately.
    uint64_t const flush_deadline = kith_reactor_now_ms(ctrl->reactor) + ctrl->sse_flush_ms;
    (void)kith_reactor_schedule(ctrl->reactor, flush_deadline, control_sse_flush, ctrl);

    return 0;
}

KITH_API void kith_control_stop(kith_control_t *ctrl)
{
    if (ctrl == NULL || !ctrl->started)
    {
        return;
    }
    (void)kith_reactor_del(ctrl->reactor, ctrl->listener_fd);
    close(ctrl->listener_fd);
    ctrl->listener_fd = -1;
    ctrl->started = false;
}

KITH_API uint16_t kith_control_listen_port(const kith_control_t *ctrl)
{
    if (ctrl == NULL || !ctrl->started || ctrl->listener_fd < 0)
    {
        return 0u;
    }
    struct sockaddr_storage addr;
    memset(&addr, 0, sizeof(addr));
    socklen_t len = sizeof(addr);
    if (getsockname(ctrl->listener_fd, (struct sockaddr *)&addr, &len) != 0)
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

[[nodiscard]] KITH_API int kith_control_subscriber_count(const kith_control_t *ctrl,
                                                         uint32_t *out_count)
{
    if (ctrl == NULL || out_count == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_count = atomic_load_explicit(&ctrl->sse_subscribers, memory_order_acquire);
    return 0;
}

// ---------------------------------------------------------------------------
// route registry
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_control_register_route(kith_control_t *ctrl,
                                                       const kith_control_route_t *route)
{
    if (ctrl == NULL || route == NULL || route->method == NULL || route->path == NULL ||
        route->handler == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (ctrl->route_count >= CONTROL_MAX_ROUTES)
    {
        return kith_error_return(KITH_EBUSY);
    }

    struct kith_control_route_entry *e = &ctrl->routes[ctrl->route_count];
    strncpy(e->method, route->method, CONTROL_MAX_METHOD - 1u);
    e->method[CONTROL_MAX_METHOD - 1u] = '\0';
    strncpy(e->path, route->path, CONTROL_MAX_URL - 1u);
    e->path[CONTROL_MAX_URL - 1u] = '\0';
    e->handler = route->handler;
    e->ctx = route->ctx;
    e->flags = route->flags;
    ctrl->route_count++;
    return 0;
}

// ---------------------------------------------------------------------------
// worker pool attach
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_control_attach_worker_pool(kith_control_t *ctrl,
                                                           kith_worker_t *pool)
{
    if (ctrl == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    ctrl->workers = pool;
    return 0;
}

// ---------------------------------------------------------------------------
// event bus
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_control_publish_event(kith_control_t *ctrl,
                                                      const kith_control_event_t *event)
{
    if (ctrl == NULL || event == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    int rc = control_event_bus_publish(&ctrl->event_bus, event);
    if (rc != 0)
    {
        return kith_error_return(KITH_EBUSY);
    }
    return 0;
}

// ---------------------------------------------------------------------------
// response builder helpers
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_control_response_status(kith_control_response_t *resp,
                                                        int status_code,
                                                        const char *content_type)
{
    if (resp == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }

    const char *status_text;
    switch (status_code)
    {
        case 200:
            status_text = "OK";
            break;
        case 201:
            status_text = "Created";
            break;
        case 204:
            status_text = "No Content";
            break;
        case 400:
            status_text = "Bad Request";
            break;
        case 404:
            status_text = "Not Found";
            break;
        case 405:
            status_text = "Method Not Allowed";
            break;
        case 500:
            status_text = "Internal Server Error";
            break;
        default:
            status_text = "OK";
            break;
    }

    int written;
    if (content_type != NULL)
    {
        written = snprintf((char *)resp->buf,
                           resp->cap,
                           "HTTP/1.1 %d %s\r\n"
                           "Content-Type: %s\r\n",
                           status_code,
                           status_text,
                           content_type);
    }
    else
    {
        written =
            snprintf((char *)resp->buf, resp->cap, "HTTP/1.1 %d %s\r\n", status_code, status_text);
    }

    if (written < 0 || (uint32_t)written >= resp->cap)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    resp->len = (uint32_t)written;
    return 0;
}

[[nodiscard]] KITH_API int
kith_control_response_header(kith_control_response_t *resp, const char *name, const char *value)
{
    if (resp == NULL || name == NULL || value == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }

    int written =
        snprintf((char *)resp->buf + resp->len, resp->cap - resp->len, "%s: %s\r\n", name, value);
    if (written < 0 || (uint32_t)written >= resp->cap - resp->len)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    resp->len += (uint32_t)written;
    return 0;
}

[[nodiscard]] KITH_API int
kith_control_response_body(kith_control_response_t *resp, const void *body, uint32_t body_len)
{
    if (resp == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }

    // write Content-Length + blank line + body
    int written = snprintf((char *)resp->buf + resp->len,
                           resp->cap - resp->len,
                           "Content-Length: %u\r\n\r\n",
                           body_len);
    if (written < 0 || (uint32_t)written >= resp->cap - resp->len)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    resp->len += (uint32_t)written;

    if (body != NULL && body_len > 0u)
    {
        if (resp->len + body_len > resp->cap)
        {
            return kith_error_return(KITH_EOVERFLOW);
        }
        memcpy(resp->buf + resp->len, body, body_len);
        resp->len += body_len;
    }

    return 0;
}

// ---------------------------------------------------------------------------
// canonical over-cap rejection
// ---------------------------------------------------------------------------
// Escape a route path for embedding in the rejection's JSON body. Quotes and
// backslashes double; control and non-ASCII bytes drop — they are not
// URL-legal, and a byte outside ASCII renders the body invalid JSON;
// truncation happens at whole-escape boundaries so the body stays valid
// JSON at any input length.
static void control_json_escape_route(const char *route, char *out, uint32_t out_cap)
{
    uint32_t n = 0u;
    for (const char *p = route; *p != '\0' && n + 2u < out_cap; p++)
    {
        unsigned char c = (unsigned char)*p;
        if (c < 0x20u || c > 0x7Eu)
        {
            continue;
        }
        if (c == '"' || c == '\\')
        {
            out[n++] = '\\';
        }
        out[n++] = (char)c;
    }
    out[n] = '\0';
}

[[nodiscard]] KITH_API int kith_control_reject_overflow(kith_control_t *ctrl,
                                                        kith_control_response_t *resp,
                                                        const char *route,
                                                        uint32_t attempted)
{
    if (resp == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }

    // The over-cap event is counted whether or not the diagnostic body
    // itself fits: the degenerate rejection (nothing fits) is the most
    // severe reading of the counter.
    if (ctrl != NULL)
    {
        (void)kith_metrics_counter_add(
            ctrl->metrics, "kith_control_response_overflow_total", nullptr, 0u, 1u);
    }

    resp->len = 0u;

    char escaped[512];
    // The escape at most doubles a 511-byte route; the key framing adds 12.
    char route_part[528];
    if (route != NULL)
    {
        control_json_escape_route(route, escaped, sizeof(escaped));
        (void)snprintf(route_part, sizeof(route_part), ",\"route\":\"%s\"", escaped);
    }
    else
    {
        route_part[0] = '\0';
    }

    char size_part[48];
    if (attempted != 0u)
    {
        (void)snprintf(
            size_part, sizeof(size_part), ",\"attempted\":%u,\"cap\":%u", attempted, resp->cap);
    }
    else
    {
        (void)snprintf(size_part, sizeof(size_part), ",\"cap\":%u", resp->cap);
    }

    // Bounded: the escaped route is capped at 511 bytes and the size keys
    // at 47, so the framing never truncates at these buffer sizes.
    char body[640];
    int body_len = snprintf(
        body, sizeof(body), "{\"error\":\"response_too_large\"%s%s}", route_part, size_part);
    if (body_len < 0 || (size_t)body_len >= sizeof(body))
    {
        resp->len = 0u;
        return kith_error_return(KITH_EOVERFLOW);
    }

    int written = snprintf((char *)resp->buf,
                           resp->cap,
                           "HTTP/1.1 500 Internal Server Error\r\n"
                           "Content-Type: application/json\r\n"
                           "Content-Length: %u\r\n"
                           "\r\n"
                           "%s",
                           (unsigned)body_len,
                           body);
    if (written < 0 || (uint32_t)written >= resp->cap)
    {
        resp->len = 0u;
        return kith_error_return(KITH_EOVERFLOW);
    }
    resp->len = (uint32_t)written;
    return 0;
}

/* Built-in control-plane routes (/health, /metrics, /events/stream,
 * /logs/stream) and their handler functions. handlers_register installs them
 * into the route table owned by control.c; each handler builds a response
 * through the public kith_control_response_* API. */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include "control/control_internal.h"
#include "kith/types.h"

// ---------------------------------------------------------------------------
// built-in route registration
// ---------------------------------------------------------------------------
int control_handlers_register(struct kith_control *ctrl)
{
    kith_control_route_t routes[] = {
        {"GET", "/health", control_handler_health, ctrl, 0},
        {"GET", "/metrics", control_handler_metrics, ctrl, 0},
        {"GET", "/events/stream", control_handler_events_stream, ctrl, 0},
        {"GET", "/logs/stream", control_handler_logs_stream, ctrl, 0},
    };

    for (size_t i = 0; i < sizeof(routes) / sizeof(routes[0]); i++)
    {
        if (ctrl->route_count >= CONTROL_MAX_ROUTES)
        {
            return kith_error_return(KITH_EBUSY);
        }
        struct kith_control_route_entry *e = &ctrl->routes[ctrl->route_count];
        strncpy(e->method, routes[i].method, CONTROL_MAX_METHOD - 1u);
        e->method[CONTROL_MAX_METHOD - 1u] = '\0';
        strncpy(e->path, routes[i].path, CONTROL_MAX_URL - 1u);
        e->path[CONTROL_MAX_URL - 1u] = '\0';
        e->handler = routes[i].handler;
        e->ctx = routes[i].ctx;
        ctrl->route_count++;
    }
    return 0;
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------
int control_handler_health(const kith_control_request_t *req,
                           kith_control_response_t *resp,
                           void *ctx)
{
    (void)req;
    (void)ctx;

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t now_ns = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;

    char body[256];
    int body_len = snprintf(
        body, sizeof(body), "{\"status\":\"ok\",\"uptime_ns\":%llu}", (unsigned long long)now_ns);
    if (body_len < 0)
    {
        return 1;
    }

    (void)kith_control_response_status(resp, 200, "application/json");
    (void)kith_control_response_body(resp, body, (uint32_t)body_len);
    return 0;
}

// ---------------------------------------------------------------------------
// GET /metrics — Prometheus text exposition
// ---------------------------------------------------------------------------
int control_handler_metrics(const kith_control_request_t *req,
                            kith_control_response_t *resp,
                            void *ctx)
{
    (void)req;
    struct kith_control *ctrl = ctx;

    if (ctrl == NULL || ctrl->metrics == NULL)
    {
        const char *empty = "";
        (void)kith_control_response_status(resp, 200, "text/plain; version=0.0.4");
        (void)kith_control_response_body(resp, empty, 0u);
        return 0;
    }

    size_t needed = kith_metrics_render_prometheus(ctrl->metrics, NULL, 0u);
    // The status prefix renders between the buffer start and the payload:
    // both must fit or the Content-Length disagrees with the body it
    // announces (and the render runs past the buffer).
    int prefix = snprintf((char *)resp->buf,
                          resp->cap,
                          "HTTP/1.1 200 OK\r\n"
                          "Content-Type: text/plain; version=0.0.4\r\n"
                          "Content-Length: %zu\r\n\r\n",
                          needed);
    if (prefix < 0 || (uint32_t)prefix >= resp->cap ||
        (uint64_t)prefix + (uint64_t)needed > (uint64_t)resp->cap)
    {
        (void)kith_control_reject_overflow(ctrl, resp, "/metrics", (uint32_t)needed);
        return 0;
    }

    resp->len = (uint32_t)prefix;
    resp->len += (uint32_t)kith_metrics_render_prometheus(
        ctrl->metrics, (char *)resp->buf + resp->len, resp->cap - resp->len);
    return 0;
}

// ---------------------------------------------------------------------------
// GET /events/stream — SSE event stream
// ---------------------------------------------------------------------------
int control_handler_events_stream(const kith_control_request_t *req,
                                  kith_control_response_t *resp,
                                  void *ctx)
{
    (void)req;
    (void)ctx;

    const char *header = "HTTP/1.1 200 OK\r\n"
                         "Content-Type: text/event-stream\r\n"
                         "Cache-Control: no-cache\r\n"
                         "Connection: keep-alive\r\n\r\n";
    // A write buffer too small even for the stream headers is a broken
    // configuration: the connection closes empty rather than overflowing.
    const size_t header_len = strlen(header);
    if (header_len > resp->cap)
    {
        return 0;
    }
    memcpy(resp->buf, header, header_len);
    resp->len = (uint32_t)header_len;
    resp->sse = true;
    return 0;
}

// ---------------------------------------------------------------------------
// GET /logs/stream — SSE log stream
// ---------------------------------------------------------------------------
int control_handler_logs_stream(const kith_control_request_t *req,
                                kith_control_response_t *resp,
                                void *ctx)
{
    (void)req;
    (void)ctx;

    const char *header = "HTTP/1.1 200 OK\r\n"
                         "Content-Type: text/event-stream\r\n"
                         "Cache-Control: no-cache\r\n"
                         "Connection: keep-alive\r\n\r\n";
    const size_t header_len = strlen(header);
    if (header_len > resp->cap)
    {
        return 0;
    }
    memcpy(resp->buf, header, header_len);
    resp->len = (uint32_t)header_len;
    resp->sse = true;
    return 0;
}

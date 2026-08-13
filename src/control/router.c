/* Route table and path-pattern matching for the control plane: registers
 * routes, matches a parsed request's method and path (with :param segments)
 * against the table, and builds the public request view. The matched entry
 * aliases stable storage in the handle so worker threads can read it. */

#include <stdio.h>
#include <string.h>

#include "control/control_internal.h"
#include "kith/types.h"

// ---------------------------------------------------------------------------
// match a path pattern against a request URL
// ---------------------------------------------------------------------------
static bool match_path(const char *route_path, const char *req_url)
{
    const char *rp = route_path;
    const char *ru = req_url;

    // The request URL may carry a query string ('?...'); the route pattern
    // matches the path portion only, so '?' terminates the request side the
    // way '\0' does.
    while (*rp != '\0' && *ru != '\0' && *ru != '?')
    {
        if (*rp == ':' && rp[1] >= 'a' && rp[1] <= 'z')
        {
            while (*ru != '\0' && *ru != '/' && *ru != '?')
            {
                ru++;
            }
            while (*rp != '\0' && *rp != '/')
            {
                rp++;
            }
            continue;
        }

        if (*rp != *ru)
        {
            return false;
        }
        rp++;
        ru++;
    }

    return *rp == '\0' && (*ru == '\0' || *ru == '?');
}

static const char *method_string(kith_control_method_t method)
{
    switch (method)
    {
        case KITH_CONTROL_METHOD_GET:
            return "GET";
        case KITH_CONTROL_METHOD_POST:
            return "POST";
        case KITH_CONTROL_METHOD_PUT:
            return "PUT";
        case KITH_CONTROL_METHOD_DELETE:
            return "DELETE";
        case KITH_CONTROL_METHOD_UNKNOWN:
            return "UNKNOWN";
    }
    return "UNKNOWN";
}

// Build the public kith_control_request_t view onto the connection's parsed
// request buffer. The view aliases conn storage; the caller (inline or
// worker) must not outlive the connection's read buffer.
static void build_request(struct kith_control_conn *conn, kith_control_request_t *req)
{
    req->method = conn->req_method;
    req->path = conn->req_path;
    req->headers = conn->req_headers;
    req->header_count = conn->req_header_count;
    req->body = conn->req_body;
    req->body_len = conn->req_body_len;
}

// Find the first route matching the connection's parsed method+path, or
// NULL when no route matches. The returned entry aliases stable storage
// inside @p ctrl->routes (valid for the control handle's lifetime), so it
// may be read by a worker thread after the reactor thread has moved on.
struct kith_control_route_entry *control_router_match(struct kith_control *ctrl,
                                                      struct kith_control_conn *conn)
{
    const char *method_str = method_string(conn->req_method);
    for (uint32_t i = 0u; i < ctrl->route_count; i++)
    {
        struct kith_control_route_entry *r = &ctrl->routes[i];
        if (strcmp(r->method, method_str) != 0)
        {
            continue;
        }
        if (match_path(r->path, conn->req_path))
        {
            return r;
        }
    }
    return nullptr;
}

// Write a 404 Not Found response into conn->resp (no route matched).
void control_router_write_404(struct kith_control_conn *conn)
{
    const char *body = "{\"error\":\"not_found\"}";
    int written = snprintf((char *)conn->resp.buf,
                           conn->resp.cap,
                           "HTTP/1.1 404 Not Found\r\n"
                           "Content-Type: application/json\r\n"
                           "Content-Length: %zu\r\n\r\n%s",
                           strlen(body),
                           body);
    // A write buffer too small even for the response is a broken
    // configuration: publish nothing rather than let len exceed the cap.
    conn->resp.len = (written < 0 || (uint32_t)written >= conn->resp.cap) ? 0u : (uint32_t)written;
}

// Write a 501 Not Implemented response into conn->resp (unsupported
// protocol upgrade requested).
void control_router_write_501(struct kith_control_conn *conn)
{
    const char *body = "{\"error\":\"not_implemented\"}";
    int written = snprintf((char *)conn->resp.buf,
                           conn->resp.cap,
                           "HTTP/1.1 501 Not Implemented\r\n"
                           "Content-Type: application/json\r\n"
                           "Content-Length: %zu\r\n\r\n%s",
                           strlen(body),
                           body);
    conn->resp.len = (written < 0 || (uint32_t)written >= conn->resp.cap) ? 0u : (uint32_t)written;
}

// Write a 503 Service Unavailable response into conn->resp (a Python-bound
// route cannot dispatch off the reactor thread).
void control_router_write_503(struct kith_control_conn *conn)
{
    const char *body = "{\"error\":\"unavailable\"}";
    int written = snprintf((char *)conn->resp.buf,
                           conn->resp.cap,
                           "HTTP/1.1 503 Service Unavailable\r\n"
                           "Content-Type: application/json\r\n"
                           "Content-Length: %zu\r\n\r\n%s",
                           strlen(body),
                           body);
    // A write buffer too small even for the status line is a broken
    // configuration: publish nothing rather than let len exceed the cap.
    conn->resp.len = (written < 0 || (uint32_t)written >= conn->resp.cap) ? 0u : (uint32_t)written;
}

// Dispatch a parsed request to the first matching route. Writes the HTTP
// response into conn->resp for inline (C) handlers and no-match 404, and
// hands the inline handler's own return value to *out_handler_rc (0 for a
// 404). Returns:
//   0 — an inline handler ran (or a 404 was written); the caller flushes
//       the response, or closes the connection with the response discarded
//       when the handler reported non-zero.
//   1 — the matched route is Python-bound (KITH_CONTROL_ROUTE_PYTHON); the
//       handler is NOT invoked here. The caller (reactor thread) submits
//       conn to the pool; with no pool attached, or when the pool's queue
//       is exhausted, the caller answers 503 and counts the drop — the
//       reactor never runs a Python-bound handler inline.
// The route decision and the handler's own return value ride separate
// channels: the handler's close contract admits any non-zero value,
// including 1, so the deferral signal cannot share the handler's return
// namespace.
int control_router_dispatch(struct kith_control *ctrl,
                            struct kith_control_conn *conn,
                            int *out_handler_rc)
{
    *out_handler_rc = 0;
    struct kith_control_route_entry *r = control_router_match(ctrl, conn);
    if (r == nullptr)
    {
        control_router_write_404(conn);
        return 0;
    }
    // Python-bound route: defer off the reactor thread unconditionally.
    // The worker invokes r->handler and posts the flush back to the
    // reactor; the caller handles the no-pool and pool-full cases.
    if ((r->flags & KITH_CONTROL_ROUTE_PYTHON) != 0u)
    {
        return 1;
    }
    kith_control_request_t req;
    build_request(conn, &req);
    conn->resp.len = 0u;
    conn->resp.sse = false;
    *out_handler_rc = r->handler(&req, &conn->resp, r->ctx);
    return 0;
}

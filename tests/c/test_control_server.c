/* Loopback integration tests for the control plane server: a real reactor
 * drives the listener and per-connection sockets on a worker thread while
 * the test process acts as the HTTP client over an ephemeral port. Covers
 * the built-in routes (/health, /metrics, /events/stream, /logs/stream),
 * registered routes with :param segments and query strings, POST bodies,
 * method mismatch rejection, scrape rendering from a real metrics registry,
 * SSE event delivery, the WebSocket upgrade handshake, worker-pool
 * dispatch of Python-flagged routes, the inline handler close contract
 * (non-zero discards the response and closes), lifecycle guards (double
 * start, bind conflict), and destruction with a live connection. */

#include <errno.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <poll.h>
#include <pthread.h>
#include <unistd.h>

#include "kith/control/control.h"
#include "kith/metrics/metrics.h"
#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/version.h"
#include "kith/worker/worker.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "control server: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond)             failures += check_cond((cond), __LINE__)
#define CHECK_RC(cond, rc_expr) failures += check_cond((cond), __LINE__)

/* One started server instance: reactor + control handle (+ optional metrics
 * registry) driven by kith_reactor_run on a dedicated thread. Teardown order
 * follows the reactor contract: stop signals the loop, join quiesces the
 * thread, and only then are handles destroyed. */
struct server_stack
{
    kith_reactor_t *reactor;
    kith_control_t *control;
    kith_metrics_t *metrics;
    pthread_t thread;
    bool thread_live;
    _Atomic bool run_returned;
};

static void *reactor_thread_fn(void *raw)
{
    struct server_stack *stack = raw;
    (void)kith_reactor_run(stack->reactor);
    atomic_store_explicit(&stack->run_returned, true, memory_order_release);
    return nullptr;
}

/* The host charges io_uring ring memory against a bounded budget and
 * reclaims it asynchronously after each reactor is destroyed, so a scenario
 * loop that creates reactors back-to-back can transiently exhaust that
 * budget (the backend reports KITH_EIO). Bounded backoff rides out the
 * reclaim window without masking persistent backend failures. */
#define REACTOR_CREATE_ATTEMPTS 60u
#define REACTOR_CREATE_PAUSE_MS 25u

static int create_reactor_retry(struct server_stack *stack)
{
    int rc = 0;
    for (unsigned int attempt = 0u; attempt < REACTOR_CREATE_ATTEMPTS; attempt++)
    {
        rc = kith_reactor_create(nullptr, nullptr, &stack->reactor);
        if (rc != kith_error_return(KITH_EIO))
        {
            return rc;
        }
        struct timespec pause = {.tv_sec = 0, .tv_nsec = REACTOR_CREATE_PAUSE_MS * 1000000L};
        (void)nanosleep(&pause, nullptr);
    }
    return rc;
}

static int control_stack_create(struct server_stack *stack, const kith_control_params_t *params)
{
    memset(stack, 0, sizeof(*stack));
    int rc = create_reactor_retry(stack);
    if (rc != 0)
    {
        (void)fprintf(stderr, "control server: reactor create failed (%d)\n", rc);
        return -1;
    }
    rc = kith_control_create(params, stack->reactor, nullptr, nullptr, nullptr, &stack->control);
    if (rc != 0)
    {
        (void)fprintf(stderr, "control server: control create failed (%d)\n", rc);
        kith_reactor_destroy(stack->reactor);
        stack->reactor = nullptr;
        return -1;
    }
    atomic_init(&stack->run_returned, false);
    return 0;
}

/* Spawn the reactor loop thread and record its liveness: stack_stop joins
 * only threads recorded here, so a spawned-but-unrecorded thread races
 * the destroy calls that follow the stop. */
static int stack_spawn_thread(struct server_stack *stack)
{
    if (pthread_create(&stack->thread, nullptr, reactor_thread_fn, stack) != 0)
    {
        return -1;
    }
    stack->thread_live = true;
    return 0;
}

static int stack_start(struct server_stack *stack, const kith_control_params_t *params)
{
    if (control_stack_create(stack, params) != 0)
    {
        return -1;
    }
    if (kith_control_start(stack->control) != 0 || kith_control_listen_port(stack->control) == 0u ||
        stack_spawn_thread(stack) != 0)
    {
        kith_control_destroy(stack->control);
        kith_reactor_destroy(stack->reactor);
        memset(stack, 0, sizeof(*stack));
        return -1;
    }
    return 0;
}

static void stack_stop(struct server_stack *stack)
{
    /* Stop is the only entry point that is safe against a running loop, and
     * the join is the quiesce barrier: after it returns, no reactor callback
     * can touch the control handle, so destroying both is race-free. A
     * connection left open at this point is closed by kith_control_destroy.
     * The stop request takes effect at the run loop's next boundary — a
     * request landing before the loop enters returns it on the first check
     * — and the wait is bounded so a wedged loop cannot hang the join. */
    if (stack->reactor != nullptr)
    {
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 50L * 1000 * 1000};
        int waited_ms = 0;
        kith_reactor_stop(stack->reactor);
        while (!atomic_load_explicit(&stack->run_returned, memory_order_acquire) &&
               waited_ms < 5000)
        {
            (void)nanosleep(&pause, nullptr);
            waited_ms += 50;
        }
        if (stack->thread_live)
        {
            if (!atomic_load_explicit(&stack->run_returned, memory_order_acquire))
            {
                (void)fprintf(stderr, "control server: reactor loop did not stop within 5 s\n");
                exit(2);
            }
            (void)pthread_join(stack->thread, nullptr);
        }
    }
    kith_control_destroy(stack->control);
    kith_metrics_destroy(stack->metrics);
    kith_reactor_destroy(stack->reactor);
    memset(stack, 0, sizeof(*stack));
}

// ---------------------------------------------------------------------------
// blocking HTTP client
// ---------------------------------------------------------------------------

#define CLIENT_TIMEOUT_MS 2000

static int connect_port(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct timeval tv = {.tv_sec = CLIENT_TIMEOUT_MS / 1000, .tv_usec = 0};
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0)
    {
        int err = errno;
        if (err == EINTR)
        {
            /* An interrupted blocking connect keeps progressing
             * asynchronously: wait for writability and read the outcome
             * from SO_ERROR instead of failing the scenario. */
            struct pollfd pfd = {.fd = fd, .events = POLLOUT, .revents = 0};
            int ready;
            do
            {
                ready = poll(&pfd, 1, CLIENT_TIMEOUT_MS);
            } while (ready < 0 && errno == EINTR);
            err = ETIMEDOUT;
            if (ready > 0)
            {
                socklen_t err_len = sizeof(err);
                (void)getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len);
            }
        }
        if (err != 0)
        {
            (void)close(fd);
            return -1;
        }
    }
    return fd;
}

static int send_all(int fd, const char *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len)
    {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0)
        {
            if (n < 0 && errno == EINTR)
            {
                continue;
            }
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

static ssize_t recv_retry(int fd, char *buf, size_t len)
{
    ssize_t n;
    do
    {
        n = recv(fd, buf, len, 0);
    } while (n < 0 && errno == EINTR);
    return n;
}

/* Read one full HTTP response into @p resp: headers through the blank line,
 * then exactly Content-Length body bytes (the server closes non-SSE
 * connections after answering, so EOF before the declared length is an
 * error). Returns the total bytes buffered, or -1 on timeout/truncation. */
static ssize_t read_response(int fd, char *resp, size_t cap)
{
    size_t have = 0;
    char *body_start = nullptr;

    while (body_start == nullptr)
    {
        if (have + 1u >= cap)
        {
            return -1;
        }
        ssize_t n = recv_retry(fd, resp + have, cap - have - 1u);
        if (n <= 0)
        {
            return -1;
        }
        have += (size_t)n;
        resp[have] = '\0';
        char *sep = strstr(resp, "\r\n\r\n");
        if (sep != nullptr)
        {
            body_start = sep + 4;
        }
    }

    size_t header_len = (size_t)(body_start - resp);
    uint32_t body_len = 0u;
    const char *cl = strstr(resp, "Content-Length:");
    if (cl == nullptr || cl >= body_start)
    {
        return -1;
    }
    cl += strlen("Content-Length:");
    char *end = nullptr;
    errno = 0;
    unsigned long parsed = strtoul(cl, &end, 10);
    if (end == cl || errno == ERANGE || parsed > UINT32_MAX)
    {
        return -1;
    }
    body_len = (uint32_t)parsed;

    while (have < header_len + body_len)
    {
        if (have >= cap - 1u)
        {
            return -1;
        }
        ssize_t n = recv_retry(fd, resp + have, cap - have - 1u);
        if (n <= 0)
        {
            return -1;
        }
        have += (size_t)n;
        resp[have] = '\0';
    }
    return (ssize_t)have;
}

static ssize_t http_roundtrip(uint16_t port, const char *request, char *resp, size_t cap)
{
    int fd = connect_port(port);
    if (fd < 0)
    {
        return -1;
    }
    ssize_t total = -1;
    if (send_all(fd, request, strlen(request)) == 0)
    {
        total = read_response(fd, resp, cap);
    }
    (void)close(fd);
    return total;
}

/* Send one request, then read until the connection ends. Returns the number
 * of bytes received before the EOF (0 = the server answered nothing), -1 on
 * error or timeout. The close contract answers nothing, so 0 is the
 * contract-held reading. */
static ssize_t request_and_drain_to_eof(uint16_t port, const char *request)
{
    int fd = connect_port(port);
    if (fd < 0)
    {
        return -1;
    }
    ssize_t result = -1;
    if (send_all(fd, request, strlen(request)) == 0)
    {
        char buf[256];
        result = 0;
        while (true)
        {
            ssize_t n = recv_retry(fd, buf, sizeof(buf));
            if (n < 0)
            {
                result = -1;
                break;
            }
            if (n == 0)
            {
                break;
            }
            result += n;
        }
    }
    (void)close(fd);
    return result;
}

static int count_occurrences(const char *hay, const char *needle)
{
    int count = 0;
    const char *cursor = hay;
    while ((cursor = strstr(cursor, needle)) != nullptr)
    {
        count++;
        cursor++;
    }
    return count;
}

/* Read at least one chunk, then keep reading until @p needle appears
 * anywhere in @p buf or the socket times out; returns the total bytes
 * buffered, or -1. @p buf must be NUL-terminated on entry (pass a zeroed
 * buffer). */
static ssize_t read_until_contains(int fd, char *buf, size_t cap, const char *needle)
{
    size_t have = (size_t)strlen(buf);
    while (true)
    {
        if (have + 1u >= cap)
        {
            return -1;
        }
        ssize_t n = recv_retry(fd, buf + have, cap - have - 1u);
        if (n <= 0)
        {
            return -1;
        }
        have += (size_t)n;
        buf[have] = '\0';
        if (strstr(buf, needle) != nullptr)
        {
            return (ssize_t)have;
        }
    }
}

static kith_control_params_t base_params(void)
{
    kith_control_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.port = 0u; // OS-assigned ephemeral port per instance
    return params;
}

/* Build a started stack whose control handle carries an attached metrics
 * registry (the plain stack_start leaves the registry NULL). */
static int stack_start_with_metrics(struct server_stack *stack, const kith_control_params_t *params)
{
    memset(stack, 0, sizeof(*stack));
    kith_metrics_params_t mparams = {
        .size = sizeof(mparams),
        .abi_version = KITH_ABI_VERSION,
    };
    if (create_reactor_retry(stack) != 0 ||
        kith_metrics_create(&mparams, nullptr, &stack->metrics) != 0 ||
        kith_control_create(
            params, stack->reactor, nullptr, stack->metrics, nullptr, &stack->control) != 0)
    {
        stack_stop(stack);
        return -1;
    }
    if (kith_control_start(stack->control) != 0 || stack_spawn_thread(stack) != 0)
    {
        stack_stop(stack);
        return -1;
    }
    return 0;
}

// ---------------------------------------------------------------------------
// route handlers under test
// ---------------------------------------------------------------------------

struct echo_ctx
{
    char seen_path[128];
    uint32_t seen_body_len;
    char seen_body[256];
};

static int echo_handler(const kith_control_request_t *req, kith_control_response_t *resp, void *ctx)
{
    struct echo_ctx *seen = ctx;
    (void)snprintf(seen->seen_path, sizeof(seen->seen_path), "%s", req->path);
    seen->seen_body_len = req->body_len;
    if (req->body != nullptr && req->body_len < sizeof(seen->seen_body))
    {
        memcpy(seen->seen_body, req->body, req->body_len);
    }

    if (kith_control_response_status(resp, 200, "text/plain") != 0)
    {
        return 1;
    }
    return kith_control_response_body(resp, req->body, req->body_len);
}

static int item_handler(const kith_control_request_t *req, kith_control_response_t *resp, void *ctx)
{
    struct echo_ctx *seen = ctx;
    (void)snprintf(seen->seen_path, sizeof(seen->seen_path), "%s", req->path);
    if (kith_control_response_status(resp, 200, "text/plain") != 0)
    {
        return 1;
    }
    return kith_control_response_body(resp, "item", 4u);
}

struct py_route_ctx
{
    bool ran;
};

static int
py_task_handler(const kith_control_request_t *req, kith_control_response_t *resp, void *ctx)
{
    (void)req;
    struct py_route_ctx *seen = ctx;
    seen->ran = true;
    if (kith_control_response_status(resp, 200, "text/plain") != 0)
    {
        return 1;
    }
    return kith_control_response_body(resp, "dispatched", 10u);
}

/* Close-contract handler: writes a full 200 response and THEN reports the
 * ctx's rc, so a passing test proves the reported non-zero discarded what
 * the handler wrote. @p rc is fixed before the server starts, so the
 * reactor thread reads it without a race. */
struct close_route_ctx
{
    _Atomic uint32_t calls;
    pthread_t self;
    int rc;
};

static int
close_route_handler(const kith_control_request_t *req, kith_control_response_t *resp, void *ctx)
{
    struct close_route_ctx *seen = ctx;
    (void)req;
    atomic_fetch_add_explicit(&seen->calls, 1u, memory_order_relaxed);
    seen->self = pthread_self();
    (void)kith_control_response_status(resp, 200, "text/plain");
    (void)kith_control_response_body(resp, "discarded", 9u);
    return seen->rc;
}

// ---------------------------------------------------------------------------
// scenarios
// ---------------------------------------------------------------------------

// GET /health answers 200 JSON liveness; GET /metrics without an attached
// registry answers 200 with an empty payload.
static int test_builtin_routes(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(stack_start(&stack, &params) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[4096];
    CHECK(http_roundtrip(port, "GET /health HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strstr(resp, "application/json") != nullptr);
    CHECK(strstr(resp, "\"status\":\"ok\"") != nullptr);

    CHECK(http_roundtrip(port, "GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strstr(resp, "Content-Length: 0") != nullptr);

    stack_stop(&stack);
    return failures;
}

// With a real registry attached, /metrics renders the Prometheus exposition
// including observed series.
static int test_metrics_scrape(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(stack_start_with_metrics(&stack, &params) == 0);
    CHECK(kith_metrics_counter_add(stack.metrics, "kith_test_hits", nullptr, 0, 1) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[8192];
    CHECK(http_roundtrip(port, "GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strstr(resp, "kith_test_hits") != nullptr);

    stack_stop(&stack);
    return failures;
}

// A scrape whose payload fits the write buffer but whose status prefix
// does not leave room for it answers 500: rendering it writes past the
// buffer or announces a Content-Length the body cannot match.
static int test_metrics_prefix_overflows_cap(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    params.write_buffer_cap = 512u;
    CHECK(stack_start_with_metrics(&stack, &params) == 0);
    for (int i = 0; i < 11; i++)
    {
        char name[64];
        (void)snprintf(name, sizeof(name), "kith_test_prefix_edge_metric_%02d_value", i);
        CHECK(kith_metrics_counter_add(stack.metrics, name, nullptr, 0, 1) == 0);
    }
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[8192];
    CHECK(http_roundtrip(port, "GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "500 Internal Server Error") != nullptr);
    CHECK(strstr(resp, "{\"error\":\"response_too_large\",\"route\":\"/metrics\",\"attempted\":") !=
          nullptr);
    CHECK(strstr(resp, ",\"cap\":512}") != nullptr);

    stack_stop(&stack);
    return failures;
}

// A scrape larger than the write buffer answers 500 instead of truncating.
static int test_metrics_too_large(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    params.write_buffer_cap = 512u;
    CHECK(stack_start_with_metrics(&stack, &params) == 0);
    for (int i = 0; i < 32; i++)
    {
        char name[64];
        (void)snprintf(name, sizeof(name), "kith_test_overflow_metric_%02d_value", i);
        CHECK(kith_metrics_counter_add(stack.metrics, name, nullptr, 0, 1) == 0);
    }
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[8192];
    CHECK(http_roundtrip(port, "GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "500 Internal Server Error") != nullptr);
    CHECK(strstr(resp, "{\"error\":\"response_too_large\",\"route\":\"/metrics\",\"attempted\":") !=
          nullptr);
    CHECK(strstr(resp, ",\"cap\":512}") != nullptr);

    stack_stop(&stack);
    return failures;
}

// A C route whose listing exceeds the write buffer adopts the canonical
// rejection itself — the escape hatch for handlers holding the control
// handle. The response is the counted 500 naming the request path, and the
// counter series shows on the next scrape.
static int
oversize_route_handler(const kith_control_request_t *req, kith_control_response_t *resp, void *ctx)
{
    static uint8_t body[300000];
    (void)kith_control_response_status(resp, 200, "application/json");
    if (kith_control_response_body(resp, body, sizeof(body)) != 0)
    {
        (void)kith_control_reject_overflow(ctx, resp, req->path, sizeof(body));
    }
    return 0;
}

static int test_c_route_oversize_canonical_reject(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();

    memset(&stack, 0, sizeof(stack));
    kith_metrics_params_t mparams = {
        .size = sizeof(mparams),
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(create_reactor_retry(&stack) == 0);
    CHECK(kith_metrics_create(&mparams, nullptr, &stack.metrics) == 0);
    CHECK(kith_control_create(
              &params, stack.reactor, nullptr, stack.metrics, nullptr, &stack.control) == 0);

    kith_control_route_t route = {.method = "GET",
                                  .path = "/big",
                                  .handler = oversize_route_handler,
                                  .ctx = stack.control,
                                  .flags = KITH_CONTROL_ROUTE_NONE};
    CHECK(kith_control_register_route(stack.control, &route) == 0);
    CHECK(kith_control_start(stack.control) == 0);
    CHECK(stack_spawn_thread(&stack) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[1024];
    CHECK(http_roundtrip(port, "GET /big HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "500 Internal Server Error") != nullptr);
    CHECK(strstr(resp,
                 "{\"error\":\"response_too_large\",\"route\":\"/big\",\"attempted\":300000,"
                 "\"cap\":262144}") != nullptr);

    char scrape[8192];
    CHECK(http_roundtrip(port, "GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n", scrape, sizeof(scrape)) >
          0);
    CHECK(strstr(scrape, "kith_control_response_overflow_total 1") != nullptr);

    stack_stop(&stack);
    return failures;
}

// An unregistered path answers the JSON 404 body.
static int test_unknown_route_404(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(stack_start(&stack, &params) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[4096];
    CHECK(http_roundtrip(
              port, "GET /definitely-not-there HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) >
          0);
    CHECK(strstr(resp, "404 Not Found") != nullptr);
    CHECK(strstr(resp, "{\"error\":\"not_found\"}") != nullptr);

    stack_stop(&stack);
    return failures;
}

// A :param segment matches any single path piece, including when the request
// URL carries a query string; the handler sees the full request path.
static int test_param_route_and_query_string(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(control_stack_create(&stack, &params) == 0);

    struct echo_ctx seen = {0};
    kith_control_route_t route = {.method = "GET",
                                  .path = "/api/items/:id",
                                  .handler = item_handler,
                                  .ctx = &seen,
                                  .flags = KITH_CONTROL_ROUTE_NONE};
    CHECK(kith_control_register_route(stack.control, &route) == 0);
    CHECK(kith_control_start(stack.control) == 0);
    CHECK(stack_spawn_thread(&stack) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[4096];
    CHECK(http_roundtrip(
              port, "GET /api/items/42 HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strcmp(seen.seen_path, "/api/items/42") == 0);

    memset(&seen, 0, sizeof(seen));
    CHECK(http_roundtrip(
              port, "GET /api/items/42?verbose=1 HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) >
          0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strcmp(seen.seen_path, "/api/items/42?verbose=1") == 0);

    // A different method on the same path misses every GET route.
    memset(&seen, 0, sizeof(seen));
    CHECK(http_roundtrip(
              port, "POST /api/items/42 HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "404 Not Found") != nullptr);
    CHECK(seen.seen_path[0] == '\0');

    stack_stop(&stack);
    return failures;
}

// A POST body arrives intact in the handler and is echoed back verbatim.
static int test_post_echo(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(control_stack_create(&stack, &params) == 0);

    struct echo_ctx seen = {0};
    kith_control_route_t route = {.method = "POST",
                                  .path = "/echo",
                                  .handler = echo_handler,
                                  .ctx = &seen,
                                  .flags = KITH_CONTROL_ROUTE_NONE};
    CHECK(kith_control_register_route(stack.control, &route) == 0);
    CHECK(kith_control_start(stack.control) == 0);
    CHECK(stack_spawn_thread(&stack) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    const char *body = "{\"cmd\":\"run\",\"id\":7}";
    char req[512];
    (void)snprintf(req,
                   sizeof(req),
                   "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: %zu\r\n\r\n%s",
                   strlen(body),
                   body);

    char resp[4096];
    CHECK(http_roundtrip(port, req, resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strstr(resp, body) != nullptr);
    CHECK(seen.seen_body_len == strlen(body));
    CHECK(memcmp(seen.seen_body, body, strlen(body)) == 0);

    stack_stop(&stack);
    return failures;
}

// The SSE event stream answers with stream headers, keeps the connection
// open, and delivers published events as data lines on the periodic flush.
static int test_sse_event_stream(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    params.sse_flush_ms = 50u;
    CHECK(stack_start(&stack, &params) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    int fd = connect_port(port);
    CHECK(fd >= 0);
    char resp[4096] = {0};
    CHECK(send_all(fd,
                   "GET /events/stream HTTP/1.1\r\nHost: t\r\n\r\n",
                   strlen("GET /events/stream HTTP/1.1\r\nHost: t\r\n\r\n")) == 0);
    CHECK(read_until_contains(fd, resp, sizeof(resp), "\r\n\r\n") > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strstr(resp, "text/event-stream") != nullptr);

    // Publish only once the stream subscription is certainly active: a
    // subscriber counted by the accessor has its subscription cursor
    // recorded, so the events published after the poll are delivered to it.
    uint32_t subscribers = 0u;
    for (int spin = 0; spin < 2000 && subscribers == 0u; spin++)
    {
        CHECK(kith_control_subscriber_count(stack.control, &subscribers) == 0);
        if (subscribers == 0u)
        {
            struct timespec tick = {.tv_sec = 0, .tv_nsec = 1 * 1000000L};
            (void)nanosleep(&tick, nullptr);
        }
    }
    CHECK(subscribers >= 1u);
    for (int i = 0; i < 3; i++)
    {
        kith_control_event_t event = {
            .ts_mono_ns = (uint64_t)(i + 1) * 1000000u,
            .type = "kith.test.tick",
        };
        CHECK(kith_control_publish_event(stack.control, &event) == 0);
    }

    for (int spin = 0; spin < 20 && count_occurrences(resp, "data:") < 3; spin++)
    {
        if (read_until_contains(fd, resp, sizeof(resp), "data:") < 0)
        {
            break;
        }
    }
    CHECK(count_occurrences(resp, "data:") == 3);
    CHECK(strstr(resp, "\"type\":\"kith.test.tick\"") != nullptr);
    (void)close(fd);

    stack_stop(&stack);
    return failures;
}

// The log stream answers with the same stream headers as the event
// stream; only the headers are pinned here.
static int test_sse_logs_stream_headers(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(stack_start(&stack, &params) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    int fd = connect_port(port);
    CHECK(fd >= 0);
    char resp[1024] = {0};
    CHECK(send_all(fd,
                   "GET /logs/stream HTTP/1.1\r\nHost: t\r\n\r\n",
                   strlen("GET /logs/stream HTTP/1.1\r\nHost: t\r\n\r\n")) == 0);
    CHECK(read_until_contains(fd, resp, sizeof(resp), "\r\n\r\n") > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strstr(resp, "text/event-stream") != nullptr);
    (void)close(fd);

    stack_stop(&stack);
    return failures;
}

// A WebSocket upgrade requests frame-by-frame servicing this server does
// not implement, so the request is refused with 501 Not Implemented
// instead of a 101 handshake; the connection then ends after the response
// like any other exchange.
static int test_ws_upgrade_rejected(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(stack_start(&stack, &params) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    static const char request[] = "GET /ws HTTP/1.1\r\n"
                                  "Host: t\r\n"
                                  "Upgrade: websocket\r\n"
                                  "Connection: Upgrade\r\n"
                                  "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                                  "Sec-WebSocket-Version: 13\r\n\r\n";

    int fd = connect_port(port);
    CHECK(fd >= 0);
    char resp[1024] = {0};
    CHECK(send_all(fd, request, sizeof(request) - 1u) == 0);
    CHECK(read_until_contains(fd, resp, sizeof(resp), "\r\n\r\n") > 0);
    CHECK(strstr(resp, "501 Not Implemented") != nullptr);
    CHECK(strstr(resp, "{\"error\":\"not_implemented\"}") != nullptr);

    char eof_probe[64];
    ssize_t n;
    do
    {
        n = recv(fd, eof_probe, sizeof(eof_probe), 0);
    } while (n < 0 && errno == EINTR);
    CHECK(n == 0);
    (void)close(fd);

    stack_stop(&stack);
    return failures;
}

// A route flagged for Python dispatch runs on the attached worker pool:
// the reactor hands the request to a pool thread and flushes the handler
// response back on the reactor thread. The round-trip is observable end
// to end from the client.
static int test_python_route_worker_dispatch(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(control_stack_create(&stack, &params) == 0);

    kith_worker_params_t wparams = {0};
    wparams.size = sizeof(wparams);
    wparams.abi_version = KITH_ABI_VERSION;
    wparams.worker_count = 1u;
    kith_worker_t *workers = nullptr;
    CHECK(kith_worker_create(&wparams, nullptr, &workers) == 0);
    CHECK(kith_control_attach_worker_pool(stack.control, workers) == 0);

    struct py_route_ctx seen = {0};
    kith_control_route_t route = {.method = "GET",
                                  .path = "/py/task",
                                  .handler = py_task_handler,
                                  .ctx = &seen,
                                  .flags = KITH_CONTROL_ROUTE_PYTHON};
    CHECK(kith_control_register_route(stack.control, &route) == 0);
    CHECK(kith_control_start(stack.control) == 0);
    CHECK(stack_spawn_thread(&stack) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[4096];
    CHECK(http_roundtrip(port, "GET /py/task HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strstr(resp, "dispatched") != nullptr);

    // The handler flag is checked after both threads are joined below: the
    // worker is joined by the pool destroy, which orders its writes ahead
    // of the read.
    stack_stop(&stack);
    kith_worker_destroy(workers);
    CHECK(seen.ran);
    return failures;
}

// A Python-bound route with no pool attached answers 503 and counts the
// drop on the metrics registry; the handler never runs, because the
// reactor never invokes a Python-bound handler inline.
static int test_python_route_no_pool_503(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(stack_start_with_metrics(&stack, &params) == 0);

    struct py_route_ctx seen = {0};
    kith_control_route_t route = {.method = "GET",
                                  .path = "/py/unattached",
                                  .handler = py_task_handler,
                                  .ctx = &seen,
                                  .flags = KITH_CONTROL_ROUTE_PYTHON};
    CHECK(kith_control_register_route(stack.control, &route) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[4096];
    CHECK(http_roundtrip(
              port, "GET /py/unattached HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "503 Service Unavailable") != nullptr);
    CHECK(strstr(resp, "{\"error\":\"unavailable\"}") != nullptr);

    CHECK(http_roundtrip(port, "GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "kith_control_dispatch_dropped_total 1") != nullptr);

    stack_stop(&stack);
    CHECK(!seen.ran);
    return failures;
}

// An inline C handler reporting non-zero discards the response it wrote and
// closes the connection: the client answers EOF with zero bytes, the handler
// runs exactly once, and the deferral signal cannot fire — rc 1 must not be
// read as a Python-bound dispatch, so the drop counter stays untouched.
static int test_c_route_nonzero_closes_without_response(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(stack_start_with_metrics(&stack, &params) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    struct close_route_ctx rc_one;
    atomic_init(&rc_one.calls, 0u);
    rc_one.self = pthread_self();
    rc_one.rc = 1;
    struct close_route_ctx rc_neg;
    atomic_init(&rc_neg.calls, 0u);
    rc_neg.self = pthread_self();
    rc_neg.rc = -7;
    kith_control_route_t route_one = {.method = "GET",
                                      .path = "/close/one",
                                      .handler = close_route_handler,
                                      .ctx = &rc_one,
                                      .flags = KITH_CONTROL_ROUTE_NONE};
    kith_control_route_t route_neg = {.method = "GET",
                                      .path = "/close/neg",
                                      .handler = close_route_handler,
                                      .ctx = &rc_neg,
                                      .flags = KITH_CONTROL_ROUTE_NONE};
    CHECK(kith_control_register_route(stack.control, &route_one) == 0);
    CHECK(kith_control_register_route(stack.control, &route_neg) == 0);

    CHECK(request_and_drain_to_eof(port, "GET /close/one HTTP/1.1\r\nHost: t\r\n\r\n") == 0);
    CHECK(request_and_drain_to_eof(port, "GET /close/neg HTTP/1.1\r\nHost: t\r\n\r\n") == 0);
    CHECK(atomic_load_explicit(&rc_one.calls, memory_order_relaxed) == 1u);
    CHECK(atomic_load_explicit(&rc_neg.calls, memory_order_relaxed) == 1u);

    char resp[4096] = {0};
    CHECK(http_roundtrip(port, "GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "kith_control_dispatch_dropped_total") == nullptr);

    stack_stop(&stack);
    return failures;
}

// With a pool attached, an inline C route reporting non-zero still closes
// inline: the handler runs exactly once (the route decision and the
// handler's return ride separate channels, so a close report never
// re-dispatches the route onto the pool).
static int test_c_route_pool_close_runs_once(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(control_stack_create(&stack, &params) == 0);

    kith_worker_params_t wparams = {0};
    wparams.size = sizeof(wparams);
    wparams.abi_version = KITH_ABI_VERSION;
    wparams.worker_count = 1u;
    kith_worker_t *workers = nullptr;
    CHECK(kith_worker_create(&wparams, nullptr, &workers) == 0);
    CHECK(kith_control_attach_worker_pool(stack.control, workers) == 0);

    struct close_route_ctx seen;
    atomic_init(&seen.calls, 0u);
    seen.self = pthread_self();
    seen.rc = 1;
    kith_control_route_t route = {.method = "GET",
                                  .path = "/close/pooled",
                                  .handler = close_route_handler,
                                  .ctx = &seen,
                                  .flags = KITH_CONTROL_ROUTE_NONE};
    CHECK(kith_control_register_route(stack.control, &route) == 0);
    CHECK(kith_control_start(stack.control) == 0);
    CHECK(stack_spawn_thread(&stack) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    CHECK(request_and_drain_to_eof(port, "GET /close/pooled HTTP/1.1\r\nHost: t\r\n\r\n") == 0);

    // The drop counter reading rides a second connection after the close;
    // the invocation count is ordered by the stop join below.
    char resp[4096] = {0};
    CHECK(http_roundtrip(port, "GET /metrics HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "kith_control_dispatch_dropped_total") == nullptr);

    stack_stop(&stack);
    kith_worker_destroy(workers);
    CHECK(atomic_load_explicit(&seen.calls, memory_order_relaxed) == 1u);
    return failures;
}

// With a pool attached, a C route (no flags) still runs inline on the
// reactor thread and its zero return flushes the response — the pool is
// the Python-bound dispatch path only.
static int test_c_route_pool_zero_runs_inline(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(control_stack_create(&stack, &params) == 0);

    kith_worker_params_t wparams = {0};
    wparams.size = sizeof(wparams);
    wparams.abi_version = KITH_ABI_VERSION;
    wparams.worker_count = 1u;
    kith_worker_t *workers = nullptr;
    CHECK(kith_worker_create(&wparams, nullptr, &workers) == 0);
    CHECK(kith_control_attach_worker_pool(stack.control, workers) == 0);

    struct close_route_ctx seen;
    atomic_init(&seen.calls, 0u);
    seen.self = pthread_self();
    seen.rc = 0;
    kith_control_route_t route = {.method = "GET",
                                  .path = "/inline/pooled",
                                  .handler = close_route_handler,
                                  .ctx = &seen,
                                  .flags = KITH_CONTROL_ROUTE_NONE};
    CHECK(kith_control_register_route(stack.control, &route) == 0);
    CHECK(kith_control_start(stack.control) == 0);
    CHECK(stack_spawn_thread(&stack) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    char resp[4096] = {0};
    CHECK(http_roundtrip(
              port, "GET /inline/pooled HTTP/1.1\r\nHost: t\r\n\r\n", resp, sizeof(resp)) > 0);
    CHECK(strstr(resp, "200 OK") != nullptr);
    CHECK(strstr(resp, "discarded") != nullptr);

    pthread_t reactor_thread = stack.thread;
    stack_stop(&stack);
    kith_worker_destroy(workers);
    CHECK(atomic_load_explicit(&seen.calls, memory_order_relaxed) == 1u);
    CHECK(pthread_equal(seen.self, reactor_thread) != 0);
    return failures;
}

// Starting twice is rejected without disturbing the running listener.
static int test_double_start_estate(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(control_stack_create(&stack, &params) == 0);
    CHECK(kith_control_start(stack.control) == 0);
    CHECK_RC(kith_control_start(stack.control) == kith_error_return(KITH_ESTATE),
             kith_error_return(KITH_ESTATE));
    CHECK(kith_control_listen_port(stack.control) > 0u);
    kith_control_destroy(stack.control);
    kith_reactor_destroy(stack.reactor);
    return failures;
}

// Binding a port already held by another socket reports EIO from start.
static int test_bind_conflict_eio(void)
{
    int failures = 0;
    struct server_stack stack;

    int holder = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(holder >= 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0u;
    CHECK(bind(holder, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    CHECK(listen(holder, 1) == 0);
    socklen_t addr_len = sizeof(addr);
    CHECK(getsockname(holder, (struct sockaddr *)&addr, &addr_len) == 0);
    uint16_t held_port = ntohs(addr.sin_port);

    kith_control_params_t params = base_params();
    params.port = held_port;
    CHECK(control_stack_create(&stack, &params) == 0);
    CHECK_RC(kith_control_start(stack.control) == kith_error_return(KITH_EIO),
             kith_error_return(KITH_EIO));
    CHECK(kith_control_listen_port(stack.control) == 0u);

    kith_control_destroy(stack.control);
    kith_reactor_destroy(stack.reactor);
    (void)close(holder);
    return failures;
}

// Destroying the handle with a connection still open closes it: the client
// observes EOF once the teardown completes (the destroy loop deregisters and
// closes live per-connection fds).
static int test_destroy_closes_live_connection(void)
{
    int failures = 0;
    struct server_stack stack;
    kith_control_params_t params = base_params();
    CHECK(stack_start(&stack, &params) == 0);
    uint16_t port = kith_control_listen_port(stack.control);

    int fd = connect_port(port);
    CHECK(fd >= 0);
    /* A truncated request keeps the connection open in READING state: the
     * parser is waiting for the rest of the request line when teardown runs. */
    CHECK(send_all(fd,
                   "GET /health HTTP/1.1\r\nHost: t\r\n",
                   strlen("GET /health HTTP/1.1\r\nHost: t\r\n")) == 0);

    // Stop accepting and tear everything down while @p fd is still open.
    // stack_stop re-arms the stop because a stop landing before the loop
    // enters is erased by run's entry-clear.
    stack_stop(&stack);

    // The accepted connection was closed by the destroy loop; the socket
    // answers EOF rather than staying open forever.
    char drain[256];
    ssize_t n;
    do
    {
        n = recv(fd, drain, sizeof(drain), 0);
    } while (n < 0 && errno == EINTR);
    CHECK(n == 0 || (n < 0 && errno == ECONNRESET));
    (void)close(fd);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int rc = 0;
    rc |= test_builtin_routes();
    rc |= test_metrics_scrape();
    rc |= test_metrics_prefix_overflows_cap();
    rc |= test_metrics_too_large();
    rc |= test_c_route_oversize_canonical_reject();
    rc |= test_unknown_route_404();
    rc |= test_param_route_and_query_string();
    rc |= test_post_echo();
    rc |= test_sse_event_stream();
    rc |= test_sse_logs_stream_headers();
    rc |= test_ws_upgrade_rejected();
    rc |= test_python_route_worker_dispatch();
    rc |= test_python_route_no_pool_503();
    rc |= test_c_route_nonzero_closes_without_response();
    rc |= test_c_route_pool_close_runs_once();
    rc |= test_c_route_pool_zero_runs_inline();
    rc |= test_double_start_estate();
    rc |= test_bind_conflict_eio();
    rc |= test_destroy_closes_live_connection();
    if (rc != 0)
    {
        (void)fprintf(stderr, "control server tests FAILED\n");
    }
    return rc;
}

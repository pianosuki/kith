#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "control/control_internal.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond_ext(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "control http: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond_ext((cond), __LINE__)

// ---------------------------------------------------------------------------
// HTTP parser tests
// ---------------------------------------------------------------------------

static struct kith_control_conn *make_conn(void)
{
    struct kith_control_conn *conn = calloc(1, sizeof(*conn));
    conn->read_cap = 4096;
    conn->read_buf = malloc(conn->read_cap);
    conn->write_cap = 8192;
    conn->write_buf = malloc(conn->write_cap);
    conn->header_scratch_cap =
        CONTROL_MAX_HEADERS * (CONTROL_MAX_HEADER_NAME + CONTROL_MAX_HEADER_VALUE);
    conn->header_scratch = malloc(conn->header_scratch_cap);
    conn->resp.buf = conn->write_buf;
    conn->resp.cap = conn->write_cap;
    conn->fd = -1;
    conn->state = KITH_CONTROL_CONN_READING;
    return conn;
}

/* Variant with a larger read buffer for scenarios whose request exceeds
 * the default 4 KiB (a max-length body, for example). */
static struct kith_control_conn *make_conn_sized(uint32_t read_cap)
{
    struct kith_control_conn *conn = make_conn();
    free(conn->read_buf);
    conn->read_cap = read_cap;
    conn->read_buf = malloc(conn->read_cap);
    return conn;
}

static void free_conn(struct kith_control_conn *conn)
{
    free(conn->read_buf);
    free(conn->write_buf);
    free(conn->header_scratch);
    free(conn);
}

static void feed_len(struct kith_control_conn *conn, const char *data, size_t len)
{
    memcpy(conn->read_buf + conn->read_pos, data, len);
    conn->read_pos += (uint32_t)len;
}

static void feed(struct kith_control_conn *conn, const char *data)
{
    feed_len(conn, data, strlen(data));
}

static int test_parse_get(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn,
         "GET /health HTTP/1.1\r\n"
         "Host: localhost\r\n"
         "Connection: keep-alive\r\n"
         "\r\n");

    int rc = control_http_parser_consume(conn);
    CHECK(rc == 0);
    CHECK(conn->state == KITH_CONTROL_CONN_RESPONDING);
    CHECK(conn->req_method == KITH_CONTROL_METHOD_GET);
    CHECK(strcmp(conn->req_path, "/health") == 0);
    CHECK(conn->req_header_count == 2);

    free_conn(conn);
    return failures;
}

static int test_parse_post_with_body(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn,
         "POST /api/v1/command HTTP/1.1\r\n"
         "Content-Length: 13\r\n"
         "\r\n"
         "{\"cmd\":\"run\"}");

    int rc = control_http_parser_consume(conn);
    CHECK(rc == 0);
    CHECK(conn->req_method == KITH_CONTROL_METHOD_POST);
    CHECK(strcmp(conn->req_path, "/api/v1/command") == 0);
    CHECK(conn->req_body_len == 13);
    CHECK(conn->req_body != NULL);

    free_conn(conn);
    return failures;
}

static int test_parse_partial(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn, "GET /health HT");
    int rc = control_http_parser_consume(conn);
    CHECK(rc == 1);
    CHECK(conn->state == KITH_CONTROL_CONN_READING);

    free_conn(conn);
    return failures;
}

static int test_parse_reset(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn, "GET /a HTTP/1.1\r\n\r\n");
    CHECK(control_http_parser_consume(conn) == 0);
    control_http_parser_reset(conn);
    CHECK(conn->state == KITH_CONTROL_CONN_READING);
    CHECK(conn->read_pos == 0u);
    CHECK(conn->req_header_count == 0u);

    free_conn(conn);
    return failures;
}

// A request line missing its spaces, its URL, or the LF after the CR is a
// hard parse error: consume reports -1 and moves the connection to CLOSING.
static int test_parse_bad_request_line(void)
{
    int failures = 0;
    static const char *vectors[] = {
        "BADLINE\r\n\r\n",           // no spaces at all
        "GET\r\n\r\n",               // method only, no URL
        "GET /x\r\n\r\n",            // no version segment
        "GET /x HTTP/1.1\rX\r\n\r\n" // CR not followed by LF
    };
    for (size_t i = 0u; i < sizeof(vectors) / sizeof(vectors[0]); i++)
    {
        struct kith_control_conn *conn = make_conn();
        feed(conn, vectors[i]);
        CHECK(control_http_parser_consume(conn) == -1);
        CHECK(conn->state == KITH_CONTROL_CONN_CLOSING);
        free_conn(conn);
    }
    return failures;
}

// The method token is capped one below CONTROL_MAX_METHOD: seven unknown
// characters still parse (as UNKNOWN), eight are rejected.
static int test_parse_method_length_cap(void)
{
    int failures = 0;

    struct kith_control_conn *conn = make_conn();
    feed(conn, "GETTERS /x HTTP/1.1\r\n\r\n");
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_method == KITH_CONTROL_METHOD_UNKNOWN);
    free_conn(conn);

    conn = make_conn();
    feed(conn, "GETTERXX /x HTTP/1.1\r\n\r\n");
    CHECK(control_http_parser_consume(conn) == -1);
    CHECK(conn->state == KITH_CONTROL_CONN_CLOSING);
    free_conn(conn);
    return failures;
}

// The URL is capped at CONTROL_MAX_URL: 255 characters parse, 256 are
// rejected.
static int test_parse_url_length_cap(void)
{
    int failures = 0;

    for (uint32_t url_len = 255u; url_len <= 256u; url_len++)
    {
        struct kith_control_conn *conn = make_conn();
        char req[512];
        size_t used = (size_t)snprintf(req, sizeof(req), "GET ");
        req[used++] = '/';
        memset(req + used, 'b', url_len - 1u);
        used += url_len - 1u;
        used += (size_t)snprintf(req + used, sizeof(req) - used, " HTTP/1.1\r\n\r\n");

        feed_len(conn, req, used);
        if (url_len == 255u)
        {
            CHECK(control_http_parser_consume(conn) == 0);
            CHECK(strlen(conn->req_path) == 255u);
        }
        else
        {
            CHECK(control_http_parser_consume(conn) == -1);
            CHECK(conn->state == KITH_CONTROL_CONN_CLOSING);
        }
        free_conn(conn);
    }
    return failures;
}

// A request split across two feeds parses once the tail arrives, both for
// a request line cut mid-token and for a POST body cut mid-content; while
// bytes are missing consume keeps reporting 1.
static int test_parse_across_two_feeds(void)
{
    int failures = 0;

    struct kith_control_conn *conn = make_conn();
    feed(conn, "GE");
    CHECK(control_http_parser_consume(conn) == 1);
    feed(conn, "T /health HTTP/1.1\r\nHost: h\r\n\r\n");
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_method == KITH_CONTROL_METHOD_GET);
    CHECK(strcmp(conn->req_path, "/health") == 0);
    CHECK(conn->req_header_count == 1u);
    free_conn(conn);

    conn = make_conn();
    feed(conn, "POST /upload HTTP/1.1\r\nContent-Length: 13\r\n\r\nabcdefghij");
    CHECK(control_http_parser_consume(conn) == 1);
    CHECK(conn->state == KITH_CONTROL_CONN_READING);
    feed(conn, "XYZ");
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_body_len == 13u);
    CHECK(memcmp(conn->req_body, "abcdefghijXYZ", 13u) == 0);
    free_conn(conn);
    return failures;
}

// Consuming after a complete parse is a no-op: the RESPONDING state guard
// answers 1 without re-touching the parsed fields.
static int test_parse_consume_after_complete(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn, "GET /once HTTP/1.1\r\n\r\n");
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(control_http_parser_consume(conn) == 1);
    CHECK(strcmp(conn->req_path, "/once") == 0);

    free_conn(conn);
    return failures;
}

// A non-numeric Content-Length on a body-carrying method is rejected.
static int test_parse_invalid_content_length(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn,
         "POST /x HTTP/1.1\r\n"
         "Content-Length: abc\r\n"
         "\r\n"
         "body");
    CHECK(control_http_parser_consume(conn) == -1);
    CHECK(conn->state == KITH_CONTROL_CONN_CLOSING);

    free_conn(conn);
    return failures;
}

// A Content-Length that overflows uint32 during parsing is rejected by the
// checked-arithmetic guard before any bounds comparison runs.
static int test_parse_content_length_overflow(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn,
         "POST /x HTTP/1.1\r\n"
         "Content-Length: 99999999999\r\n"
         "\r\n");
    CHECK(control_http_parser_consume(conn) == -1);
    CHECK(conn->state == KITH_CONTROL_CONN_CLOSING);

    free_conn(conn);
    return failures;
}

// Content-Length above CONTROL_MAX_BODY is rejected even though it fits
// uint32; exactly CONTROL_MAX_BODY with the full body present parses and
// exposes all 65536 bytes.
static int test_parse_content_length_over_max(void)
{
    int failures = 0;
    static const uint32_t body_len = CONTROL_MAX_BODY;
    const uint32_t read_cap = 4096u + body_len + 64u;
    char *req = malloc(read_cap);

    struct kith_control_conn *conn = make_conn_sized(read_cap);
    int prefix =
        snprintf(req, read_cap, "POST /big HTTP/1.1\r\nContent-Length: %u\r\n\r\n", body_len + 1u);
    memset(req + prefix, 'B', body_len);
    feed_len(conn, req, (size_t)prefix + body_len);
    CHECK(control_http_parser_consume(conn) == -1);
    CHECK(conn->state == KITH_CONTROL_CONN_CLOSING);
    free_conn(conn);

    conn = make_conn_sized(read_cap);
    prefix = snprintf(req, read_cap, "POST /big HTTP/1.1\r\nContent-Length: %u\r\n\r\n", body_len);
    memset(req + prefix, 'B', body_len);
    feed_len(conn, req, (size_t)prefix + body_len);
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_body_len == body_len);
    CHECK(((const uint8_t *)conn->req_body)[0] == 'B');
    CHECK(((const uint8_t *)conn->req_body)[body_len - 1u] == 'B');
    free_conn(conn);

    free(req);
    return failures;
}

// PUT carries a body under the same Content-Length rules as POST.
static int test_parse_put_with_body(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn,
         "PUT /store HTTP/1.1\r\n"
         "Content-Length: 4\r\n"
         "\r\n"
         "data");
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_method == KITH_CONTROL_METHOD_PUT);
    CHECK(conn->req_body_len == 4u);
    CHECK(memcmp(conn->req_body, "data", 4u) == 0);

    free_conn(conn);
    return failures;
}

// A POST with no Content-Length completes with an empty body rather than
// stalling or erroring.
static int test_parse_post_without_cl(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn, "POST /fire-and-forget HTTP/1.1\r\nHost: h\r\n\r\n");
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_method == KITH_CONTROL_METHOD_POST);
    CHECK(conn->req_body == NULL);
    CHECK(conn->req_body_len == 0u);

    free_conn(conn);
    return failures;
}

// Header storage caps at CONTROL_MAX_HEADERS: lines past the cap are
// skipped silently and the request still completes.
static int test_parse_header_table_cap(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();
    char req[2048];
    size_t used = (size_t)snprintf(req, sizeof(req), "GET /h HTTP/1.1\r\n");
    for (int i = 0; i < 40 && used < sizeof(req) - 32u; i++)
    {
        used += (size_t)snprintf(req + used, sizeof(req) - used, "X-H%d: v%d\r\n", i, i);
    }
    used += (size_t)snprintf(req + used, sizeof(req) - used, "\r\n");

    feed_len(conn, req, used);
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_header_count == CONTROL_MAX_HEADERS);
    CHECK(strcmp(conn->req_headers[CONTROL_MAX_HEADERS - 1u].name, "X-H31") == 0);

    free_conn(conn);
    return failures;
}

// A header whose name reaches CONTROL_MAX_HEADER_NAME or whose value
// reaches CONTROL_MAX_HEADER_VALUE is skipped; shorter neighbors on the
// same request are kept intact.
static int test_parse_oversized_name_value_skipped(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    char name63[64];
    char name64[65];
    char val255[256];
    char val256[257];
    memset(name63, 'n', sizeof(name63) - 1u);
    name63[sizeof(name63) - 1u] = '\0';
    memset(name64, 'n', sizeof(name64) - 1u);
    name64[sizeof(name64) - 1u] = '\0';
    memset(val255, 'x', sizeof(val255) - 1u);
    val255[sizeof(val255) - 1u] = '\0';
    memset(val256, 'x', sizeof(val256) - 1u);
    val256[sizeof(val256) - 1u] = '\0';

    char req[2048];
    size_t used = (size_t)snprintf(req, sizeof(req), "GET /n HTTP/1.1\r\n");
    used += (size_t)snprintf(req + used, sizeof(req) - used, "X-Keep: yes\r\n");
    used += (size_t)snprintf(req + used, sizeof(req) - used, "%s: v\r\n", name63);
    used += (size_t)snprintf(req + used, sizeof(req) - used, "%s: v\r\n", name64);
    used += (size_t)snprintf(req + used, sizeof(req) - used, "V255: %s\r\n", val255);
    used += (size_t)snprintf(req + used, sizeof(req) - used, "V256: %s\r\n", val256);
    used += (size_t)snprintf(req + used, sizeof(req) - used, "\r\n");

    feed_len(conn, req, used);
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_header_count == 3u);
    CHECK(strcmp(conn->req_headers[0].name, "X-Keep") == 0);
    CHECK(strlen(conn->req_headers[1].name) == CONTROL_MAX_HEADER_NAME - 1u);
    CHECK(strlen(conn->req_headers[2].value) == CONTROL_MAX_HEADER_VALUE - 1u);
    CHECK(conn->req_headers[2].value[0] == 'x');

    free_conn(conn);
    return failures;
}

// Pipelined bytes following a completed body stay untouched in the read
// buffer: the parser consumes nothing it did not announce, so the caller
// decides what happens to the next request.
static int test_parse_leftover_after_body(void)
{
    int failures = 0;
    struct kith_control_conn *conn = make_conn();

    feed(conn,
         "POST /first HTTP/1.1\r\n"
         "Content-Length: 5\r\n"
         "\r\n"
         "hello"
         "GET /second HTTP/1.1\r\n\r\n");
    CHECK(control_http_parser_consume(conn) == 0);
    CHECK(conn->req_body_len == 5u);
    CHECK(memcmp(conn->req_body, "hello", 5u) == 0);
    const char *leftover = (const char *)conn->req_body + 5u;
    CHECK(strncmp(leftover, "GET /second", 11) == 0);

    free_conn(conn);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int failures = 0;
    failures += test_parse_get();
    failures += test_parse_post_with_body();
    failures += test_parse_partial();
    failures += test_parse_reset();
    failures += test_parse_bad_request_line();
    failures += test_parse_method_length_cap();
    failures += test_parse_url_length_cap();
    failures += test_parse_across_two_feeds();
    failures += test_parse_consume_after_complete();
    failures += test_parse_invalid_content_length();
    failures += test_parse_content_length_overflow();
    failures += test_parse_content_length_over_max();
    failures += test_parse_put_with_body();
    failures += test_parse_post_without_cl();
    failures += test_parse_header_table_cap();
    failures += test_parse_oversized_name_value_skipped();
    failures += test_parse_leftover_after_body();
    if (failures)
    {
        (void)fprintf(stderr, "control http: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

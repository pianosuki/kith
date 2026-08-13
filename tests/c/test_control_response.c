/* The exported response builder is pinned end to end: status line, an
 * appended header, and the finalizing body land in one buffer in that
 * order, argument validation refuses NULL without touching the length,
 * and a header that cannot fit is refused without a partial append.
 * The control plane's sources compile directly into the test, mirroring
 * the listener-path test. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/control/control.h"
#include "kith/types.h"
#include "kith/version.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "control response: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static kith_control_response_t make_resp(uint8_t *buf, uint32_t cap)
{
    kith_control_response_t resp = {
        .buf = buf,
        .cap = cap,
        .len = 0,
        .sse = false,
    };
    return resp;
}

// ---------------------------------------------------------------------------
// builder sequence
// ---------------------------------------------------------------------------

static int test_builder_sequence(void)
{
    int failures = 0;
    uint8_t buf[256];
    kith_control_response_t resp = make_resp(buf, sizeof(buf));

    const char *body = "{\"ok\":true}";
    CHECK(kith_control_response_status(&resp, 200, "application/json") == 0);
    CHECK(kith_control_response_header(&resp, "X-Trace", "abc") == 0);
    CHECK(kith_control_response_body(&resp, body, (uint32_t)strlen(body)) == 0);

    CHECK(strstr((const char *)buf, "HTTP/1.1 200 OK\r\n") == (const char *)buf);
    CHECK(strstr((const char *)buf, "Content-Type: application/json\r\n") != nullptr);
    CHECK(strstr((const char *)buf, "X-Trace: abc\r\n") != nullptr);
    CHECK(strstr((const char *)buf, "Content-Length: 11\r\n") != nullptr);
    CHECK(strstr((const char *)buf, "{\"ok\":true}") != nullptr);

    /* The header lands between the status block and the body's
     * Content-Length line. */
    const char *hdr = strstr((const char *)buf, "X-Trace:");
    const char *content_length = strstr((const char *)buf, "Content-Length:");
    CHECK(hdr != nullptr && content_length != nullptr && hdr < content_length);
    return failures;
}

static int test_header_validation(void)
{
    int failures = 0;
    uint8_t buf[64];
    kith_control_response_t resp = make_resp(buf, sizeof(buf));

    CHECK(kith_control_response_status(&resp, 200, nullptr) == 0);
    const uint32_t after_status = resp.len;

    CHECK(kith_control_response_header(nullptr, "a", "b") == kith_error_return(KITH_EINVAL));
    CHECK(kith_control_response_header(&resp, nullptr, "b") == kith_error_return(KITH_EINVAL));
    CHECK(kith_control_response_header(&resp, "a", nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(resp.len == after_status);

    /* A header that cannot fit the remaining capacity is refused whole:
     * nothing is appended and the length is unchanged. */
    CHECK(kith_control_response_header(
              &resp, "X-Long", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") ==
          kith_error_return(KITH_EOVERFLOW));
    CHECK(resp.len == after_status);
    return failures;
}

// ---------------------------------------------------------------------------
// canonical over-cap rejection
// ---------------------------------------------------------------------------

static void fill_reject_expect(char *out, size_t cap, const char *body)
{
    (void)snprintf(out,
                   cap,
                   "HTTP/1.1 500 Internal Server Error\r\n"
                   "Content-Type: application/json\r\n"
                   "Content-Length: %u\r\n"
                   "\r\n"
                   "%s",
                   (unsigned)strlen(body),
                   body);
}

// The canonical rejection resets the response and renders one complete 500
// whose JSON body names the error, the route (when given), the attempted
// size (when nonzero), and the capacity — exact bytes on every key
// combination. A cap too small for the rejection itself resets the length
// to zero, which the flush path treats as close-without-response.
static int test_reject_overflow_shapes(void)
{
    int failures = 0;

    uint8_t buf[512];
    kith_control_response_t resp = make_resp(buf, sizeof(buf));

    const char *full_body =
        "{\"error\":\"response_too_large\",\"route\":\"/list\",\"attempted\":70000,\"cap\":512}";
    const char *bare_body = "{\"error\":\"response_too_large\",\"cap\":512}";
    const char *escaped_body =
        "{\"error\":\"response_too_large\",\"route\":\"/a\\\"b\",\"cap\":512}";
    char expect[512];

    resp.len = 99u;
    CHECK(kith_control_reject_overflow(nullptr, &resp, "/list", 70000u) == 0);
    fill_reject_expect(expect, sizeof(expect), full_body);
    CHECK(resp.len == strlen(expect));
    CHECK(memcmp(resp.buf, expect, strlen(expect)) == 0);

    resp.len = 99u;
    CHECK(kith_control_reject_overflow(nullptr, &resp, nullptr, 0u) == 0);
    fill_reject_expect(expect, sizeof(expect), bare_body);
    CHECK(resp.len == strlen(expect));
    CHECK(memcmp(resp.buf, expect, strlen(expect)) == 0);

    /* A request path carrying a quote stays valid JSON after the escape. */
    resp.len = 99u;
    CHECK(kith_control_reject_overflow(nullptr, &resp, "/a\"b", 0u) == 0);
    fill_reject_expect(expect, sizeof(expect), escaped_body);
    CHECK(resp.len == strlen(expect));
    CHECK(memcmp(resp.buf, expect, strlen(expect)) == 0);

    kith_control_response_t tiny = make_resp(buf, 64u);
    tiny.len = 32u;
    CHECK(kith_control_reject_overflow(nullptr, &tiny, "/list", 70000u) ==
          kith_error_return(KITH_EOVERFLOW));
    CHECK(tiny.len == 0u);

    CHECK(kith_control_reject_overflow(nullptr, nullptr, "/list", 1u) ==
          kith_error_return(KITH_EINVAL));
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_builder_sequence();
    rc |= test_header_validation();
    rc |= test_reject_overflow_shapes();
    if (rc != 0)
    {
        (void)fprintf(stderr, "control response tests FAILED\n");
    }
    return rc;
}

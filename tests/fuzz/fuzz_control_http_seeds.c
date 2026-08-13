/* Corpus generator for the control HTTP parser fuzz target.
 *
 * Writes the deterministic seed corpus the fuzz_control_http ctest smoke
 * replays: requests the parser accepts (each method, content-length bodies
 * including the case-insensitive header name and the zero-length body,
 * boundary-length header fields, the skipped header cases) and the reject
 * paths (bad CRLF, missing version space, oversized method/URL,
 * overflowing or oversized content-length) plus the two need-more shapes
 * (truncated prefix, read buffer exhausted without a terminator). No
 * binary artifact is committed to the repository.
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "control/control_internal.h"

#include <sys/stat.h>

static int ensure_output_dir(const char *dir)
{
    /* mkdir(2) creates the leaf only, so each prefix of a nested path is
     * created in turn; an already-present component is not an error. */
    char scratch[512];
    const size_t len = strlen(dir);
    if (len == 0 || len >= sizeof(scratch))
    {
        return -1;
    }
    memcpy(scratch, dir, len + 1u);
    for (size_t i = 1u; i < len; ++i)
    {
        if (scratch[i] != '/')
        {
            continue;
        }
        scratch[i] = '\0';
        if (mkdir(scratch, 0755) != 0 && errno != EEXIST)
        {
            return -1;
        }
        scratch[i] = '/';
    }
    if (mkdir(scratch, 0755) != 0 && errno != EEXIST)
    {
        return -1;
    }
    return 0;
}

static int write_seed(const char *dir, const char *name, const uint8_t *bytes, size_t len)
{
    char path[512];
    const int n = snprintf(path, sizeof(path), "%s/%s", dir, name);
    if (n < 0 || (size_t)n >= sizeof(path))
    {
        return -1;
    }
    FILE *out = fopen(path, "wb");
    if (out == nullptr)
    {
        return -1;
    }
    const size_t written = fwrite(bytes, 1u, len, out);
    const int closed = fclose(out);
    return (written == len && closed == 0) ? 0 : -1;
}

static int emit_text_seeds(const char *dir)
{
    static const struct
    {
        const char *name;
        const char *request;
    } seeds[] = {
        {"get_headers.bin", "GET /health HTTP/1.1\r\nHost: kith\r\nAccept: */*\r\n\r\n"},
        {"get_no_headers.bin", "GET /metrics HTTP/1.1\r\n\r\n"},
        {"post_body.bin", "POST /events HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"},
        {"put_lowercase_cl.bin", "PUT /config HTTP/1.1\r\ncontent-length: 3\r\n\r\nabc"},
        {"delete.bin", "DELETE /sessions/7 HTTP/1.1\r\n\r\n"},
        {"unknown_method.bin", "PATCH /items HTTP/1.1\r\n\r\n"},
        {"need_more_prefix.bin", "GET /health HTTP/1.1\r\nHost: k"},
        {"missing_version_space.bin", "GET /health\r\n\r\n"},
        {"bare_lf.bin", "GET / HTTP/1.1\nHost: k\n\n"},
        {"header_no_colon.bin", "GET / HTTP/1.1\r\nnocolon\r\n\r\n"},
        {"oversize_method.bin", "MMMMMMMM / HTTP/1.1\r\n\r\n"},
        {"cl_overflow.bin", "POST / HTTP/1.1\r\nContent-Length: 99999999999\r\n\r\n"},
        {"cl_oversize.bin", "POST / HTTP/1.1\r\nContent-Length: 65537\r\n\r\n"},
        {"body_pending.bin", "POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\nabc"},
        {"cl_zero.bin", "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n"},
    };

    int failures = 0;
    for (size_t i = 0; i < sizeof(seeds) / sizeof(seeds[0]); ++i)
    {
        failures += write_seed(
            dir, seeds[i].name, (const uint8_t *)seeds[i].request, strlen(seeds[i].request));
    }
    return failures;
}

/* Request line with a URL one byte past the parser's cap: the reject fires
 * off the request line alone. */
static int emit_oversize_url(const char *dir)
{
    uint8_t buf[CONTROL_MAX_URL + 32];
    memset(buf, 0, sizeof(buf));
    const char *prefix = "GET /";
    const size_t prefix_len = strlen(prefix);
    memcpy(buf, prefix, prefix_len);
    memset(buf + prefix_len, 'a', CONTROL_MAX_URL);
    const char *suffix = " HTTP/1.1\r\n\r\n";
    memcpy(buf + prefix_len + CONTROL_MAX_URL, suffix, strlen(suffix));
    return write_seed(
        dir, "oversize_url.bin", buf, prefix_len + (size_t)CONTROL_MAX_URL + strlen(suffix));
}

/* Header fields at exactly the accepted maxima: a 63-byte name and a
 * 255-byte value fill one scratch entry to its boundary. */
static int emit_boundary_header(const char *dir)
{
    uint8_t buf[512];
    memset(buf, 0, sizeof(buf));
    const char *prefix = "GET / HTTP/1.1\r\n";
    size_t n = strlen(prefix);
    memcpy(buf, prefix, n);
    memset(buf + n, 'n', CONTROL_MAX_HEADER_NAME - 1u);
    n += CONTROL_MAX_HEADER_NAME - 1u;
    n += (size_t)snprintf((char *)buf + n, sizeof(buf) - n, ": ");
    memset(buf + n, 'v', CONTROL_MAX_HEADER_VALUE - 1u);
    n += CONTROL_MAX_HEADER_VALUE - 1u;
    n += (size_t)snprintf((char *)buf + n, sizeof(buf) - n, "\r\n\r\n");
    return write_seed(dir, "boundary_header.bin", buf, n);
}

/* Thirty-three stored-length headers: the table keeps the first
 * CONTROL_MAX_HEADERS and skips the rest without error. */
static int emit_many_headers(const char *dir)
{
    uint8_t buf[512];
    size_t n = (size_t)snprintf((char *)buf, sizeof(buf), "GET / HTTP/1.1\r\n");
    for (uint32_t i = 0u; n < sizeof(buf) && i <= CONTROL_MAX_HEADERS; ++i)
    {
        n += (size_t)snprintf((char *)buf + n, sizeof(buf) - n, "h%02u: v\r\n", i);
    }
    n += (size_t)snprintf((char *)buf + n, sizeof(buf) - n, "\r\n");
    return write_seed(dir, "many_headers.bin", buf, n);
}

/* The read buffer filled with no header terminator: the exhausted-buffer
 * close fires at exactly FUZZ_MAX_INPUT bytes. */
static int emit_buffer_exhausted(const char *dir)
{
    static uint8_t buf[65'536];
    memset(buf, 'A', sizeof(buf));
    return write_seed(dir, "buffer_exhausted.bin", buf, sizeof(buf));
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stderr, "usage: fuzz_control_http_seeds <output-dir>\n");
        return EXIT_FAILURE;
    }
    if (ensure_output_dir(argv[1]) != 0)
    {
        fprintf(stderr, "fuzz_control_http_seeds: cannot create %s\n", argv[1]);
        return EXIT_FAILURE;
    }

    const int failures = emit_text_seeds(argv[1]) + emit_oversize_url(argv[1]) +
                         emit_boundary_header(argv[1]) + emit_many_headers(argv[1]) +
                         emit_buffer_exhausted(argv[1]);
    if (failures != 0)
    {
        fprintf(stderr, "fuzz_control_http_seeds: %d seed(s) failed to write\n", failures);
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

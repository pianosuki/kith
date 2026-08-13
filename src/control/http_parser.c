/* HTTP/1.1 request-line and header parsing for the control plane: reads
 * method, URL, and headers out of the connection's read buffer into the
 * parsed-request fields on kith_control_conn. Consumed by conn.c after the
 * read path fills the buffer; router.c then matches against the result. */

#include <ctype.h>
#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>

#include "control/control_internal.h"

// ---------------------------------------------------------------------------
// case-insensitive ASCII compare (replaces strcasecmp from strings.h)
// ---------------------------------------------------------------------------
static bool ci_equal(const char *a, const char *b, size_t n)
{
    for (size_t i = 0; i < n; i++)
    {
        if (tolower((unsigned char)a[i]) != tolower((unsigned char)b[i]))
        {
            return false;
        }
        if (a[i] == '\0')
        {
            return true;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// parse the request line: METHOD SP URL SP HTTP/1.1 CRLF
// ---------------------------------------------------------------------------
static bool parse_request_line(struct kith_control_conn *conn, const char *data, uint32_t len)
{
    const char *end = memchr(data, '\r', len);
    if (end == NULL || (uint32_t)(end - data) + 1u >= len || *(end + 1) != '\n')
    {
        return false;
    }

    const char *line_end = end;
    const char *p = data;

    // method
    const char *sp1 = memchr(p, ' ', (uint32_t)(line_end - p));
    if (sp1 == NULL)
    {
        return false;
    }
    uint32_t method_len = (uint32_t)(sp1 - p);
    if (method_len >= CONTROL_MAX_METHOD)
    {
        return false;
    }

    if (method_len == 3u && strncmp(p, "GET", 3) == 0)
    {
        conn->req_method = KITH_CONTROL_METHOD_GET;
    }
    else if (method_len == 4u && strncmp(p, "POST", 4) == 0)
    {
        conn->req_method = KITH_CONTROL_METHOD_POST;
    }
    else if (method_len == 3u && strncmp(p, "PUT", 3) == 0)
    {
        conn->req_method = KITH_CONTROL_METHOD_PUT;
    }
    else if (method_len == 6u && strncmp(p, "DELETE", 6) == 0)
    {
        conn->req_method = KITH_CONTROL_METHOD_DELETE;
    }
    else
    {
        conn->req_method = KITH_CONTROL_METHOD_UNKNOWN;
    }

    // URL
    p = sp1 + 1;
    const char *sp2 = memchr(p, ' ', (uint32_t)(line_end - p));
    if (sp2 == NULL)
    {
        return false;
    }
    uint32_t url_len = (uint32_t)(sp2 - p);
    if (url_len >= CONTROL_MAX_URL)
    {
        return false;
    }
    memcpy(conn->req_path, p, url_len);
    conn->req_path[url_len] = '\0';

    return true;
}

// ---------------------------------------------------------------------------
// parse headers until the blank CRLF line
// ---------------------------------------------------------------------------
static bool parse_headers(struct kith_control_conn *conn,
                          const char *data,
                          uint32_t len,
                          uint32_t *out_consumed)
{
    *out_consumed = 0u;
    // control_http_parser_consume may re-invoke this as the request arrives in
    // chunks; each invocation rebuilds the header table from the intact read
    // buffer, so reset the count and scratch first.
    conn->req_header_count = 0u;
    conn->header_scratch_used = 0u;
    const char *p = data;
    const char *end = data + len;

    while (p + 1 < end)
    {
        if (p[0] == '\r' && p[1] == '\n')
        {
            *out_consumed = (uint32_t)(p + 2 - data);
            return true;
        }

        const char *line_end = memchr(p, '\r', (uint32_t)(end - p));
        if (line_end == NULL || line_end + 1 >= end || *(line_end + 1) != '\n')
        {
            return false;
        }

        if (conn->req_header_count < CONTROL_MAX_HEADERS)
        {
            const char *colon = memchr(p, ':', (uint32_t)(line_end - p));
            if (colon != NULL)
            {
                uint32_t name_len = (uint32_t)(colon - p);
                const char *val_start = colon + 1;
                while (val_start < line_end && *val_start == ' ')
                {
                    val_start++;
                }
                uint32_t val_len = (uint32_t)(line_end - val_start);

                if (name_len < CONTROL_MAX_HEADER_NAME && val_len < CONTROL_MAX_HEADER_VALUE)
                {
                    // Copy the name and value into the per-connection scratch
                    // arena as NUL-terminated strings. The read buffer is not
                    // mutated, so a partial request that needs another read can
                    // be re-parsed without corrupted delimiters. The borrowed
                    // pointers in req_headers[] reference the scratch, which
                    // stays live for the handler's duration (like read_buf).
                    uint32_t need = name_len + 1u + val_len + 1u;
                    if (conn->header_scratch_used + need > conn->header_scratch_cap)
                    {
                        return false;
                    }
                    char *dst = conn->header_scratch + conn->header_scratch_used;
                    memcpy(dst, p, name_len);
                    dst[name_len] = '\0';
                    char *name_str = dst;
                    char *val_str = dst + name_len + 1u;
                    memcpy(val_str, val_start, val_len);
                    val_str[val_len] = '\0';
                    conn->header_scratch_used += need;

                    kith_control_header_t *hdr = &conn->req_headers[conn->req_header_count];
                    hdr->name = name_str;
                    hdr->value = val_str;
                    conn->req_header_count++;
                }
            }
        }

        p = line_end + 2;
    }

    return false;
}

// ---------------------------------------------------------------------------
// overflow-safe Content-Length parse
// ---------------------------------------------------------------------------
static bool parse_content_length(const char *s, uint32_t *out)
{
    uint32_t result = 0u;
    if (*s < '0' || *s > '9')
    {
        return false;
    }
    while (*s >= '0' && *s <= '9')
    {
        uint32_t digit = (uint32_t)(*s - '0');
        if (ckd_mul(&result, result, 10u))
        {
            return false;
        }
        if (ckd_add(&result, result, digit))
        {
            return false;
        }
        s++;
    }
    *out = result;
    return true;
}

// ---------------------------------------------------------------------------
// find header value by name (case-insensitive)
// ---------------------------------------------------------------------------
static const char *find_header(struct kith_control_conn *conn, const char *name)
{
    size_t name_len = strlen(name);
    for (uint32_t i = 0u; i < conn->req_header_count; i++)
    {
        if (ci_equal(conn->req_headers[i].name, name, name_len))
        {
            return conn->req_headers[i].value;
        }
    }
    return NULL;
}

// ---------------------------------------------------------------------------
// scan for \r\n\r\n header terminator using memchr (not memmem)
// ---------------------------------------------------------------------------
static const char *find_header_terminator(const char *data, uint32_t len)
{
    const char *scan = data;
    uint32_t remaining = len;
    while (remaining >= 4u)
    {
        const char *cr = memchr(scan, '\r', remaining);
        if (cr == NULL || (uint32_t)(cr - scan) + 4u > remaining)
        {
            break;
        }
        if (cr[1] == '\n' && cr[2] == '\r' && cr[3] == '\n')
        {
            return cr;
        }
        uint32_t consumed = (uint32_t)(cr - scan) + 1u;
        scan = cr + 1;
        remaining -= consumed;
    }
    return NULL;
}

// ---------------------------------------------------------------------------
// public: feed bytes to the parser
// ---------------------------------------------------------------------------
int control_http_parser_consume(struct kith_control_conn *conn)
{
    if (conn->state != KITH_CONTROL_CONN_READING)
    {
        return 1;
    }

    const char *data = (const char *)conn->read_buf;
    uint32_t len = conn->read_pos;

    const char *header_end = find_header_terminator(data, len);

    if (header_end == NULL)
    {
        if (len >= conn->read_cap)
        {
            conn->state = KITH_CONTROL_CONN_CLOSING;
            return -1;
        }
        return 1;
    }

    if (!parse_request_line(conn, data, len))
    {
        conn->state = KITH_CONTROL_CONN_CLOSING;
        return -1;
    }

    const char *nl = memchr(data, '\n', len);
    if (nl == NULL)
    {
        conn->state = KITH_CONTROL_CONN_CLOSING;
        return -1;
    }
    uint32_t req_line_len = (uint32_t)(nl - data) + 1u;
    uint32_t headers_consumed = 0u;
    if (!parse_headers(conn, data + req_line_len, len - req_line_len, &headers_consumed))
    {
        conn->state = KITH_CONTROL_CONN_CLOSING;
        return -1;
    }

    uint32_t headers_total = req_line_len + headers_consumed;
    conn->req_body = NULL;
    conn->req_body_len = 0u;

    if (conn->req_method == KITH_CONTROL_METHOD_POST || conn->req_method == KITH_CONTROL_METHOD_PUT)
    {
        const char *cl = find_header(conn, "content-length");
        if (cl != NULL)
        {
            uint32_t content_len = 0u;
            if (!parse_content_length(cl, &content_len) || content_len > CONTROL_MAX_BODY)
            {
                conn->state = KITH_CONTROL_CONN_CLOSING;
                return -1;
            }
            if (len - headers_total >= content_len)
            {
                conn->req_body = data + headers_total;
                conn->req_body_len = content_len;
            }
            else
            {
                return 1;
            }
        }
    }

    conn->state = KITH_CONTROL_CONN_RESPONDING;
    return 0;
}

void control_http_parser_reset(struct kith_control_conn *conn)
{
    conn->state = KITH_CONTROL_CONN_READING;
    conn->read_pos = 0u;
    conn->req_method = KITH_CONTROL_METHOD_UNKNOWN;
    conn->req_path[0] = '\0';
    conn->req_header_count = 0u;
    conn->req_body = NULL;
    conn->req_body_len = 0u;
    conn->resp.len = 0u;
    conn->resp.sse = false;
}

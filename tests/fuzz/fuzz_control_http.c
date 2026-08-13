/* libFuzzer harness for the control HTTP request parser
 * (control_http_parser_consume).
 *
 * The parser is module-local and anchored to the connection record, so the
 * harness compiles src/control/http_parser.c directly and drives a
 * locally-owned conn with production-shaped buffers: the read buffer and
 * the header scratch are allocated at exactly their caps (the scratch uses
 * control_conn_alloc's sizing formula), so any read or write past a length
 * bound is a heap-buffer-overflow under ASan rather than an overrun hidden
 * in slack. Every observable the parse produces folds into a volatile
 * sink, and the chunked-arrival contract is pinned differentially: a
 * prefix that reports "more bytes needed" must parse to the same result —
 * and fold to the same record — once the remainder arrives, because the
 * parser rebuilds its header table from the intact buffer on every call.
 */

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "control/control_internal.h"

/* Entry points the libFuzzer runtime resolves by symbol name. */
int LLVMFuzzerInitialize(int *argc, char ***argv);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

/* Inputs above this cap are dropped; invocations pass a matching -max_len.
 * The read buffer is allocated at exactly this size, so an input that
 * fills it without a header terminator drives the exhausted-buffer close. */
enum
{
    FUZZ_MAX_INPUT = 65'536,
    FUZZ_SCRATCH_CAP = CONTROL_MAX_HEADERS * (CONTROL_MAX_HEADER_NAME + CONTROL_MAX_HEADER_VALUE),
};

static volatile uint64_t fuzz_fold_sink;
static struct kith_control_conn fuzz_conn;

static void fuzz_destroy_buffers(void)
{
    free(fuzz_conn.read_buf);
    free(fuzz_conn.header_scratch);
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc;
    (void)argv;

    fuzz_conn.read_buf = malloc(FUZZ_MAX_INPUT);
    fuzz_conn.header_scratch = malloc(FUZZ_SCRATCH_CAP);
    if (fuzz_conn.read_buf == nullptr || fuzz_conn.header_scratch == nullptr)
    {
        abort();
    }
    fuzz_conn.read_cap = FUZZ_MAX_INPUT;
    fuzz_conn.header_scratch_cap = FUZZ_SCRATCH_CAP;
    fuzz_conn.fd = -1;
    if (atexit(fuzz_destroy_buffers) != 0)
    {
        abort();
    }
    return 0;
}

static uint64_t fold_bytes(uint64_t fold, const uint8_t *bytes, size_t len)
{
    for (size_t i = 0; i < len; ++i)
    {
        fold = (fold << 5u) | (fold >> 59u);
        fold ^= bytes[i];
    }
    return fold;
}

/* Fold every observable of a completed parse. Called only after the
 * contract checkers accepted the record, so every length below is
 * bounded and every borrowed pointer is NUL-terminated. */
static uint64_t fold_parse(uint64_t fold)
{
    struct kith_control_conn *conn = &fuzz_conn;

    fold ^= (uint64_t)conn->req_method;
    fold = fold_bytes(fold, (const uint8_t *)conn->req_path, strlen(conn->req_path));
    fold += (uint64_t)conn->req_header_count << 32u;
    for (uint32_t i = 0; i < conn->req_header_count; ++i)
    {
        fold = fold_bytes(
            fold, (const uint8_t *)conn->req_headers[i].name, strlen(conn->req_headers[i].name));
        fold = fold_bytes(
            fold, (const uint8_t *)conn->req_headers[i].value, strlen(conn->req_headers[i].value));
    }
    if (conn->req_body != nullptr)
    {
        fold = fold_bytes(fold, conn->req_body, conn->req_body_len);
    }
    return fold;
}

/* One borrowed header string must live inside the scratch arena's used
 * range and be NUL-terminated within its length cap; the router and the
 * ctypes binding read these as C strings. */
static void check_arena_string(const char *s, const char *arena, const char *used_end, uint32_t cap)
{
    if (s < arena || s >= used_end || memchr(s, '\0', (size_t)(used_end - s)) == nullptr ||
        strlen(s) >= cap)
    {
        abort();
    }
}

/* Verify the parsed header table against the arena the parser filled. */
static void check_header_arena(void)
{
    struct kith_control_conn *conn = &fuzz_conn;
    const char *arena = conn->header_scratch;
    const char *used_end = arena + conn->header_scratch_used;

    if (conn->header_scratch_used > conn->header_scratch_cap ||
        conn->req_header_count > CONTROL_MAX_HEADERS)
    {
        abort();
    }
    for (uint32_t i = 0; i < conn->req_header_count; ++i)
    {
        check_arena_string(conn->req_headers[i].name, arena, used_end, CONTROL_MAX_HEADER_NAME);
        check_arena_string(conn->req_headers[i].value, arena, used_end, CONTROL_MAX_HEADER_VALUE);
    }
}

/* Verify the parsed request record on a completed parse: the state
 * transition, the method tag, the request path, and the body pointer a
 * handler receives. A violation aborts — the parser claimed "request
 * parsed" for a record no handler can safely consume. */
static void check_parsed_record(void)
{
    struct kith_control_conn *conn = &fuzz_conn;

    if (conn->state != KITH_CONTROL_CONN_RESPONDING ||
        conn->req_method > KITH_CONTROL_METHOD_UNKNOWN || strlen(conn->req_path) >= CONTROL_MAX_URL)
    {
        abort();
    }
    if (conn->req_body == nullptr)
    {
        if (conn->req_body_len != 0u)
        {
            abort();
        }
        return;
    }
    /* Only the content-length block sets a body, and only for POST/PUT. */
    if ((conn->req_method != KITH_CONTROL_METHOD_POST &&
         conn->req_method != KITH_CONTROL_METHOD_PUT) ||
        conn->req_body_len > CONTROL_MAX_BODY || (const uint8_t *)conn->req_body < conn->read_buf ||
        (size_t)((const uint8_t *)conn->req_body - conn->read_buf) + conn->req_body_len >
            conn->read_pos)
    {
        abort();
    }
}

/* Stage @p fed bytes (already copied into the read buffer) and consume
 * them. Returns the parser rc; *out_fold carries the parse fold (0 unless
 * the request parsed). */
static int consume_staged(uint32_t fed, uint64_t *out_fold)
{
    struct kith_control_conn *conn = &fuzz_conn;

    conn->read_pos = fed;
    int rc = control_http_parser_consume(conn);

    *out_fold = 0;
    if (rc < 0)
    {
        if (conn->state != KITH_CONTROL_CONN_CLOSING)
        {
            abort();
        }
    }
    else if (rc == 0)
    {
        check_header_arena();
        check_parsed_record();
        *out_fold = fold_parse(0);
    }
    else if (conn->state != KITH_CONTROL_CONN_READING)
    {
        abort();
    }
    return rc;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (data == nullptr || size == 0 || size > (size_t)FUZZ_MAX_INPUT)
    {
        return 0;
    }

    struct kith_control_conn *conn = &fuzz_conn;

    control_http_parser_reset(conn);
    memcpy(conn->read_buf, data, size);
    uint64_t full_fold = 0;
    const int full_rc = consume_staged((uint32_t)size, &full_fold);

    control_http_parser_reset(conn);
    const size_t split = size / 2;
    memcpy(conn->read_buf, data, split);
    uint64_t prefix_fold = 0;
    const int prefix_rc = consume_staged((uint32_t)split, &prefix_fold);

    fuzz_fold_sink ^= full_fold ^ prefix_fold;

    /* A prefix that still needs bytes must parse to the identical result
     * once the remainder lands: the "need more" path mutates nothing on
     * the conn, so the re-parse sees the same intact buffer. */
    if (prefix_rc == 1)
    {
        memcpy(conn->read_buf + split, data + split, size - split);
        uint64_t resumed_fold = 0;
        if (consume_staged((uint32_t)size, &resumed_fold) != full_rc || resumed_fold != full_fold)
        {
            abort();
        }
        fuzz_fold_sink ^= resumed_fold;
    }
    return 0;
}

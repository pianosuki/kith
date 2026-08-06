#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/db/db.h"
#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/version.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond_ext(bool ok, int line, int rc)
{
    if (!ok)
    {
        (void)fprintf(stderr, "db create: assertion at line %d failed (rc=%d)\n", line, rc);
        return 1;
    }
    return 0;
}

#define CHECK(cond)             failures += check_cond_ext((cond), __LINE__, 0)
#define CHECK_RC(cond, rc_expr) failures += check_cond_ext((cond), __LINE__, (rc_expr))

// Create a reactor with a small io_uring ring. These are lifecycle tests that
// never pump the event loop, so a 64-fd ring is plenty.
static int db_create_reactor(kith_reactor_t **out)
{
    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 64,
        .task_capacity = 64,
    };
    return kith_reactor_create(&params, nullptr, out);
}

// Fill a params struct with valid defaults.
static void fill_params(kith_db_params_t *params)
{
    memset(params, 0, sizeof(*params));
    params->size = sizeof(*params);
    params->abi_version = KITH_ABI_VERSION;
}

// ---------------------------------------------------------------------------
// lifecycle tests
// ---------------------------------------------------------------------------

static int test_create_valid(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK_RC(db_create_reactor(&reactor) == 0, 0);
    CHECK(reactor != nullptr);

    kith_db_params_t params;
    fill_params(&params);

    int rc = kith_db_create(&params, reactor, nullptr, &db);
    CHECK_RC(rc == 0, rc);
    CHECK(db != nullptr);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_null_params(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);

    int rc = kith_db_create(nullptr, reactor, nullptr, &db);
    CHECK(rc == -(int)KITH_EINVAL);
    CHECK(db == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_null_reactor(void)
{
    int failures = 0;
    kith_db_t *db = nullptr;

    kith_db_params_t params;
    fill_params(&params);

    int rc = kith_db_create(&params, nullptr, nullptr, &db);
    CHECK(rc == -(int)KITH_EINVAL);
    CHECK(db == nullptr);

    return failures;
}

static int test_create_null_out(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);

    kith_db_params_t params;
    fill_params(&params);

    int rc = kith_db_create(&params, reactor, nullptr, nullptr);
    CHECK(rc == -(int)KITH_EINVAL);

    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_bad_abi(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);

    kith_db_params_t params;
    fill_params(&params);
    params.abi_version = 999u;

    int rc = kith_db_create(&params, reactor, nullptr, &db);
    CHECK(rc == -(int)KITH_EABIVER);
    CHECK(db == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_undersized(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);

    kith_db_params_t params;
    fill_params(&params);
    params.size = sizeof(uint32_t) * 2;

    int rc = kith_db_create(&params, reactor, nullptr, &db);
    CHECK(rc == -(int)KITH_ESIZE);
    CHECK(db == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

static int test_create_min_gt_max(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);

    kith_db_params_t params;
    fill_params(&params);
    params.min_connections = 8;
    params.max_connections = 2;

    int rc = kith_db_create(&params, reactor, nullptr, &db);
    CHECK(rc == -(int)KITH_ERANGE);
    CHECK(db == nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

static int test_destroy_null(void)
{
    int failures = 0;
    kith_db_destroy(nullptr);
    return failures;
}

static int test_create_custom_host(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);

    kith_db_params_t params;
    fill_params(&params);
    params.host = "127.0.0.1";
    params.port = 5432;
    params.connect_timeout_ms = 1000;

    int rc = kith_db_create(&params, reactor, nullptr, &db);
    CHECK(rc == 0);
    CHECK(db != nullptr);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// The handle owns a private copy of the password: the caller releases its
// heap buffer immediately after create, and the subsequent destroy scrubs
// and frees only the handle's copy. A scrub through the stale caller buffer
// trips use-after-free under ASAN, so a clean run proves the copy is
// independent of the caller's storage.
static int test_create_owns_password_copy(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);

    char *password = malloc(64);
    CHECK(password != nullptr);
    if (password == nullptr)
    {
        kith_reactor_destroy(reactor);
        return failures;
    }
    (void)snprintf(password, 64, "%s", "correct-horse-battery-staple");

    kith_db_params_t params;
    fill_params(&params);
    params.host = "127.0.0.1";
    params.port = 5432;
    params.connect_timeout_ms = 1000;
    params.password = password;

    int rc = kith_db_create(&params, reactor, nullptr, &db);
    CHECK_RC(rc == 0, rc);
    CHECK(db != nullptr);
    free(password);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// scrub observation — a recording allocator captures each freed block's
// content before the underlying release
// ---------------------------------------------------------------------------

typedef struct freed_block
{
    const void *ptr;
    size_t size;
    bool zeroed; // every byte was zero at free time
} freed_block_t;

// Fixed recording tables: the scrub test allocates three blocks, the caps
// leave headroom for registry traffic from future tests in this file.
#define CAPTURE_SLOTS 16U

typedef struct capture_state
{
    struct
    {
        const void *ptr;
        size_t size;
    } live[CAPTURE_SLOTS];
    freed_block_t freed[CAPTURE_SLOTS];
    size_t freed_count;
} capture_state_t;

static capture_state_t g_capture;

static void capture_reset(void)
{
    memset(&g_capture, 0, sizeof(g_capture));
}

static void capture_track(void *ptr, size_t size)
{
    for (unsigned i = 0; i < CAPTURE_SLOTS; i++)
    {
        if (g_capture.live[i].ptr == nullptr)
        {
            g_capture.live[i].ptr = ptr;
            g_capture.live[i].size = size;
            return;
        }
    }
}

static void *capture_alloc(void *ctx, size_t size)
{
    (void)ctx;
    void *ptr = malloc(size);
    if (ptr != nullptr)
    {
        capture_track(ptr, size);
    }
    return ptr;
}

static void *capture_alloc_zero(void *ctx, size_t count, size_t size)
{
    (void)ctx;
    void *ptr = calloc(count, size);
    if (ptr != nullptr)
    {
        capture_track(ptr, count * size);
    }
    return ptr;
}

static void *capture_realloc(void *ctx, void *ptr, size_t size)
{
    (void)ctx;
    void *moved = realloc(ptr, size);
    if (moved != nullptr && moved != ptr)
    {
        for (unsigned i = 0; i < CAPTURE_SLOTS; i++)
        {
            if (g_capture.live[i].ptr == ptr)
            {
                g_capture.live[i].ptr = nullptr;
                g_capture.live[i].size = 0U;
            }
        }
        capture_track(moved, size);
    }
    return moved;
}

static void capture_free(void *ctx, void *ptr)
{
    (void)ctx;
    if (ptr == nullptr)
    {
        return;
    }
    for (unsigned i = 0; i < CAPTURE_SLOTS; i++)
    {
        if (g_capture.live[i].ptr == ptr)
        {
            const size_t size = g_capture.live[i].size;
            g_capture.live[i].ptr = nullptr;
            g_capture.live[i].size = 0U;
            const unsigned char *bytes = ptr;
            bool zeroed = true;
            for (size_t b = 0U; b < size; b++)
            {
                if (bytes[b] != 0U)
                {
                    zeroed = false;
                    break;
                }
            }
            if (g_capture.freed_count < CAPTURE_SLOTS)
            {
                g_capture.freed[g_capture.freed_count].ptr = ptr;
                g_capture.freed[g_capture.freed_count].size = size;
                g_capture.freed[g_capture.freed_count].zeroed = zeroed;
                g_capture.freed_count++;
            }
            break;
        }
    }
    free(ptr);
}

static const kith_allocator_t g_capture_allocator = {
    .size = sizeof(kith_allocator_t),
    .abi_version = KITH_ABI_VERSION,
    .user_data = nullptr,
    .alloc = capture_alloc,
    .alloc_zero = capture_alloc_zero,
    .realloc = capture_realloc,
    .free = capture_free,
    .reserved = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
};

// The handle scrubs its password copy before releasing it: the recording
// allocator inspects each freed block's bytes before the underlying free,
// and exactly one block of the password's length (copy plus NUL) must
// arrive all-zero — the elision-resistant scrub reaches memory. The owned-
// copy test above proves independence from the caller's buffer; this test
// proves the scrub itself. A size collision with the handle or connection
// table surfaces as a failed uniqueness count.
static int test_create_scrubs_password_copy(void)
{
    static const char password[] = "correct-horse-battery-staple";
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);
    capture_reset();

    kith_db_params_t params;
    fill_params(&params);
    params.host = "127.0.0.1";
    params.port = 5432;
    params.connect_timeout_ms = 1000;
    params.password = password;

    int rc = kith_db_create(&params, reactor, &g_capture_allocator, &db);
    CHECK_RC(rc == 0, rc);
    CHECK(db != nullptr);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);

    const size_t password_len = sizeof(password);
    size_t matches = 0;
    bool zeroed = false;
    for (size_t i = 0; i < g_capture.freed_count; i++)
    {
        if (g_capture.freed[i].size == password_len)
        {
            matches++;
            zeroed = g_capture.freed[i].zeroed;
        }
    }
    CHECK(matches == 1);
    CHECK(zeroed);
    return failures;
}

// ---------------------------------------------------------------------------
// registry tests
// ---------------------------------------------------------------------------

static int test_register_valid(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);
    CHECK(kith_db_create(
              &(kith_db_params_t) {
                  .size = sizeof(kith_db_params_t), .abi_version = KITH_ABI_VERSION
              },
              reactor,
              nullptr,
              &db) == 0);

    int rc = kith_db_register_query(db, "get_value", "SELECT 1", 0);
    CHECK(rc == 0);

    uint32_t n_params = 99;
    rc = kith_db_lookup_query(db, "get_value", &n_params);
    CHECK(rc == 0);
    CHECK(n_params == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_register_duplicate(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);
    CHECK(kith_db_create(
              &(kith_db_params_t) {
                  .size = sizeof(kith_db_params_t), .abi_version = KITH_ABI_VERSION
              },
              reactor,
              nullptr,
              &db) == 0);

    CHECK(kith_db_register_query(db, "q", "SELECT 1", 0) == 0);
    int rc = kith_db_register_query(db, "q", "SELECT 2", 0);
    CHECK(rc == -(int)KITH_EEXIST);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_register_bad_args(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);
    CHECK(kith_db_create(
              &(kith_db_params_t) {
                  .size = sizeof(kith_db_params_t), .abi_version = KITH_ABI_VERSION
              },
              reactor,
              nullptr,
              &db) == 0);

    CHECK(kith_db_register_query(nullptr, "q", "SELECT 1", 0) == -(int)KITH_EINVAL);
    CHECK(kith_db_register_query(db, nullptr, "SELECT 1", 0) == -(int)KITH_EINVAL);
    CHECK(kith_db_register_query(db, "q", nullptr, 0) == -(int)KITH_EINVAL);
    CHECK(kith_db_register_query(db, "", "SELECT 1", 0) == -(int)KITH_EINVAL);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

static int test_lookup_missing(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(db_create_reactor(&reactor) == 0);
    CHECK(kith_db_create(
              &(kith_db_params_t) {
                  .size = sizeof(kith_db_params_t), .abi_version = KITH_ABI_VERSION
              },
              reactor,
              nullptr,
              &db) == 0);

    uint32_t n_params = 0;
    int rc = kith_db_lookup_query(db, "no_such", &n_params);
    CHECK(rc == -(int)KITH_ENOENT);

    CHECK(kith_db_lookup_query(nullptr, "q", &n_params) == -(int)KITH_EINVAL);
    CHECK(kith_db_lookup_query(db, nullptr, &n_params) == -(int)KITH_EINVAL);
    CHECK(kith_db_lookup_query(db, "q", nullptr) == -(int)KITH_EINVAL);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int failures = 0;
    failures += test_create_valid();
    failures += test_create_null_params();
    failures += test_create_null_reactor();
    failures += test_create_null_out();
    failures += test_create_bad_abi();
    failures += test_create_undersized();
    failures += test_create_min_gt_max();
    failures += test_destroy_null();
    failures += test_create_custom_host();
    failures += test_create_owns_password_copy();
    failures += test_create_scrubs_password_copy();
    failures += test_register_valid();
    failures += test_register_duplicate();
    failures += test_register_bad_args();
    failures += test_lookup_missing();
    if (failures)
    {
        (void)fprintf(stderr, "db create: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}

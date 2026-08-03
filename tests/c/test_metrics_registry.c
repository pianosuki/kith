#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/metrics/metrics.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "metrics registry: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int test_create_validation(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_metrics_params_t bad_abi = {
        .size = sizeof(kith_metrics_params_t),
        .abi_version = KITH_ABI_VERSION + 1u,
    };
    CHECK(kith_metrics_create(&bad_abi, nullptr, &m) == kith_error_return(KITH_EABIVER));
    CHECK(m == nullptr);

    kith_metrics_params_t small = {
        .size = 8u,
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_metrics_create(&small, nullptr, &m) == kith_error_return(KITH_ESIZE));
    CHECK(m == nullptr);
    return failures;
}

static int test_default_and_prefix(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    CHECK(m != nullptr);
    CHECK(kith_metrics_counter_add(m, "conn_accepted", nullptr, 0, 1) == 0);
    CHECK(kith_metrics_counter_add(m, "conn_accepted", nullptr, 0, 2) == 0);

    kith_metrics_params_t params = {
        .size = sizeof(kith_metrics_params_t),
        .abi_version = KITH_ABI_VERSION,
        .prefix = "kith_",
    };
    kith_metrics_t *p = nullptr;
    CHECK(kith_metrics_create(&params, nullptr, &p) == 0);
    CHECK(kith_metrics_counter_add(p, "ops", nullptr, 0, 5) == 0);

    char buf[512];
    const size_t n_default = kith_metrics_render_prometheus(m, buf, sizeof(buf));
    CHECK(n_default > 0);
    CHECK(strstr(buf, "# TYPE conn_accepted counter\n") != nullptr);
    CHECK(strstr(buf, "conn_accepted 3\n") != nullptr);

    const size_t n_prefix = kith_metrics_render_prometheus(p, buf, sizeof(buf));
    CHECK(n_prefix > 0);
    CHECK(strstr(buf, "# TYPE kith_ops counter\n") != nullptr);
    CHECK(strstr(buf, "kith_ops 5\n") != nullptr);

    kith_metrics_destroy(m);
    kith_metrics_destroy(p);
    return failures;
}

static int test_arg_validation(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    CHECK(kith_metrics_counter_add(nullptr, "x", nullptr, 0, 1) == kith_error_return(KITH_EINVAL));
    CHECK(kith_metrics_counter_add(m, nullptr, nullptr, 0, 1) == kith_error_return(KITH_EINVAL));
    CHECK(kith_metrics_counter_add(m, "x", nullptr, 1, 1) == kith_error_return(KITH_EINVAL));
    CHECK(kith_metrics_gauge_set(nullptr, "x", nullptr, 0, 1) == kith_error_return(KITH_EINVAL));
    CHECK(kith_metrics_gauge_set(m, nullptr, nullptr, 0, 1) == kith_error_return(KITH_EINVAL));
    CHECK(kith_metrics_gauge_set(m, "x", nullptr, 1, 1) == kith_error_return(KITH_EINVAL));
    CHECK(kith_metrics_histogram_observe(nullptr, "x", nullptr, 0, 1) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_metrics_histogram_observe(m, nullptr, nullptr, 0, 1) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_metrics_histogram_observe(m, "x", nullptr, 1, 1) == kith_error_return(KITH_EINVAL));
    kith_metrics_destroy(m);
    return failures;
}

static int test_label_identity(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    const kith_metrics_label_t tcp[] = {{"transport", "tcp"}};
    const kith_metrics_label_t udp[] = {{"transport", "udp"}};
    const kith_metrics_label_t swapped[] = {{"transport", "udp"}, {"path", "x"}};
    const kith_metrics_label_t ordered[] = {{"path", "x"}, {"transport", "udp"}};
    CHECK(kith_metrics_counter_add(m, "frames", tcp, 1, 1) == 0);
    CHECK(kith_metrics_counter_add(m, "frames", tcp, 1, 1) == 0);
    CHECK(kith_metrics_counter_add(m, "frames", udp, 1, 1) == 0);
    CHECK(kith_metrics_counter_add(m, "frames", swapped, 2, 1) == 0);
    CHECK(kith_metrics_counter_add(m, "frames", ordered, 2, 1) == 0);

    char buf[1024];
    const size_t n = kith_metrics_render_prometheus(m, buf, sizeof(buf));
    CHECK(n > 0);
    CHECK(strstr(buf, "frames{transport=\"tcp\"} 2\n") != nullptr);
    CHECK(strstr(buf, "frames{transport=\"udp\"} 1\n") != nullptr);
    CHECK(strstr(buf, "frames{path=\"x\",transport=\"udp\"} 2\n") != nullptr);
    kith_metrics_destroy(m);
    return failures;
}

static int test_null_key_label_skipped(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    /* A NULL-key label is skipped by the canonicalized stored set, so every
     * position of the NULL entry (and its absence) must resolve to the one
     * series the remaining labels name instead of minting a duplicate with
     * the same stored labels. */
    const kith_metrics_label_t null_last[] = {{"route", "a"}, {nullptr, "ignored"}};
    const kith_metrics_label_t null_first[] = {{nullptr, "ignored"}, {"route", "a"}};
    const kith_metrics_label_t bare[] = {{"route", "a"}};
    CHECK(kith_metrics_counter_add(m, "hits", null_last, 2, 1) == 0);
    CHECK(kith_metrics_counter_add(m, "hits", null_first, 2, 1) == 0);
    CHECK(kith_metrics_counter_add(m, "hits", bare, 1, 1) == 0);

    char buf[1024];
    const size_t n = kith_metrics_render_prometheus(m, buf, sizeof(buf));
    CHECK(n > 0);
    CHECK(strstr(buf, "hits{route=\"a\"} 3\n") != nullptr);
    CHECK(strstr(buf, "\nhits 1\n") == nullptr);
    kith_metrics_destroy(m);
    return failures;
}

static int test_name_kind_families_render_separately(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    /* A counter and a gauge sharing a name are separate families: each
     * renders under its own # TYPE header and no record lands under the
     * other kind's header. */
    CHECK(kith_metrics_counter_add(m, "pool", nullptr, 0, 1) == 0);
    CHECK(kith_metrics_gauge_set(m, "pool", nullptr, 0, 7) == 0);
    char buf[512];
    const size_t n = kith_metrics_render_prometheus(m, buf, sizeof(buf));
    CHECK(n > 0);
    const char *counter_type = strstr(buf, "# TYPE pool counter\n");
    const char *gauge_type = strstr(buf, "# TYPE pool gauge\n");
    CHECK(counter_type != nullptr);
    CHECK(gauge_type != nullptr);
    const char *counter_rec = strstr(buf, "\npool 1\n");
    /* A null gauge_type is already a recorded failure; the dependent
     * lookups only mean something once it succeeded, and strstr requires
     * a non-null haystack. */
    if (gauge_type != nullptr)
    {
        CHECK(counter_rec != nullptr && counter_rec < gauge_type);
        CHECK(strstr(gauge_type, "\npool 7\n") != nullptr);
    }
    kith_metrics_destroy(m);
    return failures;
}

static int test_gauge_signed(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    CHECK(kith_metrics_gauge_set(m, "depth", nullptr, 0, -42) == 0);
    CHECK(kith_metrics_gauge_set(m, "depth", nullptr, 0, 7) == 0);
    char buf[256];
    const size_t n = kith_metrics_render_prometheus(m, buf, sizeof(buf));
    CHECK(n > 0);
    CHECK(strstr(buf, "# TYPE depth gauge\n") != nullptr);
    CHECK(strstr(buf, "depth 7\n") != nullptr);
    kith_metrics_destroy(m);
    return failures;
}

static int test_histogram(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    const uint64_t samples[] = {1, 3, 7, 100, 9999};
    for (size_t i = 0; i < sizeof(samples) / sizeof(samples[0]); ++i)
    {
        CHECK(kith_metrics_histogram_observe(m, "latency", nullptr, 0, samples[i]) == 0);
    }
    char buf[1024];
    const size_t n = kith_metrics_render_prometheus(m, buf, sizeof(buf));
    CHECK(n > 0);
    CHECK(strstr(buf, "# TYPE latency histogram\n") != nullptr);
    /* bucket <=1 has 1; <=5 has 2; <=10 has 3; <=100 has 4; +Inf has 5 */
    CHECK(strstr(buf, "latency_bucket{le=\"1\"} 1\n") != nullptr);
    CHECK(strstr(buf, "latency_bucket{le=\"5\"} 2\n") != nullptr);
    CHECK(strstr(buf, "latency_bucket{le=\"10\"} 3\n") != nullptr);
    CHECK(strstr(buf, "latency_bucket{le=\"100\"} 4\n") != nullptr);
    CHECK(strstr(buf, "latency_bucket{le=\"+Inf\"} 5\n") != nullptr);
    /* sum = 1+3+7+100+9999 = 10110, count = 5 */
    CHECK(strstr(buf, "latency_sum 10110\n") != nullptr);
    CHECK(strstr(buf, "latency_count 5\n") != nullptr);
    kith_metrics_destroy(m);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_validation();
    rc |= test_default_and_prefix();
    rc |= test_arg_validation();
    rc |= test_label_identity();
    rc |= test_null_key_label_skipped();
    rc |= test_name_kind_families_render_separately();
    rc |= test_gauge_signed();
    rc |= test_histogram();
    if (rc != 0)
    {
        (void)fprintf(stderr, "metrics registry tests FAILED\n");
    }
    return rc;
}

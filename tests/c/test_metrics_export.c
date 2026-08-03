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
        (void)fprintf(stderr, "metrics export: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int test_prometheus_shape(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    CHECK(kith_metrics_counter_add(m, "conn_accepted", nullptr, 0, 1) == 0);
    const kith_metrics_label_t labels[] = {{"transport", "tcp"}};
    CHECK(kith_metrics_counter_add(m, "frames", labels, 1, 4) == 0);
    CHECK(kith_metrics_gauge_set(m, "depth", nullptr, 0, 9) == 0);

    char buf[1024];
    const size_t n = kith_metrics_render_prometheus(m, buf, sizeof(buf));
    CHECK(n > 0);
    buf[(n < sizeof(buf) - 1) ? n : sizeof(buf) - 1] = '\0';
    CHECK(strstr(buf, "# TYPE conn_accepted counter\n") != nullptr);
    CHECK(strstr(buf, "conn_accepted 1\n") != nullptr);
    CHECK(strstr(buf, "# TYPE frames counter\n") != nullptr);
    CHECK(strstr(buf, "frames{transport=\"tcp\"} 4\n") != nullptr);
    CHECK(strstr(buf, "# TYPE depth gauge\n") != nullptr);
    CHECK(strstr(buf, "depth 9\n") != nullptr);
    kith_metrics_destroy(m);
    return failures;
}

static int test_prometheus_measure_only(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    CHECK(kith_metrics_counter_add(m, "ops", nullptr, 0, 1) == 0);
    const size_t big = kith_metrics_render_prometheus(m, nullptr, 0);
    CHECK(big > 0);
    char *buf = malloc(big + 1);
    CHECK(buf != nullptr);
    if (buf != nullptr)
    {
        const size_t n = kith_metrics_render_prometheus(m, buf, big + 1);
        CHECK(n == big);
        buf[n] = '\0';
        CHECK(strstr(buf, "ops 1\n") != nullptr);
        free(buf);
    }
    kith_metrics_destroy(m);
    return failures;
}

static int test_prometheus_truncation(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    CHECK(kith_metrics_counter_add(m, "ops", nullptr, 0, 1) == 0);
    const size_t full = kith_metrics_render_prometheus(m, nullptr, 0);
    CHECK(full > 4);
    char tiny[4];
    const size_t n = kith_metrics_render_prometheus(m, tiny, sizeof(tiny));
    CHECK(n == full);
    kith_metrics_destroy(m);
    return failures;
}

static int test_prometheus_label_escape(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    const kith_metrics_label_t labels[] = {{"path", "C:\\tmp\\x"}, {"q", "a\"b\nc"}};
    CHECK(kith_metrics_counter_add(m, "ops", labels, 2, 1) == 0);
    char buf[512];
    const size_t n = kith_metrics_render_prometheus(m, buf, sizeof(buf));
    CHECK(n > 0);
    buf[(n < sizeof(buf) - 1) ? n : sizeof(buf) - 1] = '\0';
    CHECK(strstr(buf, "path=\"C:\\\\tmp\\\\x\"") != nullptr);
    CHECK(strstr(buf, "q=\"a\\\"b\\nc\"") != nullptr);
    kith_metrics_destroy(m);
    return failures;
}

static int test_otlp_shape(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    CHECK(kith_metrics_counter_add(m, "conn_accepted", nullptr, 0, 1) == 0);
    const kith_metrics_label_t labels[] = {{"transport", "tcp"}};
    CHECK(kith_metrics_counter_add(m, "frames", labels, 1, 4) == 0);
    CHECK(kith_metrics_gauge_set(m, "depth", nullptr, 0, -7) == 0);
    CHECK(kith_metrics_histogram_observe(m, "latency", nullptr, 0, 3) == 0);

    char buf[4096];
    const size_t n = kith_metrics_export_otlp(m, buf, sizeof(buf));
    CHECK(n > 0);
    buf[(n < sizeof(buf) - 1) ? n : sizeof(buf) - 1] = '\0';
    /* One payload object with the canonical OTLP envelope. */
    CHECK(
        strstr(
            buf,
            "{\"resource_metrics\":[{\"scope_metrics\":[{\"scope\":{\"name\":\"kith\"},\"metrics\":[") !=
        nullptr);
    /* Counter is monotonic cumulative (temporality 2). */
    CHECK(strstr(buf, "\"name\":\"conn_accepted\"") != nullptr);
    CHECK(strstr(buf, "\"sum\":{\"is_monotonic\":true,\"aggregation_temporality\":2") != nullptr);
    CHECK(strstr(buf, "\"as_int\":\"1\"") != nullptr);
    /* Labeled counter carries the attribute. */
    CHECK(strstr(buf, "\"key\":\"transport\"") != nullptr);
    CHECK(strstr(buf, "\"stringValue\":\"tcp\"") != nullptr);
    CHECK(strstr(buf, "\"as_int\":\"4\"") != nullptr);
    /* Gauge is int64 (negative value survives). */
    CHECK(strstr(buf, "\"gauge\":{\"data_points\":[") != nullptr);
    CHECK(strstr(buf, "\"as_int\":\"-7\"") != nullptr);
    /* Histogram carries explicit bounds, bucket counts, sum, count. */
    CHECK(strstr(buf, "\"histogram\":{\"aggregation_temporality\":2") != nullptr);
    CHECK(strstr(buf, "\"sum\":3") != nullptr);
    CHECK(strstr(buf, "\"count\":\"1\"") != nullptr);
    CHECK(strstr(buf, "\"explicit_bounds\":[1,2,5,10,20,50,100,200,500,1000,2000,5000]") !=
          nullptr);
    CHECK(
        strstr(
            buf,
            "\"bucket_counts\":[\"0\",\"0\",\"1\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\"]") !=
        nullptr);
    /* Single payload terminator. */
    CHECK(strstr(buf, "]}]}]}") != nullptr);
    kith_metrics_destroy(m);
    return failures;
}

static int test_otlp_measure_only(void)
{
    int failures = 0;
    kith_metrics_t *m = nullptr;
    CHECK(kith_metrics_create(nullptr, nullptr, &m) == 0);
    CHECK(kith_metrics_counter_add(m, "ops", nullptr, 0, 1) == 0);
    const size_t big = kith_metrics_export_otlp(m, nullptr, 0);
    CHECK(big > 0);
    char *buf = malloc(big + 1);
    CHECK(buf != nullptr);
    if (buf != nullptr)
    {
        const size_t n = kith_metrics_export_otlp(m, buf, big + 1);
        CHECK(n == big);
        buf[n] = '\0';
        CHECK(strstr(buf, "\"name\":\"ops\"") != nullptr);
        free(buf);
    }
    kith_metrics_destroy(m);
    return failures;
}

static int test_null_registry(void)
{
    int failures = 0;
    CHECK(kith_metrics_render_prometheus(nullptr, nullptr, 0) == 0);
    CHECK(kith_metrics_export_otlp(nullptr, nullptr, 0) == 0);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_prometheus_shape();
    rc |= test_prometheus_measure_only();
    rc |= test_prometheus_truncation();
    rc |= test_prometheus_label_escape();
    rc |= test_otlp_shape();
    rc |= test_otlp_measure_only();
    rc |= test_null_registry();
    if (rc != 0)
    {
        (void)fprintf(stderr, "metrics export tests FAILED\n");
    }
    return rc;
}

/* Pins for the interpreter's handler-exception counter: the util atomic
 * counts every raised exception and never resets, and the server's tick-path
 * recorder folds the process-global delta into the metrics registry as
 * kith_python_handler_exceptions_total — the delta advances only when new
 * exceptions were recorded, so a quiet interval leaves the rendered series
 * untouched. Compiled against src/server/wiring.c so the internal recorder
 * is directly observable. */

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/metrics/metrics.h"
#include "kith/util/util.h"
#include "kith/version.h"
#include "server/server_internal.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "check failed at line %d\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int test_note_and_getter(void)
{
    int failures = 0;
    uint64_t before = kith_python_handler_exceptions();
    kith_python_note_handler_exception();
    kith_python_note_handler_exception();
    kith_python_note_handler_exception();
    CHECK(kith_python_handler_exceptions() == before + 3u);
    return failures;
}

static int test_recorder_delta_folding(void)
{
    int failures = 0;
    kith_metrics_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.size = sizeof(mp);
    mp.abi_version = KITH_ABI_VERSION;
    kith_metrics_t *metrics = nullptr;
    CHECK(kith_metrics_create(&mp, nullptr, &metrics) == 0);
    CHECK(metrics != nullptr);

    struct kith_server s;
    memset(&s, 0, sizeof(s));
    s.metrics = metrics;
    // Baseline the last-value field at the current process count: the
    // counter is process-global and earlier tests in this binary may have
    // noted exceptions, so the recorder's first fold carries only the
    // exceptions noted after the baseline.
    s.python_handler_exceptions_last = kith_python_handler_exceptions();

    kith_python_note_handler_exception();
    kith_python_note_handler_exception();
    server_record_python_handler_exceptions(&s);

    char buf[4096];
    memset(buf, 0, sizeof(buf));
    (void)kith_metrics_render_prometheus(metrics, buf, sizeof(buf));
    CHECK(strstr(buf, "kith_python_handler_exceptions_total 2\n") != nullptr);

    // A quiet interval takes no delta: the recorder records nothing and the
    // rendered series stays at the folded total.
    server_record_python_handler_exceptions(&s);
    memset(buf, 0, sizeof(buf));
    (void)kith_metrics_render_prometheus(metrics, buf, sizeof(buf));
    CHECK(strstr(buf, "kith_python_handler_exceptions_total 2\n") != nullptr);

    kith_python_note_handler_exception();
    server_record_python_handler_exceptions(&s);
    memset(buf, 0, sizeof(buf));
    (void)kith_metrics_render_prometheus(metrics, buf, sizeof(buf));
    CHECK(strstr(buf, "kith_python_handler_exceptions_total 3\n") != nullptr);

    kith_metrics_destroy(metrics);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_note_and_getter();
    rc |= test_recorder_delta_folding();
    if (rc != 0)
    {
        (void)fprintf(stderr, "python handler exception tests FAILED\n");
    }
    return rc;
}

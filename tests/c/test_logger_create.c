#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/logger/logger.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "logger create: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int test_noop_logger(void)
{
    int failures = 0;
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(nullptr, nullptr, &lg) == 0);
    CHECK(lg != nullptr);
    CHECK(kith_logger_level(lg) == KITH_LOG_LEVEL_ERROR);
    CHECK(kith_logger_enabled(lg, KITH_LOG_LEVEL_ERROR) == true);
    CHECK(kith_logger_enabled(lg, KITH_LOG_LEVEL_INFO) == false);
    const kith_logger_field_t fields[] = {{"actor", "1"}};
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_ERROR, "f.c", 9, fields, 1, "noop") == 0);
    CHECK(kith_logger_log(nullptr, KITH_LOG_LEVEL_INFO, "f.c", 1, nullptr, 0, "x") == 0);
    kith_logger_destroy(lg);
    kith_logger_destroy(nullptr);
    return failures;
}

static int test_create_validation(void)
{
    int failures = 0;
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_logger_params_t bad_abi = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION + 1u,
    };
    CHECK(kith_logger_create(&bad_abi, nullptr, &lg) == kith_error_return(KITH_EABIVER));
    CHECK(lg == nullptr);

    kith_logger_params_t small = {
        .size = 8u,
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_logger_create(&small, nullptr, &lg) == kith_error_return(KITH_ESIZE));
    CHECK(lg == nullptr);

    kith_logger_params_t bad_path = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "x",
        .file_path = "/tmp/kith_no_such_dir_zzz/log.txt",
    };
    CHECK(kith_logger_create(&bad_path, nullptr, &lg) == kith_error_return(KITH_EIO));
    CHECK(lg == nullptr);
    return failures;
}

static int test_level_get_set_enabled(void)
{
    int failures = 0;
    kith_logger_params_t params = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "aoi",
        .min_level = KITH_LOG_LEVEL_WARN,
    };
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(&params, nullptr, &lg) == 0);
    CHECK(kith_logger_level(lg) == KITH_LOG_LEVEL_WARN);
    CHECK(kith_logger_enabled(lg, KITH_LOG_LEVEL_WARN) == true);
    CHECK(kith_logger_enabled(lg, KITH_LOG_LEVEL_INFO) == false);
    CHECK(kith_logger_enabled(lg, KITH_LOG_LEVEL_ERROR) == true);
    CHECK(kith_logger_enabled(nullptr, KITH_LOG_LEVEL_ERROR) == false);
    CHECK(kith_logger_set_level(lg, KITH_LOG_LEVEL_TRACE) == 0);
    CHECK(kith_logger_level(lg) == KITH_LOG_LEVEL_TRACE);
    CHECK(kith_logger_enabled(lg, KITH_LOG_LEVEL_TRACE) == true);
    CHECK(kith_logger_set_level(nullptr, KITH_LOG_LEVEL_INFO) == kith_error_return(KITH_EINVAL));
    CHECK(kith_logger_level(nullptr) == KITH_LOG_LEVEL_ERROR);
    kith_logger_destroy(lg);
    return failures;
}

static int test_log_arg_validation(void)
{
    int failures = 0;
    kith_logger_params_t params = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "aoi",
        .min_level = KITH_LOG_LEVEL_TRACE,
    };
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(&params, nullptr, &lg) == 0);
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "f.c", 1, nullptr, 1, "x") ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "f.c", 1, nullptr, 0, "x") == 0);
    kith_logger_destroy(lg);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_noop_logger();
    rc |= test_create_validation();
    rc |= test_level_get_set_enabled();
    rc |= test_log_arg_validation();
    if (rc != 0)
    {
        (void)fprintf(stderr, "logger create tests FAILED\n");
    }
    return rc;
}

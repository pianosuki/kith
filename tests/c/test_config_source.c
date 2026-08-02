/* Units for the configuration source: file parsing edges, environment
 * overlay quirks, typed accessor parse/range errors, and argument guards.
 * Drives src/config/config.c through the public contract in
 * include/kith/config/config.h. */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/config/config.h"
#include "kith/types.h"
#include "kith/version.h"

#include <sys/stat.h>

/* Accumulating assertion: each CHECK adds 0 branches and 1 statement at the
 * call site (the branch lives once in check_cond), so a scenario function
 * stays within the function-size limits. Out-variables are initialized to
 * safe values so a failed lookup does not feed garbage to a follow-up check. */
static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "config source: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

/* Write @p body to a fresh file and return its path (static storage).
 * Returns NULL on I/O failure. */
static const char *write_tmp(const char *name, const char *body)
{
    static char path[256];
    (void)snprintf(path, sizeof(path), "/tmp/%s", name);
    FILE *f = fopen(path, "wb");
    if (f == nullptr || fputs(body, f) == EOF || fclose(f) != 0)
    {
        return nullptr;
    }
    return path;
}

static void cleanup_tmp(const char *name)
{
    char path[256];
    (void)snprintf(path, sizeof(path), "/tmp/%s", name);
    (void)remove(path);
}

static int test_empty_source(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(nullptr, nullptr, &cfg) == 0);
    CHECK(cfg != nullptr);
    CHECK(kith_config_has(cfg, "anything") == false);
    const char *s = "";
    CHECK(kith_config_string(cfg, "x", &s) == kith_error_return(KITH_ENOENT));
    kith_config_destroy(cfg);
    kith_config_destroy(nullptr);
    return failures;
}

static int test_create_validation(void)
{
    int failures = 0;
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_config_params_t bad_abi = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION + 1u,
    };
    CHECK(kith_config_create(&bad_abi, nullptr, &cfg) == kith_error_return(KITH_EABIVER));
    CHECK(cfg == nullptr);

    kith_config_params_t small = {
        .size = 8u,
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_config_create(&small, nullptr, &cfg) == kith_error_return(KITH_ESIZE));
    CHECK(cfg == nullptr);
    return failures;
}

static int test_file_loading(void)
{
    int failures = 0;
    const char *body = "# a comment\n"
                       "\n"
                       "  name = kith \n"
                       "port=9000\n"
                       "quoted=\"hello world\"\n"
                       "empty=\n"
                       "noequals\n"
                       "trailing = value  \n";
    const char *path = write_tmp("kith_cfg_file.conf", body);
    CHECK(path != nullptr);
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = path,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);

    const char *s = "";
    CHECK(kith_config_string(cfg, "name", &s) == 0);
    CHECK(strcmp(s, "kith") == 0);
    CHECK(kith_config_string(cfg, "quoted", &s) == 0);
    CHECK(strcmp(s, "hello world") == 0);
    CHECK(kith_config_string(cfg, "empty", &s) == 0);
    CHECK(strcmp(s, "") == 0);
    CHECK(kith_config_string(cfg, "trailing", &s) == 0);
    CHECK(strcmp(s, "value") == 0);
    CHECK(kith_config_has(cfg, "noequals") == false);

    uint16_t port = 0;
    CHECK(kith_config_u16(cfg, "port", 1, 65535, &port) == 0);
    CHECK(port == 9000);

    kith_config_destroy(cfg);
    cleanup_tmp("kith_cfg_file.conf");
    return failures;
}

static int test_missing_file_is_not_error(void)
{
    int failures = 0;
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = "/tmp/kith_does_not_exist_zzzz",
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);
    CHECK(kith_config_has(cfg, "anything") == false);
    kith_config_destroy(cfg);
    return failures;
}

static int test_env_overlay_and_prefix(void)
{
    int failures = 0;
    CHECK(setenv("KITHTEST_port", "7000", 1) == 0);
    CHECK(setenv("KITHTEST_flag", "true", 1) == 0);
    CHECK(setenv("PATH_LIKE", "ignore", 1) == 0);

    const char *path = write_tmp("kith_cfg_env.conf", "port=9000\nother=keep\n");
    CHECK(path != nullptr);
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .env_prefix = "KITHTEST_",
        .file_path = path,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);

    uint16_t port = 0;
    CHECK(kith_config_u16(cfg, "port", 1, 65535, &port) == 0);
    CHECK(port == 7000);
    bool flag = false;
    CHECK(kith_config_bool(cfg, "flag", &flag) == 0);
    CHECK(flag == true);
    const char *s = "";
    CHECK(kith_config_string(cfg, "other", &s) == 0);
    CHECK(strcmp(s, "keep") == 0);
    CHECK(kith_config_has(cfg, "PATH_LIKE") == false);
    CHECK(kith_config_has(cfg, "LIKE") == false);

    kith_config_destroy(cfg);
    cleanup_tmp("kith_cfg_env.conf");
    unsetenv("KITHTEST_port");
    unsetenv("KITHTEST_flag");
    unsetenv("PATH_LIKE");
    return failures;
}

static int test_typed_accessors_u16(void)
{
    int failures = 0;
    const char *body = "good_u16=100\n"
                       "big_u16=99999\n"
                       "bad_int=notanumber\n";
    const char *path = write_tmp("kith_cfg_u16.conf", body);
    CHECK(path != nullptr);
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = path,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);

    uint16_t v = 0;
    CHECK(kith_config_u16(cfg, "good_u16", 0, 1000, &v) == 0);
    CHECK(v == 100);
    CHECK(kith_config_u16(cfg, "good_u16", 0, 50, &v) == kith_error_return(KITH_ERANGE));
    CHECK(kith_config_u16(cfg, "big_u16", 0, 65535, &v) == kith_error_return(KITH_ERANGE));
    CHECK(kith_config_u16(cfg, "bad_int", 0, 65535, &v) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u16(cfg, "absent", 0, 65535, &v) == kith_error_return(KITH_ENOENT));
    CHECK(kith_config_u16(cfg, "good_u16", 50, 40, &v) == kith_error_return(KITH_ERANGE));

    kith_config_destroy(cfg);
    cleanup_tmp("kith_cfg_u16.conf");
    return failures;
}

static int test_typed_accessors_wide_and_bool(void)
{
    int failures = 0;
    const char *body = "u32=3000000000\n"
                       "u64=18446744073709551615\n"
                       "bt=true\n"
                       "bf=0\n"
                       "byt=yes\n"
                       "bon=on\n"
                       "bunk=maybe\n";
    const char *path = write_tmp("kith_cfg_wide.conf", body);
    CHECK(path != nullptr);
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = path,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);

    uint32_t v32 = 0;
    CHECK(kith_config_u32(cfg, "u32", 0, 4000000000u, &v32) == 0);
    CHECK(v32 == 3000000000u);
    uint64_t v64 = 0;
    CHECK(kith_config_u64(cfg, "u64", 0, UINT64_MAX, &v64) == 0);
    CHECK(v64 == UINT64_MAX);

    bool b = false;
    CHECK(kith_config_bool(cfg, "bt", &b) == 0);
    CHECK(b == true);
    CHECK(kith_config_bool(cfg, "bf", &b) == 0);
    CHECK(b == false);
    CHECK(kith_config_bool(cfg, "byt", &b) == 0);
    CHECK(b == true);
    CHECK(kith_config_bool(cfg, "bon", &b) == 0);
    CHECK(b == true);
    CHECK(kith_config_bool(cfg, "bunk", &b) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_bool(cfg, "absent", &b) == kith_error_return(KITH_ENOENT));

    kith_config_destroy(cfg);
    cleanup_tmp("kith_cfg_wide.conf");
    return failures;
}

/* Present-but-unreadable file paths report KITH_EIO rather than silently
 * degrading to an empty source: a directory fails the regular-file check up
 * front, and a zero-permission file fails fopen with EACCES. */
static int test_file_error_paths(void)
{
    int failures = 0;
    const char *dir = "/tmp/kith_cfg_dir_probe";
    (void)remove(dir);
    CHECK(mkdir(dir, 0755) == 0);

    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = dir,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == kith_error_return(KITH_EIO));
    CHECK(cfg == nullptr);
    (void)remove(dir);

    const char *path = write_tmp("kith_cfg_noperm.conf", "port=9000\n");
    CHECK(path != nullptr);
    params.file_path = path;
    CHECK(chmod(path, 0000) == 0);
    CHECK(kith_config_create(&params, nullptr, &cfg) == kith_error_return(KITH_EIO));
    CHECK(cfg == nullptr);
    (void)chmod(path, S_IRUSR | S_IWUSR);
    cleanup_tmp("kith_cfg_noperm.conf");
    return failures;
}

/* Line-format edges: CRLF terminators trim as whitespace, a value containing
 * '=' keeps everything past the first '=', and a final line without a
 * newline still parses. */
static int test_line_format_edges(void)
{
    int failures = 0;
    const char *body = "crlf_num=42\r\n"
                       "crlf_text = hello \r\n"
                       "split=a=b=c\r\n"
                       "last_no_newline=tail";
    const char *path = write_tmp("kith_cfg_lines.conf", body);
    CHECK(path != nullptr);
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = path,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);
    CHECK(cfg != nullptr);

    uint16_t num = 0;
    CHECK(kith_config_u16(cfg, "crlf_num", 0, 65535, &num) == 0);
    CHECK(num == 42);
    const char *s = "";
    CHECK(kith_config_string(cfg, "crlf_text", &s) == 0);
    CHECK(strcmp(s, "hello") == 0);
    CHECK(kith_config_string(cfg, "split", &s) == 0);
    CHECK(strcmp(s, "a=b=c") == 0);
    CHECK(kith_config_string(cfg, "last_no_newline", &s) == 0);
    CHECK(strcmp(s, "tail") == 0);

    kith_config_destroy(cfg);
    cleanup_tmp("kith_cfg_lines.conf");
    return failures;
}

/* Inserting past the initial 16-slot capacity grows the entry table through
 * its realloc path; every entry stays addressable after the growth. */
static int test_entry_table_growth(void)
{
    int failures = 0;
    const char *body = "grow_00=value_00\n"
                       "grow_01=value_01\n"
                       "grow_02=value_02\n"
                       "grow_03=value_03\n"
                       "grow_04=value_04\n"
                       "grow_05=value_05\n"
                       "grow_06=value_06\n"
                       "grow_07=value_07\n"
                       "grow_08=value_08\n"
                       "grow_09=value_09\n"
                       "grow_10=value_10\n"
                       "grow_11=value_11\n"
                       "grow_12=value_12\n"
                       "grow_13=value_13\n"
                       "grow_14=value_14\n"
                       "grow_15=value_15\n"
                       "grow_16=value_16\n"
                       "grow_17=value_17\n"
                       "grow_18=value_18\n"
                       "grow_19=value_19\n";
    const char *path = write_tmp("kith_cfg_growth.conf", body);
    CHECK(path != nullptr);
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = path,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);
    CHECK(cfg != nullptr);

    for (int i = 0; i < 20; ++i)
    {
        char key[16];
        char want[16];
        const char *s = "";
        CHECK(snprintf(key, sizeof(key), "grow_%02d", i) > 0);
        CHECK(snprintf(want, sizeof(want), "value_%02d", i) > 0);
        CHECK(kith_config_string(cfg, key, &s) == 0);
        CHECK(strcmp(s, want) == 0);
    }

    kith_config_destroy(cfg);
    cleanup_tmp("kith_cfg_growth.conf");
    return failures;
}

/* putenv retains the caller's pointer, so the malformed entry lives in
 * static storage. The name carries the prefix but no '='; the environment
 * scan skips it and the remaining prefixed variable loads normally. */
static char env_without_equals[] = "KITHTEST_MALFORMED_NOEQ";

static int test_env_entry_without_equals(void)
{
    int failures = 0;
    CHECK(setenv("KITHTEST_present", "1", 1) == 0);
    CHECK(putenv(env_without_equals) == 0);

    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .env_prefix = "KITHTEST_",
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);
    CHECK(cfg != nullptr);

    CHECK(kith_config_has(cfg, "present"));
    CHECK(!kith_config_has(cfg, "MALFORMED_NOEQ"));

    kith_config_destroy(cfg);
    unsetenv("KITHTEST_present");
    unsetenv("KITHTEST_MALFORMED_NOEQ");
    return failures;
}

/* Every accepted boolean spelling parses. The key mirrors the value so each
 * row of the parser's spelling table reads back against its expected result,
 * and the out-variable is pre-seeded opposite to the expectation so a no-op
 * write cannot pass. */
struct config_bool_case
{
    const char *key;
    bool want;
};

static int test_bool_spelling_table(void)
{
    int failures = 0;
    const char *body = "TRUE=TRUE\n"
                       "True=True\n"
                       "YES=YES\n"
                       "Yes=Yes\n"
                       "ON=ON\n"
                       "On=On\n"
                       "FALSE=FALSE\n"
                       "False=False\n"
                       "NO=NO\n"
                       "No=No\n"
                       "OFF=OFF\n"
                       "Off=Off\n";
    static const struct config_bool_case cases[] = {
        {"TRUE", true},
        {"True", true},
        {"YES", true},
        {"Yes", true},
        {"ON", true},
        {"On", true},
        {"FALSE", false},
        {"False", false},
        {"NO", false},
        {"No", false},
        {"OFF", false},
        {"Off", false},
    };
    const char *path = write_tmp("kith_cfg_bool_spellings.conf", body);
    CHECK(path != nullptr);
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = path,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);
    CHECK(cfg != nullptr);

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i)
    {
        bool value = !cases[i].want;
        CHECK(kith_config_bool(cfg, cases[i].key, &value) == 0);
        CHECK(value == cases[i].want);
    }

    kith_config_destroy(cfg);
    cleanup_tmp("kith_cfg_bool_spellings.conf");
    return failures;
}

/* Range and parse failures stay distinct: an inverted caller-supplied bound,
 * a value below the minimum, and a representable value above the maximum
 * report ERANGE, as do literals whose magnitude exceeds the destination
 * width; non-numeric text and trailing garbage report EINVAL as parse
 * failures. */
static int test_numeric_edge_cases(void)
{
    int failures = 0;
    const char *body = "small_u16=5\n"
                       "u32_range=100\n"
                       "junk_u32=abc\n"
                       "trail_u32=12x\n"
                       "huge_u64=18446744073709551615\n"
                       "over_u16=99999999999999999999999\n"
                       "over_u32=99999999999999999999999\n"
                       "over_u64=99999999999999999999999\n"
                       "junk_u64=nope\n";
    const char *path = write_tmp("kith_cfg_numeric_edges.conf", body);
    CHECK(path != nullptr);
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
        .file_path = path,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);
    CHECK(cfg != nullptr);

    uint16_t v16 = 0;
    CHECK(kith_config_u16(cfg, "small_u16", 10, 65535, &v16) == kith_error_return(KITH_ERANGE));
    CHECK(kith_config_u16(cfg, "over_u16", 0, UINT16_MAX, &v16) == kith_error_return(KITH_ERANGE));
    uint32_t v32 = 0;
    CHECK(kith_config_u32(cfg, "u32_range", 100, 50, &v32) == kith_error_return(KITH_ERANGE));
    CHECK(kith_config_u32(cfg, "junk_u32", 0, UINT32_MAX, &v32) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u32(cfg, "trail_u32", 0, UINT32_MAX, &v32) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u32(cfg, "over_u32", 0, UINT32_MAX, &v32) == kith_error_return(KITH_ERANGE));
    uint64_t v64 = 0;
    CHECK(kith_config_u64(cfg, "huge_u64", 0, INT64_MAX, &v64) == kith_error_return(KITH_ERANGE));
    CHECK(kith_config_u64(cfg, "over_u64", 0, UINT64_MAX, &v64) == kith_error_return(KITH_ERANGE));
    CHECK(kith_config_u64(cfg, "junk_u64", 0, UINT64_MAX, &v64) == kith_error_return(KITH_EINVAL));

    kith_config_destroy(cfg);
    cleanup_tmp("kith_cfg_numeric_edges.conf");
    return failures;
}

/* Null arguments guard before any lookup on every accessor family, and the
 * predicate reports absence for a null handle or key. */
static int test_accessor_argument_guards(void)
{
    int failures = 0;
    kith_config_params_t params = {
        .size = sizeof(kith_config_params_t),
        .abi_version = KITH_ABI_VERSION,
    };
    kith_config_t *cfg = nullptr;
    CHECK(kith_config_create(&params, nullptr, &cfg) == 0);
    CHECK(cfg != nullptr);

    CHECK(kith_config_has(nullptr, "x") == false);
    CHECK(kith_config_has(cfg, nullptr) == false);

    const char *s = "";
    CHECK(kith_config_string(nullptr, "x", &s) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_string(cfg, nullptr, &s) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_string(cfg, "x", nullptr) == kith_error_return(KITH_EINVAL));

    bool b = false;
    CHECK(kith_config_bool(nullptr, "x", &b) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_bool(cfg, nullptr, &b) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_bool(cfg, "x", nullptr) == kith_error_return(KITH_EINVAL));

    uint16_t v16 = 0;
    CHECK(kith_config_u16(nullptr, "x", 0, 1, &v16) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u16(cfg, nullptr, 0, 1, &v16) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u16(cfg, "x", 0, 1, nullptr) == kith_error_return(KITH_EINVAL));

    uint32_t v32 = 0;
    CHECK(kith_config_u32(nullptr, "x", 0, 1, &v32) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u32(cfg, nullptr, 0, 1, &v32) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u32(cfg, "x", 0, 1, nullptr) == kith_error_return(KITH_EINVAL));

    uint64_t v64 = 0;
    CHECK(kith_config_u64(nullptr, "x", 0, 1, &v64) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u64(cfg, nullptr, 0, 1, &v64) == kith_error_return(KITH_EINVAL));
    CHECK(kith_config_u64(cfg, "x", 0, 1, nullptr) == kith_error_return(KITH_EINVAL));

    kith_config_destroy(cfg);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_empty_source();
    rc |= test_create_validation();
    rc |= test_file_loading();
    rc |= test_missing_file_is_not_error();
    rc |= test_file_error_paths();
    rc |= test_line_format_edges();
    rc |= test_entry_table_growth();
    rc |= test_env_overlay_and_prefix();
    rc |= test_env_entry_without_equals();
    rc |= test_bool_spelling_table();
    rc |= test_typed_accessors_u16();
    rc |= test_typed_accessors_wide_and_bool();
    rc |= test_numeric_edge_cases();
    rc |= test_accessor_argument_guards();
    if (rc != 0)
    {
        (void)fprintf(stderr, "config source tests FAILED\n");
    }
    return rc;
}

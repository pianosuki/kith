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
        (void)fprintf(stderr, "logger sink: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

/* Build a /tmp path for @p name into a caller-supplied buffer. */
static void tmp_path(const char *name, char *out, size_t cap)
{
    (void)snprintf(out, cap, "/tmp/%s", name);
}

/* Read a whole file into a freshly allocated NUL-terminated buffer, or return
 * NULL when the file is absent or cannot be read. The caller frees the
 * buffer. */
static char *read_file(const char *path)
{
    FILE *f = fopen(path, "rb");
    if (f == nullptr)
    {
        return nullptr;
    }
    if (fseek(f, 0, SEEK_END) != 0)
    {
        (void)fclose(f);
        return nullptr;
    }
    long sz = ftell(f);
    if (sz < 0)
    {
        (void)fclose(f);
        return nullptr;
    }
    if (fseek(f, 0, SEEK_SET) != 0)
    {
        (void)fclose(f);
        return nullptr;
    }
    char *buf = malloc((size_t)sz + 1);
    if (buf == nullptr)
    {
        (void)fclose(f);
        return nullptr;
    }
    size_t n = fread(buf, 1, (size_t)sz, f);
    int err = ferror(f);
    (void)fclose(f);
    if (err != 0)
    {
        free(buf);
        return nullptr;
    }
    buf[n] = '\0';
    return buf;
}

static bool contains(const char *hay, const char *needle)
{
    return strstr(hay, needle) != nullptr;
}

static int test_human_line(void)
{
    int failures = 0;
    char path[256];
    tmp_path("kith_log_human.txt", path, sizeof(path));
    (void)remove(path);

    kith_logger_params_t params = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "aoi",
        .file_path = path,
        .min_level = KITH_LOG_LEVEL_TRACE,
    };
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(&params, nullptr, &lg) == 0);
    const kith_logger_field_t fields[] = {
        {"actor", "a1"},
        {"zone", "z1"},
    };
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "aoi/view.c", 42, fields, 2, "entered") == 0);
    kith_logger_destroy(lg);

    char *content = read_file(path);
    CHECK(content != nullptr);
    if (content != nullptr)
    {
        CHECK(contains(content, " | INFO | aoi | entered [actor=a1, zone=z1]\n"));
        CHECK(!contains(content, "TRACE"));
        free(content);
    }
    (void)remove(path);
    return failures;
}

static int test_level_filter_drops(void)
{
    int failures = 0;
    char path[256];
    tmp_path("kith_log_filter.txt", path, sizeof(path));
    (void)remove(path);

    kith_logger_params_t params = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "sim",
        .file_path = path,
        .min_level = KITH_LOG_LEVEL_WARN,
    };
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(&params, nullptr, &lg) == 0);
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_DEBUG, "sim.c", 1, nullptr, 0, "dropped-debug") == 0);
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "sim.c", 2, nullptr, 0, "dropped-info") == 0);
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_WARN, "sim.c", 3, nullptr, 0, "kept-warn") == 0);
    kith_logger_destroy(lg);

    char *content = read_file(path);
    CHECK(content != nullptr);
    if (content != nullptr)
    {
        CHECK(contains(content, "WARN | sim | kept-warn"));
        CHECK(!contains(content, "dropped-debug"));
        CHECK(!contains(content, "dropped-info"));
        free(content);
    }
    (void)remove(path);
    return failures;
}

// set_level retunes the filter for subsequent entries: an entry below the
// create-time minimum never reaches the sink, the same level appears once
// the minimum is lowered, and it is dropped again once the minimum is
// raised. Entries the filter already dropped stay dropped.
static int test_set_level_dynamic_effect(void)
{
    int failures = 0;
    char path[256];
    tmp_path("kith_log_setlevel.txt", path, sizeof(path));
    (void)remove(path);

    kith_logger_params_t params = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "sim",
        .file_path = path,
        .min_level = KITH_LOG_LEVEL_WARN,
    };
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(&params, nullptr, &lg) == 0);
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "sim.c", 1, nullptr, 0, "dropped-at-create") ==
          0);
    CHECK(kith_logger_set_level(lg, KITH_LOG_LEVEL_INFO) == 0);
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "sim.c", 2, nullptr, 0, "kept-after-lower") ==
          0);
    CHECK(kith_logger_set_level(lg, KITH_LOG_LEVEL_ERROR) == 0);
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "sim.c", 3, nullptr, 0, "dropped-after-raise") ==
          0);
    kith_logger_destroy(lg);

    char *content = read_file(path);
    CHECK(content != nullptr);
    if (content != nullptr)
    {
        CHECK(!contains(content, "dropped-at-create"));
        CHECK(contains(content, "INFO | sim | kept-after-lower"));
        CHECK(!contains(content, "dropped-after-raise"));
        free(content);
    }
    (void)remove(path);
    return failures;
}

static int test_jsonl_envelope(void)
{
    int failures = 0;
    char path[256];
    tmp_path("kith_log.jsonl", path, sizeof(path));
    (void)remove(path);

    kith_logger_params_t params = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "aoi",
        .jsonl_path = path,
        .min_level = KITH_LOG_LEVEL_TRACE,
    };
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(&params, nullptr, &lg) == 0);
    const kith_logger_field_t fields[] = {
        {"event", "actor.enter"},
        {"actor", "a1"},
        {"zone", "z1"},
        {"correlation_id", "1a2b3c4d5e6f7081"},
    };
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "aoi/view.c", 42, fields, 4, "entered") == 0);
    kith_logger_destroy(lg);

    char *content = read_file(path);
    CHECK(content != nullptr);
    if (content != nullptr)
    {
        CHECK(content[0] == '{');
        CHECK(contains(content, "\"ts\":\""));
        CHECK(contains(content, "\"level\":\"info\""));
        CHECK(contains(content, "\"module\":\"aoi\""));
        CHECK(contains(content, "\"file\":\"aoi/view.c\""));
        CHECK(contains(content, "\"line\":42"));
        CHECK(contains(content, "\"message\":\"entered\""));
        CHECK(contains(content, "\"event\":\"actor.enter\""));
        CHECK(contains(content, "\"actor\":\"a1\""));
        CHECK(contains(content, "\"zone\":\"z1\""));
        CHECK(contains(content, "\"correlation_id\":\"1a2b3c4d5e6f7081\""));
        CHECK(contains(content, "}\n"));
        CHECK(!contains(content, "\n\n"));
        free(content);
    }
    (void)remove(path);
    return failures;
}

static int test_reserved_key_collision(void)
{
    int failures = 0;
    char jpath[256];
    char hpath[256];
    tmp_path("kith_log_col.jsonl", jpath, sizeof(jpath));
    tmp_path("kith_log_col.txt", hpath, sizeof(hpath));
    (void)remove(jpath);
    (void)remove(hpath);

    kith_logger_params_t params = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "fabric",
        .file_path = hpath,
        .jsonl_path = jpath,
        .min_level = KITH_LOG_LEVEL_TRACE,
    };
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(&params, nullptr, &lg) == 0);
    const kith_logger_field_t fields[] = {
        {"module", "hijack"},
        {"event", "stream.append"},
    };
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_INFO, "fabric.c", 7, fields, 2, "append") == 0);
    kith_logger_destroy(lg);

    char *jsonl = read_file(jpath);
    CHECK(jsonl != nullptr);
    if (jsonl != nullptr)
    {
        CHECK(contains(jsonl, "\"module\":\"fabric\""));
        CHECK(!contains(jsonl, "hijack"));
        CHECK(contains(jsonl, "\"event\":\"stream.append\""));
        free(jsonl);
    }
    char *human = read_file(hpath);
    CHECK(human != nullptr);
    if (human != nullptr)
    {
        CHECK(contains(human, " | INFO | fabric | append [module=hijack, event=stream.append]\n"));
        free(human);
    }
    (void)remove(jpath);
    (void)remove(hpath);
    return failures;
}

static int test_json_escape(void)
{
    int failures = 0;
    char path[256];
    tmp_path("kith_log_esc.jsonl", path, sizeof(path));
    (void)remove(path);

    kith_logger_params_t params = {
        .size = sizeof(kith_logger_params_t),
        .abi_version = KITH_ABI_VERSION,
        .name = "aoi",
        .jsonl_path = path,
        .min_level = KITH_LOG_LEVEL_TRACE,
    };
    kith_logger_t *lg = nullptr;
    CHECK(kith_logger_create(&params, nullptr, &lg) == 0);
    const kith_logger_field_t fields[] = {{"path", "C:\\tmp\\x"}, {"tag", "a\"b\\c\n"}};
    CHECK(kith_logger_log(lg, KITH_LOG_LEVEL_WARN, "aoi.c", 1, fields, 2, "bad\"val") == 0);
    kith_logger_destroy(lg);

    char *content = read_file(path);
    CHECK(content != nullptr);
    if (content != nullptr)
    {
        CHECK(contains(content, "\"path\":\"C:\\\\tmp\\\\x\""));
        CHECK(contains(content, "\"tag\":\"a\\\"b\\\\c\\n\""));
        CHECK(contains(content, "\"message\":\"bad\\\"val\""));
        CHECK(!contains(content, "\n\n"));
        free(content);
    }
    (void)remove(path);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_human_line();
    rc |= test_level_filter_drops();
    rc |= test_set_level_dynamic_effect();
    rc |= test_jsonl_envelope();
    rc |= test_reserved_key_collision();
    rc |= test_json_escape();
    if (rc != 0)
    {
        (void)fprintf(stderr, "logger sink tests FAILED\n");
    }
    return rc;
}

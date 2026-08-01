/* Configuration loader: parses a simple key=value file, merges the process
 * environment with environment-wins precedence, and exposes a sorted lookup
 * interface. Single source file for the config module; the public contract
 * is include/kith/config/config.h. */

#include "kith/config/config.h"

#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/types.h"
#include "kith/version.h"

#include <sys/stat.h>

/* The process environment. Declared here rather than pulled from <unistd.h>
 * to avoid a feature-test-macro dependency; the symbol is available on every
 * target platform. */
extern char **environ;

struct entry
{
    char *key;
    char *value;
};

struct kith_config
{
    const kith_allocator_t *allocator;
    struct entry *entries;
    size_t count;
    size_t cap;
};

static int entry_cmp(const void *a, const void *b)
{
    const struct entry *ea = a;
    const struct entry *eb = b;
    return strcmp(ea->key, eb->key);
}

static int ensure_cap(struct kith_config *cfg)
{
    if (cfg->count < cfg->cap)
    {
        return 0;
    }
    size_t next_cap = cfg->cap == 0 ? 16 : cfg->cap * 2;
    struct entry *next = kith_realloc(cfg->allocator, cfg->entries, next_cap * sizeof(*next));
    if (next == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    cfg->entries = next;
    cfg->cap = next_cap;
    return 0;
}

/* Insert or override a key. Both strings are copied. The value overrides an
 * existing entry with the same key, so loading the environment after the file
 * implements the "environment wins" precedence. */
static int entry_set(struct kith_config *cfg, const char *key, const char *value)
{
    for (size_t i = 0; i < cfg->count; ++i)
    {
        if (strcmp(cfg->entries[i].key, key) == 0)
        {
            char *copy = kith_strdup(cfg->allocator, value);
            if (copy == nullptr)
            {
                return kith_error_return(KITH_ENOMEM);
            }
            kith_free(cfg->allocator, cfg->entries[i].value);
            cfg->entries[i].value = copy;
            return 0;
        }
    }

    if (ensure_cap(cfg) != 0)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    char *key_copy = kith_strdup(cfg->allocator, key);
    char *val_copy = kith_strdup(cfg->allocator, value);
    if (key_copy == nullptr || val_copy == nullptr)
    {
        kith_free(cfg->allocator, key_copy);
        kith_free(cfg->allocator, val_copy);
        return kith_error_return(KITH_ENOMEM);
    }
    cfg->entries[cfg->count].key = key_copy;
    cfg->entries[cfg->count].value = val_copy;
    cfg->count += 1;
    return 0;
}

static char *trim(char *s)
{
    while (*s != '\0' && isspace((unsigned char)*s))
    {
        s += 1;
    }
    if (*s == '\0')
    {
        return s;
    }
    char *end = s + strlen(s) - 1;
    while (end > s && isspace((unsigned char)*end))
    {
        *end = '\0';
        end -= 1;
    }
    return s;
}

static bool parse_bool_value(const char *value, bool *out)
{
    if (strcmp(value, "1") == 0 || strcmp(value, "true") == 0 || strcmp(value, "TRUE") == 0 ||
        strcmp(value, "True") == 0 || strcmp(value, "yes") == 0 || strcmp(value, "YES") == 0 ||
        strcmp(value, "Yes") == 0 || strcmp(value, "on") == 0 || strcmp(value, "ON") == 0 ||
        strcmp(value, "On") == 0)
    {
        *out = true;
        return true;
    }
    if (strcmp(value, "0") == 0 || strcmp(value, "false") == 0 || strcmp(value, "FALSE") == 0 ||
        strcmp(value, "False") == 0 || strcmp(value, "no") == 0 || strcmp(value, "NO") == 0 ||
        strcmp(value, "No") == 0 || strcmp(value, "off") == 0 || strcmp(value, "OFF") == 0 ||
        strcmp(value, "Off") == 0)
    {
        *out = false;
        return true;
    }
    return false;
}

/* Tokenize a NUL-terminated buffer into key=value entries. The buffer is
 * modified in place: each newline and the first '=' of a usable line are
 * overwritten to split it. Comment lines (#) and blank lines are skipped. */
static int parse_lines(struct kith_config *cfg, char *buf)
{
    int rc = 0;
    char *line = buf;
    while (line != nullptr && *line != '\0' && rc == 0)
    {
        char *nl = strchr(line, '\n');
        if (nl != nullptr)
        {
            *nl = '\0';
        }
        char *cur = trim(line);
        if (*cur == '\0' || *cur == '#')
        {
            line = (nl != nullptr) ? nl + 1 : nullptr;
            continue;
        }
        char *eq = strchr(cur, '=');
        if (eq != nullptr)
        {
            *eq = '\0';
            char *key = trim(cur);
            char *val = trim(eq + 1);
            if (*val == '"' && strlen(val) >= 2 && val[strlen(val) - 1] == '"')
            {
                val[strlen(val) - 1] = '\0';
                val += 1;
            }
            rc = entry_set(cfg, key, val);
        }
        line = (nl != nullptr) ? nl + 1 : nullptr;
    }
    return rc;
}

/* Load a key=value file. A missing file (ENOENT) is not an error: the source
 * is built from the environment alone. Any other open failure is reported so
 * an unreadable but present file is not silently skipped. A directory or
 * special file is rejected up front: fopen() on a directory succeeds on some
 * platforms and the subsequent fseek/ftell yield a bogus size, so stat first
 * to report KITH_EIO deterministically for any non-regular file. */
static int load_file(struct kith_config *cfg, const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0)
    {
        return errno == ENOENT ? 0 : kith_error_return(KITH_EIO);
    }
    if (!S_ISREG(st.st_mode))
    {
        return kith_error_return(KITH_EIO);
    }

    FILE *f = fopen(path, "rb");
    if (f == nullptr)
    {
        return errno == ENOENT ? 0 : kith_error_return(KITH_EIO);
    }

    if (fseek(f, 0, SEEK_END) != 0)
    {
        (void)fclose(f);
        return kith_error_return(KITH_EIO);
    }
    long size = ftell(f);
    if (size < 0)
    {
        (void)fclose(f);
        return kith_error_return(KITH_EIO);
    }
    if (fseek(f, 0, SEEK_SET) != 0)
    {
        (void)fclose(f);
        return kith_error_return(KITH_EIO);
    }

    char *buf = kith_alloc(cfg->allocator, (size_t)size + 1);
    if (buf == nullptr)
    {
        (void)fclose(f);
        return kith_error_return(KITH_ENOMEM);
    }
    size_t nread = fread(buf, 1, (size_t)size, f);
    int read_err = ferror(f);
    if (fclose(f) != 0)
    {
        kith_free(cfg->allocator, buf);
        return kith_error_return(KITH_EIO);
    }
    if (read_err != 0)
    {
        kith_free(cfg->allocator, buf);
        return kith_error_return(KITH_EIO);
    }
    buf[nread] = '\0';

    int rc = parse_lines(cfg, buf);
    kith_free(cfg->allocator, buf);
    return rc;
}

static int load_env(struct kith_config *cfg, const char *prefix)
{
    if (prefix == nullptr || environ == nullptr)
    {
        return 0;
    }
    size_t plen = strlen(prefix);
    for (char **e = environ; *e != nullptr; ++e)
    {
        char *eq = strchr(*e, '=');
        if (eq == nullptr)
        {
            continue;
        }
        size_t name_len = (size_t)(eq - *e);
        if (name_len < plen || strncmp(*e, prefix, plen) != 0)
        {
            continue;
        }
        size_t key_len = name_len - plen;
        char *keybuf = kith_alloc(cfg->allocator, key_len + 1);
        if (keybuf == nullptr)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        memcpy(keybuf, *e + plen, key_len);
        keybuf[key_len] = '\0';
        int rc = entry_set(cfg, keybuf, eq + 1);
        kith_free(cfg->allocator, keybuf);
        if (rc != 0)
        {
            return rc;
        }
    }
    return 0;
}

static const struct entry *find_entry(const struct kith_config *cfg, const char *key)
{
    if (cfg->count == 0)
    {
        return nullptr;
    }
    struct entry needle;
    needle.key = (char *)key;
    needle.value = nullptr;
    return bsearch(&needle, cfg->entries, cfg->count, sizeof(*cfg->entries), entry_cmp);
}

int kith_config_create(const kith_config_params_t *params,
                       const kith_allocator_t *alloc,
                       kith_config_t **out_cfg)
{
    if (out_cfg == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_cfg = nullptr;

    if (params != nullptr)
    {
        if (params->abi_version != KITH_ABI_VERSION)
        {
            return kith_error_return(KITH_EABIVER);
        }
        if (params->size < sizeof(*params))
        {
            return kith_error_return(KITH_ESIZE);
        }
    }
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }

    const kith_allocator_t *allocator = alloc != nullptr ? alloc : kith_allocator_default();
    struct kith_config *cfg = kith_alloc(allocator, sizeof(*cfg));
    if (cfg == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    cfg->allocator = allocator;
    cfg->entries = nullptr;
    cfg->count = 0;
    cfg->cap = 0;

    int rc = 0;
    if (params != nullptr && params->file_path != nullptr)
    {
        rc = load_file(cfg, params->file_path);
    }
    if (rc == 0 && params != nullptr)
    {
        rc = load_env(cfg, params->env_prefix);
    }

    if (rc != 0)
    {
        kith_config_destroy(cfg);
        return rc;
    }

    if (cfg->count > 1)
    {
        qsort(cfg->entries, cfg->count, sizeof(*cfg->entries), entry_cmp);
    }
    *out_cfg = cfg;
    return 0;
}

void kith_config_destroy(kith_config_t *cfg)
{
    if (cfg == nullptr)
    {
        return;
    }
    for (size_t i = 0; i < cfg->count; ++i)
    {
        kith_free(cfg->allocator, cfg->entries[i].key);
        kith_free(cfg->allocator, cfg->entries[i].value);
    }
    kith_free(cfg->allocator, cfg->entries);
    kith_free(cfg->allocator, cfg);
}

bool kith_config_has(const kith_config_t *cfg, const char *key)
{
    if (cfg == nullptr || key == nullptr)
    {
        return false;
    }
    return find_entry(cfg, key) != nullptr;
}

int kith_config_string(const kith_config_t *cfg, const char *key, const char **out_val)
{
    if (cfg == nullptr || key == nullptr || out_val == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    const struct entry *e = find_entry(cfg, key);
    if (e == nullptr)
    {
        return kith_error_return(KITH_ENOENT);
    }
    *out_val = e->value;
    return 0;
}

int kith_config_bool(const kith_config_t *cfg, const char *key, bool *out_val)
{
    if (cfg == nullptr || key == nullptr || out_val == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    const struct entry *e = find_entry(cfg, key);
    if (e == nullptr)
    {
        return kith_error_return(KITH_ENOENT);
    }
    if (!parse_bool_value(e->value, out_val))
    {
        return kith_error_return(KITH_EINVAL);
    }
    return 0;
}

static int parse_ulong(const char *s, unsigned long *out)
{
    errno = 0;
    char *end = nullptr;
    unsigned long v = strtoul(s, &end, 10);
    if (end == s || *end != '\0')
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (errno == ERANGE)
    {
        return kith_error_return(KITH_ERANGE);
    }
    if (errno != 0)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out = v;
    return 0;
}

int kith_config_u16(
    const kith_config_t *cfg, const char *key, uint16_t min, uint16_t max, uint16_t *out_val)
{
    if (cfg == nullptr || key == nullptr || out_val == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (min > max)
    {
        return kith_error_return(KITH_ERANGE);
    }
    const struct entry *e = find_entry(cfg, key);
    if (e == nullptr)
    {
        return kith_error_return(KITH_ENOENT);
    }
    unsigned long v = 0;
    int rc = parse_ulong(e->value, &v);
    if (rc != 0)
    {
        return rc;
    }
    if (v < min || v > max)
    {
        return kith_error_return(KITH_ERANGE);
    }
    *out_val = (uint16_t)v;
    return 0;
}

int kith_config_u32(
    const kith_config_t *cfg, const char *key, uint32_t min, uint32_t max, uint32_t *out_val)
{
    if (cfg == nullptr || key == nullptr || out_val == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (min > max)
    {
        return kith_error_return(KITH_ERANGE);
    }
    const struct entry *e = find_entry(cfg, key);
    if (e == nullptr)
    {
        return kith_error_return(KITH_ENOENT);
    }
    unsigned long v = 0;
    int rc = parse_ulong(e->value, &v);
    if (rc != 0)
    {
        return rc;
    }
    if (v < min || v > max)
    {
        return kith_error_return(KITH_ERANGE);
    }
    *out_val = (uint32_t)v;
    return 0;
}

int kith_config_u64(
    const kith_config_t *cfg, const char *key, uint64_t min, uint64_t max, uint64_t *out_val)
{
    if (cfg == nullptr || key == nullptr || out_val == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (min > max)
    {
        return kith_error_return(KITH_ERANGE);
    }
    const struct entry *e = find_entry(cfg, key);
    if (e == nullptr)
    {
        return kith_error_return(KITH_ENOENT);
    }
    errno = 0;
    char *end = nullptr;
    unsigned long long v = strtoull(e->value, &end, 10);
    if (end == e->value || *end != '\0')
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (errno == ERANGE)
    {
        return kith_error_return(KITH_ERANGE);
    }
    if (errno != 0)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (v < min || v > max)
    {
        return kith_error_return(KITH_ERANGE);
    }
    *out_val = (uint64_t)v;
    return 0;
}

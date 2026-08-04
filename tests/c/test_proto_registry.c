#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "proto registry: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int test_register_and_lookup(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);
    CHECK(kith_proto_register_type_id(p, "spawn", 1001u) == 0);

    uint16_t id = 0;
    CHECK(kith_proto_lookup_type(p, "move", &id) == 0);
    CHECK(id == 1000u);
    CHECK(kith_proto_lookup_type(p, "spawn", &id) == 0);
    CHECK(id == 1001u);

    CHECK(strcmp(kith_proto_type_name(p, 1000u), "move") == 0);
    CHECK(strcmp(kith_proto_type_name(p, 1001u), "spawn") == 0);

    kith_proto_destroy(p);
    return failures;
}

static int test_register_idempotent(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);
    uint16_t id = 99u;
    CHECK(kith_proto_lookup_type(p, "move", &id) == 0);
    CHECK(id == 1000u);
    kith_proto_destroy(p);
    return failures;
}

static int test_register_name_conflict(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);
    /* Same name, different id: rejected. */
    CHECK(kith_proto_register_type_id(p, "move", 1001u) == kith_error_return(KITH_EINVAL));
    /* The original registration is unchanged. */
    uint16_t id = 99u;
    CHECK(kith_proto_lookup_type(p, "move", &id) == 0);
    CHECK(id == 1000u);
    kith_proto_destroy(p);
    return failures;
}

static int test_register_id_conflict(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);
    /* Different name, same id: rejected. */
    CHECK(kith_proto_register_type_id(p, "stop", 1000u) == kith_error_return(KITH_EEXIST));
    CHECK(kith_proto_type_name(p, 1000u) != nullptr);
    CHECK(strcmp(kith_proto_type_name(p, 1000u), "move") == 0);
    kith_proto_destroy(p);
    return failures;
}

static int test_lookup_missing(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    uint16_t id = 99u;
    CHECK(kith_proto_lookup_type(p, "nope", &id) == kith_error_return(KITH_ENOENT));
    CHECK(kith_proto_type_name(p, 5000u) == nullptr);
    kith_proto_destroy(p);
    return failures;
}

static int test_arg_validation(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(nullptr, "move", 1000u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_register_type_id(p, nullptr, 1000u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_register_type_id(p, "", 1000u) == kith_error_return(KITH_EINVAL));

    uint16_t id = 0;
    CHECK(kith_proto_lookup_type(nullptr, "move", &id) == kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_lookup_type(p, nullptr, &id) == kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_lookup_type(p, "move", nullptr) == kith_error_return(KITH_EINVAL));

    CHECK(kith_proto_type_name(nullptr, 1000u) == nullptr);
    kith_proto_destroy(p);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_register_and_lookup();
    rc |= test_register_idempotent();
    rc |= test_register_name_conflict();
    rc |= test_register_id_conflict();
    rc |= test_lookup_missing();
    rc |= test_arg_validation();
    if (rc != 0)
    {
        (void)fprintf(stderr, "proto registry tests FAILED\n");
    }
    return rc;
}

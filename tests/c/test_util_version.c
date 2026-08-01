#include <string.h>

#include "kith/util/util.h"

/* The runtime query, the compile-time macro, and the build's project
 * version (passed in as KITH_PROJECT_VERSION from the test build) must
 * all agree; the ABI generation is 1. */
int main(void)
{
    if (kith_version_abi() != KITH_ABI_VERSION)
    {
        return 1;
    }
    if (strcmp(kith_version_string(), KITH_VERSION_STRING) != 0)
    {
        return 1;
    }
    if (strcmp(kith_version_string(), KITH_PROJECT_VERSION) != 0)
    {
        return 1;
    }
    if (kith_version_abi() != 1u)
    {
        return 1;
    }
    return 0;
}

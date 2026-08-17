/* Out-of-tree downstream consumer: drives the installed framework through
 * the public C headers via find_package(kith), linking the composition root
 * (kith_server) and the foundation utilities (kith_util). A non-zero exit
 * signals a broken install, an unresolvable symbol, or a version mismatch
 * between the installed library and the header it was built against. The
 * pkg-config install channel is exercised separately by pkgconfig_probe.c. */

#include <stdio.h>

#include "kith/server/server.h"
#include "kith/util/util.h"

int main(void)
{
    /* kith_version_string (libkith_util) — resolves at link time, reads the
     * static storage the framework was compiled with, and returns non-NULL. */
    const char *const version = kith_version_string();
    if (version == nullptr)
    {
        return 1;
    }

    /* kith_server_status (libkith_server) — NULL is handled and returns the
     * CREATED state, so the call is safe without a real server handle. It
     * forces the linker to record libkith_server.so as a DT_NEEDED entry so
     * the composition root is genuinely linked, not just present on the link
     * line. */
    (void)kith_server_status(nullptr);

    printf("kith %s\n", version);
    return 0;
}

/* Out-of-tree downstream consumer for the pkg-config install channel: the
 * verify.sh build stage compiles this file with exactly `pkg-config
 * --cflags kith` and links it with exactly `pkg-config --libs kith`, so the
 * shipped .pc link line is exercised end-to-end, not just queried. The call
 * targets kith_version_string (libkith_util), the last library on the .pc
 * link line — the exact symbol class that fails to resolve when the line
 * names only the composition root. Whether the line names every installed
 * library is asserted separately by the same leg. The .pc Cflags carries
 * no -std, so the probe stays within the compiler's ambient standard: NULL,
 * not a C23-only keyword. A non-zero exit signals a broken install or an
 * unresolvable symbol. */

#include <stdio.h>

#include "kith/util/util.h"

int main(void)
{
    const char *const version = kith_version_string();
    if (version == NULL)
    {
        return 1;
    }

    printf("kith %s\n", version);
    return 0;
}

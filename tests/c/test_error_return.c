#include <limits.h>

#include "kith/types.h"

/* The error-return helper negates an unsigned kith_error enumerator into the
 * negative int a public function reports on failure, and success (KITH_OK) is
 * unchanged. The enum is fixed to unsigned int; the helper applies the sign
 * cast so callers stay sign-conversion-clean. */
int main(void)
{
    if (kith_error_return(KITH_OK) != 0)
    {
        return 1;
    }
    if (kith_error_return(KITH_EINVAL) != -(int)KITH_EINVAL)
    {
        return 1;
    }
    if (kith_error_return(KITH_EUSER) != -(int)KITH_EUSER)
    {
        return 1;
    }
    if (kith_error_return((kith_error_t)0) != 0)
    {
        return 1;
    }
    return 0;
}

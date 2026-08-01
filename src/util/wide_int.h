#pragma once

#include <stdint.h>

/**
 * Signed 128-bit integer for wide intermediate arithmetic: sums,
 * differences, and squares of int64 magnitudes stay exact at this width.
 */
typedef signed _BitInt(128) kith_i128_t;

/**
 * Unsigned 128-bit integer for wide intermediate arithmetic, pairing with
 * @p kith_i128_t where magnitudes are known non-negative.
 */
typedef unsigned _BitInt(128) kith_u128_t;

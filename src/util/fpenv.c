/* Compile-time assertion of the floating-point environment contract
 * documented in cmake/fpenv.c: float/double must evaluate without excess
 * precision (FLT_EVAL_METHOD == 0). x86-64 guarantees this via its SSE2
 * baseline; the 32-bit x86 build forces it via -mfpmath=sse. Compiled into
 * libkith_util so every framework configuration verifies the assumption
 * deterministic math rests on. */

static_assert(__FLT_EVAL_METHOD__ == 0, "float/double must evaluate without excess precision");

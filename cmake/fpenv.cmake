# Floating-point environment contract for deterministic simulation.
#
# The replay contract (docs/architecture/adr/0014-deterministic-simulation-
# contract.md) requires bit-identical floating-point results for identical
# inputs. Two compiler behaviors break that, and both are pinned here:
#
#   - FMA contraction: a*b+c may fuse into an fma() call, changing the
#     rounding sequence. aarch64 has FMA in the baseline ISA, so contraction
#     can appear without any -march request; x86-64 gains it under
#     -march=+fma. -ffp-contract=off forbids the fusion everywhere.
#   - Excess precision: x87 evaluates float/double at 80-bit internal
#     precision (FLT_EVAL_METHOD != 0), so intermediates carry bits the
#     source types never see. x86-64 has no such exposure — SSE2 is the
#     architecture baseline and float/double never touch x87. 32-bit x86
#     defaults to x87, so it is forced onto SSE2 math instead.
#
# src/util/fpenv.c asserts the FLT_EVAL_METHOD == 0 assumption at compile
# time, so a toolchain that violates it fails the build instead of
# silently diverging at replay time.
add_compile_options(-ffp-contract=off)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(i[3-6]86|x86)$")
    # 32-bit x86 defaults to x87 math (FLT_EVAL_METHOD == 1); SSE2 math
    # matches the x86-64 baseline bit-for-bit.
    add_compile_options(-msse2 -mfpmath=sse)
    if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
        # Store float results to memory on any x87 path that survives the
        # -mfpmath switch; a no-op for SSE2 math, kept as a guard against
        # long-double leakage into deterministic expressions.
        add_compile_options(-ffloat-store)
    endif()
endif()

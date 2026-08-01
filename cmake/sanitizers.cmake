# Sanitizer, coverage, and fuzzing configuration.
#
# Consumes the KITH_ENABLE_* cache variables established by CMakePresets.json:
# KITH_ENABLE_ASAN, KITH_ENABLE_UBSAN, KITH_ENABLE_TSAN,
# KITH_ENABLE_COVERAGE, KITH_ENABLE_FUZZ. Each is OFF by default so plain
# debug/release presets are unaffected. flags are applied globally via
# add_compile_options() / add_link_options() so every target inherits them.
#
# _FORTIFY_SOURCE gating: -D_FORTIFY_SOURCE=3 conflicts with Address
# and ThreadSanitizer. This module sets KITH_ENABLE_FORTIFY=OFF as a normal
# variable before hardening.cmake is included (framework.cmake includes
# sanitizers.cmake first, which is the preset-logic ordering), so a -D cannot
# override it for a sanitizer build. The debug and coverage presets gate
# their own KITH_ENABLE_FORTIFY cacheVariables instead. Other hardening flags
# stay on under sanitizers.

# Guard: ASan/LSan and TSan are mutually exclusive (a hard toolchain
# constraint, not a policy choice).
if(KITH_ENABLE_TSAN AND KITH_ENABLE_ASAN)
    message(FATAL_ERROR
        "ThreadSanitizer and AddressSanitizer are mutually exclusive; "
        "the 'fuzz' preset pairs libFuzzer with ASan, not TSan.")
endif()

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    # AddressSanitizer + libFuzzer's own ASan use; also the fuzz preset's ASan.
    if(KITH_ENABLE_ASAN)
        set(KITH_ENABLE_FORTIFY OFF)
        add_compile_options(-fsanitize=address -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=address)
    endif()

    # UndefinedBehaviorSanitizer: C-level undefined behavior (signed
    # overflow, shift, misaligned/null pointer use, bounds). Composes with
    # both ASan and TSan. -fno-sanitize-recover makes each report abort the
    # process instead of printing and continuing, so a ctest run fails on
    # the first finding rather than burying it in subsequent output; the flag is
    # compile-time because it selects which checks get the trapping form.
    if(KITH_ENABLE_UBSAN)
        add_compile_options(-fsanitize=undefined -fno-sanitize-recover=undefined
            -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=undefined)
    endif()

    # ThreadSanitizer: data-race detection. Available through the Debug
    # tsan preset; the weekly sanitizer lane's tsan job drives the preset
    # over the C test suite, where every observable racing access is
    # instrumented on both sides, and its pytest-tsan job drives it over
    # the Python integration suite under the preloaded shared runtime,
    # with uninstrumented third-party libpq frames scoped out via
    # tests/tsan_python.supp.
    if(KITH_ENABLE_TSAN)
        set(KITH_ENABLE_FORTIFY OFF)
        add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=thread)
    endif()

    # Coverage: Clang source-based coverage consumed by llvm-cov.
    if(KITH_ENABLE_COVERAGE)
        add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
        add_link_options(-fprofile-instr-generate)
    endif()

    # libFuzzer: instrumentation on every TU (fuzzer-no-link), the fuzzer
    # runtime linked only into fuzz target executables (-fsanitize=fuzzer).
    # The fuzz preset sets KITH_ENABLE_FUZZ alongside KITH_ENABLE_ASAN, so the
    # ASan flags above are already in effect here.
    if(KITH_ENABLE_FUZZ)
        add_compile_options(-fsanitize=fuzzer-no-link -fno-omit-frame-pointer -g)
        # -fsanitize=fuzzer links the runtime; applied at link time for fuzz
        # targets only, not globally, to avoid pulling the fuzzer main() into
        # every library/executable. Each target in tests/fuzz/ carries
        # target_link_options(-fsanitize=fuzzer); every other target links
        # without it.
    endif()
endif()

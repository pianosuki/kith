# libFuzzer targets for the protocol decoder and the control HTTP parser.
# The top level enters this directory only when KITH_ENABLE_FUZZ is set (the
# fuzz preset), so default debug/release builds never configure it.

if(NOT CMAKE_C_COMPILER_ID STREQUAL "Clang")
    message(FATAL_ERROR
        "The fuzz targets require Clang: libFuzzer ships with Clang alone "
        "(GCC provides sanitizer coverage callbacks but no -fsanitize=fuzzer "
        "runtime). Configure through the fuzz preset, which pins CC=clang.")
endif()

# The harness links the libFuzzer runtime (which carries main()); the corpus
# generator stays a plain executable so fixture setup can run it directly.
add_executable(fuzz_proto_decode fuzz_proto_decode.c)
target_link_libraries(fuzz_proto_decode PRIVATE kith_proto)
target_link_options(fuzz_proto_decode PRIVATE -fsanitize=fuzzer)

add_executable(fuzz_proto_seeds fuzz_proto_seeds.c)
target_link_libraries(fuzz_proto_seeds PRIVATE kith_proto)
target_compile_definitions(fuzz_proto_seeds PRIVATE _POSIX_C_SOURCE=200809L)

# The control HTTP parser is module-local and conn-anchored, so its harness
# compiles the parser source directly (the test_control_http pattern) and
# drives a locally-owned connection record with exact-size buffers.
add_executable(fuzz_control_http fuzz_control_http.c
    ${PROJECT_SOURCE_DIR}/src/control/http_parser.c)
target_link_libraries(fuzz_control_http PRIVATE kith_control)
target_include_directories(fuzz_control_http PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_options(fuzz_control_http PRIVATE -fsanitize=fuzzer)

add_executable(fuzz_control_http_seeds fuzz_control_http_seeds.c)
target_link_libraries(fuzz_control_http_seeds PRIVATE kith_control)
target_include_directories(fuzz_control_http_seeds PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_compile_definitions(fuzz_control_http_seeds PRIVATE _POSIX_C_SOURCE=200809L)

# Smoke gate under ctest: regenerate the build-tree corpus, then replay it
# (and the committed regression corpus, when one is present) without
# mutating past it. -runs=0 keeps ctest deterministic; a fixed -runs budget runs
# on past the corpus with run-varying RNG state. -max_len mirrors
# FUZZ_MAX_INPUT in both harnesses.
set(fuzz_max_input_bytes 65536)
set(fuzz_corpus_dir "${CMAKE_BINARY_DIR}/corpus/proto")
set(fuzz_regression_dir "${CMAKE_CURRENT_SOURCE_DIR}/corpus/proto")

add_test(NAME fuzz_proto_seeds COMMAND fuzz_proto_seeds "${fuzz_corpus_dir}")
set_tests_properties(fuzz_proto_seeds PROPERTIES FIXTURES_SETUP fuzz_corpus TIMEOUT 60)

set(fuzz_smoke_command
    fuzz_proto_decode -runs=0 -max_len=${fuzz_max_input_bytes} "${fuzz_corpus_dir}")
if(EXISTS "${fuzz_regression_dir}")
    list(APPEND fuzz_smoke_command "${fuzz_regression_dir}")
endif()
add_test(NAME fuzz_proto_decode COMMAND ${fuzz_smoke_command})
set_tests_properties(fuzz_proto_decode PROPERTIES FIXTURES_REQUIRED fuzz_corpus TIMEOUT 60)

set(fuzz_control_corpus_dir "${CMAKE_BINARY_DIR}/corpus/control-http")
set(fuzz_control_regression_dir "${CMAKE_CURRENT_SOURCE_DIR}/corpus/control-http")

add_test(NAME fuzz_control_http_seeds
         COMMAND fuzz_control_http_seeds "${fuzz_control_corpus_dir}")
set_tests_properties(fuzz_control_http_seeds
    PROPERTIES FIXTURES_SETUP fuzz_control_corpus TIMEOUT 60)

set(fuzz_control_smoke_command
    fuzz_control_http -runs=0 -max_len=${fuzz_max_input_bytes} "${fuzz_control_corpus_dir}")
if(EXISTS "${fuzz_control_regression_dir}")
    list(APPEND fuzz_control_smoke_command "${fuzz_control_regression_dir}")
endif()
add_test(NAME fuzz_control_http COMMAND ${fuzz_control_smoke_command})
set_tests_properties(fuzz_control_http PROPERTIES FIXTURES_REQUIRED fuzz_control_corpus TIMEOUT 60)

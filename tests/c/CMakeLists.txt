# C unit tests. Each test is a standalone executable linked against the
# library under test and registered with ctest via add_test.

add_executable(test_util_version test_util_version.c)
target_link_libraries(test_util_version PRIVATE kith_util)
# The project version rides in so the test can hold the header macros and
# the CMake project(kith VERSION ...) definition to the same value.
target_compile_definitions(
    test_util_version PRIVATE KITH_PROJECT_VERSION="${PROJECT_VERSION}"
)
add_test(NAME test_util_version COMMAND test_util_version)

add_executable(test_util_rng test_util_rng.c)
target_link_libraries(test_util_rng PRIVATE kith_util)
add_test(NAME test_util_rng COMMAND test_util_rng)

add_executable(test_util_clock test_util_clock.c)
target_link_libraries(test_util_clock PRIVATE kith_util)
add_test(NAME test_util_clock COMMAND test_util_clock)

add_executable(test_error_return test_error_return.c)
target_link_libraries(test_error_return PRIVATE kith_util)
add_test(NAME test_error_return COMMAND test_error_return)

add_executable(test_config_source test_config_source.c)
target_link_libraries(test_config_source PRIVATE kith_config)
# _XOPEN_SOURCE=700 implies POSIX.1-2008 and additionally exposes putenv,
# which glibc guards behind the X/Open feature set.
target_compile_definitions(
    test_config_source PRIVATE _POSIX_C_SOURCE=200809L _XOPEN_SOURCE=700
)
add_test(NAME test_config_source COMMAND test_config_source)

add_executable(test_logger_create test_logger_create.c)
target_link_libraries(test_logger_create PRIVATE kith_logger)
add_test(NAME test_logger_create COMMAND test_logger_create)

add_executable(test_logger_sink test_logger_sink.c)
target_link_libraries(test_logger_sink PRIVATE kith_logger)
add_test(NAME test_logger_sink COMMAND test_logger_sink)

add_executable(test_metrics_registry test_metrics_registry.c)
target_link_libraries(test_metrics_registry PRIVATE kith_metrics)
target_compile_definitions(test_metrics_registry PRIVATE _POSIX_C_SOURCE=200809L)
add_test(NAME test_metrics_registry COMMAND test_metrics_registry)

add_executable(test_metrics_export test_metrics_export.c)
target_link_libraries(test_metrics_export PRIVATE kith_metrics)
target_compile_definitions(test_metrics_export PRIVATE _POSIX_C_SOURCE=200809L)
add_test(NAME test_metrics_export COMMAND test_metrics_export)

add_executable(test_proto_codec test_proto_codec.c)
target_link_libraries(test_proto_codec PRIVATE kith_proto)
target_compile_definitions(test_proto_codec PRIVATE _POSIX_C_SOURCE=200809L)
add_test(NAME test_proto_codec COMMAND test_proto_codec)

add_executable(test_proto_registry test_proto_registry.c)
target_link_libraries(test_proto_registry PRIVATE kith_proto)
target_compile_definitions(test_proto_registry PRIVATE _POSIX_C_SOURCE=200809L)
add_test(NAME test_proto_registry COMMAND test_proto_registry)

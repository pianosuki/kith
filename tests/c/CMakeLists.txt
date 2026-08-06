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

add_executable(test_net_transport test_net_transport.c)
target_link_libraries(test_net_transport PRIVATE kith_net kith_proto)
target_compile_definitions(test_net_transport PRIVATE _GNU_SOURCE)
add_test(NAME test_net_transport COMMAND test_net_transport)

add_executable(test_net_frame test_net_frame.c)
target_link_libraries(test_net_frame PRIVATE kith_net kith_proto)
target_compile_definitions(test_net_frame PRIVATE _GNU_SOURCE)
add_test(NAME test_net_frame COMMAND test_net_frame)

add_executable(test_net_drain_cap test_net_drain_cap.c)
target_link_libraries(test_net_drain_cap PRIVATE kith_net kith_proto)
target_compile_definitions(test_net_drain_cap PRIVATE _GNU_SOURCE)
add_test(NAME test_net_drain_cap COMMAND test_net_drain_cap)

# The ring buffer is connection-internal with hidden symbols, so its unit
# test compiles the source directly instead of linking the shared library.
add_executable(test_net_ringbuf test_net_ringbuf.c ${CMAKE_SOURCE_DIR}/src/net/ringbuf.c)
target_include_directories(test_net_ringbuf PRIVATE ${CMAKE_SOURCE_DIR}/src/net ${CMAKE_SOURCE_DIR}/include)
target_compile_definitions(test_net_ringbuf PRIVATE _GNU_SOURCE)
add_test(NAME test_net_ringbuf COMMAND test_net_ringbuf)

add_executable(test_reactor_basic test_reactor_basic.c)
target_link_libraries(test_reactor_basic PRIVATE kith_reactor kith_util Threads::Threads)
target_compile_definitions(test_reactor_basic PRIVATE _GNU_SOURCE)
add_test(NAME test_reactor_basic COMMAND test_reactor_basic)

add_executable(test_reactor_ring_clamp test_reactor_ring_clamp.c)
target_link_libraries(test_reactor_ring_clamp PRIVATE kith_reactor kith_util)
target_compile_definitions(test_reactor_ring_clamp PRIVATE _GNU_SOURCE)
add_test(NAME test_reactor_ring_clamp COMMAND test_reactor_ring_clamp)
set_tests_properties(test_reactor_ring_clamp PROPERTIES SKIP_RETURN_CODE 77)

add_executable(test_reactor_drain_cap test_reactor_drain_cap.c)
target_link_libraries(test_reactor_drain_cap PRIVATE kith_reactor kith_util)
target_compile_definitions(test_reactor_drain_cap PRIVATE _GNU_SOURCE)
add_test(NAME test_reactor_drain_cap COMMAND test_reactor_drain_cap)

# Reads the fd map through the internal handle to pin tombstone accounting.
add_executable(test_reactor_fd_churn test_reactor_fd_churn.c)
target_link_libraries(test_reactor_fd_churn PRIVATE kith_reactor kith_util)
target_compile_definitions(test_reactor_fd_churn PRIVATE _GNU_SOURCE)
target_include_directories(test_reactor_fd_churn PRIVATE ${CMAKE_SOURCE_DIR}/src)
add_test(NAME test_reactor_fd_churn COMMAND test_reactor_fd_churn)

add_executable(test_worker_basic test_worker_basic.c)
target_link_libraries(test_worker_basic PRIVATE kith_worker kith_util)
target_compile_definitions(test_worker_basic PRIVATE _GNU_SOURCE)
add_test(NAME test_worker_basic COMMAND test_worker_basic)

add_executable(test_worker_bridge test_worker_bridge.c)
target_link_libraries(test_worker_bridge PRIVATE kith_worker kith_reactor kith_util)
target_compile_definitions(test_worker_bridge PRIVATE _GNU_SOURCE)
add_test(NAME test_worker_bridge COMMAND test_worker_bridge)

add_executable(test_worker_signal_mask test_worker_signal_mask.c)
target_link_libraries(test_worker_signal_mask PRIVATE kith_worker kith_util)
target_compile_definitions(test_worker_signal_mask PRIVATE _GNU_SOURCE)
add_test(NAME test_worker_signal_mask COMMAND test_worker_signal_mask)

add_executable(test_state_create test_state_create.c)
target_link_libraries(test_state_create PRIVATE kith_state kith_reactor)
target_compile_definitions(test_state_create PRIVATE _GNU_SOURCE)
add_test(NAME test_state_create COMMAND test_state_create)

add_executable(test_state_overflow test_state_overflow.c)
target_link_libraries(test_state_overflow PRIVATE kith_state kith_reactor)
target_compile_definitions(test_state_overflow PRIVATE _GNU_SOURCE)
add_test(NAME test_state_overflow COMMAND test_state_overflow)

add_executable(test_state_ops test_state_ops.c)
target_link_libraries(test_state_ops PRIVATE kith_state kith_reactor Threads::Threads)
target_compile_definitions(test_state_ops PRIVATE _GNU_SOURCE)
add_test(NAME test_state_ops COMMAND test_state_ops)

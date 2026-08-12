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

add_executable(test_db_create test_db_create.c)
target_link_libraries(test_db_create PRIVATE kith_db kith_reactor)
target_compile_definitions(test_db_create PRIVATE _GNU_SOURCE)
add_test(NAME test_db_create COMMAND test_db_create)

add_executable(test_db_ops test_db_ops.c)
target_link_libraries(test_db_ops PRIVATE kith_db kith_reactor Threads::Threads)
target_compile_definitions(test_db_ops PRIVATE _GNU_SOURCE)
add_test(NAME test_db_ops COMMAND test_db_ops)

add_executable(test_db_session test_db_session.c)
target_link_libraries(test_db_session PRIVATE kith_db kith_reactor Threads::Threads)
target_compile_definitions(test_db_session PRIVATE _GNU_SOURCE)
add_test(NAME test_db_session COMMAND test_db_session)

add_executable(test_sim_registry test_sim_registry.c)
target_link_libraries(test_sim_registry PRIVATE kith_sim)
target_include_directories(test_sim_registry PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_sim_registry COMMAND test_sim_registry)

add_executable(test_sim_tile2d test_sim_tile2d.c)
target_link_libraries(test_sim_tile2d PRIVATE kith_sim)
target_compile_definitions(test_sim_tile2d PRIVATE _GNU_SOURCE)
add_test(NAME test_sim_tile2d COMMAND test_sim_tile2d)

add_executable(test_sim_free2d test_sim_free2d.c)
target_link_libraries(test_sim_free2d PRIVATE kith_sim)
add_test(NAME test_sim_free2d COMMAND test_sim_free2d)

add_executable(test_sim_artifact test_sim_artifact.c)
target_link_libraries(test_sim_artifact PRIVATE kith_sim)
add_test(NAME test_sim_artifact COMMAND test_sim_artifact)

add_executable(test_sim_pending_concurrent test_sim_pending_concurrent.c)
target_link_libraries(test_sim_pending_concurrent PRIVATE kith_sim Threads::Threads)
add_test(NAME test_sim_pending_concurrent COMMAND test_sim_pending_concurrent)

add_executable(test_fabric_create test_fabric_create.c)
target_link_libraries(test_fabric_create PRIVATE kith_fabric kith_sim)
add_test(NAME test_fabric_create COMMAND test_fabric_create)

add_executable(test_fabric_cell_store test_fabric_cell_store.c)
target_link_libraries(test_fabric_cell_store PRIVATE kith_fabric kith_sim)
add_test(NAME test_fabric_cell_store COMMAND test_fabric_cell_store)

add_executable(test_fabric_stream test_fabric_stream.c)
target_link_libraries(test_fabric_stream PRIVATE kith_fabric kith_sim)
add_test(NAME test_fabric_stream COMMAND test_fabric_stream)

add_executable(test_fabric_snapshot test_fabric_snapshot.c)
target_link_libraries(test_fabric_snapshot PRIVATE kith_fabric kith_sim)
add_test(NAME test_fabric_snapshot COMMAND test_fabric_snapshot)

add_executable(test_fabric_subscription test_fabric_subscription.c)
target_link_libraries(test_fabric_subscription PRIVATE kith_fabric kith_sim)
add_test(NAME test_fabric_subscription COMMAND test_fabric_subscription)

add_executable(test_fabric_concurrent_subscription test_fabric_concurrent_subscription.c)
target_link_libraries(test_fabric_concurrent_subscription PRIVATE kith_fabric kith_sim Threads::Threads)

add_executable(test_aoi_create test_aoi_create.c)
target_link_libraries(test_aoi_create PRIVATE kith_aoi)
add_test(NAME test_aoi_create COMMAND test_aoi_create)

add_executable(test_aoi_index test_aoi_index.c)
target_link_libraries(test_aoi_index PRIVATE kith_aoi)
add_test(NAME test_aoi_index COMMAND test_aoi_index)

add_executable(test_aoi_query test_aoi_query.c)
target_link_libraries(test_aoi_query PRIVATE kith_aoi)
add_test(NAME test_aoi_query COMMAND test_aoi_query)

add_executable(test_aoi_overflow test_aoi_overflow.c)
target_link_libraries(test_aoi_overflow PRIVATE kith_aoi)
add_test(NAME test_aoi_overflow COMMAND test_aoi_overflow)

add_executable(test_aoi_buckets test_aoi_buckets.c ${CMAKE_SOURCE_DIR}/src/aoi/aoi.c)
target_link_libraries(test_aoi_buckets PRIVATE kith_util)
target_include_directories(test_aoi_buckets PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_compile_definitions(test_aoi_buckets PRIVATE _GNU_SOURCE)
add_test(NAME test_aoi_buckets COMMAND test_aoi_buckets)

add_executable(test_gateway_create test_gateway_create.c)
target_link_libraries(test_gateway_create PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_util)
add_test(NAME test_gateway_create COMMAND test_gateway_create)

# Gateway-internal sources that gateway tests compile directly to reach
# private functions; one list so a new gateway source lands in every test
# that links the internal layout.
set(KITH_GATEWAY_TEST_SOURCES
    ${PROJECT_SOURCE_DIR}/src/gateway/session/session.c
    ${PROJECT_SOURCE_DIR}/src/gateway/view/view.c
    ${PROJECT_SOURCE_DIR}/src/gateway/cache/cache.c
    ${PROJECT_SOURCE_DIR}/src/gateway/delivery/delivery.c
    ${PROJECT_SOURCE_DIR}/src/gateway/delivery/executor.c
    ${PROJECT_SOURCE_DIR}/src/gateway/delivery/strategy.c
    ${PROJECT_SOURCE_DIR}/src/gateway/delivery/strategy_full.c
    ${PROJECT_SOURCE_DIR}/src/gateway/delivery/strategy_tiered.c
)

add_executable(test_gateway_session test_gateway_session.c ${KITH_GATEWAY_TEST_SOURCES})
target_link_libraries(test_gateway_session PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker Threads::Threads kith_util)
target_compile_definitions(test_gateway_session PRIVATE _GNU_SOURCE)
target_include_directories(test_gateway_session PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_gateway_session COMMAND test_gateway_session)

add_executable(test_gateway_cache test_gateway_cache.c)
target_link_libraries(test_gateway_cache PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_util)
add_test(NAME test_gateway_cache COMMAND test_gateway_cache)

add_executable(test_gateway_phase_stats test_gateway_phase_stats.c)
target_link_libraries(test_gateway_phase_stats PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_util)
target_compile_definitions(test_gateway_phase_stats PRIVATE _GNU_SOURCE)
add_test(NAME test_gateway_phase_stats COMMAND test_gateway_phase_stats)

add_executable(test_gateway_compose_stats test_gateway_compose_stats.c)
target_link_libraries(test_gateway_compose_stats PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_util)
target_compile_definitions(test_gateway_compose_stats PRIVATE _GNU_SOURCE)
add_test(NAME test_gateway_compose_stats COMMAND test_gateway_compose_stats)

add_executable(test_gateway_compose_budget test_gateway_compose_budget.c)
target_link_libraries(test_gateway_compose_budget PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_util)
target_compile_definitions(test_gateway_compose_budget PRIVATE _GNU_SOURCE)
add_test(NAME test_gateway_compose_budget COMMAND test_gateway_compose_budget)

add_executable(test_gateway_handler test_gateway_handler.c)
target_link_libraries(test_gateway_handler PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker kith_util)
target_compile_definitions(test_gateway_handler PRIVATE _GNU_SOURCE)
add_test(NAME test_gateway_handler COMMAND test_gateway_handler)

add_executable(test_gateway_lifecycle test_gateway_lifecycle.c)
target_link_libraries(test_gateway_lifecycle PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker kith_util)
target_compile_definitions(test_gateway_lifecycle PRIVATE _GNU_SOURCE)
add_test(NAME test_gateway_lifecycle COMMAND test_gateway_lifecycle)

add_executable(test_gateway_window test_gateway_window.c)
target_link_libraries(test_gateway_window PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker kith_util)
target_compile_definitions(test_gateway_window PRIVATE _GNU_SOURCE)
add_test(NAME test_gateway_window COMMAND test_gateway_window)

add_executable(test_gateway_view test_gateway_view.c ${KITH_GATEWAY_TEST_SOURCES})
target_link_libraries(test_gateway_view PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker Threads::Threads kith_util)
target_compile_definitions(test_gateway_view PRIVATE _GNU_SOURCE)
target_include_directories(test_gateway_view PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_gateway_view COMMAND test_gateway_view)

add_executable(test_gateway_delivery test_gateway_delivery.c ${KITH_GATEWAY_TEST_SOURCES})
target_compile_definitions(test_gateway_delivery PRIVATE _GNU_SOURCE)
target_include_directories(test_gateway_delivery PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(test_gateway_delivery PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker kith_util)
add_test(NAME test_gateway_delivery COMMAND test_gateway_delivery)

add_executable(test_gateway_broadcast test_gateway_broadcast.c ${KITH_GATEWAY_TEST_SOURCES})
target_compile_definitions(test_gateway_broadcast PRIVATE _GNU_SOURCE)
target_include_directories(test_gateway_broadcast PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(test_gateway_broadcast PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker Threads::Threads kith_util)
add_test(NAME test_gateway_broadcast COMMAND test_gateway_broadcast)

add_executable(test_gateway_registry test_gateway_registry.c ${KITH_GATEWAY_TEST_SOURCES})
target_link_libraries(test_gateway_registry PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker Threads::Threads kith_util)
target_compile_definitions(test_gateway_registry PRIVATE _GNU_SOURCE)
target_include_directories(test_gateway_registry PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_gateway_registry COMMAND test_gateway_registry)
add_executable(test_gateway_tiered test_gateway_tiered.c ${KITH_GATEWAY_TEST_SOURCES})
target_link_libraries(test_gateway_tiered PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker Threads::Threads kith_util)
target_compile_definitions(test_gateway_tiered PRIVATE _GNU_SOURCE)
target_include_directories(test_gateway_tiered PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_gateway_tiered COMMAND test_gateway_tiered)

add_executable(test_gateway_executor test_gateway_executor.c ${KITH_GATEWAY_TEST_SOURCES})
target_link_libraries(test_gateway_executor PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker Threads::Threads kith_util)
target_compile_definitions(test_gateway_executor PRIVATE _GNU_SOURCE)
target_include_directories(test_gateway_executor PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_gateway_executor COMMAND test_gateway_executor)

add_executable(test_gateway_totals test_gateway_totals.c ${KITH_GATEWAY_TEST_SOURCES})
target_link_libraries(test_gateway_totals PRIVATE kith_gateway kith_fabric kith_net kith_proto kith_sim kith_worker Threads::Threads kith_util)
target_compile_definitions(test_gateway_totals PRIVATE _GNU_SOURCE)
target_include_directories(test_gateway_totals PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_gateway_totals COMMAND test_gateway_totals)



add_executable(test_coord_create test_coord_create.c)
target_link_libraries(test_coord_create PRIVATE kith_coord)
add_test(NAME test_coord_create COMMAND test_coord_create)

add_executable(test_coord_authority test_coord_authority.c)
target_link_libraries(test_coord_authority PRIVATE kith_coord)
add_test(NAME test_coord_authority COMMAND test_coord_authority)

add_executable(test_coord_split test_coord_split.c)
target_link_libraries(test_coord_split PRIVATE kith_coord)
add_test(NAME test_coord_split COMMAND test_coord_split)

add_executable(test_coord_bus test_coord_bus.c)
target_link_libraries(test_coord_bus PRIVATE kith_coord)
add_test(NAME test_coord_bus COMMAND test_coord_bus)

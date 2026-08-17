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

add_executable(test_allocator_failure test_allocator_failure.c)
target_link_libraries(test_allocator_failure PRIVATE kith_util kith_config kith_logger kith_metrics kith_proto kith_net kith_reactor kith_worker kith_state kith_db kith_aoi kith_sim kith_fabric kith_gateway kith_coord kith_control kith_client kith_server)
# setenv and unsetenv are POSIX.1-2008; glibc hides them under strict C23
# without a feature-test macro.
target_compile_definitions(test_allocator_failure PRIVATE _POSIX_C_SOURCE=200809L)
add_test(NAME test_allocator_failure COMMAND test_allocator_failure)

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

# The control plane's sources compile directly into this test so the
# listener's internal bind path is reachable: the shared library keeps
# control_listener_create hidden, and keeping it unexported holds the public ABI stable.
add_executable(test_control_create test_control_create.c
    ${PROJECT_SOURCE_DIR}/src/control/control.c
    ${PROJECT_SOURCE_DIR}/src/control/conn.c
    ${PROJECT_SOURCE_DIR}/src/control/event_bus.c
    ${PROJECT_SOURCE_DIR}/src/control/handlers.c
    ${PROJECT_SOURCE_DIR}/src/control/http_parser.c
    ${PROJECT_SOURCE_DIR}/src/control/router.c)
target_link_libraries(test_control_create PRIVATE kith_util kith_reactor kith_worker kith_metrics)
target_compile_definitions(test_control_create PRIVATE _GNU_SOURCE)
target_include_directories(test_control_create PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_control_create COMMAND test_control_create)

# The response builder test reuses the compile-the-plane shape: the builder
# lives in control.c and the test exercises the exported API against it
# directly.
add_executable(test_control_response test_control_response.c
    ${PROJECT_SOURCE_DIR}/src/control/control.c
    ${PROJECT_SOURCE_DIR}/src/control/conn.c
    ${PROJECT_SOURCE_DIR}/src/control/event_bus.c
    ${PROJECT_SOURCE_DIR}/src/control/handlers.c
    ${PROJECT_SOURCE_DIR}/src/control/http_parser.c
    ${PROJECT_SOURCE_DIR}/src/control/router.c)
target_link_libraries(test_control_response PRIVATE kith_util kith_reactor kith_worker kith_metrics)
target_compile_definitions(test_control_response PRIVATE _GNU_SOURCE)
target_include_directories(test_control_response PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_control_response COMMAND test_control_response)

add_executable(test_control_http test_control_http.c
    ${PROJECT_SOURCE_DIR}/src/control/http_parser.c)
target_link_libraries(test_control_http PRIVATE kith_control kith_util)
target_compile_definitions(test_control_http PRIVATE _GNU_SOURCE)
target_include_directories(test_control_http PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_control_http COMMAND test_control_http)

add_executable(test_control_bus test_control_bus.c
    ${PROJECT_SOURCE_DIR}/src/control/event_bus.c)
target_link_libraries(test_control_bus PRIVATE kith_control kith_util)
target_compile_definitions(test_control_bus PRIVATE _GNU_SOURCE)
target_include_directories(test_control_bus PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_control_bus COMMAND test_control_bus)

add_executable(test_control_server test_control_server.c)
target_link_libraries(test_control_server PRIVATE kith_control kith_reactor kith_metrics kith_worker Threads::Threads kith_util)
target_compile_definitions(test_control_server PRIVATE _GNU_SOURCE)
add_test(NAME test_control_server COMMAND test_control_server)

# conn.c compiles once as a redirected object so its drain call reaches
# the test's injection shim; the redirect macro lives only in that
# one-object build, leaving the shared library build of conn.c untouched.
add_library(test_control_conn_shim OBJECT ${PROJECT_SOURCE_DIR}/src/control/conn.c)
target_compile_definitions(test_control_conn_shim PRIVATE _GNU_SOURCE write=kith_test_write)
target_include_directories(test_control_conn_shim PRIVATE
    ${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}/include)
set_target_properties(test_control_conn_shim PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_executable(test_control_conn test_control_conn.c
    $<TARGET_OBJECTS:test_control_conn_shim>
    ${PROJECT_SOURCE_DIR}/src/control/control.c
    ${PROJECT_SOURCE_DIR}/src/control/router.c
    ${PROJECT_SOURCE_DIR}/src/control/event_bus.c
    ${PROJECT_SOURCE_DIR}/src/control/handlers.c
    ${PROJECT_SOURCE_DIR}/src/control/http_parser.c)
target_link_libraries(test_control_conn PRIVATE kith_util kith_reactor kith_worker kith_metrics Threads::Threads)
target_compile_definitions(test_control_conn PRIVATE _GNU_SOURCE)
target_include_directories(test_control_conn PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_control_conn COMMAND test_control_conn)

add_executable(test_client_create test_client_create.c)
target_link_libraries(test_client_create PRIVATE kith_client kith_proto)
target_compile_definitions(test_client_create PRIVATE _GNU_SOURCE)
add_test(NAME test_client_create COMMAND test_client_create)

add_executable(test_client_bootstrap test_client_bootstrap.c
    ${PROJECT_SOURCE_DIR}/src/client/bootstrap.c
    ${PROJECT_SOURCE_DIR}/src/client/client.c
    ${PROJECT_SOURCE_DIR}/src/client/queue.c)
target_link_libraries(test_client_bootstrap PRIVATE kith_proto)
target_compile_definitions(test_client_bootstrap PRIVATE _GNU_SOURCE)
target_include_directories(test_client_bootstrap PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_client_bootstrap COMMAND test_client_bootstrap)

# The tick test drives the keepalive schedule through the internal struct,
# so the client sources compile directly into the binary.
add_executable(test_client_tick test_client_tick.c
    ${PROJECT_SOURCE_DIR}/src/client/bootstrap.c
    ${PROJECT_SOURCE_DIR}/src/client/client.c
    ${PROJECT_SOURCE_DIR}/src/client/queue.c)
target_link_libraries(test_client_tick PRIVATE kith_proto)
target_compile_definitions(test_client_tick PRIVATE _GNU_SOURCE)
target_include_directories(test_client_tick PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_client_tick COMMAND test_client_tick)

add_executable(test_client_events test_client_events.c)
target_link_libraries(test_client_events PRIVATE kith_client kith_proto)
target_compile_definitions(test_client_events PRIVATE _GNU_SOURCE)
add_test(NAME test_client_events COMMAND test_client_events)

add_executable(test_client_queue test_client_queue.c
    ${PROJECT_SOURCE_DIR}/src/client/queue.c)
target_link_libraries(test_client_queue PRIVATE kith_client kith_proto)
target_compile_definitions(test_client_queue PRIVATE _GNU_SOURCE)
target_include_directories(test_client_queue PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_client_queue COMMAND test_client_queue)

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

add_executable(test_server_create test_server_create.c
    ${PROJECT_SOURCE_DIR}/src/server/server.c
    ${PROJECT_SOURCE_DIR}/src/server/wire.c
    ${PROJECT_SOURCE_DIR}/src/server/wiring.c)
target_link_libraries(test_server_create PRIVATE
    kith_sim
    kith_fabric
    kith_gateway
    kith_coord
    kith_reactor
    kith_worker
    kith_net
    kith_proto
    kith_config
    kith_logger
    kith_metrics)
if(TARGET kith_control)
    target_link_libraries(test_server_create PRIVATE kith_control kith_util)
    target_compile_definitions(test_server_create PRIVATE KITH_CONTROL_PLANE_ENABLED=1)
endif()
target_compile_definitions(test_server_create PRIVATE _GNU_SOURCE)
target_include_directories(test_server_create PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_server_create COMMAND test_server_create)

add_executable(test_server_accessors test_server_accessors.c)
target_link_libraries(test_server_accessors PRIVATE kith_server kith_proto kith_util)
add_test(NAME test_server_accessors COMMAND test_server_accessors)

add_executable(test_server_run test_server_run.c)
target_link_libraries(test_server_run PRIVATE kith_server Threads::Threads kith_util)
target_compile_definitions(test_server_run PRIVATE _GNU_SOURCE)
add_test(NAME test_server_run COMMAND test_server_run)

add_executable(test_server_tick_hook test_server_tick_hook.c)
target_link_libraries(test_server_tick_hook PRIVATE kith_server Threads::Threads kith_util)
target_compile_definitions(test_server_tick_hook PRIVATE _GNU_SOURCE)
add_test(NAME test_server_tick_hook COMMAND test_server_tick_hook)

add_executable(test_server_poll_observer test_server_poll_observer.c)
target_link_libraries(test_server_poll_observer PRIVATE kith_server Threads::Threads kith_util)
target_compile_definitions(test_server_poll_observer PRIVATE _GNU_SOURCE)
add_test(NAME test_server_poll_observer COMMAND test_server_poll_observer)

add_executable(test_server_wire_write test_server_wire_write.c
    ${PROJECT_SOURCE_DIR}/src/server/wire.c)
target_link_libraries(test_server_wire_write PRIVATE kith_gateway kith_net kith_proto kith_reactor
    kith_fabric kith_sim kith_util Threads::Threads)
target_compile_definitions(test_server_wire_write PRIVATE _GNU_SOURCE)
target_include_directories(test_server_wire_write PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_server_wire_write COMMAND test_server_wire_write)

add_executable(test_server_wire_churn test_server_wire_churn.c
    ${PROJECT_SOURCE_DIR}/src/server/wire.c)
target_link_libraries(test_server_wire_churn PRIVATE kith_gateway kith_net kith_proto kith_reactor
    kith_fabric kith_sim kith_util Threads::Threads)
target_compile_definitions(test_server_wire_churn PRIVATE _GNU_SOURCE)
target_include_directories(test_server_wire_churn PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_server_wire_churn COMMAND test_server_wire_churn)

add_executable(test_server_wiring test_server_wiring.c
    ${PROJECT_SOURCE_DIR}/src/server/wiring.c
    ${PROJECT_SOURCE_DIR}/src/server/wire.c)
target_link_libraries(test_server_wiring PRIVATE kith_gateway kith_config kith_logger
    kith_metrics kith_proto kith_net kith_reactor kith_worker kith_state kith_db
    kith_aoi kith_sim kith_fabric kith_coord kith_control kith_util Threads::Threads)
target_compile_definitions(test_server_wiring PRIVATE _GNU_SOURCE)
target_include_directories(test_server_wiring PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_server_wiring COMMAND test_server_wiring)

add_executable(test_python_handler_exceptions test_python_handler_exceptions.c
    ${PROJECT_SOURCE_DIR}/src/server/wiring.c
    ${PROJECT_SOURCE_DIR}/src/server/wire.c)
target_link_libraries(test_python_handler_exceptions PRIVATE kith_gateway kith_config
    kith_logger kith_metrics kith_proto kith_net kith_reactor kith_worker kith_state
    kith_db kith_aoi kith_sim kith_fabric kith_coord kith_control kith_util
    Threads::Threads)
target_compile_definitions(test_python_handler_exceptions PRIVATE _GNU_SOURCE)
target_include_directories(test_python_handler_exceptions PRIVATE ${PROJECT_SOURCE_DIR}/src)
add_test(NAME test_python_handler_exceptions COMMAND test_python_handler_exceptions)

add_executable(test_python_exit_gate test_python_exit_gate.c)
target_link_libraries(test_python_exit_gate PRIVATE kith_server kith_gateway kith_fabric kith_net
    kith_proto kith_sim kith_worker kith_util Threads::Threads)
target_compile_definitions(test_python_exit_gate PRIVATE _GNU_SOURCE)
add_test(NAME test_python_exit_gate COMMAND test_python_exit_gate)

# One uniform ceiling for every test in the directory: a wedged test costs
# the gate minutes rather than ctest's default 1500 s, and the threaded
# server/gateway tests keep headroom on a loaded host where their wall
# time stretches.
get_property(_kith_c_tests DIRECTORY PROPERTY TESTS)
if(_kith_c_tests)
    set_tests_properties(${_kith_c_tests} PROPERTIES TIMEOUT 300)
endif()

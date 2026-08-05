# libkith_worker — GIL-aware worker pool shared library.
#
# Layer-1 shared-infrastructure target. The worker module owns a fixed-size
# pool of native threads and a lock-free MPSC task queue. The reactor thread
# submits Python-bound callbacks (gateway message handlers, control-plane
# route handlers, and standalone-reactor callbacks bridged through the
# reactor-bridge API) to the pool and continues draining file-descriptor
# readiness without entering the Python interpreter; the pool's worker
# threads run the callbacks and post any reactor-side completion work back
# via kith_reactor_submit. This is the thread contract: the reactor thread
# never calls Python.
#
# The pool is sized 1 under standard Python (the GIL serializes Python
# execution anyway) and sized N under free-threaded Python (real parallelism).
# The size is a creation parameter derived from the server's
# python_worker_count config field; the server composition root creates and
# owns the pool and passes it to the gateway and control planes. The
# standalone reactor wrapper creates and owns its own pool through the Worker
# Python type.
#
# The reactor-bridge API (kith_worker_bridge_*) lets a standalone reactor
# register a C hop function that submits the user's Python callback to the
# pool from C, so the reactor thread never enters Python — the same
# discipline the gateway plane already follows internally.
#
# The version script (src/worker/kith_worker.map) is generated from the
# library's public header by tools/gen_symbols.py and committed alongside the
# header change that produced it.

set(KITH_WORKER_SOURCES
    src/worker/worker.c
    src/worker/bridge.c
)

add_library(kith_worker SHARED ${KITH_WORKER_SOURCES})

target_include_directories(kith_worker PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_worker PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

find_package(Threads REQUIRED)

target_link_libraries(kith_worker PRIVATE
    kith_util
    Threads::Threads
)

target_compile_definitions(kith_worker PRIVATE _GNU_SOURCE)

# SONAME + version script.
kith_apply_abi(kith_worker
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/worker/kith_worker.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_worker
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/worker/worker.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/worker)

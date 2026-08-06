# libkith_db — Postgres-backed asynchronous persistence pool shared library.
#
# Layer-2 shared-plane target. The db module owns a pool of non-blocking
# Postgres connections (via the libpq async API) and integrates with the
# reactor for event-driven I/O. The composition root registers named
# parameterized queries at startup; a generic execute submits a registered
# query by name with caller-supplied text parameters, and the result
# arrives via a callback invoked from the reactor event loop.
#
# The module is generic: it knows nothing about the application schema,
# tables, or query semantics. Game-specific SQL is registered by the
# caller, not baked into the library.
#
# The version script (src/db/kith_db.map) is generated from the library's
# public header and committed alongside the header change that produced it.

find_package(PkgConfig QUIET)
pkg_check_modules(libpq REQUIRED IMPORTED_TARGET libpq)

set(KITH_DB_SOURCES
    src/db/db.c
    src/db/conn.c
)

add_library(kith_db SHARED ${KITH_DB_SOURCES})

target_include_directories(kith_db PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_db PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(kith_db PRIVATE
    kith_reactor
    PkgConfig::libpq
)

# SONAME + version script.
kith_apply_abi(kith_db
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/db/kith_db.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_db
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/db/db.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/db)

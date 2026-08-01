# libkith_config — typed, layered configuration source shared library.
#
# Layer-0 foundation target. The config library is a loading and validation
# mechanism: it builds an immutable key=value snapshot from an optional
# key=value file and an optional environment namespace (env wins over file),
# and exposes typed accessors that parse, range-check, and report a missing
# key as an error so required fields surface at startup. Modules own their own
# config data and query this source by pointer (no global accessor).
#
# The version script (src/config/kith_config.map) is generated from the
# library's public headers by tools/gen_symbols.py and committed alongside the
# header change that produced it; re-run the generator when a new KITH_API
# symbol is added to include/kith/config/.

add_library(kith_config SHARED
    src/config/config.c
)

target_include_directories(kith_config PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# SONAME + version script. kith_apply_abi (cmake/abi.cmake) sets VERSION
# (and SOVERSION = major) from PROJECT_VERSION and links the version script
# that locks the exported
# surface to the KITH_API symbols listed in the .map.
kith_apply_abi(kith_config
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/config/kith_config.map
)

# Install the library and the public headers it exposes. The framework-level
# headers (api.h, version.h, types.h) are shared dependencies installed by
# lib_util.cmake; the config module header goes to include/kith/config/.
include(GNUInstallDirs)
install(TARGETS kith_config
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/config/config.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/config)

# libkith_util — foundation utilities shared library.
#
# Layer-0 foundation target. Establishes the per-library pattern every
# cmake/lib_<module>.cmake fragment follows: a SHARED target with the
# public include root, a generated version script exporting only the
# KITH_API symbols via kith_apply_abi, a versioned SONAME, and install
# rules for the library and its public headers.
#
# The version script (src/util/kith_util.map) is generated from the
# library's public headers by tools/gen_symbols.py and committed
# alongside the header change that produced it; re-run the generator
# when a new KITH_API symbol is added to include/kith/util/.

add_library(kith_util SHARED
    src/util/util.c
    src/util/fpenv.c
    src/util/rng.c
    src/util/tick_clock.c
    src/util/wall_clock.c
)

# strict C23 mode does not declare clock_gettime without a POSIX feature
# macro. _GNU_SOURCE is the established repo-wide choice (see
# cmake/lib_gateway.cmake).
target_compile_definitions(kith_util PRIVATE _GNU_SOURCE)

target_include_directories(kith_util PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# SONAME + version script. kith_apply_abi (cmake/abi.cmake) sets VERSION
# (and SOVERSION = major) from PROJECT_VERSION and links the version script
# that locks the
# exported surface to the KITH_API symbols listed in the .map.
kith_apply_abi(kith_util
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/util/kith_util.map
)

# Install the library and the public headers it exposes. Framework-level
# headers (api.h, version.h, types.h) are shared dependencies and are
# installed to include/kith/; the util module headers go to include/kith/util/.
include(GNUInstallDirs)
install(TARGETS kith_util
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/api.h
    ${PROJECT_SOURCE_DIR}/include/kith/version.h
    ${PROJECT_SOURCE_DIR}/include/kith/types.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith)
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/util/util.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/util)

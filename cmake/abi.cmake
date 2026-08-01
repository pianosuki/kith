# ABI / version-script helpers.
#
# Every layer-2 module is a shared library with a versioned SONAME (.1) and a
# generated .map version script exporting only KITH_API symbols. This module
# provides the kith_apply_abi() function used by each cmake/lib_<module>.cmake
# fragment so the SONAME + version-script convention is applied
# consistently from a single place.
#
#   kith_apply_abi(<target>
#       [MAP_FILE <path>]    # generated .map (from tools/gen_symbols.py)
#       [VERSION <x.y.z>])   # full version; SOVERSION = major
#                             # (defaults to PROJECT_VERSION)
#
# The VERSION default is PROJECT_VERSION (set by the top-level
# project(kith VERSION ... LANGUAGES C) call). SOVERSION is the major component, so a
# 1.0.0 project version yields libkith_<m>.so.1.0.0 with the SONAME symlink
# libkith_<m>.so.1 — the binary-compat generation the size-versioned public
# structs (KITH_ABI_VERSION) track. MAP_FILE is optional; lib_*.cmake passes
# each module's generated map so the exported surface is locked to KITH_API
# symbols via -Wl,--version-script.

function(kith_apply_abi target)
    set(options)
    set(one_value_args MAP_FILE VERSION)
    set(multi_value_args)
    cmake_parse_arguments(KITH_ABI "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

    if(NOT TARGET ${target})
        message(FATAL_ERROR "kith_apply_abi: '${target}' is not a CMake target")
    endif()

    # The library VERSION (and SOVERSION) tracks PROJECT_VERSION so the
    # SONAME and the pkg-config / find_package version agree on a single
    # value. SOVERSION is the major component (every layer-2 lib carries
    # SONAME .1 under a 1.0.0 project version).
    if(NOT KITH_ABI_VERSION)
        if(NOT PROJECT_VERSION)
            message(FATAL_ERROR
                "kith_apply_abi: no VERSION given and PROJECT_VERSION is unset "
                "(the top-level project() call must declare a VERSION)")
        endif()
        set(KITH_ABI_VERSION "${PROJECT_VERSION}")
    endif()
    string(REPLACE "." ";" _kith_abi_vparts "${KITH_ABI_VERSION}")
    list(GET _kith_abi_vparts 0 _kith_abi_major)
    set_target_properties(${target} PROPERTIES
        VERSION "${KITH_ABI_VERSION}"
        SOVERSION "${_kith_abi_major}")
    unset(_kith_abi_vparts)
    unset(_kith_abi_major)

    # Hidden visibility is a global default (visibility.cmake); enforce it on
    # the target too so a stray set() upstream cannot accidentally widen it.
    set_target_properties(${target} PROPERTIES
        C_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON)

    if(KITH_ABI_MAP_FILE)
        if(NOT EXISTS "${KITH_ABI_MAP_FILE}")
            message(FATAL_ERROR "kith_apply_abi: version script not found: ${KITH_ABI_MAP_FILE}")
        endif()
        target_link_options(${target} PRIVATE
            "LINKER:--version-script,${KITH_ABI_MAP_FILE}")
    endif()
endfunction()

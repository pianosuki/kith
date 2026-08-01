# Security hardening flags applied to every build.
#
# The framework handles untrusted network input (the protocol decoder is the
# primary attack surface), so hardening is not optional and is applied
# across debug, release, and sanitizer presets; the one exception is
# _FORTIFY_SOURCE on the unoptimized presets, where it is inert. Flags
# target Clang 22 / GCC 14 on Ubuntu 24.04+ and are guarded on compiler
# family.
#
# _FORTIFY_SOURCE interaction: -D_FORTIFY_SOURCE=3 conflicts with Address/
# ThreadSanitizer (duplicate/instrumented memory checks), and fortification
# itself requires optimization. Two gating paths exist: sanitizers.cmake sets
# KITH_ENABLE_FORTIFY=OFF as a normal variable before this module is included
# for the sanitizer presets (a -D cannot override it), and the debug and
# coverage presets carry KITH_ENABLE_FORTIFY=OFF in their own cacheVariables
# (a -D can override it there). Every other hardening flag stays active under
# sanitizers. KITH_ENABLE_FORTIFY defaults to ON when unset so this module is
# safe to include standalone; a bare cmake -DCMAKE_BUILD_TYPE=Debug configure
# outside the presets therefore still emits the glibc optimization diagnostic.

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -fstack-protector-all
        -fcf-protection=full
        -fstack-clash-protection
    )
    # Fortification requires optimization: without it the object-size checks
    # cannot fold and glibc emits a diagnostic on every translation unit.
    # The unoptimized presets disable the define (sanitizers.cmake for the
    # sanitizer presets, the debug and coverage presets' cacheVariables);
    # release keeps it.
    if(NOT DEFINED KITH_ENABLE_FORTIFY)
        set(KITH_ENABLE_FORTIFY ON)
    endif()
    if(KITH_ENABLE_FORTIFY)
        add_compile_options(-D_FORTIFY_SOURCE=3)
    endif()

    # ARM64 branch protection (BTI + PAC) only when targeting aarch64/arm64.
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
        add_compile_options(-mbranch-protection=standard)
    endif()

    add_link_options(
        -Wl,-z,relro
        -Wl,-z,now
        -Wl,-z,noexecstack
        -Wl,-z,separate-code
    )
    # -pie produces position-independent executables; it is meaningless for
    # shared libraries (already PIC) and Clang warns "argument unused during
    # compilation" when it is passed on a shared-library link line. Scope it
    # to executables only via CMAKE_EXE_LINKER_FLAGS so the relro/now/noexec-
    # stack/separate-code flags above still apply to every link type.
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pie")
endif()

# No absolute RPATH: run-time search paths must resolve relative to the
# binary's own location ($ORIGIN), never from an absolute, potentially
# user-writable directory. Skip auto-adding the absolute link-time library
# paths and pin the relative install rpath so no built artifact embeds an
# absolute library path.
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)
set(CMAKE_BUILD_RPATH_USE_ORIGIN ON)
set(CMAKE_INSTALL_RPATH "$ORIGIN/../lib")

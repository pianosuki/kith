# Compiler warning flags for the kith framework.
#
# Applied to every translation unit via add_compile_options() so that the
# flags are inherited by targets created in any subdirectory. Warnings are
# *not* promoted to errors here, so local development builds remain
# iterative; the clang-tidy review lane is the strict surface, promoting its
# own diagnostics via -warnings-as-errors. The set is tuned for security-critical
# C23: it favors precision (value/category warnings) over noise and assumes
# Clang 22 / GCC 14 feature availability.
#
# Guarded on compiler family so a foreign compiler (unsupported in practice)
# does not emit "unknown option" errors that mask real diagnostics.

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -Wall
        -Wextra
        -Wpedantic
        -Wshadow
        -Wconversion
        -Wnull-dereference
        -Wdouble-promotion
        -Wformat=2
        -Wundef
        -Wcast-align
        -Wwrite-strings
        -Wredundant-decls
        -Wmissing-prototypes
        -Wstrict-prototypes
        -Wswitch-enum
        -Wfloat-equal
        -Wno-unused-parameter
    )
endif()

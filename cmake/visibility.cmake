# Symbol visibility setup.
#
# The project defaults to hidden visibility so that only symbols explicitly
# marked KITH_API (defined in include/kith/api.h) are exported from
# each shared library; KITH_LOCAL marks internal-but-header symbols. A
# generated .map version script per library (cmake/abi.cmake + gen_symbols.py)
# further constrains the exported surface at link time.
#
# CMAKE_C_VISIBILITY_PRESET=hidden and CMAKE_POSITION_INDEPENDENT_CODE=ON are
# established by the top-level CMakeLists.txt. This module only guards
# those defaults so it is safe to include standalone, and records the contract
# that the api.h macros rely on; it deliberately does not re-issue the flags to
# avoid duplicating the top-level settings.

if(NOT CMAKE_C_VISIBILITY_PRESET)
    set(CMAKE_C_VISIBILITY_PRESET hidden)
endif()
if(NOT DEFINED CMAKE_VISIBILITY_INLINES_HIDDEN)
    # No-op for a pure-C project, but harmless and correct if C++ ever links in.
    set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
endif()

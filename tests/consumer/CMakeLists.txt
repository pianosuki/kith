# tests/consumer — out-of-tree downstream consumers of an installed kith.
# CMakeLists.txt + main.c exercise the find_package(kith) channel: a
# downstream project configures against an installed kith, links the
# imported targets, builds, and runs. The verify.sh build stage drives that
# end-to-end (install to a prefix, configure with -Dkith_DIR=<prefix>/lib/
# cmake/kith, build, run) and drives the pkg-config channel separately with
# pkgconfig_probe.c, which compiles and links against exactly the flags the
# installed .pc file reports.
#
# A real downstream project calls `find_package(kith 1.0.0 REQUIRED)` and links
# `kith::server` (the composition root); this minimal consumer links both
# `kith::server` and `kith::util` and calls one function from each so the
# shared libraries are actually pulled in at link time and resolved at run
# time.

cmake_minimum_required(VERSION 4.4)

project(kith_consumer C)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

find_package(kith 1.0.0 REQUIRED)

add_executable(kith_consumer main.c)
target_link_libraries(kith_consumer PRIVATE kith::server kith::util)

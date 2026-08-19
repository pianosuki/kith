# Examples: standalone programs that exercise the framework's public
# surface. Each subdirectory owns its own wiring; the build only compiles
# the C boots that link against the composition root (kith_server). Python
# examples are plain modules the pytest suite or `python -m` drives.

add_executable(minimal_c_boot minimal/main.c)
target_link_libraries(minimal_c_boot PRIVATE kith_server Threads::Threads)
target_compile_definitions(minimal_c_boot PRIVATE _GNU_SOURCE)
set_target_properties(minimal_c_boot PROPERTIES OUTPUT_NAME minimal_c_boot)

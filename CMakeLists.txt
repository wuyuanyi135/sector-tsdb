cmake_minimum_required(VERSION 3.24)
project(sd_tsdb)

set(CMAKE_CXX_STANDARD 20)
add_library(catch catch/catch_amalgamated.cpp)
target_compile_definitions(catch PUBLIC CATCH_CONFIG_FAST_COMPILE)
target_link_options(catch PUBLIC -fuse-ld=lld)
add_subdirectory(fmt)

include_directories(catch)
add_executable(test test_io.cpp test_header_sectors_manager.cpp test_series.cpp test_crc.cpp test_common.cpp test_simulated.cpp)
target_link_libraries(test catch fmt::fmt-header-only)

add_executable(continuous_running_example continuous_running_example.cpp)
target_link_libraries(continuous_running_example fmt::fmt-header-only)

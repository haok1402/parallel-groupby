cmake_minimum_required(VERSION 3.10)
project(parallel-groupby)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)
add_compile_options(-O3 -march=native -Wall -Wextra -Wpedantic)

include_directories(SYSTEM
    ${CMAKE_SOURCE_DIR}/lib/CLI11/include
    ${CMAKE_SOURCE_DIR}/lib/duckdb/include
)

add_executable(
    generate src/generate.cpp
)

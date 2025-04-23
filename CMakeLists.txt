cmake_minimum_required(VERSION 3.10)
project(parallel-groupby)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Use old ABI for compatibility
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

# Base compile options
add_compile_options(-march=native -Wall -Wextra -Wpedantic)

# Add Release-specific optimization
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3)
endif()

# Include third-party headers
include_directories(SYSTEM 
    ${CMAKE_SOURCE_DIR}/lib/CLI11/include
    ${CMAKE_SOURCE_DIR}/lib/indicators/include
)

# Find and collect required libraries
find_package(ZLIB REQUIRED)

# Define executable targets and their sources
add_executable(generate src/generate.cpp)
add_executable(two-phase-central-merge src/two-phase-central-merge.cpp)

# Link dependencies for each target
target_link_libraries(generate 
    PRIVATE ZLIB::ZLIB
)

target_link_libraries(two-phase-central-merge 
    PRIVATE ZLIB::ZLIB
)

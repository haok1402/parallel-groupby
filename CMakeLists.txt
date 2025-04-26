cmake_minimum_required(VERSION 3.10)
project(parallel-groupby)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Use old ABI for compatibility
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)


# Add Release-specific optimization
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3)
endif()

# Include third-party headers
include_directories(SYSTEM 
    ${CMAKE_SOURCE_DIR}/lib/CLI11/include
    ${CMAKE_SOURCE_DIR}/lib/indicators/include
)

# Define executables
add_executable(generate src/generate.cpp)
add_executable(main src/main.cpp)
add_executable(two-phase-central-merge src/two-phase-central-merge.cpp)
add_executable(lock-free-hash-table src/lock-free-hash-table.cpp)
add_executable(radix-partition-then-merge src/radix-partition-then-merge.cpp)

# Platform-specific setup
if(APPLE)
    find_package(ZLIB REQUIRED)
    # Base compile options
    add_compile_options(-march=native)

    # DuckDB
    include_directories(/opt/homebrew/Cellar/duckdb/1.2.2/include)
    set(DUCKDB_LIB /opt/homebrew/Cellar/duckdb/1.2.2/lib/libduckdb.dylib)

    # OpenMP
    include_directories(/opt/homebrew/opt/libomp/include)
    link_directories(/opt/homebrew/opt/libomp/lib)
    set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include")
    set(OpenMP_CXX_LIB_NAMES "omp")
    set(OpenMP_omp_LIBRARY /opt/homebrew/opt/libomp/lib/libomp.dylib)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    # Link dependencies
    target_link_libraries(generate 
        PRIVATE ZLIB::ZLIB
        PRIVATE ${DUCKDB_LIB}
        PRIVATE ${OpenMP_omp_LIBRARY}
    )
    target_link_libraries(two-phase-central-merge 
        PRIVATE ZLIB::ZLIB
        PRIVATE ${DUCKDB_LIB}
        PRIVATE ${OpenMP_omp_LIBRARY}
    )
    target_link_libraries(main
        PRIVATE ZLIB::ZLIB
        PRIVATE ${DUCKDB_LIB}
        PRIVATE ${OpenMP_omp_LIBRARY}
    )
    target_link_libraries(lock-free-hash-table
        PRIVATE ZLIB::ZLIB
        PRIVATE ${DUCKDB_LIB}
    )
    target_link_libraries(radix-partition-then-merge
        PRIVATE ZLIB::ZLIB
        PRIVATE ${DUCKDB_LIB}
        PRIVATE ${OpenMP_omp_LIBRARY}
    )

else()
    # Base compile options
    add_compile_options(-march=native -Wall -Wextra -Wpedantic)

    find_package(ZLIB REQUIRED)
    find_package(OpenMP REQUIRED)

    target_link_libraries(generate 
        PRIVATE ZLIB::ZLIB
        PRIVATE OpenMP::OpenMP_CXX
    )
    target_link_libraries(two-phase-central-merge 
        PRIVATE ZLIB::ZLIB
        PRIVATE OpenMP::OpenMP_CXX
    )
    target_link_libraries(lock-free-hash-table
        PRIVATE ZLIB::ZLIB
    )
    target_link_libraries(radix-partition-then-merge
        PRIVATE ZLIB::ZLIB
        PRIVATE OpenMP::OpenMP_CXX
    )

endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

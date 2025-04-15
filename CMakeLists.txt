cmake_minimum_required(VERSION 3.10)
project(parallel-groupby)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

add_executable(main src/main.cpp)

if(APPLE)
    include_directories(/opt/homebrew/Cellar/duckdb/1.2.2/include)
    target_link_libraries(main /opt/homebrew/Cellar/duckdb/1.2.2/lib/libduckdb.dylib)
    include_directories(/opt/homebrew/opt/libomp/include)
    target_link_libraries(main /opt/homebrew/opt/libomp/lib/libomp.dylib)
    set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include")
    set(OpenMP_CXX_LIB_NAMES "omp")
    set(OpenMP_omp_LIBRARY /opt/homebrew/opt/libomp/lib/libomp.dylib)
elseif(UNIX AND NOT APPLE)
    include_directories(${CMAKE_SOURCE_DIR}/lib/duckdb/include)
    target_link_libraries(main ${CMAKE_SOURCE_DIR}/lib/duckdb/lib/libduckdb.so /usr/lib/gcc/x86_64-linux-gnu/13/libgomp.so)
else()
    message(FATAL_ERROR "Unsupported platform.")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 ${OpenMP_CXX_FLAGS}")

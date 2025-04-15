cmake_minimum_required(VERSION 3.10)
project(parallel-groupby)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

add_executable(main src/main.cpp)

if(APPLE)
    include_directories(/opt/homebrew/Cellar/duckdb/1.2.2/include)
    include_directories(/opt/homebrew/opt/libomp/include)
    target_link_libraries(main /opt/homebrew/Cellar/duckdb/1.2.2/lib/libduckdb.dylib)
    target_link_libraries(main /opt/homebrew/opt/libomp/lib/libomp.dylib)
else()
    include_directories(${CMAKE_SOURCE_DIR}/lib/duckdb/include)
    target_link_libraries(main ${CMAKE_SOURCE_DIR}/lib/duckdb/lib/libduckdb.so)
endif()

cmake_minimum_required(VERSION 3.19)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_CXX_FLAGS_DEBUG "-U_FORTIFY_SOURCE -O0 -g3")
project(simdb CXX)

set(SIMDB_BASE ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${SIMDB_BASE}/include)
include(${SIMDB_BASE}/cmake/simdb-config.cmake)

## Testing
add_subdirectory(test EXCLUDE_FROM_ALL)

## Installation
install(DIRECTORY include/simdb/ DESTINATION include/simdb)
install(DIRECTORY cmake/ DESTINATION lib/cmake/simdb)

cmake_minimum_required(VERSION 3.19)
set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_CXX_FLAGS_DEBUG "-U_FORTIFY_SOURCE -O0 -g3")

project(simdb LANGUAGES CXX)
add_library(simdb INTERFACE)

find_package(SQLite3 3.19 REQUIRED)
message (STATUS "Using SQLite3 ${SQLite3_VERSION}")

find_package(ZLIB REQUIRED)
message (STATUS "Using zlib ${ZLIB_VERSION_STRING}")

find_package(TBB REQUIRED)
message(STATUS "Using TBB ${TBB_VERSION}")

set(SIMDB_BASE ${CMAKE_CURRENT_SOURCE_DIR})

target_include_directories(simdb INTERFACE
    $<BUILD_INTERFACE:${SIMDB_BASE}/include>
    $<INSTALL_INTERFACE:include>
)

target_include_directories(simdb SYSTEM INTERFACE
    ${SQLite3_INCLUDE_DIRS}
    ${ZLIB_INCLUDE_DIRS}
)

target_link_libraries(simdb INTERFACE
    sqlite3 ZLIB::ZLIB TBB::tbb pthread
)

## Testing
add_subdirectory(test EXCLUDE_FROM_ALL)

## Installation
install(DIRECTORY include/simdb/ DESTINATION include/simdb)
install(DIRECTORY cmake/ DESTINATION lib/cmake/simdb)

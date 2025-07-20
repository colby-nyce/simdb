cmake_minimum_required(VERSION 3.19)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_DEBUG "-U_FORTIFY_SOURCE -O0 -g3")

project(simdb LANGUAGES CXX)
add_library(simdb INTERFACE)

find_package(SQLite3 3.19 REQUIRED)
message(STATUS "Using SQLite3 ${SQLite3_VERSION}")

find_package(ZLIB REQUIRED)
message(STATUS "Using zlib ${ZLIB_VERSION_STRING}")

set(SIMDB_BASE ${CMAKE_CURRENT_SOURCE_DIR})
option(SIMDB_PEDANTIC "Enable build with -Wall -Wextra -Werror" ON)

if(SIMDB_PEDANTIC)
    target_compile_options(simdb INTERFACE
        $<$<CXX_COMPILER_ID:GNU>:-Wall -Wextra -Werror>
        $<$<CXX_COMPILER_ID:Clang>:-Wall -Wextra -Werror>
    )
endif()

target_include_directories(simdb INTERFACE
    $<BUILD_INTERFACE:${SIMDB_BASE}/include>
    $<INSTALL_INTERFACE:include>
)

target_include_directories(simdb SYSTEM INTERFACE
    ${SQLite3_INCLUDE_DIRS}
    ${ZLIB_INCLUDE_DIRS}
)

target_link_libraries(simdb INTERFACE
    sqlite3 ZLIB::ZLIB pthread
)

add_subdirectory(test EXCLUDE_FROM_ALL)
add_subdirectory(examples EXCLUDE_FROM_ALL)

install(DIRECTORY include/simdb/ DESTINATION include/simdb)
install(DIRECTORY cmake/ DESTINATION lib/cmake/simdb)

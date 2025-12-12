cmake_minimum_required(VERSION 3.19)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_DEBUG "-U_FORTIFY_SOURCE -O0 -g3")

project(simdb LANGUAGES CXX)
add_library(simdb INTERFACE)

find_package(SQLite3 3.19 REQUIRED)
message(STATUS "Using SQLite3 ${SQLite3_VERSION}")

find_package(ZLIB REQUIRED)
message(STATUS "Using zlib ${ZLIB_VERSION_STRING}")

find_package(Threads REQUIRED)

set(SIMDB_BASE ${CMAKE_CURRENT_SOURCE_DIR})
option(SIMDB_PEDANTIC "Enable build with -Wall -Wextra -Werror" ON)

if(SIMDB_PEDANTIC)
    target_compile_options(simdb INTERFACE
        $<$<CXX_COMPILER_ID:GNU>:-Wall -Wextra -Werror>
        $<$<CXX_COMPILER_ID:Clang>:-Wall -Wextra -Werror>
    )
endif()

target_include_directories(simdb INTERFACE
    $<INSTALL_INTERFACE:include>
)

target_include_directories(simdb SYSTEM INTERFACE
    $<BUILD_INTERFACE:${SIMDB_BASE}/include>
    ${SQLite3_INCLUDE_DIRS}
    ${ZLIB_INCLUDE_DIRS}
)

target_link_libraries(simdb INTERFACE
    sqlite3 ZLIB::ZLIB Threads::Threads
)

include(CTest)
add_custom_target(simdb_regress)
add_custom_command(
    TARGET simdb_regress 
    POST_BUILD COMMAND ctest -LE --test-action test)

macro(simdb_test target file)
    project(${target})
    add_executable(${target} ${file})
    add_test(NAME ${target}_test COMMAND $<TARGET_FILE:${target}>)
    add_dependencies(simdb_regress ${target})
    target_link_libraries(${target} PRIVATE simdb)
    target_include_directories(${target} PRIVATE ${SIMDB_BASE}/test ${SIMDB_BASE}/examples)
endmacro()

add_subdirectory(test EXCLUDE_FROM_ALL)
add_subdirectory(examples EXCLUDE_FROM_ALL)

install(DIRECTORY include/simdb/ DESTINATION include/simdb)

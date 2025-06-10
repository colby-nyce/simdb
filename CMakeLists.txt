cmake_minimum_required(VERSION 3.19)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_CXX_FLAGS_DEBUG "-U_FORTIFY_SOURCE -O0 -g3")
project(simdb CXX)

set(SIMDB_BASE ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${SIMDB_BASE}/include)
include(${SIMDB_BASE}/cmake/simdb-config.cmake)

# Create include/simdb/3p if it doesn't exist
file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/include/simdb/3p)

# Copy the third-party headers to include/simdb/3p
file(GLOB CONCURRENTQUEUE_HEADERS ${PROJECT_SOURCE_DIR}/concurrentqueue/*.h)
foreach(hdr ${CONCURRENTQUEUE_HEADERS})
    get_filename_component(name ${hdr} NAME)
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${hdr}
            ${PROJECT_SOURCE_DIR}/include/simdb/3p/${name}
    )
endforeach()


add_subdirectory(test EXCLUDE_FROM_ALL)

#
# Installation
#
install(DIRECTORY include/simdb/ DESTINATION include/simdb)
install(DIRECTORY cmake/ DESTINATION lib/cmake/simdb)

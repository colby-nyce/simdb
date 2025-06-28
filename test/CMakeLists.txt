project(simdb_test)
include(CTest)
add_custom_target(simdb_regress)
add_custom_command(
    TARGET simdb_regress 
    POST_BUILD COMMAND ctest -LE --test-action test)

macro(simdb_test target)
    project(${target})
    add_executable(${target} main.cpp)
    add_test(NAME ${target}_test COMMAND $<TARGET_FILE:${target}>)
    add_dependencies(simdb_regress ${target})
    target_link_libraries(${target} simdb)
    target_include_directories(${target} PRIVATE ${SIMDB_BASE}/test)
endmacro()

add_subdirectory(SQLite)
add_subdirectory(TBB)

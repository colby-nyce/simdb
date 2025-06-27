project(simdb_test)
include(CTest)
add_custom_target(simdb_regress)
add_custom_command(
    TARGET simdb_regress 
    POST_BUILD COMMAND ctest -LE --test-action test)

macro(simdb_test target)
    set(PROJECT ${target})
    project(${PROJECT})
    add_executable(${PROJECT} main.cpp)
    add_test(NAME ${PROJECT}_test COMMAND $<TARGET_FILE:${PROJECT}>)
    add_dependencies(simdb_regress ${PROJECT})
    target_link_libraries(${PROJECT} simdb)
endmacro()

add_subdirectory(SQLite)
add_subdirectory(Pipeline)
add_subdirectory(TBB)

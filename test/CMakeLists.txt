project(simdb_test)
include(CTest)
add_custom_target(simdb_regress)
add_custom_command(
    TARGET simdb_regress 
    POST_BUILD COMMAND ctest -LE --test-action test)

add_subdirectory(apps)
add_subdirectory(SQLiteBasic)
add_subdirectory(SQLiteAdvanced)
add_subdirectory(Pipeline)

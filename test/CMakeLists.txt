project(simdb_test)
include(CTest)
add_custom_target(simdb_regress)
add_custom_command(
    TARGET simdb_regress 
    POST_BUILD COMMAND ctest -LE --test-action test)

add_subdirectory(CollectionMgr)
add_subdirectory(SQLiteBasic)

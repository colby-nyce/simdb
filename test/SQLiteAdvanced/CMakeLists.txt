project(SQLiteAdvanced_test)
add_executable(SQLiteAdvanced_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(SQLiteAdvanced_test SQLiteAdvanced_test_RUN)

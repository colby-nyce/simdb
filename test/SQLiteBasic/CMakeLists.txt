project(SQLiteBasic_test)
add_executable(SQLiteBasic_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(SQLiteBasic_test SQLiteBasic_test_RUN)

project(SQLite_test)
add_executable(SQLite_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(SQLite_test SQLite_test_RUN)

project(ArgosCollection_test)
add_executable(ArgosCollection_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(ArgosCollection_test ArgosCollection_test_RUN)

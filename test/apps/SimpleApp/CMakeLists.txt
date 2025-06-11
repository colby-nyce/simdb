project(SimpleApp_test)
add_executable(SimpleApp_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(SimpleApp_test SimpleApp_test_RUN)

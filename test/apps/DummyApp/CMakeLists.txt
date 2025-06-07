project(DummyApp_test)
add_executable(DummyApp_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(DummyApp_test DummyApp_test_RUN)

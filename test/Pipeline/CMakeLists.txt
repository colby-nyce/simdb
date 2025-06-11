project(Pipeline_test)
add_executable(Pipeline_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(Pipeline_test Pipeline_test_RUN)

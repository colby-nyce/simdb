project(UniformSerializer_test)
add_executable(UniformSerializer_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(UniformSerializer_test UniformSerializer_test_RUN)

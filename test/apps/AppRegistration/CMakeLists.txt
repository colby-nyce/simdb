project(AppRegistration_test)
add_executable(AppRegistration_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(AppRegistration_test AppRegistration_test_RUN)

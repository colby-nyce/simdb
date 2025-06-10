project(UnifiedSerializer_test)
add_executable(UnifiedSerializer_test main.cpp)
include(../../TestingMacros.cmake)
simdb_test(UnifiedSerializer_test UnifiedSerializer_test_RUN)

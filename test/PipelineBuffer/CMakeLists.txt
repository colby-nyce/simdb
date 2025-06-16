project(PipelineBuffer_test)
add_executable(PipelineBuffer_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(PipelineBuffer_test PipelineBuffer_test_RUN)

simdb_test(PipelineSnoopers main.cpp)
find_package(Boost REQUIRED COMPONENTS serialization)
target_link_libraries(PipelineSnoopers PRIVATE Boost::serialization)

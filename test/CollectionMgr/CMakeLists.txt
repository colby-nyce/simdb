project(CollectionMgr_test)
add_executable(CollectionMgr_test main.cpp)
include(../TestingMacros.cmake)
simdb_test(CollectionMgr_test CollectionMgr_test_RUN)

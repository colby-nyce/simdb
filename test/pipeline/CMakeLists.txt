add_subdirectory(pipeline_cache)
add_subdirectory(dot_product)
add_subdirectory(elements)
add_subdirectory(shared_threads)

#TODO cnyce: re-enable test
#if(TBB_VERSION)
#    add_subdirectory(TBB)
#endif()

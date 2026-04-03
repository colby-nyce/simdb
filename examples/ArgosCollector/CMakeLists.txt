simdb_test(ArgosCollector main.cpp)

find_package(RapidJSON CONFIG QUIET)
if(RapidJSON_FOUND)
    if(TARGET RapidJSON)
        target_link_libraries(ArgosCollector PRIVATE RapidJSON)
    elseif(TARGET rapidjson)
        target_link_libraries(ArgosCollector PRIVATE rapidjson)
    else()
        target_include_directories(ArgosCollector PRIVATE ${RapidJSON_INCLUDE_DIRS})
    endif()
    target_compile_definitions(ArgosCollector PRIVATE RAPIDJSON_INSTALLED=1)
    message(STATUS "ArgosCollector: RapidJSON found; JSON dump in RunFrontEndValidation enabled")
else()
    message(STATUS "ArgosCollector: RapidJSON not found; RunFrontEndValidation will warn and skip")
endif()

add_custom_command(
    TARGET ArgosCollector
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E rm -f $<TARGET_FILE_DIR:ArgosCollector>/validate.py
    COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${CMAKE_CURRENT_SOURCE_DIR}/validate.py
            $<TARGET_FILE_DIR:ArgosCollector>/validate.py
    COMMENT "Symlink validate.py next to ArgosCollector"
    VERBATIM)

set_tests_properties(ArgosCollector_test PROPERTIES WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

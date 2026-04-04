simdb_test(ArgosCollector main.cpp)

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

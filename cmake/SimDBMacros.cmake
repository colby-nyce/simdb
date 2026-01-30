function(add_simdb_app_library target)
    add_library(${target} SHARED ${ARGN})

    target_link_libraries(${target}
        PUBLIC
            SimDB::simdb
    )

    install(TARGETS ${target}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}  # .so (Linux) / .dylib (macOS)
    )
endfunction()

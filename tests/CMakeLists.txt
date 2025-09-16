file(GLOB TEST_FILES PRIVATE *.cpp)

foreach (file ${TEST_FILES})
    get_filename_component(basename ${file} NAME_WE)
    add_executable(${basename} ${file} ${SRC_FILES})
    target_link_libraries(${basename} ${DEPEND_LIBS})
    add_test(NAME ${basename} COMMAND ${basename})
endforeach ()
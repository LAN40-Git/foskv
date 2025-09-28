file(GLOB EXAMPLE_SRCS PRIVATE *.cpp)

foreach (source ${EXAMPLE_SRCS})
    get_filename_component(basename ${source} NAME_WE)
    add_executable(${basename} ${source} ${FOSKV_SRCS})
    target_link_libraries(${basename} ${DEPEND_LIBS})
    add_test(NAME ${basename} COMMAND ${basename})
endforeach ()
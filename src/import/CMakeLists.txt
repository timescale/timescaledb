set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/utils/adt/selfuncs.c
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer/path/allpaths.c
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer/plan/createplan.c
    ${CMAKE_CURRENT_SOURCE_DIR}/heapswap.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nodes/list.c
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer/util/plancat.c
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer/plan/setrefs.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ts_explain.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ts_inherit.c)

# Disable clang-tidy for imported code
add_library(src_import_no_static_code_analysis OBJECT ${SOURCES})
set_target_properties(src_import_no_static_code_analysis PROPERTIES C_CLANG_TIDY
                                                                    "")
target_sources(${PROJECT_NAME}
               PRIVATE $<TARGET_OBJECTS:src_import_no_static_code_analysis>)

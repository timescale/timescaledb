set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/gapfill_functions.c
    ${CMAKE_CURRENT_SOURCE_DIR}/gapfill_plan.c
    ${CMAKE_CURRENT_SOURCE_DIR}/gapfill_exec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/locf.c
    ${CMAKE_CURRENT_SOURCE_DIR}/interpolate.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

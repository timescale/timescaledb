set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/exec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/functions.c
    ${CMAKE_CURRENT_SOURCE_DIR}/plan.c
    ${CMAKE_CURRENT_SOURCE_DIR}/grouping_policy_all.c
    ${CMAKE_CURRENT_SOURCE_DIR}/grouping_policy_segmentby.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

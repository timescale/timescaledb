set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/continuous_aggregate_api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/job.c
    ${CMAKE_CURRENT_SOURCE_DIR}/job_api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/reorder_api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/retention_api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/policy_utils.c
    ${CMAKE_CURRENT_SOURCE_DIR}/policies_v2.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
target_include_directories(${TSL_LIBRARY_NAME} PRIVATE ${CMAKE_SOURCE_DIR})

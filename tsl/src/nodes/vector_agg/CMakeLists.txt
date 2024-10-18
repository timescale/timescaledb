add_subdirectory(function)
set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/exec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_single_fixed_2.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_single_fixed_4.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_single_fixed_8.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_single_text.c
    ${CMAKE_CURRENT_SOURCE_DIR}/grouping_policy_batch.c
    ${CMAKE_CURRENT_SOURCE_DIR}/grouping_policy_hash.c
    ${CMAKE_CURRENT_SOURCE_DIR}/plan.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

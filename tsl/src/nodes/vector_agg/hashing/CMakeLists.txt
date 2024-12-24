set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_strategy_single_fixed_2.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_strategy_single_fixed_4.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_strategy_single_fixed_8.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_strategy_common.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

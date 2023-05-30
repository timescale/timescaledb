set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/common.c
    ${CMAKE_CURRENT_SOURCE_DIR}/finalize.c
    ${CMAKE_CURRENT_SOURCE_DIR}/create.c
    ${CMAKE_CURRENT_SOURCE_DIR}/insert.c
    ${CMAKE_CURRENT_SOURCE_DIR}/materialize.c
    ${CMAKE_CURRENT_SOURCE_DIR}/options.c
    ${CMAKE_CURRENT_SOURCE_DIR}/refresh.c
    ${CMAKE_CURRENT_SOURCE_DIR}/repair.c
    ${CMAKE_CURRENT_SOURCE_DIR}/invalidation.c
    ${CMAKE_CURRENT_SOURCE_DIR}/invalidation_threshold.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

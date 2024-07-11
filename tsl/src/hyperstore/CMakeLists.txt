set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/arrow_tts.c
    ${CMAKE_CURRENT_SOURCE_DIR}/arrow_cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/arrow_array.c
    ${CMAKE_CURRENT_SOURCE_DIR}/arrow_cache_explain.c
    ${CMAKE_CURRENT_SOURCE_DIR}/attr_capture.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hyperstore_handler.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hsproxy.c
    ${CMAKE_CURRENT_SOURCE_DIR}/relstats.c
    ${CMAKE_CURRENT_SOURCE_DIR}/utils.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

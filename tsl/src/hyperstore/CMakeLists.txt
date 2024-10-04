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
if(PG_VERSION VERSION_GREATER_EQUAL "17.0")
  list(APPEND SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/import/analyze.c)
endif()
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

set(SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/ts_observ_agg.c
            ${CMAKE_CURRENT_SOURCE_DIR}/ts_observ_keys.c
            ${CMAKE_CURRENT_SOURCE_DIR}/ts_observ_segment.c
            ${CMAKE_CURRENT_SOURCE_DIR}/ts_observ_srf.c
            ${CMAKE_CURRENT_SOURCE_DIR}/ts_observ_write.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})

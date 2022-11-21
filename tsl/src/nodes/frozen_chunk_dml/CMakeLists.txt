# Add all *.c to sources in upperlevel directory
set(SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/frozen_chunk_dml.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

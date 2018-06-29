# Add all *.c to sources in upperlevel directory
set(SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/conn.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/http_request.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/http_response.c"
)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})

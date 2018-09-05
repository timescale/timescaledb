set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/conn.c
  ${CMAKE_CURRENT_SOURCE_DIR}/conn_plain.c
  ${CMAKE_CURRENT_SOURCE_DIR}/utils.c
  ${CMAKE_CURRENT_SOURCE_DIR}/http.c
  ${CMAKE_CURRENT_SOURCE_DIR}/http_response.c
  ${CMAKE_CURRENT_SOURCE_DIR}/http_request.c
)

if (USE_OPENSSL)
  list(APPEND SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/conn_ssl.c)
  target_include_directories(${PROJECT_NAME} PUBLIC ${OPENSSL_INCLUDE_DIR})
  target_link_libraries(${PROJECT_NAME} ${OPENSSL_LIBRARIES})
endif (USE_OPENSSL)

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})

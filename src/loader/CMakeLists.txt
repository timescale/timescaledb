set(HEADERS
  bgw_message_queue.h
  bgw_counter.h
  bgw_launcher.h
  bgw_interface.h
  loader.h
  ../compat.h
  ../extension_constants.h
)

set(SOURCES
  loader.c
  bgw_message_queue.c
  bgw_counter.c
  bgw_launcher.c
  bgw_interface.c)

add_library(${PROJECT_NAME}-loader MODULE ${SOURCES} ${HEADERS})

set_target_properties(${PROJECT_NAME}-loader PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}
    PREFIX "")

install(
  TARGETS ${PROJECT_NAME}-loader
  DESTINATION ${PG_PKGLIBDIR})

if(CMAKE_BUILD_TYPE MATCHES Debug)
  add_subdirectory(../../test/loader-mock/ "${CMAKE_CURRENT_BINARY_DIR}/mock")
endif(CMAKE_BUILD_TYPE MATCHES Debug)

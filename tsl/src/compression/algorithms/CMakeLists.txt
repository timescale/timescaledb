set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/array.c
    ${CMAKE_CURRENT_SOURCE_DIR}/datum_serialize.c
    ${CMAKE_CURRENT_SOURCE_DIR}/deltadelta.c
    ${CMAKE_CURRENT_SOURCE_DIR}/dictionary.c
    ${CMAKE_CURRENT_SOURCE_DIR}/gorilla.c
    ${CMAKE_CURRENT_SOURCE_DIR}/bool_compress.c
    ${CMAKE_CURRENT_SOURCE_DIR}/null.c
    ${CMAKE_CURRENT_SOURCE_DIR}/uuid_compress.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

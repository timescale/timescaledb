set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_ffor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_ffor_8.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_ffor_16.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_ffor_32.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_ffor_64.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_ffor_128.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_ffor_256.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_pack.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_pack_8.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_pack_16.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_pack_32.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_pack_64.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_pack_128.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_pack_256.c
    ${CMAKE_CURRENT_SOURCE_DIR}/fastlanes_sizing.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

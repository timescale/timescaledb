set(SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_privacy.c
            ${CMAKE_CURRENT_SOURCE_DIR}/test_telemetry.c)

target_sources(${TESTS_LIB_NAME} PRIVATE ${SOURCES})

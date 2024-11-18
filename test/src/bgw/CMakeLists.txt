set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/log.c
    ${CMAKE_CURRENT_SOURCE_DIR}/timer_mock.c
    ${CMAKE_CURRENT_SOURCE_DIR}/scheduler_mock.c
    ${CMAKE_CURRENT_SOURCE_DIR}/params.c
    ${CMAKE_CURRENT_SOURCE_DIR}/test_job_refresh.c
    ${CMAKE_CURRENT_SOURCE_DIR}/test_job_utils.c)

target_sources(${TESTS_LIB_NAME} PRIVATE ${SOURCES})

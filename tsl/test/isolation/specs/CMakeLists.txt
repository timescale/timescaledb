## these tests need MODULE_NAME and cannot be added as is to TEST_FILES
set(TEST_TEMPLATES_MODULE
  reorder_deadlock.spec.in
  reorder_vs_insert_other_chunk.spec.in
)

set(TEST_TEMPLATES_MODULE_DEBUG
  reorder_vs_insert.spec.in
  reorder_vs_select.spec.in
)

list(APPEND TEST_FILES
  continuous_aggs_insert.spec
  continuous_aggs_multi.spec
)

if (${PG_VERSION_MAJOR} GREATER "9")
  list(APPEND TEST_FILES
    compression_ddl.spec
  )
endif ()

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES_MODULE ${TEST_TEMPLATES_MODULE_DEBUG})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

##need to generate MODULE name for the .spec files
foreach(TEMP_FILE ${TEST_TEMPLATES_MODULE})
  get_filename_component(TEMPLATE ${TEMP_FILE} NAME_WE)
  set(TEST_FILE ${TEMPLATE}.spec)
  configure_file(${TEMP_FILE} ${TEST_FILE})
  list(APPEND TEST_FILES "${TEST_FILE}")
endforeach(TEMP_FILE)

##generate PG version specific .spec file
foreach(TEMPLATE_FILE ${TEST_TEMPLATES})
  get_filename_component(TEMPLATE ${TEMPLATE_FILE} NAME_WE)
  set(TEST_FILE ${TEMPLATE}-${TEST_VERSION_SUFFIX}.spec)
  configure_file(${TEMPLATE_FILE} ${TEST_FILE})
  list(APPEND TEST_FILES "${TEST_FILE}")
endforeach(TEMPLATE_FILE)

file(REMOVE ${ISOLATION_TEST_SCHEDULE})

list(SORT TEST_FILES)

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.spec" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${ISOLATION_TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)

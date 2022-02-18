set(TEST_FILES
    deadlock_dropchunks_select.spec
    insert_dropchunks_race.spec
    isolation_nop.spec
    read_committed_insert.spec
    read_uncommitted_insert.spec
    repeatable_read_insert.spec
    serializable_insert_rollback.spec
    serializable_insert.spec)

file(REMOVE ${ISOLATION_TEST_SCHEDULE})

set(TEST_TEMPLATES)

set(TEST_TEMPLATES_DEBUG
    dropchunks_race.spec.in multi_transaction_indexing.spec.in
    concurrent_query_and_drop_chunks.spec.in)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES ${TEST_TEMPLATES_DEBUG})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

foreach(TEMPLATE_FILE ${TEST_TEMPLATES})
  get_filename_component(TEMPLATE ${TEMPLATE_FILE} NAME_WE)
  set(TEST_FILE ${TEMPLATE}.spec)
  configure_file(${TEMPLATE_FILE} ${TEST_FILE})
  list(APPEND TEST_FILES "${TEST_FILE}")
endforeach(TEMPLATE_FILE)

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.spec" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${ISOLATION_TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)

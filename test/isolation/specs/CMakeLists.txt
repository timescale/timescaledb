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

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES concurrent_add_dimension.spec
       concurrent_query_and_drop_chunks.spec dropchunks_race.spec
       multi_transaction_indexing.spec)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.spec" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${ISOLATION_TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)

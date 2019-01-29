
set(TEST_FILES
    deadlock_dropchunks_select.spec
    isolation_nop.spec
    read_committed_insert.spec
    read_uncommitted_insert.spec
    repeatable_read_insert.spec
    serializable_insert_rollback.spec
    serializable_insert.spec)

file(REMOVE ${ISOLATION_TEST_SCHEDULE})

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES
    multi_transaction_indexing.spec)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

foreach(TEST_FILE ${TEST_FILES})
    string(REGEX REPLACE "(.+)\.spec" "\\1" TESTS_TO_RUN ${TEST_FILE})
    file(APPEND ${ISOLATION_TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)

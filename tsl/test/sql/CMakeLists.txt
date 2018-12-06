set(TEST_FILES
	bgw_policy.sql
    edition.sql
    tsl_tables.sql
    gapfill.sql
    plan_gapfill.sql
)

list(SORT TEST_FILES)
file(REMOVE ${TEST_SCHEDULE})

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.sql" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)

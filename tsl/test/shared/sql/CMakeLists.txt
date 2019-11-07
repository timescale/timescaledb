set(TEST_FILES_SHARED
  constraint_exclusion_prepared.sql
)

set(TEST_TEMPLATES_SHARED
  ordered_append.sql.in
)

# Regression tests that vary with PostgreSQL version. Generated test
# files are put in the original source directory since all tests must
# be in the same directory. These files are updated when the template
# is edited, but not when the output file is deleted. If the output is
# deleted either recreate it manually, or rerun cmake on the root dir.
foreach(TEMPLATE_FILE ${TEST_TEMPLATES_SHARED})
  string(LENGTH ${TEMPLATE_FILE} TEMPLATE_NAME_LEN)
  math(EXPR TEMPLATE_NAME_LEN ${TEMPLATE_NAME_LEN}-7)
  string(SUBSTRING ${TEMPLATE_FILE} 0 ${TEMPLATE_NAME_LEN} TEMPLATE)
  set(TEST_FILE ${TEMPLATE}-${TEST_VERSION_SUFFIX}.sql)
  configure_file(${TEMPLATE_FILE} ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_FILE} COPYONLY)
  list(APPEND TEST_FILES_SHARED ${TEST_FILE})
endforeach(TEMPLATE_FILE)

if (TEST_FILES_SHARED)
  list(SORT TEST_FILES_SHARED)
endif()
file(REMOVE ${TEST_SCHEDULE_SHARED})

foreach(TEST_FILE ${TEST_FILES_SHARED})
  string(REGEX REPLACE "(.+)\.sql" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${TEST_SCHEDULE_SHARED} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)

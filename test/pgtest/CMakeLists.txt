set(PG_REGRESS_DIR
    ${PG_SOURCE_DIR}/src/test/regress
    CACHE PATH "Path to PostgreSQL's regress directory")

set(PGTEST_DIRS ${PG_REGRESS_DIR}/data ${PG_REGRESS_DIR}/sql
                ${PG_REGRESS_DIR}/expected)

# Copy the test files from PostgreSQL's test suite. The test suite generates
# some SQL scripts and output files from template source files and require
# directories to be colocated
file(COPY ${PGTEST_DIRS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

file(READ ${PG_REGRESS_DIR}/parallel_schedule PG_TEST_SCHEDULE)

# create directory for tablespace test
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testtablespace)

# Tests to ignore
set(PG_IGNORE_TESTS
    advisory_lock
    amutils
    database
    event_trigger
    jsonb_jsonpath
    opr_sanity
    sanity_check
    type_sanity
    create_am
    # Ignoring because it spawns different number of workers in different
    # versions.
    select_parallel
    psql)

# Modify the test schedule to ignore some tests
foreach(IGNORE_TEST ${PG_IGNORE_TESTS})
  # pg_regress dropped the ignore feature in PG16, so drop the test from the
  # schedule instead
  # https://github.com/postgres/postgres/commit/bd8d453e9b5f8b632a400a9e796fc041aed76d82
  string(REPLACE "test: ${IGNORE_TEST}\n" "" PG_TEST_SCHEDULE
                 "${PG_TEST_SCHEDULE}")
  string(REPLACE " ${IGNORE_TEST} " " " PG_TEST_SCHEDULE "${PG_TEST_SCHEDULE}")
  string(REPLACE " ${IGNORE_TEST}\n" "\n" PG_TEST_SCHEDULE
                 "${PG_TEST_SCHEDULE}")
endforeach(IGNORE_TEST)

# Write the final test schedule
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/schedule ${PG_TEST_SCHEDULE})

set(PG_REGRESS_OPTS_PGTEST --schedule=${CMAKE_CURRENT_BINARY_DIR}/schedule
                           --load-extension=timescaledb)

add_custom_target(
  pginstallcheck
  COMMAND ${PG_REGRESS} ${PG_REGRESS_OPTS_BASE} ${PG_REGRESS_OPTS_PGTEST}
          ${PG_REGRESS_OPTS_TEMP_INSTANCE_PGTEST}
  USES_TERMINAL)

add_custom_target(
  pginstallchecklocal
  COMMAND ${PG_REGRESS} ${PG_REGRESS_OPTS_BASE} ${PG_REGRESS_OPTS_PGTEST}
          ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
  USES_TERMINAL)

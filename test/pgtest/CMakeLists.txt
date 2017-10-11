set(PG_REGRESS_DIR
  ${PG_SOURCE_DIR}/src/test/regress
  CACHE PATH
  "Path to PostgreSQL's regress directory")

# Copy the input and output files from PostgreSQL's test suite. The
# test suite generates some SQL scripts and output files from template
# source files and require directories to be colocated
file(COPY
  ${PG_REGRESS_DIR}/data
  ${PG_REGRESS_DIR}/input
  ${PG_REGRESS_DIR}/output
  ${PG_REGRESS_DIR}/sql
  ${PG_REGRESS_DIR}/expected
  DESTINATION
  ${CMAKE_CURRENT_BINARY_DIR})

file(READ
  ${PG_REGRESS_DIR}/serial_schedule
  PG_TEST_SCHEDULE)

# Tests to ignore
set(PG_IGNORE_TESTS
  tablespace
  opr_sanity
  sanity_check)

# Modify the test schedule to ignore some tests
foreach(IGNORE_TEST ${PG_IGNORE_TESTS})
  string(REPLACE
    "test: ${IGNORE_TEST}" "ignore: ${IGNORE_TEST}\ntest: ${IGNORE_TEST}"
    PG_TEST_SCHEDULE
    ${PG_TEST_SCHEDULE})
endforeach(IGNORE_TEST)

# Write the final test schedule
file(WRITE
  ${CMAKE_CURRENT_BINARY_DIR}/serial_schedule
  ${PG_TEST_SCHEDULE})

# Need --dlpath set to PostgreSQL's test directory so that the tests
# can load libraries there
set(PG_REGRESS_OPTS_PGTEST
  --schedule=${CMAKE_CURRENT_BINARY_DIR}/serial_schedule
  --load-extension=timescaledb
  --dlpath=${PG_REGRESS_DIR})

add_custom_target(pginstallcheck
  COMMAND
  ${PG_REGRESS}
  ${PG_REGRESS_OPTS_BASE}
  ${PG_REGRESS_OPTS_PGTEST}
  ${PG_REGRESS_OPTS_TEMP_INSTANCE}
  USES_TERMINAL)

add_custom_target(pginstallchecklocal
  COMMAND
  ${PG_REGRESS}
  ${PG_REGRESS_OPTS_BASE}
  ${PG_REGRESS_OPTS_PGTEST}
  ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
  USES_TERMINAL)

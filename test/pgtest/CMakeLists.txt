set(PG_REGRESS_DIR
    ${PG_SOURCE_DIR}/src/test/regress
    CACHE PATH "Path to PostgreSQL's regress directory")

# input and output directory got removed in PG15
set(PGTEST_DIRS ${PG_REGRESS_DIR}/data ${PG_REGRESS_DIR}/sql
                ${PG_REGRESS_DIR}/expected)
if(EXISTS ${PG_REGRESS_DIR}/input AND EXISTS ${PG_REGRESS_DIR}/output)
  list(APPEND PGTEST_DIRS ${PG_REGRESS_DIR}/input ${PG_REGRESS_DIR}/output)
endif()

# Copy the input and output files from PostgreSQL's test suite. The test suite
# generates some SQL scripts and output files from template source files and
# require directories to be colocated
file(COPY ${PGTEST_DIRS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

file(READ ${PG_REGRESS_DIR}/parallel_schedule PG_TEST_SCHEDULE)

# create directory for tablespace test
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testtablespace)

# Tests to ignore
set(PG_IGNORE_TESTS
    advisory_lock
    event_trigger
    foreign_data
    identity
    opr_sanity
    rolenames
    rules
    sanity_check
    security_label
    type_sanity)

# Modify the test schedule to ignore some tests
foreach(IGNORE_TEST ${PG_IGNORE_TESTS})
  string(CONCAT PG_TEST_SCHEDULE "ignore: ${IGNORE_TEST}\n" ${PG_TEST_SCHEDULE})
endforeach(IGNORE_TEST)

# Write the final test schedule
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/schedule ${PG_TEST_SCHEDULE})

# Need --dlpath set to PostgreSQL's test directory so that the tests can load
# libraries there
set(PG_REGRESS_OPTS_PGTEST
    --schedule=${CMAKE_CURRENT_BINARY_DIR}/schedule
    --load-extension=timescaledb --dlpath=${PG_REGRESS_DIR})

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

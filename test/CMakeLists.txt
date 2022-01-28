set(PRIMARY_TEST_DIR ${CMAKE_CURRENT_LIST_DIR})
set(PRIMARY_TEST_DIR
    ${CMAKE_CURRENT_LIST_DIR}
    PARENT_SCOPE)

set(_local_install_checks)
set(_install_checks)

# Testing support
include(test-defs.cmake)

# No checks for REGRESS_CHECKS needed here since all the checks are done in the
# parent CMakeLists.txt.

if(PG_REGRESS)
  message(STATUS "Using pg_regress ${PG_REGRESS}")

  add_custom_target(
    regresscheck
    COMMAND
      ${CMAKE_COMMAND} -E env ${PG_REGRESS_ENV}
      TEST_PGPORT=${TEST_PGPORT_TEMP_INSTANCE} TEST_SCHEDULE=${TEST_SCHEDULE}
      ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh ${PG_REGRESS_OPTS_BASE}
      ${PG_REGRESS_OPTS_EXTRA} ${PG_REGRESS_OPTS_INOUT}
      ${PG_REGRESS_OPTS_TEMP_INSTANCE}
      --temp-config=${TEST_OUTPUT_DIR}/postgresql.conf
    USES_TERMINAL)

  add_custom_target(
    regresschecklocal
    COMMAND
      ${CMAKE_COMMAND} -E env ${PG_REGRESS_ENV} TEST_PGPORT=${TEST_PGPORT_LOCAL}
      ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh ${PG_REGRESS_OPTS_BASE}
      ${PG_REGRESS_OPTS_EXTRA} ${PG_REGRESS_OPTS_INOUT}
      ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
    USES_TERMINAL)

  list(APPEND _local_install_checks regresschecklocal)
  list(APPEND _install_checks regresscheck)
elseif(REQUIRE_ALL_TESTS)
  message(
    FATAL_ERROR "All tests were required but 'pg_regress' could not be found")
endif()

if(PG_ISOLATION_REGRESS)
  message(STATUS "Using pg_isolation_regress ${PG_ISOLATION_REGRESS}")

  add_custom_target(
    isolationcheck
    COMMAND
      ${CMAKE_COMMAND} -E env ${PG_ISOLATION_REGRESS_ENV}
      SPECS_DIR=${CMAKE_CURRENT_SOURCE_DIR}/isolation/specs
      TEST_PGPORT=${TEST_PGPORT_TEMP_INSTANCE}
      ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh ${PG_REGRESS_OPTS_BASE}
      ${PG_ISOLATION_REGRESS_OPTS_EXTRA} ${PG_ISOLATION_REGRESS_OPTS_INOUT}
      ${PG_REGRESS_OPTS_TEMP_INSTANCE}
      --temp-config=${TEST_OUTPUT_DIR}/postgresql.conf
    USES_TERMINAL)

  add_custom_target(
    isolationchecklocal
    COMMAND
      ${CMAKE_COMMAND} -E env ${PG_ISOLATION_REGRESS_ENV}
      SPECS_DIR=${CMAKE_CURRENT_SOURCE_DIR}/isolation/specs
      TEST_PGPORT=${TEST_PGPORT_LOCAL} ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh
      ${PG_REGRESS_OPTS_BASE} ${PG_ISOLATION_REGRESS_OPTS_EXTRA}
      ${PG_ISOLATION_REGRESS_OPTS_INOUT} ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
    USES_TERMINAL)

  list(APPEND _local_install_checks isolationchecklocal)
  list(APPEND _install_checks isolationcheck)
elseif(REQUIRE_ALL_TESTS)
  message(
    FATAL_ERROR
      "All tests were required but 'pg_isolation_regress' could not be found")
endif()

# We add the installcheck target even when _install_checks is empty as tsl code
# might add dependencies to it even when regress checks are disabled.
add_custom_target(installcheck DEPENDS ${_install_checks})

# Define a post test hook that is invoked after the installcheck target
# finishes. One can use add_dependencies on post hook target to run other
# targets after tests complete. This is used, e.g., by code coverage.
add_custom_target(installcheck-post-hook COMMENT "Post test hook")
add_custom_command(
  TARGET installcheck
  POST_BUILD
  COMMAND cmake --build ${CMAKE_CURRENT_BINARY_DIR} --target
          installcheck-post-hook)

# installchecklocal tests against an existing postgres instance
add_custom_target(installchecklocal DEPENDS ${_local_install_checks})

add_subdirectory(sql)
add_subdirectory(isolation)

if(PG_SOURCE_DIR)
  add_subdirectory(pgtest)
endif(PG_SOURCE_DIR)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  add_subdirectory(src)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

add_subdirectory(perl)

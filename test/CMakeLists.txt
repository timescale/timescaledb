# Testing support
find_program(PG_REGRESS pg_regress
  HINTS
  "${PG_PKGLIBDIR}/pgxs/src/test/regress/")

message(STATUS "Using pg_regress ${PG_REGRESS}")

find_program(PG_ISOLATION_REGRESS pg_isolation_regress
  HINTS
  ${BINDIR}/
  ${PG_PKGLIBDIR}/pgxs/src/test/isolation/
  ${PG_SOURCE_DIR}/src/test/isolation)

message(STATUS "Using pg_isolation_regress ${PG_ISOLATION_REGRESS}")

set(PRIMARY_TEST_DIR ${CMAKE_CURRENT_LIST_DIR})
set(PRIMARY_TEST_DIR ${CMAKE_CURRENT_LIST_DIR} PARENT_SCOPE)

include(test-defs.cmake)

# installcheck starts up new temporary instances for testing code
if(NOT APACHE_ONLY)
  add_custom_target(installcheck
  DEPENDS regresscheck isolationcheck installcheck-t)
else()
  add_custom_target(installcheck
    DEPENDS regresscheck isolationcheck)
endif()

add_custom_target(regresscheck
  COMMAND ${CMAKE_COMMAND} -E env
  ${PG_REGRESS_ENV}
  ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh
  ${PG_REGRESS_OPTS_BASE}
  ${PG_REGRESS_OPTS_EXTRA}
  ${PG_REGRESS_OPTS_INOUT}
  ${PG_REGRESS_OPTS_TEMP_INSTANCE}
  USES_TERMINAL)

add_custom_target(isolationcheck
  COMMAND ${CMAKE_COMMAND} -E env
  ${PG_ISOLATION_REGRESS_ENV}
  SPECS_DIR=${CMAKE_CURRENT_SOURCE_DIR}/isolation/specs
  ${CMAKE_CURRENT_SOURCE_DIR}/pg_isolation_regress.sh
  ${PG_REGRESS_OPTS_BASE}
  ${PG_ISOLATION_REGRESS_OPTS_EXTRA}
  ${PG_ISOLATION_REGRESS_OPTS_INOUT}
  ${PG_REGRESS_OPTS_TEMP_INSTANCE}
  USES_TERMINAL)

# installchecklocal tests against an existing postgres instance
add_custom_target(installchecklocal
  DEPENDS regresschecklocal isolationchecklocal)

add_custom_target(regresschecklocal
  COMMAND ${CMAKE_COMMAND} -E env
  ${PG_REGRESS_ENV}
  ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh
  ${PG_REGRESS_OPTS_BASE}
  ${PG_REGRESS_OPTS_EXTRA}
  ${PG_REGRESS_OPTS_INOUT}
  ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
  USES_TERMINAL)

add_custom_target(isolationchecklocal
  COMMAND ${CMAKE_COMMAND} -E env
  ${PG_ISOLATION_REGRESS_ENV}
  SPECS_DIR=${CMAKE_CURRENT_SOURCE_DIR}/isolation/specs
  ${CMAKE_CURRENT_SOURCE_DIR}/pg_isolation_regress.sh
  ${PG_REGRESS_OPTS_BASE}
  ${PG_ISOLATION_REGRESS_OPTS_EXTRA}
  ${PG_ISOLATION_REGRESS_OPTS_INOUT}
  ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
  USES_TERMINAL)

add_subdirectory(sql)
add_subdirectory(isolation)

if (PG_SOURCE_DIR)
  add_subdirectory(pgtest)
endif (PG_SOURCE_DIR)

if (CMAKE_BUILD_TYPE MATCHES Debug)
  add_subdirectory(src)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

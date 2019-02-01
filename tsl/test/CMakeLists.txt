include("${PRIMARY_TEST_DIR}/test-defs.cmake")

add_custom_target(installcheck-t
  DEPENDS regresscheck-t isolationcheck-t)
  #TODO after we have isolation checks isolationcheck-t)

add_custom_target(regresscheck-t
  COMMAND ${CMAKE_COMMAND} -E env
  ${PG_REGRESS_ENV}
  EXE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
  ${PRIMARY_TEST_DIR}/pg_regress.sh
  ${PG_REGRESS_OPTS_BASE}
  ${PG_REGRESS_OPTS_EXTRA}
  ${PG_REGRESS_OPTS_INOUT}
  ${PG_REGRESS_OPTS_TEMP_INSTANCE}
  USES_TERMINAL)

add_custom_target(isolationcheck-t
  COMMAND ${CMAKE_COMMAND} -E env
  ${PG_ISOLATION_REGRESS_ENV}
  EXE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
  SPECS_DIR=${CMAKE_CURRENT_BINARY_DIR}/isolation/specs
  ${PRIMARY_TEST_DIR}/pg_isolation_regress.sh
  ${PG_REGRESS_OPTS_BASE}
  ${PG_ISOLATION_REGRESS_OPTS_EXTRA}
  ${PG_ISOLATION_REGRESS_OPTS_INOUT}
  ${PG_REGRESS_OPTS_TEMP_INSTANCE}
  USES_TERMINAL)

# installchecklocal tests against an existing postgres instance
add_custom_target(installchecklocal-t
  DEPENDS regresschecklocal isolationchecklocal)

add_custom_target(regresschecklocal-t
  COMMAND ${CMAKE_COMMAND} -E env
  ${PG_REGRESS_ENV}
  EXE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
  ${PRIMARY_TEST_DIR}/pg_regress.sh
  ${PG_REGRESS_OPTS_BASE}
  ${PG_REGRESS_OPTS_EXTRA}
  ${PG_REGRESS_OPTS_INOUT}
  ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
  USES_TERMINAL)

add_custom_target(isolationchecklocal-t
  COMMAND ${CMAKE_COMMAND} -E env
  ${PG_ISOLATION_REGRESS_ENV}
  EXE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
  SPECS_DIR=${CMAKE_CURRENT_BINARY_DIR}/isolation/specs
  ${PRIMARY_TEST_DIR}/pg_isolation_regress.sh
  ${PG_REGRESS_OPTS_BASE}
  ${PG_ISOLATION_REGRESS_OPTS_EXTRA}
  ${PG_ISOLATION_REGRESS_OPTS_INOUT}
  ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
  USES_TERMINAL)

add_subdirectory(sql)
add_subdirectory(isolation)

if (CMAKE_BUILD_TYPE MATCHES Debug)
  add_subdirectory(src)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

include("${PRIMARY_TEST_DIR}/test-defs.cmake")

#TODO after we have isolation checks isolationcheck-t)

set(_local_install_checks)
set(_install_checks)

if(PG_REGRESS)
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

  list(APPEND _local_install_checks regresschecklocal-t)
  list(APPEND _install_checks regresscheck-t)

  # shared tests also provide compressed hypertables which we do not support on 9.6
  if(${PG_VERSION_MAJOR} GREATER "9")

    add_custom_target(regresscheck-shared
      COMMAND ${CMAKE_COMMAND} -E env
      ${PG_REGRESS_ENV}
      EXE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/shared
      TEST_SCHEDULE=${TEST_SCHEDULE_SHARED}
      ${PRIMARY_TEST_DIR}/pg_regress.sh
      ${PG_REGRESS_OPTS_BASE}
      ${PG_REGRESS_SHARED_OPTS_EXTRA}
      ${PG_REGRESS_SHARED_OPTS_INOUT}
      ${PG_REGRESS_OPTS_TEMP_INSTANCE}
      USES_TERMINAL)

    add_custom_target(regresschecklocal-shared
      COMMAND ${CMAKE_COMMAND} -E env
      ${PG_REGRESS_ENV}
      EXE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/shared
      TEST_SCHEDULE=${TEST_SCHEDULE_SHARED}
      ${PRIMARY_TEST_DIR}/pg_regress.sh
      ${PG_REGRESS_OPTS_BASE}
      ${PG_REGRESS_SHARED_OPTS_EXTRA}
      ${PG_REGRESS_SHARED_OPTS_INOUT}
      ${PG_REGRESS_OPTS_LOCAL_INSTANCE}
      USES_TERMINAL)

    list(APPEND _install_checks regresscheck-shared)
    list(APPEND _local_install_checks regresschecklocal-shared)
  endif()

endif()

if(PG_ISOLATION_REGRESS)
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

  list(APPEND _local_install_checks isolationchecklocal-t)
  list(APPEND _install_checks isolationcheck-t)
endif()

add_subdirectory(shared)
add_subdirectory(sql)
add_subdirectory(isolation)

# installchecklocal tests against an existing postgres instance
add_custom_target(installchecklocal-t DEPENDS ${_local_install_checks})
add_custom_target(installcheck-t DEPENDS ${_install_checks})

if (CMAKE_BUILD_TYPE MATCHES Debug)
  add_subdirectory(src)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

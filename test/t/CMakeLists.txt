set(PROVE_DEBUG_TEST_FILES)
if(${PG_VERSION_MAJOR} GREATER "12")
  list(APPEND PROVE_DEBUG_TEST_FILES 001_extension.pl
       002_replication_telemetry.pl)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND PROVE_TEST_FILES ${PROVE_DEBUG_TEST_FILES})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

foreach(P_FILE ${PROVE_TEST_FILES})
  configure_file(${P_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${P_FILE} COPYONLY)
endforeach(P_FILE)

set(MODULE_PATHNAME "$libdir/timescaledb-${PROJECT_VERSION_MOD}")
configure_file(functions.sql.in functions.sql)

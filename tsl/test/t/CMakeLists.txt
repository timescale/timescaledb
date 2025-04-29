set(PROVE_TEST_FILES 001_job_crash_log.pl 002_logrepl_decomp_marker.pl)

set(PROVE_DEBUG_TEST_FILES 003_mvcc_cagg.pl)

# The API for PostgreSQL::Cluster::BackgroundPsql was changed in PG16. We only
# use the new version of the API, so run TAP tests that use it only on later
# versions.

if((${PG_VERSION_MAJOR} GREATER_EQUAL "16"))
  list(APPEND PROVE_TEST_FILES 004_truncate_or_delete_spin_lock.pl)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND PROVE_TEST_FILES ${PROVE_DEBUG_TEST_FILES})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

foreach(P_FILE ${PROVE_TEST_FILES})
  configure_file(${P_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${P_FILE} COPYONLY)
endforeach(P_FILE)

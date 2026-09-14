set(PROVE_TEST_FILES 001_job_crash_log.pl 002_logrepl_decomp_marker.pl
                     004_truncate_or_delete_spin_lock.pl)

# The custom toaster only runs on PG 17.11+ and 18.6+, which is what
# COMPRESSION_TOASTER_SUPPORTED in tsl/src/import/compression_toast.h allows.
# Everywhere else this test has nothing to look at.
if((PG_VERSION_MAJOR EQUAL 17 AND PG_VERSION_MINOR GREATER_EQUAL 11)
   OR (PG_VERSION_MAJOR EQUAL 18 AND PG_VERSION_MINOR GREATER_EQUAL 6))
  list(APPEND PROVE_TEST_FILES 007_custom_toaster.pl)
endif()

set(PROVE_DEBUG_TEST_FILES 003_mvcc_cagg.pl 005_recompression_spin_lock_test.pl)
if(TMP_ENABLE_GRANULAR_REFRESH_TESTS)
  list(APPEND PROVE_DEBUG_TEST_FILES 006_granular_refresh_restart.pl)
endif()

# The API for PostgreSQL::Cluster::BackgroundPsql was changed in PG16. We only
# use the new version of the API, so run TAP tests that use it only on later
# versions.

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND PROVE_TEST_FILES ${PROVE_DEBUG_TEST_FILES})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

foreach(P_FILE ${PROVE_TEST_FILES})
  configure_file(${P_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${P_FILE} COPYONLY)
endforeach(P_FILE)

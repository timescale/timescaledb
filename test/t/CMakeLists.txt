if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND PROVE_TEST_FILES 001_replication_telemetry.pl
       002_compressed_chunk_apache.pl 003_old_loader_compat.pl
       004_pu_shim_preload.pl)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

foreach(P_FILE ${PROVE_TEST_FILES})
  # The old-loader test needs the versioned library name, the others are copied
  # verbatim.
  if(P_FILE STREQUAL "003_old_loader_compat.pl")
    configure_file(${P_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${P_FILE} @ONLY)
  else()
    configure_file(${P_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${P_FILE} COPYONLY)
  endif()
endforeach(P_FILE)

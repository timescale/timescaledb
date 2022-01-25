set(PROVE_TEST_FILES 001_simple_multinode.pl 003_connections_privs.pl)
set(PROVE_DEBUG_TEST_FILES 002_chunk_copy_move.pl 004_multinode_rdwr_1pc.pl
                           005_add_data_node.pl)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND PROVE_TEST_FILES ${PROVE_DEBUG_TEST_FILES})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

foreach(P_FILE ${PROVE_TEST_FILES})
  configure_file(${P_FILE} ${CMAKE_CURRENT_BINARY_DIR}/${P_FILE} COPYONLY)
endforeach(P_FILE)

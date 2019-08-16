# Copy certificates and keys used for testing
#

#
# We need to ensure, that private key files have user only
# access permissions in order to work.
#

# CA cert
file(COPY ts_root.crt
     DESTINATION ${TEST_OUTPUT_DIR})

# Data node cert and private key
file(COPY ts_data_node.crt
     DESTINATION ${TEST_OUTPUT_DIR})

file(COPY ts_data_node.key
     DESTINATION ${TEST_OUTPUT_DIR}
     NO_SOURCE_PERMISSIONS
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

# Users used for clustering testing 
file(COPY cluster_super_user.crt
     DESTINATION ${TEST_OUTPUT_DIR})

file(COPY cluster_super_user.key
     DESTINATION ${TEST_OUTPUT_DIR}
     NO_SOURCE_PERMISSIONS
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

file(COPY test_role_1.crt
     DESTINATION ${TEST_OUTPUT_DIR})

file(COPY test_role_1.key
     DESTINATION ${TEST_OUTPUT_DIR}
     NO_SOURCE_PERMISSIONS
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

file(COPY test_role_2.crt
     DESTINATION ${TEST_OUTPUT_DIR})

file(COPY test_role_2.key
     DESTINATION ${TEST_OUTPUT_DIR}
     NO_SOURCE_PERMISSIONS
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

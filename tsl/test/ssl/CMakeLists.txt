# Copy certificates and keys used for testing
#

#
# We need to ensure, that private key files have user only access permissions in
# order to work.
#
# Certificates for users uses the MD5 sum in hex for the base file name. To
# generate a base filename, pass the role name *without terminating newline or
# null* into md5sum, for example:
#
# echo -n cluster_super_user | md5sum
#

set(TEST_USERS_DIR "${TEST_OUTPUT_DIR}/timescaledb/certs")

# CA cert
file(COPY ts_root.crt DESTINATION ${TEST_OUTPUT_DIR})

# Data node cert and private key
file(COPY ts_data_node.crt DESTINATION ${TEST_OUTPUT_DIR})

file(
  COPY ts_data_node.key
  DESTINATION ${TEST_OUTPUT_DIR}
  NO_SOURCE_PERMISSIONS
  FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

# User cluster_super_user
file(COPY f458066b3557e1db0a54e19f4ab201d9.crt DESTINATION ${TEST_USERS_DIR})

file(
  COPY f458066b3557e1db0a54e19f4ab201d9.key
  DESTINATION ${TEST_USERS_DIR}
  NO_SOURCE_PERMISSIONS
  FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

# User test_role_1
file(COPY 83d86aa56283c8f681d9fc710e8240ad.crt DESTINATION ${TEST_USERS_DIR})

file(
  COPY 83d86aa56283c8f681d9fc710e8240ad.key
  DESTINATION ${TEST_USERS_DIR}
  NO_SOURCE_PERMISSIONS
  FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

file(COPY 2beea6688fa571d4ebacdaf472fb9eb3.crt DESTINATION ${TEST_USERS_DIR})

file(
  COPY 2beea6688fa571d4ebacdaf472fb9eb3.key
  DESTINATION ${TEST_USERS_DIR}
  NO_SOURCE_PERMISSIONS
  FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

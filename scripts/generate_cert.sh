#!/bin/sh

# Generate CA and DN certificates
./generate_ca_cert.sh
./generate_data_node_cert.sh

# Generate clustering users certificates
./generate_client_cert.sh cluster_super_user
./generate_client_cert.sh test_role_1

# Make invalid certificate for test_role_2
cp test_role_1.crt test_role_2.crt
cp test_role_1.key test_role_2.key

# Create root certificate chain
cat ts_ca.crt > ts_root.crt

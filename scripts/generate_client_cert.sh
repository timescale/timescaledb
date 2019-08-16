#!/bin/sh

CA_FILE_PREFIX=${CA_FILE_PREFIX:-ts_ca}
CA_KEY=${CA_KEY:-${CA_FILE_PREFIX}.key}
CA_CRT=${CA_CRT:-${CA_FILE_PREFIX}.crt}
CA_SRL=${CA_SRL:-${CA_FILE_PREFIX}.srl}

CLIENT_NAME=$1
CLIENT_KEY=${CLIENT_NAME}.key
CLIENT_CRT=${CLIENT_NAME}.crt
CLIENT_CSR=${CLIENT_NAME}.csr

# Generate client private key (must be at least 2048 bits long)
openssl genrsa -out ${CLIENT_KEY} 2048

# Create CA sign request
openssl req -new -key ${CLIENT_KEY} \
-subj "/C=SE/ST=Stockholm/L=Stockholm/O=${CLIENT_NAME}/CN=${CLIENT_NAME}" \
-out ${CLIENT_CSR}

# Generate client certificate
openssl x509 -days 1095 -req -in ${CLIENT_CSR} \
-CA ${CA_CRT} \
-CAkey ${CA_KEY} \
-CAcreateserial \
-out ${CLIENT_CRT}

# Clean up
rm -f ${CLIENT_CSR}
rm -f ${CA_SRL}

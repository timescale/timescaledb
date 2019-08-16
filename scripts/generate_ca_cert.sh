#!/bin/sh

CA_FILE_PREFIX=${CA_FILE_PREFIX:-ts_ca}
CA_KEY=${CA_KEY:-${CA_FILE_PREFIX}.key}
CA_CRT=${CA_CRT:-${CA_FILE_PREFIX}.crt}

# Generate CA private key
openssl genrsa -out ${CA_KEY} 2048

# Generate CA certificate
openssl req -new -x509 -days 1095 \
-subj '/C=SE/ST=Stockholm/L=Stockholm/O=TSCA/CN=TSCA' \
-key ts_ca.key -out ${CA_CRT}

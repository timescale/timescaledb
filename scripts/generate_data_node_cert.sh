#!/bin/sh

CA_FILE_PREFIX=${CA_FILE_PREFIX:-ts_ca}
CA_KEY=${CA_KEY:-${CA_FILE_PREFIX}.key}
CA_CRT=${CA_CRT:-${CA_FILE_PREFIX}.crt}
CA_SRL=${CA_SRL:-${CA_FILE_PREFIX}.srl}

DN_FILE_PREFIX=${DN_FILE_PREFIX:-ts_data_node}
DN_HOSTNAME=${DN_HOSTNAME:-localhost}
DN_KEY=${DN_KEY:-${DN_FILE_PREFIX}.key}
DN_CRT=${DN_CRT:-${DN_FILE_PREFIX}.crt}
DN_CSR=${DN_CSR:-${DN_FILE_PREFIX}.csr}
DN_SRL=${DN_SRL:-${DN_FILE_PREFIX}.srl}

# Generate data node private key
openssl genrsa -out ${DN_KEY} 2048

# Request CA to sign data node key
openssl req -new -nodes -key ${DN_KEY} \
-subj "/C=SE/ST=Stockholm/L=Stockholm/O=${DN_HOSTNAME}/CN=${DN_HOSTNAME}" \
-out ${DN_CSR}

# Sign data node key with CA private key
openssl x509 -days 1095 \
-req -in ${DN_CSR} \
-CA ${CA_CRT} \
-CAkey ${CA_KEY} -CAcreateserial \
-out ${DN_CRT}

# Clean up
rm -f ${DN_CSR}
rm -f ${CA_SRL}

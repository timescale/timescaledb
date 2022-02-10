#! /bin/bash

SCRIPT_DIR=$(dirname ${0})
SRC_DIR=$(dirname ${SCRIPT_DIR})

if grep -ir "IF NOT EXISTS" ${SRC_DIR}/sql; then
  cat <<EOF

Update scripts must unconditionally add new objects and fail when the object
already exists otherwise this might enable privilege escalation attacks where
an attacker can precreate objects that get used in later parts of the scripts
instead of the objects created by timescaledb.

EOF
  exit 1
fi

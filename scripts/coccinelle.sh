#!/bin/bash

set -o pipefail

SCRIPT_DIR=$(dirname "${0}")
FAILED=false
true > coccinelle.diff

PG_SERVER_INC=$(pg_config --includedir-server)

for f in "${SCRIPT_DIR}"/../coccinelle/*.cocci; do
  spatch --very-quiet \
        --all-includes --include-headers-for-types \
        -I "${PG_SERVER_INC}" \
        -I "${SCRIPT_DIR}/../src" \
        -I "${SCRIPT_DIR}/../tsl/src" \
        --sp-file "$f" --dir "${SCRIPT_DIR}"/.. --include-headers \
    | tee -a coccinelle.diff
  rc=$?
  if [ $rc -ne 0 ]; then
    FAILED=true
  fi
done

if [ $FAILED = true ] || [ -s coccinelle.diff ] ; then exit 1; fi


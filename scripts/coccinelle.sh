#!/bin/bash

set -o pipefail

SCRIPT_DIR=$(dirname "${0}")
FAILED=false
true > coccinelle.diff

for f in "${SCRIPT_DIR}"/../coccinelle/*.cocci; do
  spatch --very-quiet --include-headers --sp-file "$f" --dir "${SCRIPT_DIR}"/.. | tee -a coccinelle.diff
  rc=$?
  if [ $rc -ne 0 ]; then
    FAILED=true
  fi
done

if [ $FAILED = true ] || [ -s coccinelle.diff ] ; then exit 1; fi


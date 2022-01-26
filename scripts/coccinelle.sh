#!/bin/bash

set -o pipefail

SCRIPT_DIR=$(dirname "${0}")
FAILED=false
true > coccinelle.diff

for f in "${SCRIPT_DIR}"/../coccinelle/*.cocci; do
	find "${SCRIPT_DIR}"/.. -name '*.c' -exec spatch --very-quiet -sp_file "$f" {} + | tee -a coccinelle.diff
  rc=$?
  if [ $rc -ne 0 ]; then
    FAILED=true
  fi
done

if [ $FAILED = true ] || [ -s coccinelle.diff ] ; then exit 1; fi


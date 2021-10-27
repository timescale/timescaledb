#!/bin/sh

SCRIPT_DIR=$(dirname "${0}")

true > coccinelle.diff

for f in "${SCRIPT_DIR}"/../coccinelle/*.cocci; do
	find "${SCRIPT_DIR}"/.. -name '*.c' -exec spatch --very-quiet -sp_file "$f" {} + | tee -a coccinelle.diff
done

if [ -s coccinelle.diff ] ; then exit 1; fi


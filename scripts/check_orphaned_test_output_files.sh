#!/bin/bash

ERROR=0

for FILE in $(git ls-files test/expected/*-[0-9][0-9].out tsl/test/expected/*-[0-9][0-9].out)
do
  DIRNAME=$(dirname "${FILE}" | sed 's/expected/sql/g')
  TESTOUTPUTNAME=$(basename "${FILE}" .out | sed 's/-[0-9][0-9]//g') 

  if [ ! -f "${DIRNAME}/${TESTOUTPUTNAME}.sql.in" ]; then
      echo "ERROR: template SQL test does not found: \"${DIRNAME}/${TESTOUTPUTNAME}.sql.in\""
      echo "HINT: Please remove the output test file \"${FILE}\""
      ERROR=1
  fi
done

exit ${ERROR}

#!/bin/bash

ERROR=0

for FILE in $(git ls-files test/sql/*.sql.in tsl/test/sql/*.sql.in)
do
  DIRNAME=$(dirname "${FILE}" | sed 's/sql/expected/g')
  TESTNAME=$(basename "${FILE}" .sql.in)

  if diff --from-file ${DIRNAME}/${TESTNAME}-*.out > /dev/null 2>&1; then
      echo "ERROR: all template output test files are equal: \"${DIRNAME}/${TESTNAME}-*.out\""
      echo "HINT: Please turn template test file \"${FILE}\" into a regular test file"
      ERROR=1
  fi
done

exit ${ERROR}

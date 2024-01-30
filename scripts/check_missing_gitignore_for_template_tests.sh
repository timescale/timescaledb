#!/bin/bash

ERROR=0

for FILE in $(git ls-files | grep '\.sql\.in')
do
  DIRNAME=$(dirname "${FILE}")
  FILENAME=$(basename "${FILE}" .sql.in)
  GITIGNORE=${DIRNAME}/.gitignore
  if [ -f "${GITIGNORE}" ]; then
    if git ls-files --others --exclude-standard $DIRNAME | grep --silent "${FILENAME}-"; then
      echo "Missing entry in ${GITIGNORE} for template file ${FILE}"
      ERROR=1
    fi
  fi
done

exit ${ERROR}

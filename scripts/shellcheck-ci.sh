#!/bin/bash
set -e

find ./* .github -type f -executable \
  -exec bash -c '[ "$(file --brief --mime-type "$1")" == "text/x-shellscript" ]' sh {} \; \
  -not -exec shellcheck -x --exclude=SC2086 {} \; \
  -print \
  | grep . \
  && exit 1 \
  || exit 0

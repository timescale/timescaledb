#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)

TAGS="1.7.0-pg12"
TEST_VERSION="v6-pg12"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
echo $SCRIPT_DIR

TAGS="2.1.0-pg13"
TEST_VERSION="v7"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh "$@"
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    exit $EXIT_CODE
fi

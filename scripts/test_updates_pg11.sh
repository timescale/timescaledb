#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)

TAGS="1.1.0-pg11 1.1.1-pg11 1.2.0-pg11 1.2.1-pg11 1.2.2-pg11"
TEST_VERSION="v2"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi


TAGS="1.3.0-pg11 1.3.1-pg11 1.3.2-pg11 1.4.0-pg11 1.4.1-pg11 1.4.2-pg11"
TEST_VERSION="v4"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

TAGS="1.5.0-pg11 1.5.1-pg11 1.6.0-pg11 1.6.1-pg11"
TEST_VERSION="v5-pg11"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

TAGS="1.7.0-pg11 1.7.1-pg11 1.7.2-pg11 1.7.3-pg11 1.7.4-pg11"
TEST_VERSION="v6-pg11"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

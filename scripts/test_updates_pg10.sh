#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)

TAGS="0.7.0-pg10 0.7.1-pg10 0.8.0-pg10 0.9.0-pg10 0.9.1-pg10 0.9.2-pg10 0.10.0-pg10 0.10.1-pg10 0.11.0-pg10 0.12.0-pg10 1.0.0-pg10 1.0.1-pg10 1.1.0-pg10 1.1.1-pg10 1.2.0-pg10 1.2.1-pg10 1.2.2-pg10"
TEST_VERSION="v2"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi


TAGS="1.3.0-pg10 1.3.1-pg10 1.3.2-pg10 1.4.0-pg10 1.4.1-pg10 1.4.2-pg10"
TEST_VERSION="v3"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

TAGS="1.5.0-pg10 1.5.1-pg10 1.6.0-pg10 1.6.1-pg10"
TEST_VERSION="v5-pg10"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

#!/usr/bin/env bash
#
# Prior to version 0.5.0, TimescaleDB did not fully support most constraints
# so we don't include update tests that test constraint support in pre-0.5.0
# versions.

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
TAGS="0.1.0 0.2.0 0.3.0 0.4.0 0.4.1 0.4.2"
TEST_VERSION="v1"

. ${SCRIPT_DIR}/test_updates.sh

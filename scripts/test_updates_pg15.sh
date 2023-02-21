#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

# shellcheck source=scripts/test_functions.inc
source ${SCRIPT_DIR}/test_functions.inc

run_tests "$@" -v8 \
          2.9.0-pg15 2.9.1-pg15 2.9.2-pg15 2.9.3-pg15 2.10.0-pg15

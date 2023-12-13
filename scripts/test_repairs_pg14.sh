#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

# shellcheck source=scripts/test_functions.inc
source ${SCRIPT_DIR}/test_functions.inc

# Run repair tests for >=2.10.x versions due to PR #5441
run_tests "$@" -r -v8 \
          2.10.0-pg14 2.10.1-pg14 2.10.2-pg14 2.10.3-pg14 2.11.0-pg14 2.11.1-pg14 2.11.2-pg14 \
          2.12.0-pg14 2.12.1-pg14 2.12.2-pg14 2.13.0-pg14


#!/usr/bin/env bash

set -e

PG_VERSION=${PG_VERSION:-14.11}
export PG_VERSION

SCRIPT_DIR=$(dirname $0)

# shellcheck source=scripts/test_functions.inc
source ${SCRIPT_DIR}/test_functions.inc

run_tests "$@" -v7 \
          2.5.0-pg14 2.5.1-pg14

run_tests "$@" -v8 \
          2.5.0-pg14 2.5.1-pg14 2.5.2-pg14 2.6.0-pg14 2.6.1-pg14 2.7.0-pg14 2.7.1-pg14 2.7.2-pg14 \
          2.8.0-pg14 2.8.1-pg14 2.9.0-pg14 2.9.1-pg14 2.9.2-pg14 2.9.3-pg14

run_tests "$@" -v8 \
          2.10.0-pg14 2.10.1-pg14 2.10.2-pg14 2.10.3-pg14 2.11.0-pg14 2.11.1-pg14 2.11.2-pg14 \
          2.12.0-pg14 2.12.1-pg14 2.12.2-pg14 2.13.0-pg14 2.13.1-pg14 2.14.0-pg14 2.14.1-pg14 \
          2.14.2-pg14

# Run repair tests for >=2.10.x versions due to PR #5441
run_tests "$@" -r -v8 \
          2.10.0-pg14 2.10.1-pg14 2.10.2-pg14 2.10.3-pg14 2.11.0-pg14 2.11.1-pg14 2.11.2-pg14 \
          2.12.0-pg14 2.12.1-pg14 2.12.2-pg14 2.13.0-pg14 2.13.1-pg14 2.14.0-pg14 2.14.1-pg14 \
          2.14.2-pg14


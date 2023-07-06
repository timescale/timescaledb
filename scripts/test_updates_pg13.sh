#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

# shellcheck source=scripts/test_functions.inc
source ${SCRIPT_DIR}/test_functions.inc

run_tests "$@" -v7 \
          2.1.0-pg13 2.1.1-pg13 2.2.0-pg13 2.2.1-pg13 2.3.0-pg13 2.3.1-pg13 \
          2.4.0-pg13 2.4.1-pg13 2.4.2-pg13
run_tests "$@" -v8 \
          2.5.0-pg13 2.5.1-pg13 2.5.2-pg13 2.6.0-pg13 2.6.1-pg13 2.7.0-pg13 2.7.1-pg13 2.7.2-pg13 \
          2.8.0-pg13 2.8.1-pg13 2.9.0-pg13 2.9.1-pg13 2.9.2-pg13 2.9.3-pg13

# Also run repair tests for >=2.10.x versions due to PR #5441
run_tests "$@" -r -v8 \
          2.10.0-pg13 2.10.1-pg13 2.10.2-pg13 2.10.3-pg13 2.11.0-pg13 2.11.1-pg13

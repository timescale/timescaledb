#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

# shellcheck source=scripts/test_functions.inc
source ${SCRIPT_DIR}/test_functions.inc

run_tests -v7 \
          2.1.0-pg13 2.1.1-pg13 2.2.0-pg13 2.2.1-pg13 2.3.0-pg13 2.3.1-pg13 \
          2.4.0-pg13 2.4.1-pg13 2.4.2-pg13 
run_tests -v8 \
          2.5.0-pg13 2.5.1-pg13 2.5.2-pg13 2.6.0-pg13


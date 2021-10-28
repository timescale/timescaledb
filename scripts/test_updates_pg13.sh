#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

source ${SCRIPT_DIR}/test_functions.inc

run_tests -v7 \
          2.1.0-pg13 2.1.1-pg13 2.2.0-pg13 2.2.1-pg13 2.3.0-pg13 2.3.1-pg13 \
          2.4.0-pg13 2.4.1-pg13 2.4.2-pg13 2.5.0-pg13

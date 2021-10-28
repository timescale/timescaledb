#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

source ${SCRIPT_DIR}/test_functions.inc

run_tests -v7 \
          2.5.0-pg14

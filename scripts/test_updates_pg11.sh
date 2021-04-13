#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

source ${SCRIPT_DIR}/test_functions.inc

# There are repair steps between:
#    1.7.1 and 1.7.2
#    2.0.0-rc1 and 2.0.0-rc2
#
# Please extend this list if repairs are needed between more steps.
run_tests "$@" -r -v2 \
          1.1.0-pg11 1.1.1-pg11 1.2.0-pg11 1.2.1-pg11 1.2.2-pg11
run_tests "$@" -r -v4 \
          1.3.0-pg11 1.3.1-pg11 1.3.2-pg11 1.4.0-pg11 1.4.1-pg11 1.4.2-pg11
run_tests "$@" -r -v5 \
          1.5.0-pg11 1.5.1-pg11 1.6.0-pg11 1.6.1-pg11
run_tests "$@" -r -v6 \
          1.7.0-pg11 1.7.1-pg11 1.7.2-pg11 1.7.3-pg11 1.7.4-pg11 1.7.5-pg11
run_tests "$@" -r -v7 \
          2.0.0-rc1-pg11 
run_tests "$@" -v7 \
          2.0.0-rc2-pg11 2.0.0-rc3-pg11 2.0.0-rc4-pg11 2.0.0-pg11 2.0.1-pg11 2.0.2-pg11 2.1.0-pg11 \
          2.1.1-pg11

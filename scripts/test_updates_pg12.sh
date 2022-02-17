#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname $0)

# shellcheck source=scripts/test_functions.inc
source ${SCRIPT_DIR}/test_functions.inc

# There are repair steps between:
#    1.7.1 and 1.7.2
#    2.0.0-rc1 and 2.0.0-rc2
#
# Please extend this list if repairs are needed between more steps.
run_tests "$@" -r -v6 \
          1.7.0-pg12 1.7.1-pg12 1.7.2-pg12 1.7.3-pg12 1.7.4-pg12 1.7.5-pg12
run_tests "$@" -r -v7 \
          2.0.0-rc1-pg12 
run_tests "$@" -v7 \
          2.0.0-rc2-pg12 2.0.0-rc3-pg12 2.0.0-rc4-pg12 2.0.0-pg12 2.0.1-pg12 2.0.2-pg12 2.1.0-pg12 \
          2.1.1-pg12 2.2.0-pg12 2.2.1-pg12 2.3.0-pg12 2.3.1-pg12 2.4.0-pg12 2.4.1-pg12 2.4.2-pg12 
run_tests "$@" -v8 \
          2.5.0-pg12 2.5.1-pg12 2.5.2-pg12 2.6.0-pg12

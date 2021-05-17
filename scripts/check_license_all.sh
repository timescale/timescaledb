#!/bin/bash
SCRIPT_DIR=$(dirname $0)
BASE_DIR=$(dirname ${SCRIPT_DIR})

SRC_DIR=$BASE_DIR ${SCRIPT_DIR}/check_license.sh -c ${BASE_DIR}/src -s ${BASE_DIR}/sql -c ${BASE_DIR}/test -s ${BASE_DIR}/test -i ${BASE_DIR}/test
exit_apache=$?
SRC_DIR=$BASE_DIR ${SCRIPT_DIR}/check_license.sh -e ${BASE_DIR}/tsl/src -t ${BASE_DIR}/tsl/test -e ${BASE_DIR}/tsl/test -j ${BASE_DIR}/tsl/test -p ${BASE_DIR}/tsl/test -p ${BASE_DIR}/test/perl
exit_tsl=$?

if [ ${exit_apache} -ne 0 ] || [ ${exit_tsl} -ne 0 ]; then
  exit 1
fi


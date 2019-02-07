#!/bin/bash

# we need to convert script dir to an absolute path
SCRIPT_DIR=$(cd $(dirname $0); pwd)
BASE_DIR=$(dirname $SCRIPT_DIR)

find ${BASE_DIR} \( -path "${BASE_DIR}/src/*" -or -path "${BASE_DIR}/test/*" -or -path "${BASE_DIR}/tsl/*" \) \
    -and -not \( -path "*/.*" -or -path "*CMake*" \) \
    -and \( -name '*.c' -or -name '*.h' \) -print | xargs ${SCRIPT_DIR}/clang_format_wrapper.sh -style=file -i

#!/bin/bash

# we need to convert script dir to an absolute path
SCRIPT_DIR=$(cd "$(dirname $0)" || exit; pwd)
BASE_DIR=$(dirname $SCRIPT_DIR)

find ${BASE_DIR} \( -path "${BASE_DIR}/src/*" -or -path "${BASE_DIR}/test/*" -or -path "${BASE_DIR}/tsl/*" \) \
    -and -not \( -path "*/.*" -or -path "*CMake*" \) \
    -and \( -name '*.c' -or -name '*.h' \) -print0 | xargs -0 ${SCRIPT_DIR}/clang_format_wrapper.sh -style=file -i

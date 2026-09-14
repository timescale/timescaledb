#!/bin/bash

# we need to convert script dir to an absolute path
SCRIPT_DIR=$(cd "$(dirname $0)" || exit; pwd)
BASE_DIR=$(dirname $SCRIPT_DIR)

CLANG_FORMAT=${CLANG_FORMAT:-clang-format}

${CLANG_FORMAT} --version

find ${BASE_DIR} \( -path "${BASE_DIR}/src/*" -or -path "${BASE_DIR}/test/*" -or -path "${BASE_DIR}/tsl/*" \) \
    -and -not \( -path "*/.*" -or -path "*CMake*" -or -path "${BASE_DIR}/src/import/*" -or -path "${BASE_DIR}/tsl/src/import/*" \) \
    -and \( -name '*.c' -or -name '*.h' \) -print0 \
    | xargs -0 -n 16 -P 0 ${CLANG_FORMAT} -Wno-error=unknown -style=file -i

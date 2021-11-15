#!/bin/bash

# clang-format misunderstand sql function written in C because they have the signature
#   Datum my_func(PG_FUNCTION_ARGS)
# and clang-format does not interpret PG_FUNCTION_ARGS as a type, name pair.
# This script replaces PG_FUNCTION_ARGS with "PG_FUNCTION_ARGS fake_var_for_clang" to
# make it look like a proper function for clang and then converts it back after clang runs.

SCRIPT_DIR=$(cd "$(dirname $0)"; pwd)
BASE_DIR=$(dirname $SCRIPT_DIR)
TEMP_DIR="/tmp/timescaledb_format"

OPTIONS=("${@}")

if [ -e ${TEMP_DIR} ]
then
    echo "error: ${TEMP_DIR} already exists"
    echo "       delete ${TEMP_DIR} to format"
    exit 1
fi

cleanup() {
    echo "cleaning"
    rm -rf ${TEMP_DIR}
}

trap cleanup EXIT SIGINT SIGTERM

if ! mkdir ${TEMP_DIR}
then
    echo "error: could not create temporary directory ${TEMP_DIR}"
    exit 1
fi

#from this point on, if we get a failure we end up in an invalid state, so exit
set -e

CLANG_FORMAT_FLAGS=""
FILE_NAMES=""

for opt in "${OPTIONS[@]}"
do
    if [[ "${opt:0:1}" != "-" ]]
    then
        file_path=${opt#"$BASE_DIR/"}
        FILE_NAMES="${FILE_NAMES} $file_path"
    else
        CLANG_FORMAT_FLAGS="${CLANG_FORMAT_FLAGS} $opt"
    fi
done

echo "copying to ${TEMP_DIR}"

for name in ${FILE_NAMES}
do
    # sed -i have different semantics on mac and linux, don't use
    mkdir -p "$(dirname "${TEMP_DIR}/${name}")"
    sed -e 's/(PG_FUNCTION_ARGS)/(PG_FUNCTION_ARGS fake_var_for_clang)/' ${BASE_DIR}/${name} > ${TEMP_DIR}/${name}
done

cp ${BASE_DIR}/.clang-format ${TEMP_DIR}/.clang-format

CURR_DIR=${PWD}

echo "formatting"

cd ${TEMP_DIR}

${CLANG_FORMAT:-clang-format} ${CLANG_FORMAT_FLAGS} ${FILE_NAMES}

cd ${CURR_DIR}

echo "copying back"

for name in ${FILE_NAMES}
do
    if sed -e 's/PG_FUNCTION_ARGS fake_var_for_clang/PG_FUNCTION_ARGS/' ${TEMP_DIR}/${name} > ${TEMP_DIR}/replace_file; then
	if ! cmp -s ${TEMP_DIR}/replace_file ${BASE_DIR}/${name}; then
	    echo "Updating ${BASE_DIR}/${name}"
            mv ${TEMP_DIR}/replace_file ${BASE_DIR}/${name}
	fi
    fi
done

exit 0;

#! /bin/bash

SCRIPT_DIR=$(dirname ${0})
SRC_DIR=$(dirname ${SCRIPT_DIR})

# we skip license checks for:
#   - the update script fragments, because the generated update scripts will
#     contain the license at top, and we don't want to repeat it in the middle
#   - test/sql/dump which contains auto-generated code
#   - src/chunk_adatptive since it's still in BETA

check_file() {
    SUFFIX0=
    SUFFIX1=

    if [[ ${1} == '-c' || ${1} == '-e' ]]; then
        SUFFIX0='*.c'
        SUFFIX1='*.h'
    else
        SUFFIX0='*.sql'
        SUFFIX1='*.sql'
    fi

    find $2 -type f \( -name "${SUFFIX0}" -or -name "${SUFFIX1}" \) -and -not -path "${SRC_DIR}/sql/updates/*.sql" -and -not -path "${SRC_DIR}/test/sql/dump/*.sql" -and -not -path "${SRC_DIR}/src/chunk_adaptive.*" -print0 | xargs -0 -n1 $(dirname ${0})/check_file_license.sh ${1}
}

args=`getopt "c:e:s:t:u:" $*`; errcode=$?; set -- $args

ERRORCODE=0

while [[ ${1} ]]; do
    if [[ ${1} == "--" ]]; then
        break;
    fi
    check_file ${1} ${2}
    FILE_ERR=${?}
    ERRORCODE=$((${FILE_ERR} | ${ERRORCODE}));
    shift; shift;
done

exit ${ERRORCODE};

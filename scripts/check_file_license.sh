#!/bin/bash

get_c_license() {
    awk 'BEGIN {ORS=""}{if($1 == "*/") {print; exit;}} {print}' $1
}

get_sql_license() {
    awk 'BEGIN {ORS=""}{if($1 == "") {print; exit;}} {print}' $1
}

check_file() {
    FLAG=${1}
    FILE=${2}
    SCRIPTPATH="$( cd "$(dirname "${0}")" || exit ; pwd -P )"
    TIMESCALE_LOCATION=$(dirname ${SCRIPTPATH})

    LICENSE_FILE=
    LICENSE_STRING=
    FIRST_COMMENT=

    case ${FLAG} in
        ('-c')
            LICENSE_FILE="${SCRIPTPATH}/c_license_header-apache.h"
            LICENSE_STRING=$(get_c_license ${LICENSE_FILE})
            FIRST_COMMENT=$(get_c_license ${FILE})
            ;;
        ('-e')
            LICENSE_FILE="${SCRIPTPATH}/c_license_header-timescale.h"
            LICENSE_STRING=$(get_c_license ${LICENSE_FILE})
            FIRST_COMMENT=$(get_c_license ${FILE})
            ;;
        ('-i')
            LICENSE_FILE="${SCRIPTPATH}/license_apache.spec"
            LICENSE_STRING=$(get_sql_license ${LICENSE_FILE})
            FIRST_COMMENT=$(get_sql_license ${FILE})
            ;;
        ('-j')
            LICENSE_FILE="${SCRIPTPATH}/license_tsl.spec"
            LICENSE_STRING=$(get_sql_license ${LICENSE_FILE})
            FIRST_COMMENT=$(get_sql_license ${FILE})
            ;;
        ('-s')
            LICENSE_FILE="${SCRIPTPATH}/sql_license_apache.sql"
            LICENSE_STRING=$(get_sql_license ${LICENSE_FILE})
            FIRST_COMMENT=$(get_sql_license ${FILE})
            ;;
        ('-t')
            LICENSE_FILE="${SCRIPTPATH}/sql_license_tsl.sql"
            LICENSE_STRING=$(get_sql_license ${LICENSE_FILE})
            FIRST_COMMENT=$(get_sql_license ${FILE})
            ;;
        ('-p')
            LICENSE_FILE="${SCRIPTPATH}/license_tsl.spec"
            LICENSE_STRING=$(get_sql_license ${LICENSE_FILE})
            FIRST_COMMENT=$(get_sql_license ${FILE})
            ;;
        ("--")
            return 0;
            ;;
        (*)
            echo "Unkown flag" ${1}
            return 1;
    esac

    if [[ "${FIRST_COMMENT}" != "${LICENSE_STRING}" ]]; then
        echo ${FILE#"$TIMESCALE_LOCATION/"} "lacks a license header. Add";
        echo
        cat ${LICENSE_FILE}
        echo
        echo "to the top of the file";
        return 1;
    fi
}

args=$(getopt "c:e:i:j:p:s:t:" "$@"); errcode=$?; set -- $args

if [[ ${errcode} != 0 ]]; then
        echo 'Usage: check_file_license ((-c|-e|-i|-j|-s|-t) <filename> ...)'
        return 2
fi


ERRORCODE=0

while [[ ${1} ]]; do
    if [[ ${1} == "--" ]]; then
        break;
    fi
    check_file ${1} ${2}
    FILE_ERR=${?}
    ERRORCODE=$((FILE_ERR | ERRORCODE));
    shift; shift;
done

exit ${ERRORCODE};

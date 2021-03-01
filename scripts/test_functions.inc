#!/usr/bin/env bash

SCRIPT_DIR=$(dirname $0)

# Run tests given as arguments.
#
# Options:
#   -r    Run repair tests as a separate pass (optional)
#   -vN   Use version N of the update tests (required)
run_tests() (
    export TEST_VERSION
    export TEST_REPAIR=false

    OPTIND=1
    while getopts "v:r" opt;
    do
        case $opt in
             v)
                 TEST_VERSION=v$OPTARG
                 ;;
             r)
                 TEST_REPAIR=true
                 ;;
        esac
    done

    shift $((OPTIND-1))

    export TAGS="$@"
    bash ${SCRIPT_DIR}/test_updates.sh
    if [[ "$TEST_REPAIR" = "true" ]]; then
        bash ${SCRIPT_DIR}/test_repairs.sh
    fi
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        exit $EXIT_CODE
    fi
)


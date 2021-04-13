#!/usr/bin/env bash

SCRIPT_DIR=$(dirname $0)

# Run tests given as arguments.
#
# Options:
#   -r    Run repair tests as a separate pass (optional)
#   -k    Keep temporary directory
#   -vN   Use version N of the update tests (required)
run_tests() (
    export TEST_VERSION
    export TEST_REPAIR=false
    export DO_CLEANUP=true
    
    OPTIND=1
    while getopts "kv:r" opt;
    do
        case $opt in
             v)
                 TEST_VERSION=v$OPTARG
                 ;;
	     k)
		 DO_CLEANUP=false
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


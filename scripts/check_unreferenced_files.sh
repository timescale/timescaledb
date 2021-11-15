#!/bin/bash

SCRIPT_DIR=$(dirname ${0})
BASE_DIR=$(pwd)/$SCRIPT_DIR/..

unreferenced=0

function get_filenames {
  echo ./*.$2 ./*.$2.in | xargs git ls-files | xargs -IFILE basename FILE
}

function check_directory {
  cd $BASE_DIR/$1 || exit
  test_files=$(get_filenames $1 $2)

  for file in $test_files; do
    output=$(grep --files-without-match $file CMakeLists.txt)

    # return value from grep --files-without-match seems to differ
    # between grep versions so we use output instead of return value
    if [ "$output" != "" ]; then
      echo -e "\nUnreferenced file in $1: $file\n"
      unreferenced=1
    fi
  done
}

check_directory test/sql sql
check_directory tsl/test/sql sql
check_directory tsl/test/shared/sql sql

check_directory test/isolation/specs spec
check_directory tsl/test/isolation/specs spec

exit $unreferenced
